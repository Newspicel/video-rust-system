use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    time::Duration,
};

use reqwest::Url;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tokio::process::Command as TokioCommand;
use url::ParseError;
use uuid::Uuid;

use crate::{
    cleanup,
    error::AppError,
    jobs::JobStage,
    state::AppState,
    storage::ensure_parent,
    transcode::{EncodeParams, process_video},
};

const ARIA2_BIN: &str = "aria2c";

pub(super) fn spawn_local_pipeline(state: AppState, id: Uuid, temp_path: PathBuf) {
    tokio::spawn(async move {
        if let Err(err) = run_local_pipeline(state.clone(), id, temp_path.clone()).await {
            tracing::error!(%id, error = %err, "local processing failed");
            if let Err(store_err) = state.jobs.fail(id, err.to_string()).await {
                tracing::error!(%id, error = %store_err, "failed to mark job as failed");
            }
            match tokio::fs::remove_file(&temp_path).await {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                    tracing::warn!(path = %temp_path.display(), ?e, "cleanup failed");
                }
                _ => {}
            }
        }
    });
}

pub(super) fn spawn_remote_pipeline(
    state: AppState,
    id: Uuid,
    url: String,
    encode: Option<EncodeParams>,
) {
    tokio::spawn(async move {
        if let Err(err) = run_remote_pipeline(state.clone(), id, url.clone(), encode).await {
            tracing::error!(%id, url, error = %err, "remote processing failed");
            if let Err(store_err) = state.jobs.fail(id, err.to_string()).await {
                tracing::error!(%id, url, error = %store_err, "failed to mark remote job failure");
            }
        }
    });
}

pub(super) fn spawn_ytdlp_pipeline(
    state: AppState,
    id: Uuid,
    url: String,
    encode: Option<EncodeParams>,
) {
    tokio::spawn(async move {
        if let Err(err) = run_ytdlp_pipeline(state.clone(), id, url.clone(), encode).await {
            tracing::error!(%id, url, error = %err, "yt-dlp processing failed");
            if let Err(store_err) = state.jobs.fail(id, err.to_string()).await {
                tracing::error!(%id, url, error = %store_err, "failed to mark yt-dlp job failure");
            }
        }
    });
}

async fn run_local_pipeline(state: AppState, id: Uuid, temp_path: PathBuf) -> Result<(), AppError> {
    tracing::debug!(%id, path = %temp_path.display(), "starting local pipeline");
    cleanup::ensure_capacity(&state.storage, &state.jobs, &state.cleanup).await?;
    state.jobs.update_stage(id, JobStage::Transcoding).await?;
    process_video(&state.storage, &state.jobs, &id, temp_path.as_path(), None).await?;
    state.jobs.complete(id).await?;

    tracing::debug!(%id, "local pipeline finished");

    Ok(())
}

async fn run_remote_pipeline(
    state: AppState,
    id: Uuid,
    url: String,
    encode: Option<EncodeParams>,
) -> Result<(), AppError> {
    cleanup::ensure_capacity(&state.storage, &state.jobs, &state.cleanup).await?;
    state.jobs.update_stage(id, JobStage::Downloading).await?;

    let temp_path = state.storage.incoming_path(&id);
    ensure_parent(&temp_path).await?;
    tracing::debug!(%id, %url, path = %temp_path.display(), "remote download starting");

    let parsed_url = Url::parse(&url);
    if should_use_aria2(&url, &parsed_url) {
        state.jobs.update_progress(id, 0.0).await?;
        download_with_aria2(&url, &temp_path).await?;
        state.jobs.update_progress(id, 1.0).await?;
        tracing::debug!(%id, %url, path = %temp_path.display(), "remote download completed via aria2");
    } else {
        let http_url = parsed_url.map_err(|err| AppError::validation(err.to_string()))?;
        let mut response = state
            .http_client
            .get(http_url)
            .timeout(Duration::from_secs(60 * 10))
            .send()
            .await?
            .error_for_status()?;

        let mut file = File::create(&temp_path).await?;
        let content_length = response.content_length();
        let mut downloaded: u64 = 0;

        while let Some(chunk) = response.chunk().await? {
            file.write_all(&chunk).await?;
            downloaded += chunk.len() as u64;
            if let Some(total) = content_length {
                let ratio = (downloaded as f32 / total as f32).clamp(0.0, 1.0);
                state.jobs.update_progress(id, ratio).await?;
            }
        }
        file.flush().await?;

        state.jobs.update_progress(id, 1.0).await?;
        tracing::debug!(
            %id,
            %url,
            path = %temp_path.display(),
            bytes = downloaded,
            "remote download completed"
        );
    }

    state.jobs.update_stage(id, JobStage::Transcoding).await?;
    tracing::debug!(%id, %url, path = %temp_path.display(), "starting transcode for remote job");

    process_video(
        &state.storage,
        &state.jobs,
        &id,
        temp_path.as_path(),
        encode,
    )
    .await?;
    state.jobs.complete(id).await?;
    tracing::debug!(%id, %url, "remote pipeline finished");

    Ok(())
}

async fn run_ytdlp_pipeline(
    state: AppState,
    id: Uuid,
    url: String,
    encode: Option<EncodeParams>,
) -> Result<(), AppError> {
    cleanup::ensure_capacity(&state.storage, &state.jobs, &state.cleanup).await?;
    state.jobs.update_stage(id, JobStage::Downloading).await?;

    let temp_path = state.storage.incoming_path(&id);
    ensure_parent(&temp_path).await?;
    tracing::debug!(%id, %url, path = %temp_path.display(), "yt-dlp download starting");

    let downloaded_path = download_with_ytdlp_cli(&url, &temp_path).await?;

    if downloaded_path != temp_path {
        fs::rename(&downloaded_path, &temp_path).await?;
    }
    tracing::debug!(%id, %url, path = %temp_path.display(), "yt-dlp download finished");

    state.jobs.update_stage(id, JobStage::Transcoding).await?;
    tracing::debug!(%id, %url, path = %temp_path.display(), "starting transcode for yt-dlp job");

    process_video(
        &state.storage,
        &state.jobs,
        &id,
        temp_path.as_path(),
        encode,
    )
    .await?;
    state.jobs.complete(id).await?;
    tracing::debug!(%id, %url, "yt-dlp pipeline finished");

    Ok(())
}

async fn download_with_ytdlp_cli(url: &str, destination: &Path) -> Result<PathBuf, AppError> {
    let parent = destination
        .parent()
        .ok_or_else(|| AppError::transcode("temporary destination missing parent directory"))?;

    let template_path = destination.with_extension("%(ext)s");

    let output = TokioCommand::new("yt-dlp")
        .arg("--ignore-config")
        .arg("--no-warnings")
        .arg("--quiet")
        .arg("--no-progress")
        .arg("--no-playlist")
        .arg("--no-part")
        .arg("--no-write-comments")
        .arg("--no-write-subs")
        .arg("--no-write-description")
        .arg("--no-write-info-json")
        .arg("--output")
        .arg(&template_path)
        .arg("--print")
        .arg("after_move:filepath")
        .arg("-f")
        .arg("bv*+ba/b")
        .arg(url)
        .output()
        .await
        .map_err(|err| map_spawn_error(err, "yt-dlp"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(AppError::dependency(format!(
            "yt-dlp exited with status {}: {}",
            output.status,
            stderr.trim()
        )));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let reported_path = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .ok_or_else(|| AppError::dependency("yt-dlp did not report an output file"))?
        .trim()
        .to_string();

    let mut resolved = PathBuf::from(&reported_path);
    if resolved.is_relative() {
        resolved = parent.join(resolved);
    }

    if !resolved.exists() {
        return Err(AppError::dependency(format!(
            "yt-dlp reported output {}, but file is missing",
            resolved.display()
        )));
    }

    Ok(resolved)
}

async fn download_with_aria2(source: &str, destination: &Path) -> Result<(), AppError> {
    let parent = destination
        .parent()
        .ok_or_else(|| AppError::transcode("temporary destination missing parent directory"))?;

    let before = dir_snapshot(parent).await?;
    let file_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| AppError::transcode("temporary destination missing file name"))?;

    let is_magnet = source.starts_with("magnet:");
    let is_torrent = source.to_ascii_lowercase().ends_with(".torrent");

    let mut command = TokioCommand::new(ARIA2_BIN);
    command
        .arg("--allow-overwrite=true")
        .arg("--auto-file-renaming=false")
        .arg("--summary-interval=0")
        .arg("--seed-time=0")
        .arg("--bt-seed-until=0")
        .arg("--bt-stop-timeout=0")
        .arg("--bt-remove-unselected-file=true")
        .arg("--bt-save-metadata=false")
        .arg("--dir")
        .arg(parent);

    if !is_magnet && !is_torrent {
        command.arg("--out").arg(file_name);
    }

    command.arg(source);

    let status = command
        .status()
        .await
        .map_err(|err| map_spawn_error(err, ARIA2_BIN))?;

    if !status.success() {
        return Err(AppError::dependency(format!(
            "aria2c exited with status {status}"
        )));
    }

    if destination.exists() {
        tracing::debug!(source, dest = %destination.display(), "aria2 produced target file directly");
        return Ok(());
    }

    let after = dir_snapshot(parent).await?;
    let mut new_entries: Vec<PathBuf> = after.difference(&before).cloned().collect();

    if new_entries.len() == 1 {
        let candidate = new_entries.remove(0);
        if tokio::fs::metadata(&candidate)
            .await
            .map_err(AppError::from)?
            .is_file()
        {
            tokio::fs::rename(&candidate, destination).await?;
            tracing::debug!(source, temp = %candidate.display(), dest = %destination.display(), "aria2 download moved into place");
            return Ok(());
        }
    }

    Err(AppError::transcode(
        "aria2c produced unexpected output (expected a single file)",
    ))
}

fn should_use_aria2(url_str: &str, parsed: &Result<Url, ParseError>) -> bool {
    let lower = url_str.to_ascii_lowercase();
    if url_str.starts_with("magnet:") || lower.ends_with(".torrent") {
        return true;
    }

    if let Ok(url) = parsed {
        matches!(url.scheme(), "ftp" | "ftps" | "p2p")
    } else {
        false
    }
}

async fn dir_snapshot(dir: &Path) -> Result<HashSet<PathBuf>, AppError> {
    let mut entries = fs::read_dir(dir).await?;
    let mut set = HashSet::new();
    while let Some(entry) = entries.next_entry().await? {
        set.insert(entry.path());
    }
    Ok(set)
}

fn map_spawn_error(err: std::io::Error, tool: &str) -> AppError {
    match err.kind() {
        std::io::ErrorKind::NotFound => AppError::dependency(format!("{tool} not found on PATH")),
        _ => AppError::dependency(format!("failed to spawn {tool}: {err}")),
    }
}
// Tests for this module live under `tests/` to keep source files focused.
