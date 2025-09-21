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
use yt_dlp::{Youtube, fetcher::deps::Libraries};

use crate::{
    cleanup,
    error::AppError,
    jobs::JobStage,
    state::AppState,
    storage::{ensure_dir, ensure_parent},
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

    let fetcher = prepare_ytdlp_fetcher(&state).await?;

    let file_name = temp_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| AppError::transcode("temporary file path missing file name"))?;

    let downloaded_path = fetcher
        .download_video_from_url(url.clone(), file_name)
        .await
        .map_err(|err| AppError::dependency(format!("yt-dlp download failed: {err}")))?;

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

async fn prepare_ytdlp_fetcher(state: &AppState) -> Result<Youtube, AppError> {
    let libs_dir = state.storage.libs_dir();
    ensure_dir(libs_dir.as_path()).await?;
    let output_dir: PathBuf = state.storage.tmp_dir();

    let youtube_path = libs_dir.join(binary_name("yt-dlp"));
    let ffmpeg_path = libs_dir.join(binary_name("ffmpeg"));

    if youtube_path.exists() && ffmpeg_path.exists() {
        let libraries = Libraries::new(youtube_path, ffmpeg_path);
        match Youtube::new(libraries, output_dir.clone()) {
            Ok(fetcher) => return Ok(fetcher),
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "failed to initialize existing yt-dlp binaries, reinstalling"
                );
            }
        }
    }

    install_ytdlp_binaries(libs_dir, output_dir).await
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

fn binary_name(base: &str) -> String {
    if cfg!(target_os = "windows") {
        format!("{base}.exe")
    } else {
        base.to_string()
    }
}

async fn install_ytdlp_binaries(
    libs_dir: PathBuf,
    output_dir: PathBuf,
) -> Result<Youtube, AppError> {
    let handle = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        handle.block_on(async move { Youtube::with_new_binaries(libs_dir, output_dir).await })
    })
    .await
    .map_err(|err| AppError::dependency(format!("yt-dlp installer task panicked: {err}")))?
    .map_err(|err| AppError::dependency(format!("failed to install yt-dlp binaries: {err}")))
}

// Tests for this module live under `tests/` to keep source files focused.
