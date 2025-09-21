use std::{
    collections::HashSet,
    future::Future,
    path::{Path, PathBuf},
    time::Duration,
};

use axum::{
    Json,
    body::Body,
    extract::{FromRequestParts, Multipart, Path as AxumPath, State},
    http::{self, HeaderValue, StatusCode},
    response::Response,
};
use reqwest::Url;
use serde::Deserialize;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::process::Command as TokioCommand;
use tokio_util::io::ReaderStream;
use url::ParseError;
use uuid::Uuid;
use yt_dlp::{Youtube, fetcher::deps::Libraries};

use crate::{
    cleanup,
    error::AppError,
    jobs::{JobStage, JobStatusResponse},
    state::AppState,
    storage::{ensure_dir, ensure_parent},
    transcode::{EncodeParams, ensure_dash_ready, ensure_hls_ready, process_video},
};

const ARIA2_BIN: &str = "aria2c";

#[derive(Debug, serde::Serialize)]
pub struct UploadResponse {
    pub id: String,
    pub status_url: String,
    pub download_url: String,
    pub hls_master_url: String,
    pub dash_manifest_url: String,
}

#[derive(Debug, Deserialize, Clone, Copy, Default)]
pub struct ClientTranscodeOptions {
    pub crf: Option<u8>,
    #[serde(default, rename = "cpu_used")]
    pub cpu_used: Option<u8>,
}

impl From<ClientTranscodeOptions> for EncodeParams {
    fn from(options: ClientTranscodeOptions) -> Self {
        let mut params = EncodeParams::default();
        if let Some(crf) = options.crf {
            params.crf = crf;
        }
        if let Some(cpu) = options.cpu_used {
            params.cpu_used = cpu;
        }
        params.sanitized()
    }
}

#[derive(Debug, Deserialize)]
pub struct RemoteUploadRequest {
    pub url: String,
    #[serde(default)]
    pub transcode: Option<ClientTranscodeOptions>,
}

#[derive(Debug, Deserialize)]
pub struct YtDlpDownloadRequest {
    pub url: String,
    #[serde(default)]
    pub transcode: Option<ClientTranscodeOptions>,
}

pub async fn upload_multipart(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<UploadResponse>, AppError> {
    while let Some(mut field) = multipart.next_field().await? {
        if field.file_name().is_none() {
            continue;
        }

        let id = Uuid::new_v4();
        state.jobs.create_job(id).await?;
        state
            .jobs
            .set_plan(id, vec![JobStage::Uploading, JobStage::Transcoding])
            .await?;
        state.jobs.update_stage(id, JobStage::Uploading).await?;
        let temp_path = state.storage.incoming_path(&id);
        ensure_parent(&temp_path).await?;

        let mut file = File::create(&temp_path).await?;
        while let Some(chunk) = field.chunk().await? {
            file.write_all(&chunk).await?;
        }
        file.flush().await?;

        state.jobs.update_progress(id, 1.0).await?;
        spawn_local_pipeline(state.clone(), id, temp_path);
        return Ok(Json(build_upload_response(id)));
    }

    Err(AppError::validation("multipart payload missing file field"))
}

pub async fn upload_remote(
    State(state): State<AppState>,
    Json(payload): Json<RemoteUploadRequest>,
) -> Result<Json<UploadResponse>, AppError> {
    let encode = payload.transcode.map(EncodeParams::from);
    let id = Uuid::new_v4();
    state.jobs.create_job(id).await?;
    state
        .jobs
        .set_plan(id, vec![JobStage::Downloading, JobStage::Transcoding])
        .await?;

    let state_for_task = state.clone();
    let raw_url = payload.url.clone();
    if !raw_url.starts_with("magnet:") {
        Url::parse(&raw_url).map_err(|err| AppError::validation(format!("invalid url: {err}")))?;
    }
    spawn_remote_pipeline(state_for_task, id, raw_url, encode);

    Ok(Json(build_upload_response(id)))
}

pub async fn download_via_ytdlp(
    State(state): State<AppState>,
    Json(payload): Json<YtDlpDownloadRequest>,
) -> Result<Json<UploadResponse>, AppError> {
    let url = Url::parse(&payload.url)
        .map_err(|err| AppError::validation(format!("invalid url: {err}")))?;
    let encode = payload.transcode.map(EncodeParams::from);
    let id = Uuid::new_v4();
    state.jobs.create_job(id).await?;
    state
        .jobs
        .set_plan(id, vec![JobStage::Downloading, JobStage::Transcoding])
        .await?;

    let state_for_task = state.clone();
    let url_string: String = url.into();
    spawn_ytdlp_pipeline(state_for_task, id, url_string, encode);

    Ok(Json(build_upload_response(id)))
}

pub async fn download_video(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    RangeHeader(range_header): RangeHeader,
) -> Result<Response, AppError> {
    let video_id =
        Uuid::parse_str(&id).map_err(|_| AppError::validation("invalid video identifier"))?;
    let path = state.storage.download_path(&video_id);
    serve_video_file(path, range_header.as_deref()).await
}

pub async fn get_hls_asset(
    State(state): State<AppState>,
    AxumPath((id, asset)): AxumPath<(String, String)>,
) -> Result<Response, AppError> {
    let video_id =
        Uuid::parse_str(&id).map_err(|_| AppError::validation("invalid video identifier"))?;
    validate_relative_path(&asset)?;
    ensure_hls_ready(&state.storage, &video_id).await?;
    let path = state.storage.hls_dir(&video_id).join(asset);
    serve_static_file(path).await
}

pub async fn get_dash_asset(
    State(state): State<AppState>,
    AxumPath((id, asset)): AxumPath<(String, String)>,
) -> Result<Response, AppError> {
    let video_id =
        Uuid::parse_str(&id).map_err(|_| AppError::validation("invalid video identifier"))?;
    validate_relative_path(&asset)?;
    ensure_dash_ready(&state.storage, &video_id).await?;
    let path = state.storage.dash_dir(&video_id).join(asset);
    serve_static_file(path).await
}

fn build_upload_response(id: Uuid) -> UploadResponse {
    let id_str = id.to_string();
    UploadResponse {
        id: id_str.clone(),
        status_url: format!("/jobs/{id_str}"),
        download_url: format!("/videos/{id_str}/download"),
        hls_master_url: format!("/videos/{id_str}/hls/master.m3u8"),
        dash_manifest_url: format!("/videos/{id_str}/dash/manifest.mpd"),
    }
}

async fn serve_video_file(path: PathBuf, range_header: Option<&str>) -> Result<Response, AppError> {
    if !path.exists() {
        return Err(AppError::not_found(format!(
            "video not found under {}",
            path.display()
        )));
    }

    let mut file = File::open(&path).await?;
    let metadata = file.metadata().await?;
    let file_size = metadata.len();

    let range = if let Some(range) = range_header {
        Some(parse_range(range, file_size)?)
    } else {
        None
    };

    let (status, body, content_length, content_range) = if let Some(range) = range {
        file.seek(std::io::SeekFrom::Start(range.start)).await?;
        let reader = BufReader::new(file).take(range.length);
        let body = Body::from_stream(ReaderStream::new(reader));
        let content_range = format!("bytes {}-{}/{}", range.start, range.end, file_size);
        (
            StatusCode::PARTIAL_CONTENT,
            body,
            range.length,
            Some(content_range),
        )
    } else {
        let body = Body::from_stream(ReaderStream::new(file));
        (StatusCode::OK, body, file_size, None)
    };

    let mut response = Response::builder().status(status).body(body).unwrap();

    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("video/webm"),
    );
    response.headers_mut().insert(
        http::header::ACCEPT_RANGES,
        HeaderValue::from_static("bytes"),
    );
    response.headers_mut().insert(
        http::header::CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string()).unwrap_or(HeaderValue::from_static("0")),
    );
    if let Some(content_range) = content_range {
        response.headers_mut().insert(
            http::header::CONTENT_RANGE,
            HeaderValue::from_str(&content_range).unwrap_or(HeaderValue::from_static("bytes */0")),
        );
    }
    response.headers_mut().insert(
        http::header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!(
            "inline; filename=\"{}.webm\"",
            path.file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or("video")
        ))
        .unwrap_or(HeaderValue::from_static("inline")),
    );

    Ok(response)
}

async fn serve_static_file(path: PathBuf) -> Result<Response, AppError> {
    if !path.exists() {
        return Err(AppError::not_found(format!(
            "asset not found: {}",
            path.display()
        )));
    }

    let file = File::open(&path).await?;
    let body = Body::from_stream(ReaderStream::new(file));
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .body(body)
        .unwrap();

    if let Some(mime) = mime_guess::from_path(&path).first() {
        if let Ok(value) = HeaderValue::from_str(mime.as_ref()) {
            response
                .headers_mut()
                .insert(http::header::CONTENT_TYPE, value);
        }
    } else if path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("m3u8"))
        .unwrap_or(false)
    {
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/vnd.apple.mpegurl"),
        );
    } else if path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("mpd"))
        .unwrap_or(false)
    {
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/dash+xml"),
        );
    }

    Ok(response)
}

#[derive(Debug, Clone, Copy)]
struct ByteRange {
    start: u64,
    end: u64,
    length: u64,
}

fn parse_range(raw: &str, file_size: u64) -> Result<ByteRange, AppError> {
    let raw = raw.trim();
    if !raw.starts_with("bytes=") {
        return Err(AppError::validation("unsupported range unit"));
    }
    let range = &raw[6..];
    let mut parts = range.splitn(2, '-');
    let start_str = parts
        .next()
        .ok_or_else(|| AppError::validation("invalid range format"))?;
    let end_str = parts
        .next()
        .ok_or_else(|| AppError::validation("invalid range format"))?;

    let start = start_str
        .parse::<u64>()
        .map_err(|_| AppError::validation("range start must be numeric"))?;

    let end = if end_str.is_empty() {
        file_size.saturating_sub(1)
    } else {
        end_str
            .parse::<u64>()
            .map_err(|_| AppError::validation("range end must be numeric"))?
    };

    if start > end || end >= file_size {
        return Err(AppError::validation("invalid range bounds"));
    }

    let length = end - start + 1;
    Ok(ByteRange { start, end, length })
}

#[derive(Debug, Clone)]
pub struct RangeHeader(Option<String>);

impl<S> FromRequestParts<S> for RangeHeader
where
    S: Send + Sync,
{
    type Rejection = AppError;

    fn from_request_parts(
        parts: &mut http::request::Parts,
        _state: &S,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        let range = parts
            .headers
            .get(http::header::RANGE)
            .map(|value| value.to_str().map(|s| s.to_owned()))
            .transpose()
            .map_err(|_| AppError::validation("invalid Range header"));

        async move { range.map(RangeHeader) }
    }
}

pub async fn job_status(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<JobStatusResponse>, AppError> {
    let job_id =
        Uuid::parse_str(&id).map_err(|_| AppError::validation("invalid job identifier"))?;
    match state.jobs.status(&job_id).await? {
        Some(status) => Ok(Json(status)),
        None => Err(AppError::not_found(format!("job {job_id} not found"))),
    }
}

fn validate_relative_path(path: &str) -> Result<(), AppError> {
    if path.starts_with('/') || path.contains("..") {
        return Err(AppError::validation("invalid asset path"));
    }
    Ok(())
}

fn spawn_local_pipeline(state: AppState, id: Uuid, temp_path: PathBuf) {
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

fn spawn_remote_pipeline(state: AppState, id: Uuid, url: String, encode: Option<EncodeParams>) {
    tokio::spawn(async move {
        if let Err(err) = run_remote_pipeline(state.clone(), id, url.clone(), encode).await {
            tracing::error!(%id, url, error = %err, "remote processing failed");
            if let Err(store_err) = state.jobs.fail(id, err.to_string()).await {
                tracing::error!(%id, url, error = %store_err, "failed to mark remote job failure");
            }
        }
    });
}

fn spawn_ytdlp_pipeline(state: AppState, id: Uuid, url: String, encode: Option<EncodeParams>) {
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
    cleanup::ensure_capacity(&state.storage, &state.jobs, &state.cleanup).await?;
    state.jobs.update_stage(id, JobStage::Transcoding).await?;
    process_video(&state.storage, &state.jobs, &id, temp_path.as_path(), None).await?;
    state.jobs.complete(id).await?;

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

    let parsed_url = Url::parse(&url);
    if should_use_aria2(&url, &parsed_url) {
        state.jobs.update_progress(id, 0.0).await?;
        download_with_aria2(&url, &temp_path).await?;
        state.jobs.update_progress(id, 1.0).await?;
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
    }

    state.jobs.update_stage(id, JobStage::Transcoding).await?;

    process_video(
        &state.storage,
        &state.jobs,
        &id,
        temp_path.as_path(),
        encode,
    )
    .await?;
    state.jobs.complete(id).await?;

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

    state.jobs.update_stage(id, JobStage::Transcoding).await?;

    process_video(
        &state.storage,
        &state.jobs,
        &id,
        temp_path.as_path(),
        encode,
    )
    .await?;
    state.jobs.complete(id).await?;

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
                tracing::warn!(error = %err, "failed to initialize existing yt-dlp binaries, reinstalling");
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

#[cfg(test)]
mod tests {
    use super::*;
    use url::ParseError;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/unit/handlers_unit.rs"
    ));

    #[test]
    fn aria2_detection_handles_magnet() {
        let err: Result<Url, ParseError> = Err(ParseError::RelativeUrlWithoutBase);
        assert!(super::should_use_aria2("magnet:?xt=urn:btih:test", &err));
    }

    #[test]
    fn aria2_detection_handles_ftp() {
        let parsed: Result<Url, ParseError> =
            Ok(Url::parse("ftp://example.com/video.mp4").unwrap());
        assert!(super::should_use_aria2(
            "ftp://example.com/video.mp4",
            &parsed
        ));
    }

    #[test]
    fn aria2_detection_skips_https() {
        let parsed: Result<Url, ParseError> =
            Ok(Url::parse("https://example.com/video.mp4").unwrap());
        assert!(!super::should_use_aria2(
            "https://example.com/video.mp4",
            &parsed
        ));
    }

    #[tokio::test]
    async fn dir_snapshot_detects_new_files() {
        let temp = tempfile::tempdir().unwrap();
        let before = super::dir_snapshot(temp.path()).await.unwrap();
        assert!(before.is_empty());

        let file_path = temp.path().join("example.bin");
        tokio::fs::write(&file_path, b"data").await.unwrap();

        let after = super::dir_snapshot(temp.path()).await.unwrap();
        assert!(after.contains(&file_path));
    }

    #[test]
    fn map_spawn_error_formats_messages() {
        let err = std::io::Error::new(std::io::ErrorKind::NotFound, "missing");
        let mapped = super::map_spawn_error(err, "aria2c");
        assert!(mapped.to_string().contains("aria2c"));

        let err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let mapped = super::map_spawn_error(err, "aria2c");
        assert!(mapped.to_string().contains("failed to spawn"));
    }
}
