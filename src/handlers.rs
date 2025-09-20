use std::{future::Future, path::PathBuf, time::Duration};

use axum::{
    Json,
    body::Body,
    extract::{FromRequestParts, Multipart, Path, State},
    http::{self, HeaderValue, StatusCode},
    response::Response,
};
use reqwest::Url;
use serde::Deserialize;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio_util::io::ReaderStream;
use uuid::Uuid;
use yt_dlp::{Youtube, fetcher::deps::Libraries};

use crate::{
    cleanup,
    error::AppError,
    jobs::{JobStage, JobStatusResponse},
    state::AppState,
    storage::{ensure_dir, ensure_parent},
    transcode::process_video,
};

#[derive(Debug, serde::Serialize)]
pub struct UploadResponse {
    pub id: String,
    pub status_url: String,
    pub download_url: String,
    pub hls_master_url: String,
    pub dash_manifest_url: String,
}

#[derive(Debug, Deserialize)]
pub struct RemoteUploadRequest {
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct YtDlpDownloadRequest {
    pub url: String,
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
        state.jobs.update_stage(id, JobStage::Uploading).await?;
        state.jobs.update_progress(id, START_PROGRESS).await?;
        let temp_path = state.storage.incoming_path(&id);
        ensure_parent(&temp_path).await?;

        let mut file = File::create(&temp_path).await?;
        while let Some(chunk) = field.chunk().await? {
            file.write_all(&chunk).await?;
        }
        file.flush().await?;

        state.jobs.update_progress(id, UPLOAD_PROGRESS_END).await?;
        spawn_local_pipeline(state.clone(), id, temp_path);
        return Ok(Json(build_upload_response(id)));
    }

    Err(AppError::validation("multipart payload missing file field"))
}

pub async fn upload_remote(
    State(state): State<AppState>,
    Json(payload): Json<RemoteUploadRequest>,
) -> Result<Json<UploadResponse>, AppError> {
    let url = Url::parse(&payload.url)
        .map_err(|err| AppError::validation(format!("invalid url: {err}")))?;
    let id = Uuid::new_v4();
    state.jobs.create_job(id).await?;
    state.jobs.update_progress(id, START_PROGRESS).await?;

    let state_for_task = state.clone();
    let url_string: String = url.into();
    spawn_remote_pipeline(state_for_task, id, url_string);

    Ok(Json(build_upload_response(id)))
}

pub async fn download_via_ytdlp(
    State(state): State<AppState>,
    Json(payload): Json<YtDlpDownloadRequest>,
) -> Result<Json<UploadResponse>, AppError> {
    let url = Url::parse(&payload.url)
        .map_err(|err| AppError::validation(format!("invalid url: {err}")))?;
    let id = Uuid::new_v4();
    state.jobs.create_job(id).await?;
    state.jobs.update_progress(id, START_PROGRESS).await?;

    let state_for_task = state.clone();
    let url_string: String = url.into();
    spawn_ytdlp_pipeline(state_for_task, id, url_string);

    Ok(Json(build_upload_response(id)))
}

pub async fn download_video(
    State(state): State<AppState>,
    Path(id): Path<String>,
    RangeHeader(range_header): RangeHeader,
) -> Result<Response, AppError> {
    let video_id =
        Uuid::parse_str(&id).map_err(|_| AppError::validation("invalid video identifier"))?;
    let path = state.storage.download_path(&video_id);
    serve_video_file(path, range_header.as_deref()).await
}

pub async fn get_hls_asset(
    State(state): State<AppState>,
    Path((id, asset)): Path<(String, String)>,
) -> Result<Response, AppError> {
    let video_id =
        Uuid::parse_str(&id).map_err(|_| AppError::validation("invalid video identifier"))?;
    validate_relative_path(&asset)?;
    let path = state.storage.hls_dir(&video_id).join(asset);
    serve_static_file(path).await
}

pub async fn get_dash_asset(
    State(state): State<AppState>,
    Path((id, asset)): Path<(String, String)>,
) -> Result<Response, AppError> {
    let video_id =
        Uuid::parse_str(&id).map_err(|_| AppError::validation("invalid video identifier"))?;
    validate_relative_path(&asset)?;
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
    Path(id): Path<String>,
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

const DOWNLOAD_PROGRESS_END: f32 = 0.4;
const UPLOAD_PROGRESS_END: f32 = 0.35;
const START_PROGRESS: f32 = 0.05;

fn spawn_local_pipeline(state: AppState, id: Uuid, temp_path: PathBuf) {
    tokio::spawn(async move {
        if let Err(err) = run_local_pipeline(state.clone(), id, temp_path.clone()).await {
            tracing::error!(%id, error = %err, "local processing failed");
            if let Err(store_err) = state.jobs.fail(id, err.to_string()).await {
                tracing::error!(%id, error = %store_err, "failed to mark job as failed");
            }
            if let Err(e) = tokio::fs::remove_file(&temp_path).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(path = %temp_path.display(), ?e, "cleanup failed");
                }
            }
        }
    });
}

fn spawn_remote_pipeline(state: AppState, id: Uuid, url: String) {
    tokio::spawn(async move {
        if let Err(err) = run_remote_pipeline(state.clone(), id, url.clone()).await {
            tracing::error!(%id, url, error = %err, "remote processing failed");
            if let Err(store_err) = state.jobs.fail(id, err.to_string()).await {
                tracing::error!(%id, url, error = %store_err, "failed to mark remote job failure");
            }
        }
    });
}

fn spawn_ytdlp_pipeline(state: AppState, id: Uuid, url: String) {
    tokio::spawn(async move {
        if let Err(err) = run_ytdlp_pipeline(state.clone(), id, url.clone()).await {
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
    state
        .jobs
        .update_progress(id, DOWNLOAD_PROGRESS_END)
        .await?;

    process_video(&state.storage, &state.jobs, &id, temp_path.as_path()).await?;
    state.jobs.complete(id).await?;

    Ok(())
}

async fn run_remote_pipeline(state: AppState, id: Uuid, url: String) -> Result<(), AppError> {
    cleanup::ensure_capacity(&state.storage, &state.jobs, &state.cleanup).await?;
    state.jobs.update_stage(id, JobStage::Downloading).await?;
    state.jobs.update_progress(id, START_PROGRESS).await?;

    let temp_path = state.storage.incoming_path(&id);
    ensure_parent(&temp_path).await?;

    let mut response = state
        .http_client
        .get(Url::parse(&url).map_err(|err| AppError::validation(err.to_string()))?)
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
            let span = DOWNLOAD_PROGRESS_END - START_PROGRESS;
            let ratio = (downloaded as f32 / total as f32).clamp(0.0, 1.0);
            let progress = START_PROGRESS + span * ratio;
            state.jobs.update_progress(id, progress).await?;
        }
    }
    file.flush().await?;

    state.jobs.update_stage(id, JobStage::Transcoding).await?;
    state
        .jobs
        .update_progress(id, DOWNLOAD_PROGRESS_END)
        .await?;

    process_video(&state.storage, &state.jobs, &id, temp_path.as_path()).await?;
    state.jobs.complete(id).await?;

    Ok(())
}

async fn run_ytdlp_pipeline(state: AppState, id: Uuid, url: String) -> Result<(), AppError> {
    cleanup::ensure_capacity(&state.storage, &state.jobs, &state.cleanup).await?;
    state.jobs.update_stage(id, JobStage::Downloading).await?;
    state.jobs.update_progress(id, START_PROGRESS).await?;

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
    state
        .jobs
        .update_progress(id, DOWNLOAD_PROGRESS_END)
        .await?;

    process_video(&state.storage, &state.jobs, &id, temp_path.as_path()).await?;
    state.jobs.complete(id).await?;

    Ok(())
}

async fn prepare_ytdlp_fetcher(state: &AppState) -> Result<Youtube, AppError> {
    let libs_dir = state.storage.libs_dir();
    ensure_dir(libs_dir.as_path()).await?;
    let output_dir = state.storage.tmp_dir();

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

fn binary_name(base: &str) -> String {
    if cfg!(target_os = "windows") {
        format!("{base}.exe")
    } else {
        base.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_range_handles_explicit_bounds() {
        let range = parse_range("bytes=10-19", 100).expect("range should parse");
        assert_eq!(range.start, 10);
        assert_eq!(range.end, 19);
        assert_eq!(range.length, 10);
    }

    #[test]
    fn parse_range_handles_open_end() {
        let range = parse_range("bytes=100-", 200).expect("range should parse");
        assert_eq!(range.start, 100);
        assert_eq!(range.end, 199);
        assert_eq!(range.length, 100);
    }

    #[test]
    fn parse_range_rejects_invalid_prefix() {
        let err = parse_range("items=0-10", 100).unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
    }

    #[test]
    fn upload_response_uses_expected_urls() {
        let id = Uuid::new_v4();
        let response = build_upload_response(id);
        assert_eq!(response.id, id.to_string());
        assert!(response.status_url.ends_with(&response.id));
        assert!(response.download_url.contains(&response.id));
        assert!(response.hls_master_url.contains("hls"));
        assert!(response.dash_manifest_url.contains("dash"));
    }

    #[test]
    fn validate_relative_path_rules() {
        assert!(validate_relative_path("segment.m4s").is_ok());
        assert!(validate_relative_path("subdir/segment.m4s").is_ok());
        assert!(validate_relative_path("../evil").is_err());
        assert!(validate_relative_path("/absolute").is_err());
    }

    #[test]
    fn binary_name_reflects_platform() {
        let result = binary_name("yt-dlp");
        if cfg!(target_os = "windows") {
            assert_eq!(result, "yt-dlp.exe");
        } else {
            assert_eq!(result, "yt-dlp");
        }
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
