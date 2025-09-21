use std::{future::Future, path::PathBuf};

use axum::{
    body::Body,
    extract::{FromRequestParts, Path as AxumPath, State},
    http::{self, HeaderValue, StatusCode},
    response::Response,
};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::{
    error::AppError,
    state::AppState,
    transcode::{ensure_dash_ready, ensure_hls_ready},
};

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

fn validate_relative_path(path: &str) -> Result<(), AppError> {
    if path.starts_with('/') || path.contains("..") {
        return Err(AppError::validation("invalid asset path"));
    }
    Ok(())
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

impl RangeHeader {
    pub fn new(value: Option<String>) -> Self {
        Self(value)
    }

    pub fn as_deref(&self) -> Option<&str> {
        self.0.as_deref()
    }
}

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

// Tests for this module live under `tests/` to keep source files focused.
