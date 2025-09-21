use axum::{
    Json,
    extract::{Multipart, State},
};
use reqwest::Url;
use serde::Deserialize;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::{
    error::AppError, jobs::JobStage, state::AppState, storage::ensure_parent,
    transcode::EncodeParams,
};

use super::pipeline::{spawn_local_pipeline, spawn_remote_pipeline, spawn_ytdlp_pipeline};

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

    let raw_url = payload.url.clone();
    if !raw_url.starts_with("magnet:") {
        Url::parse(&raw_url).map_err(|err| AppError::validation(format!("invalid url: {err}")))?;
    }

    spawn_remote_pipeline(state.clone(), id, raw_url, encode);

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

    let url_string: String = url.into();
    spawn_ytdlp_pipeline(state.clone(), id, url_string, encode);

    Ok(Json(build_upload_response(id)))
}

pub(super) fn build_upload_response(id: Uuid) -> UploadResponse {
    let id_str = id.to_string();
    UploadResponse {
        id: id_str.clone(),
        status_url: format!("/jobs/{id_str}"),
        download_url: format!("/videos/{id_str}/download"),
        hls_master_url: format!("/videos/{id_str}/hls/master.m3u8"),
        dash_manifest_url: format!("/videos/{id_str}/dash/manifest.mpd"),
    }
}
