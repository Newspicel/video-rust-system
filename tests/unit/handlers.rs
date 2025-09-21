use axum::body;
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use std::sync::Arc;
use tempfile::tempdir;
use uuid::Uuid;
use vrs::cleanup::CleanupConfig;
use vrs::error::AppError;
use vrs::handlers::{ClientTranscodeOptions, RangeHeader, download_video, job_status};
use vrs::state::AppState;
use vrs::storage::{Storage, ensure_parent};
use vrs::transcode::EncodeParams;
use vrs::{DynJobStore, JobStage, LocalJobStore};

const BODY_LIMIT: usize = 1024 * 1024;

fn encode_params_from(options: ClientTranscodeOptions) -> EncodeParams {
    options.into()
}

async fn build_state(root: &std::path::Path) -> AppState {
    let storage = Storage::initialize(root).await.expect("storage");
    let jobs: DynJobStore = Arc::new(LocalJobStore::new());
    let http_client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .expect("client");
    let cleanup = CleanupConfig::from_env();

    AppState {
        storage,
        http_client,
        jobs,
        cleanup,
    }
}

#[test]
fn client_options_override_defaults() {
    let params = encode_params_from(ClientTranscodeOptions {
        crf: Some(12),
        cpu_used: Some(2),
    });
    assert_eq!(params.crf, 12);
    assert_eq!(params.cpu_used, 2);

    let sanitized = encode_params_from(ClientTranscodeOptions {
        crf: Some(80),
        cpu_used: Some(99),
    });
    assert_eq!(sanitized.crf, 63);
    assert_eq!(sanitized.cpu_used, 8);
}

#[tokio::test]
async fn download_video_supports_range_requests() -> Result<(), AppError> {
    let temp = tempdir().unwrap();
    let state = build_state(temp.path()).await;
    let id = Uuid::new_v4();

    let path = state.storage.download_path(&id);
    ensure_parent(&path).await?;
    tokio::fs::write(&path, b"hello world").await?;

    let response = download_video(
        State(state.clone()),
        AxumPath(id.to_string()),
        RangeHeader::new(Some("bytes=0-4".to_string())),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok()),
        Some("bytes 0-4/11")
    );

    let body = body::to_bytes(response.into_body(), BODY_LIMIT)
        .await
        .unwrap();
    assert_eq!(body.as_ref(), b"hello");

    Ok(())
}

#[tokio::test]
async fn download_video_rejects_invalid_ids() {
    let temp = tempdir().unwrap();
    let state = build_state(temp.path()).await;

    let result = download_video(
        State(state),
        AxumPath("not-a-uuid".to_string()),
        RangeHeader::new(None),
    )
    .await;

    assert!(matches!(result, Err(AppError::Validation(_))));
}

#[tokio::test]
async fn job_status_returns_not_found() {
    let temp = tempdir().unwrap();
    let state = build_state(temp.path()).await;

    let response = job_status(State(state.clone()), AxumPath(Uuid::new_v4().to_string())).await;

    assert!(matches!(response, Err(AppError::NotFound(_))));
}

#[tokio::test]
async fn job_status_returns_latest_snapshot() -> Result<(), AppError> {
    let temp = tempdir().unwrap();
    let state = build_state(temp.path()).await;
    let job_id = Uuid::new_v4();

    state.jobs.create_job(job_id).await?;
    state
        .jobs
        .set_plan(job_id, vec![JobStage::Downloading, JobStage::Transcoding])
        .await?;
    state
        .jobs
        .update_stage(job_id, JobStage::Downloading)
        .await?;
    state.jobs.update_progress(job_id, 0.5).await?;

    let response = job_status(State(state), AxumPath(job_id.to_string())).await?;
    let payload = response.0;

    assert_eq!(payload.stage, JobStage::Downloading);
    assert!((payload.progress - 0.25).abs() < f32::EPSILON);
    Ok(())
}
