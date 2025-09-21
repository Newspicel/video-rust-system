use std::sync::Arc;

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::Value;
use tempfile::tempdir;
use tower::ServiceExt;
use uuid::Uuid;
use vrs::{
    cleanup::CleanupConfig,
    handlers,
    jobs::{DynJobStore, JobStage, LocalJobStore},
    state::AppState,
    storage::{self, Storage},
};

const BODY_LIMIT: usize = 1024 * 1024;

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

fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", axum::routing::get(health))
        .route(
            "/upload/multipart",
            axum::routing::post(handlers::upload_multipart),
        )
        .route(
            "/upload/remote",
            axum::routing::post(handlers::upload_remote),
        )
        .route(
            "/download/yt-dlp",
            axum::routing::post(handlers::download_via_ytdlp),
        )
        .route(
            "/videos/{id}/download",
            axum::routing::get(handlers::download_video),
        )
        .route("/videos/{id}", axum::routing::get(handlers::download_video))
        .route(
            "/videos/{id}/hls/{*asset}",
            axum::routing::get(handlers::get_hls_asset),
        )
        .route(
            "/videos/{id}/dash/{*asset}",
            axum::routing::get(handlers::get_dash_asset),
        )
        .route("/jobs/{id}", axum::routing::get(handlers::job_status))
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}

#[tokio::test]
async fn health_endpoint_returns_ok() {
    let temp = tempdir().unwrap();
    let state = build_state(temp.path()).await;
    let app = build_app(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), BODY_LIMIT).await.unwrap();
    assert_eq!(body.as_ref(), b"ok");
}

#[tokio::test]
async fn job_status_returns_not_found_for_unknown_job() {
    let temp = tempdir().unwrap();
    let state = build_state(temp.path()).await;
    let app = build_app(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/jobs/{}", Uuid::new_v4()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn job_status_returns_latest_snapshot() {
    let temp = tempdir().unwrap();
    let state = build_state(temp.path()).await;
    let job_id = Uuid::new_v4();

    state.jobs.create_job(job_id).await.unwrap();
    state
        .jobs
        .set_plan(job_id, vec![JobStage::Downloading, JobStage::Transcoding])
        .await
        .unwrap();
    state
        .jobs
        .update_stage(job_id, JobStage::Downloading)
        .await
        .unwrap();
    state.jobs.update_progress(job_id, 0.42).await.unwrap();

    let app = build_app(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/jobs/{job_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), BODY_LIMIT).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["stage"], "downloading");
    let overall = json["progress"].as_f64().unwrap();
    let stage_progress = json["stage_progress"].as_f64().unwrap();
    assert!((overall - 0.21).abs() < 1e-6);
    assert!((stage_progress - 0.42).abs() < 1e-6);
}

#[tokio::test]
async fn download_video_serves_file() {
    let temp = tempdir().unwrap();
    let state = build_state(temp.path()).await;
    let video_id = Uuid::new_v4();
    let download_path = state.storage.download_path(&video_id);
    storage::ensure_parent(&download_path).await.unwrap();
    tokio::fs::write(&download_path, b"abcdef").await.unwrap();

    let app = build_app(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/videos/{video_id}/download"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .unwrap(),
        "video/webm"
    );
    let body = to_bytes(response.into_body(), BODY_LIMIT).await.unwrap();
    assert_eq!(body.as_ref(), b"abcdef");
}

#[tokio::test]
async fn download_video_honors_range_requests() {
    let temp = tempdir().unwrap();
    let state = build_state(temp.path()).await;
    let video_id = Uuid::new_v4();
    let download_path = state.storage.download_path(&video_id);
    storage::ensure_parent(&download_path).await.unwrap();
    tokio::fs::write(&download_path, b"abcdef").await.unwrap();

    let app = build_app(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/videos/{video_id}/download"))
                .header(axum::http::header::RANGE, "bytes=1-3")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = to_bytes(response.into_body(), BODY_LIMIT).await.unwrap();
    assert_eq!(body.as_ref(), b"bcd");
}

#[tokio::test]
async fn hls_asset_serves_playlist() {
    let temp = tempdir().unwrap();
    let state = build_state(temp.path()).await;
    let video_id = Uuid::new_v4();
    let download = state.storage.download_path(&video_id);
    storage::ensure_parent(&download).await.unwrap();
    tokio::fs::write(&download, b"av1").await.unwrap();
    let master = state.storage.hls_dir(&video_id).join("master.m3u8");
    storage::ensure_parent(&master).await.unwrap();
    tokio::fs::write(&master, b"#EXTM3U\n").await.unwrap();

    let app = build_app(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/videos/{video_id}/hls/master.m3u8"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), BODY_LIMIT).await.unwrap();
    assert!(body.starts_with(b"#EXTM3U"));
}

#[tokio::test]
async fn dash_asset_serves_manifest() {
    let temp = tempdir().unwrap();
    let state = build_state(temp.path()).await;
    let video_id = Uuid::new_v4();
    let download = state.storage.download_path(&video_id);
    storage::ensure_parent(&download).await.unwrap();
    tokio::fs::write(&download, b"av1").await.unwrap();
    let manifest = state.storage.dash_dir(&video_id).join("manifest.mpd");
    storage::ensure_parent(&manifest).await.unwrap();
    tokio::fs::write(&manifest, b"<MPD/>").await.unwrap();

    let app = build_app(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/videos/{video_id}/dash/manifest.mpd"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), BODY_LIMIT).await.unwrap();
    assert_eq!(body.as_ref(), b"<MPD/>");
}

#[tokio::test]
async fn missing_video_download_returns_not_found() {
    let temp = tempdir().unwrap();
    let state = build_state(temp.path()).await;
    let video_id = Uuid::new_v4();
    let app = build_app(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/videos/{video_id}/download"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
