use std::{env, net::SocketAddr, sync::Arc};

use axum::{
    Router,
    http::Request,
    routing::{get, post},
};
use tower_http::cors::CorsLayer;
use tower_http::trace::{DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tracing::Level;
use vrs::{
    cleanup::CleanupConfig,
    handlers,
    jobs::{DynJobStore, LocalJobStore},
    state::AppState,
    storage::Storage,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_tracing();

    let addr: SocketAddr = env::var("VIDEO_SERVER_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:3000".to_string())
        .parse()?;
    let storage_root = env::var("VIDEO_STORAGE_DIR").unwrap_or_else(|_| "data".to_string());

    let storage = Storage::initialize(&storage_root).await?;
    let jobs: DynJobStore = Arc::new(LocalJobStore::new());
    let http_client = reqwest::Client::builder().build()?;
    let cleanup = CleanupConfig::from_env();

    let state = AppState {
        storage,
        http_client,
        jobs,
        cleanup,
    };

    let cors = CorsLayer::permissive();

    let trace_layer = TraceLayer::new_for_http()
        .make_span_with(|request: &Request<_>| {
            tracing::span!(
                Level::DEBUG,
                "http_request",
                method = %request.method(),
                uri = %request.uri(),
                version = ?request.version()
            )
        })
        .on_request(DefaultOnRequest::new().level(Level::DEBUG))
        .on_response(DefaultOnResponse::new().level(Level::DEBUG));

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/upload/multipart", post(handlers::upload_multipart))
        .route("/upload/remote", post(handlers::upload_remote))
        .route("/download/yt-dlp", post(handlers::download_via_ytdlp))
        .route("/videos/{id}/download", get(handlers::download_video))
        .route("/videos/{id}", get(handlers::download_video))
        .route("/videos/{id}/hls/{*asset}", get(handlers::get_hls_asset))
        .route("/videos/{id}/dash/{*asset}", get(handlers::get_dash_asset))
        .route("/jobs/{id}", get(handlers::job_status))
        .with_state(state)
        .layer(cors)
        .layer(trace_layer);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "video server listening");
    axum::serve(listener, app.into_make_service()).await?;

    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

fn setup_tracing() {
    if tracing::dispatcher::has_been_set() {
        return;
    }

    let env_filter =
        env::var("RUST_LOG").unwrap_or_else(|_| "vrs=debug,axum=info,tower_http=info".to_string());

    let init_result = tracing_subscriber::fmt()
        .with_env_filter(env_filter.clone())
        .with_target(false)
        .with_level(true)
        .compact()
        .try_init();

    if init_result.is_ok() {
        tracing::debug!(current_filter = %env_filter, "tracing initialized");
    }
}
