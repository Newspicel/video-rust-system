use std::{
    convert::Infallible,
    env,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use axum::{
    Router,
    http::Request,
    response::Response as AxumResponse,
    routing::{get, post},
};
use tower::{Service, layer::Layer};
use tower_http::cors::CorsLayer;
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
    let request_logger = RequestLoggerLayer::default();

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
        .layer(request_logger);

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

#[derive(Clone, Default)]
struct RequestLoggerLayer;

impl<S> Layer<S> for RequestLoggerLayer {
    type Service = RequestLogger<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestLogger { inner }
    }
}

#[derive(Clone)]
struct RequestLogger<S> {
    inner: S,
}

impl<S, Body> Service<Request<Body>> for RequestLogger<S>
where
    S: Service<Request<Body>, Response = AxumResponse, Error = Infallible> + Send + 'static,
    S::Future: Send + 'static,
    Body: Send + 'static,
{
    type Response = AxumResponse;
    type Error = Infallible;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let method = request.method().as_str().to_owned();
        let target = request
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str().to_owned())
            .unwrap_or_else(|| request.uri().path().to_owned());
        let fut = self.inner.call(request);

        Box::pin(async move {
            let response = fut.await?;
            let status = response.status().as_u16();
            tracing::debug!("{} {} {}", status, method, target);
            Ok(response)
        })
    }
}
