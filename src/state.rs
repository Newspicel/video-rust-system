use reqwest::Client;

use crate::{cleanup::CleanupConfig, jobs::DynJobStore, storage::Storage};

#[derive(Clone)]
pub struct AppState {
    pub storage: Storage,
    pub http_client: Client,
    pub jobs: DynJobStore,
    pub cleanup: CleanupConfig,
}
