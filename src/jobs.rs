use async_trait::async_trait;
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::error::AppError;

#[async_trait]
pub trait JobStore: Send + Sync {
    async fn create_job(&self, id: Uuid) -> Result<(), AppError>;
    async fn update_stage(&self, id: Uuid, stage: JobStage) -> Result<(), AppError>;
    async fn update_progress(&self, id: Uuid, progress: f32) -> Result<(), AppError>;
    async fn fail(&self, id: Uuid, error: String) -> Result<(), AppError>;
    async fn complete(&self, id: Uuid) -> Result<(), AppError>;
    async fn status(&self, id: &Uuid) -> Result<Option<JobStatusResponse>, AppError>;
    async fn list(&self) -> Result<Vec<JobStatusResponse>, AppError>;
}

#[derive(Clone)]
pub struct LocalJobStore {
    inner: Arc<Mutex<HashMap<Uuid, JobRecord>>>,
}

impl LocalJobStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl JobStore for LocalJobStore {
    async fn create_job(&self, id: Uuid) -> Result<(), AppError> {
        let mut guard = self.inner.lock().await;
        guard.insert(id, JobRecord::new());
        Ok(())
    }

    async fn update_stage(&self, id: Uuid, stage: JobStage) -> Result<(), AppError> {
        if let Some(record) = self.inner.lock().await.get_mut(&id) {
            record.stage = stage;
            record.touch();
        }
        Ok(())
    }

    async fn update_progress(&self, id: Uuid, progress: f32) -> Result<(), AppError> {
        if let Some(record) = self.inner.lock().await.get_mut(&id) {
            record.progress = progress.clamp(0.0, 1.0);
            record.touch();
        }
        Ok(())
    }

    async fn fail(&self, id: Uuid, error: String) -> Result<(), AppError> {
        if let Some(record) = self.inner.lock().await.get_mut(&id) {
            record.stage = JobStage::Failed;
            record.error = Some(error);
            record.progress = record.progress.max(0.0);
            record.touch();
        }
        Ok(())
    }

    async fn complete(&self, id: Uuid) -> Result<(), AppError> {
        if let Some(record) = self.inner.lock().await.get_mut(&id) {
            record.stage = JobStage::Complete;
            record.progress = 1.0;
            record.touch();
        }
        Ok(())
    }

    async fn status(&self, id: &Uuid) -> Result<Option<JobStatusResponse>, AppError> {
        let guard = self.inner.lock().await;
        Ok(guard.get(id).map(|record| record.to_response(*id)))
    }

    async fn list(&self) -> Result<Vec<JobStatusResponse>, AppError> {
        let guard = self.inner.lock().await;
        Ok(guard
            .iter()
            .map(|(id, record)| record.to_response(*id))
            .collect())
    }
}

pub type DynJobStore = Arc<dyn JobStore>;

struct JobRecord {
    stage: JobStage,
    progress: f32,
    started_at_instant: Instant,
    last_update_instant: Instant,
    started_at_system: SystemTime,
    last_update_system: SystemTime,
    error: Option<String>,
}

impl JobRecord {
    fn new() -> Self {
        let now_instant = Instant::now();
        let now_system = SystemTime::now();
        Self {
            stage: JobStage::Queued,
            progress: 0.0,
            started_at_instant: now_instant,
            last_update_instant: now_instant,
            started_at_system: now_system,
            last_update_system: now_system,
            error: None,
        }
    }

    fn touch(&mut self) {
        self.last_update_instant = Instant::now();
        self.last_update_system = SystemTime::now();
    }

    fn to_response(&self, id: Uuid) -> JobStatusResponse {
        let elapsed = self
            .last_update_instant
            .duration_since(self.started_at_instant);
        let elapsed_seconds = elapsed.as_secs_f64();

        let estimated_remaining_seconds = if self.progress >= 1.0 {
            Some(0.0)
        } else if self.progress > 0.0 {
            let total_estimated = elapsed_seconds / self.progress as f64;
            Some((total_estimated - elapsed_seconds).max(0.0))
        } else {
            None
        };

        JobStatusResponse {
            id,
            stage: self.stage,
            progress: self.progress,
            elapsed_seconds,
            estimated_remaining_seconds,
            error: self.error.clone(),
            started_at_unix_ms: millis_since_epoch(self.started_at_system),
            last_update_unix_ms: millis_since_epoch(self.last_update_system),
        }
    }
}

fn millis_since_epoch(system_time: SystemTime) -> u128 {
    system_time
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_millis()
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStage {
    Queued,
    Uploading,
    Downloading,
    Transcoding,
    Finalizing,
    Complete,
    Failed,
}

#[derive(Debug, Serialize)]
pub struct JobStatusResponse {
    pub id: Uuid,
    pub stage: JobStage,
    pub progress: f32,
    pub elapsed_seconds: f64,
    pub estimated_remaining_seconds: Option<f64>,
    pub error: Option<String>,
    pub started_at_unix_ms: u128,
    pub last_update_unix_ms: u128,
}
