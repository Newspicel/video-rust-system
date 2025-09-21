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
    async fn set_plan(&self, id: Uuid, plan: Vec<JobStage>) -> Result<(), AppError>;
    async fn update_stage(&self, id: Uuid, stage: JobStage) -> Result<(), AppError>;
    async fn update_progress(&self, id: Uuid, progress: f32) -> Result<(), AppError>;
    async fn update_stage_eta(&self, id: Uuid, eta_seconds: Option<f64>) -> Result<(), AppError>;
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

impl Default for LocalJobStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl JobStore for LocalJobStore {
    async fn create_job(&self, id: Uuid) -> Result<(), AppError> {
        let mut guard = self.inner.lock().await;
        guard.insert(id, JobRecord::new());
        Ok(())
    }

    async fn set_plan(&self, id: Uuid, plan: Vec<JobStage>) -> Result<(), AppError> {
        let mut guard = self.inner.lock().await;
        if let Some(record) = guard.get_mut(&id) {
            record.set_plan(plan);
        }
        Ok(())
    }

    async fn update_stage(&self, id: Uuid, stage: JobStage) -> Result<(), AppError> {
        if let Some(record) = self.inner.lock().await.get_mut(&id) {
            record.set_stage(stage);
        }
        Ok(())
    }

    async fn update_progress(&self, id: Uuid, progress: f32) -> Result<(), AppError> {
        if let Some(record) = self.inner.lock().await.get_mut(&id) {
            record.set_stage_progress(progress);
        }
        Ok(())
    }

    async fn update_stage_eta(&self, id: Uuid, eta_seconds: Option<f64>) -> Result<(), AppError> {
        if let Some(record) = self.inner.lock().await.get_mut(&id) {
            record.stage_eta_seconds = eta_seconds;
            record.touch();
        }
        Ok(())
    }

    async fn fail(&self, id: Uuid, error: String) -> Result<(), AppError> {
        if let Some(record) = self.inner.lock().await.get_mut(&id) {
            record.fail(error);
            record.stage_eta_seconds = None;
        }
        Ok(())
    }

    async fn complete(&self, id: Uuid) -> Result<(), AppError> {
        if let Some(record) = self.inner.lock().await.get_mut(&id) {
            record.complete();
            record.stage_eta_seconds = Some(0.0);
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
    stage_progress: f32,
    started_at_instant: Instant,
    last_update_instant: Instant,
    started_at_system: SystemTime,
    last_update_system: SystemTime,
    error: Option<String>,
    plan: Vec<JobStage>,
    stage_started_at_instant: Instant,
    stage_started_at_system: SystemTime,
    stage_eta_seconds: Option<f64>,
}

impl JobRecord {
    fn new() -> Self {
        let now_instant = Instant::now();
        let now_system = SystemTime::now();
        Self {
            stage: JobStage::Queued,
            stage_progress: 0.0,
            started_at_instant: now_instant,
            last_update_instant: now_instant,
            started_at_system: now_system,
            last_update_system: now_system,
            error: None,
            plan: Vec::new(),
            stage_started_at_instant: now_instant,
            stage_started_at_system: now_system,
            stage_eta_seconds: None,
        }
    }

    fn set_plan(&mut self, plan: Vec<JobStage>) {
        self.plan = plan;
        self.touch();
    }

    fn set_stage(&mut self, stage: JobStage) {
        self.stage = stage;
        self.stage_progress = 0.0;
        self.stage_started_at_instant = Instant::now();
        self.stage_started_at_system = SystemTime::now();
        self.stage_eta_seconds = None;
        self.touch();
    }

    fn set_stage_progress(&mut self, progress: f32) {
        self.stage_progress = progress.clamp(0.0, 1.0);
        self.touch();
    }

    fn fail(&mut self, error: String) {
        self.stage = JobStage::Failed;
        self.error = Some(error);
        self.touch();
    }

    fn complete(&mut self) {
        self.stage = JobStage::Complete;
        self.stage_progress = 1.0;
        self.stage_eta_seconds = Some(0.0);
        self.touch();
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

        let (overall_progress, stage_progress, stage_index, total_stages) =
            self.compute_progress_metrics();

        let estimated_remaining_seconds = self.estimate_remaining_seconds(stage_progress);

        JobStatusResponse {
            id,
            stage: self.stage,
            progress: overall_progress,
            stage_progress,
            current_stage_index: stage_index,
            total_stages,
            elapsed_seconds,
            estimated_remaining_seconds,
            error: self.error.clone(),
            started_at_unix_ms: millis_since_epoch(self.started_at_system),
            last_update_unix_ms: millis_since_epoch(self.last_update_system),
        }
    }

    fn compute_progress_metrics(&self) -> (f32, f32, Option<u32>, u32) {
        if self.stage == JobStage::Complete {
            return (
                1.0,
                1.0,
                Some(self.plan.len() as u32),
                self.plan.len() as u32,
            );
        }

        let total_stages = self.plan.len() as f32;

        if total_stages == 0.0 {
            let stage_progress = if matches!(self.stage, JobStage::Failed) {
                self.stage_progress.min(1.0)
            } else {
                self.stage_progress
            };
            return (stage_progress, stage_progress, None, 0);
        }

        let stage_index = self.plan.iter().position(|stage| *stage == self.stage);
        match stage_index {
            Some(idx) => {
                let completed = idx as f32;
                let clamped_stage = self.stage_progress.clamp(0.0, 1.0);
                let overall = ((completed + clamped_stage) / total_stages).clamp(0.0, 1.0);
                (
                    overall,
                    clamped_stage,
                    Some((idx + 1) as u32),
                    self.plan.len() as u32,
                )
            }
            None => {
                let overall = match self.stage {
                    JobStage::Failed => self.stage_progress.clamp(0.0, 1.0),
                    JobStage::Queued => 0.0,
                    JobStage::Uploading | JobStage::Downloading | JobStage::Transcoding => {
                        (self.stage_progress / total_stages).clamp(0.0, 1.0)
                    }
                    JobStage::Finalizing => {
                        ((total_stages - 1.0 + self.stage_progress) / total_stages).clamp(0.0, 1.0)
                    }
                    JobStage::Complete => 1.0,
                };
                (
                    overall,
                    self.stage_progress.clamp(0.0, 1.0),
                    None,
                    self.plan.len() as u32,
                )
            }
        }
    }

    fn estimate_remaining_seconds(&self, stage_progress: f32) -> Option<f64> {
        const INITIAL_ESTIMATE_SECONDS: f64 = 45.0 * 60.0; // 45 minutes as an upper-bound guess
        const MIN_STAGE_PROGRESS_FOR_ESTIMATE: f32 = 0.02;

        if matches!(self.stage, JobStage::Complete) {
            return Some(0.0);
        }

        if let Some(eta) = self.stage_eta_seconds {
            return Some(eta.max(0.0));
        }

        let stage_elapsed = self.stage_elapsed_seconds();

        if stage_progress < MIN_STAGE_PROGRESS_FOR_ESTIMATE {
            let baseline = INITIAL_ESTIMATE_SECONDS.max(stage_elapsed.max(1.0) * 6.0);
            return Some(baseline);
        }

        let divisor = stage_progress.max(MIN_STAGE_PROGRESS_FOR_ESTIMATE) as f64;
        let total_estimated = stage_elapsed / divisor;
        Some((total_estimated - stage_elapsed).max(0.0))
    }

    fn stage_elapsed_seconds(&self) -> f64 {
        self.stage_started_at_instant.elapsed().as_secs_f64()
    }
}

fn millis_since_epoch(system_time: SystemTime) -> u128 {
    system_time
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_millis()
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
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
    pub stage_progress: f32,
    pub current_stage_index: Option<u32>,
    pub total_stages: u32,
    pub elapsed_seconds: f64,
    pub estimated_remaining_seconds: Option<f64>,
    pub error: Option<String>,
    pub started_at_unix_ms: u128,
    pub last_update_unix_ms: u128,
}

#[cfg(test)]
mod tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/unit/jobs_unit.rs"
    ));
}
