use std::{collections::HashSet, env, path::Path};

use fs2::{available_space, total_space};
use tokio::task;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    error::AppError,
    jobs::{DynJobStore, JobStage},
    storage::Storage,
};

#[derive(Debug, Clone)]
pub struct CleanupConfig {
    pub minimum_free_bytes: u64,
    pub minimum_free_ratio: f32,
    pub max_cleanup_batch: usize,
}

impl CleanupConfig {
    pub fn from_env() -> Self {
        let minimum_free_bytes = env::var("VIDEO_STORAGE_MIN_FREE_BYTES")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(5 * 1024 * 1024 * 1024); // 5 GiB

        let minimum_free_ratio = env::var("VIDEO_STORAGE_MIN_FREE_RATIO")
            .ok()
            .and_then(|val| val.parse::<f32>().ok())
            .map(|ratio| ratio.clamp(0.0, 0.9))
            .unwrap_or(0.1); // 10%

        let max_cleanup_batch = env::var("VIDEO_STORAGE_CLEANUP_BATCH")
            .ok()
            .and_then(|val| val.parse::<usize>().ok())
            .filter(|&value| value > 0)
            .unwrap_or(5);

        Self {
            minimum_free_bytes,
            minimum_free_ratio,
            max_cleanup_batch,
        }
    }
}

pub async fn ensure_capacity(
    storage: &Storage,
    jobs: &DynJobStore,
    config: &CleanupConfig,
) -> Result<(), AppError> {
    if !needs_cleanup(storage, config).await? {
        return Ok(());
    }

    let statuses = jobs.list().await?;

    let active_ids: HashSet<Uuid> = statuses
        .iter()
        .filter(|status| !matches!(status.stage, JobStage::Complete | JobStage::Failed))
        .map(|status| status.id)
        .collect();

    let mut candidates: Vec<_> = statuses
        .into_iter()
        .filter(|status| matches!(status.stage, JobStage::Complete | JobStage::Failed))
        .collect();

    if candidates.is_empty() {
        warn!("storage cleanup requested but no completed jobs available to prune");
        return Ok(());
    }

    candidates.sort_by_key(|status| status.last_update_unix_ms);

    let mut cleaned = 0usize;

    for candidate in candidates {
        if cleaned >= config.max_cleanup_batch {
            break;
        }

        if active_ids.contains(&candidate.id) {
            continue;
        }

        if storage.prune_transcodes(&candidate.id).await? {
            cleaned += 1;
            info!(video_id = %candidate.id, "pruned derived renditions during cleanup");
        }

        if !needs_cleanup(storage, config).await? {
            break;
        }
    }

    Ok(())
}

async fn needs_cleanup(storage: &Storage, config: &CleanupConfig) -> Result<bool, AppError> {
    let root = storage.root_dir();
    let status = task::spawn_blocking(move || disk_status(&root))
        .await
        .map_err(|err| AppError::dependency(format!("cleanup blocking task failed: {err}")))??;

    let free_ratio = if status.total_bytes > 0 {
        status.free_bytes as f32 / status.total_bytes as f32
    } else {
        1.0
    };

    Ok(status.free_bytes < config.minimum_free_bytes || free_ratio < config.minimum_free_ratio)
}

struct DiskStatus {
    total_bytes: u64,
    free_bytes: u64,
}

fn disk_status(path: &Path) -> Result<DiskStatus, AppError> {
    let free_bytes = available_space(path)?;
    let total_bytes = total_space(path)?;
    Ok(DiskStatus {
        total_bytes,
        free_bytes,
    })
}
