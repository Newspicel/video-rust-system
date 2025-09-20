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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::{DynJobStore, JobStage, LocalJobStore};
    use crate::storage::{Storage, ensure_dir};
    use std::sync::{Arc, Mutex, OnceLock};
    use tempfile::tempdir;
    use tokio::fs;

    static ENV_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

    #[test]
    fn config_from_env_overrides_defaults() {
        let lock = ENV_MUTEX.get_or_init(|| Mutex::new(())).lock().unwrap();

        let prev_bytes = env::var("VIDEO_STORAGE_MIN_FREE_BYTES").ok();
        let prev_ratio = env::var("VIDEO_STORAGE_MIN_FREE_RATIO").ok();
        let prev_batch = env::var("VIDEO_STORAGE_CLEANUP_BATCH").ok();

        unsafe {
            env::set_var("VIDEO_STORAGE_MIN_FREE_BYTES", "1024");
            env::set_var("VIDEO_STORAGE_MIN_FREE_RATIO", "0.25");
            env::set_var("VIDEO_STORAGE_CLEANUP_BATCH", "2");
        }

        let config = CleanupConfig::from_env();
        assert_eq!(config.minimum_free_bytes, 1024);
        assert!((config.minimum_free_ratio - 0.25).abs() < f32::EPSILON);
        assert_eq!(config.max_cleanup_batch, 2);

        if let Some(value) = prev_bytes {
            unsafe { env::set_var("VIDEO_STORAGE_MIN_FREE_BYTES", value) };
        } else {
            unsafe { env::remove_var("VIDEO_STORAGE_MIN_FREE_BYTES") };
        }
        if let Some(value) = prev_ratio {
            unsafe { env::set_var("VIDEO_STORAGE_MIN_FREE_RATIO", value) };
        } else {
            unsafe { env::remove_var("VIDEO_STORAGE_MIN_FREE_RATIO") };
        }
        if let Some(value) = prev_batch {
            unsafe { env::set_var("VIDEO_STORAGE_CLEANUP_BATCH", value) };
        } else {
            unsafe { env::remove_var("VIDEO_STORAGE_CLEANUP_BATCH") };
        }

        drop(lock);
    }

    #[tokio::test]
    async fn ensure_capacity_prunes_transcodes() -> Result<(), AppError> {
        let temp_dir = tempdir().expect("tempdir");
        let storage = Storage::initialize(temp_dir.path()).await?;
        let jobs: DynJobStore = Arc::new(LocalJobStore::new());
        let job_id = Uuid::new_v4();

        jobs.create_job(job_id).await?;
        jobs.update_stage(job_id, JobStage::Complete).await?;

        let hls_dir = storage.hls_dir(&job_id).join("1080p");
        ensure_dir(hls_dir.parent().unwrap()).await?;
        fs::create_dir_all(&hls_dir).await?;
        fs::write(hls_dir.join("index.m3u8"), b"#EXTM3U").await?;

        let dash_dir = storage.dash_dir(&job_id);
        fs::create_dir_all(&dash_dir).await?;
        fs::write(dash_dir.join("manifest.mpd"), b"<MPD>").await?;

        let config = CleanupConfig {
            minimum_free_bytes: u64::MAX,
            minimum_free_ratio: 1.0,
            max_cleanup_batch: 10,
        };

        ensure_capacity(&storage, &jobs, &config).await?;

        assert!(!storage.hls_dir(&job_id).exists());
        assert!(!storage.dash_dir(&job_id).exists());

        Ok(())
    }
}
