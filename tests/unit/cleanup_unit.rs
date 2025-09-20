use crate::jobs::{DynJobStore, JobStage, LocalJobStore};
use crate::error::AppError;
use crate::storage::ensure_dir;
use std::sync::{Arc, Mutex, OnceLock};
use tempfile::tempdir;
use tokio::fs;
use uuid::Uuid;

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
