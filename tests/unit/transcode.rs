use tempfile::tempdir;
use uuid::Uuid;
use vrs::error::AppError;
use vrs::storage::{self, Storage};
use vrs::transcode::ensure_hls_ready;

#[tokio::test]
async fn ensure_hls_ready_backfills_master_playlist() -> Result<(), AppError> {
    let temp = tempdir().expect("tempdir");
    let storage = Storage::initialize(temp.path()).await?;
    let video_id = Uuid::new_v4();

    // Pretend the download already completed so ensure_hls_ready will skip re-encoding.
    let download = storage.download_path(&video_id);
    storage::ensure_parent(&download).await?;
    tokio::fs::write(&download, b"stub").await?;

    let hls_dir = storage.hls_dir(&video_id);
    storage::ensure_dir(&hls_dir).await?;
    let index = hls_dir.join("index.m3u8");
    tokio::fs::write(&index, b"#EXTM3U\n").await?;
    let master = hls_dir.join("master.m3u8");
    assert!(!master.exists());

    ensure_hls_ready(&storage, &video_id).await?;

    assert!(master.exists());
    let master_contents = tokio::fs::read(&master).await?;
    let index_contents = tokio::fs::read(&index).await?;
    assert_eq!(master_contents, index_contents);

    Ok(())
}
