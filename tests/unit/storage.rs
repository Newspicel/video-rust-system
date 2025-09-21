use std::env;
use tempfile::tempdir;
use uuid::Uuid;
use vrs::error::AppError;
use vrs::storage::{Storage, ensure_dir};

#[tokio::test]
async fn initialize_sets_up_directories() -> Result<(), AppError> {
    let temp = tempdir().expect("tempdir");
    let storage = Storage::initialize(temp.path()).await?;
    let id = Uuid::new_v4();

    let incoming = storage.incoming_path(&id);
    let expected_name = format!("{}.incoming", id.simple());
    assert_eq!(
        incoming.file_name().and_then(|s| s.to_str()),
        Some(expected_name.as_str())
    );

    let video_dir = storage.video_dir(&id);
    assert!(video_dir.starts_with(temp.path()));
    assert_eq!(video_dir.parent(), Some(temp.path()));

    let download = storage.download_path(&id);
    assert!(download.ends_with("download.webm"));

    let tmp_root = storage.tmp_dir();
    assert!(tmp_root.exists());
    assert!(tmp_root.starts_with(env::temp_dir()));
    assert!(tmp_root.ends_with("vrs"));

    let incoming_root = tmp_root.join("incoming");
    assert!(incoming.starts_with(&incoming_root));

    Ok(())
}

#[tokio::test]
async fn prune_transcodes_removes_variant_dirs() -> Result<(), AppError> {
    let temp = tempdir().expect("tempdir");
    let storage = Storage::initialize(temp.path()).await?;
    let id = Uuid::new_v4();

    let hls = storage.hls_dir(&id);
    ensure_dir(&hls).await?;
    ensure_dir(&hls.join("720p")).await?;
    let dash = storage.dash_dir(&id);
    ensure_dir(&dash).await?;

    assert!(storage.prune_transcodes(&id).await?);
    assert!(!hls.exists());
    assert!(!dash.exists());

    Ok(())
}

#[tokio::test]
async fn prune_transcodes_noop_when_missing() -> Result<(), AppError> {
    let temp = tempdir().expect("tempdir");
    let storage = Storage::initialize(temp.path()).await?;
    let id = Uuid::new_v4();

    assert!(!storage.prune_transcodes(&id).await?);

    Ok(())
}
