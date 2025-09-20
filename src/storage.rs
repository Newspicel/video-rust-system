use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::fs;

use crate::error::AppError;

#[derive(Clone)]
pub struct Storage {
    inner: Arc<StorageInner>,
}

struct StorageInner {
    root_dir: PathBuf,
    videos_dir: PathBuf,
    tmp_dir: PathBuf,
    libs_dir: PathBuf,
}

impl Storage {
    pub async fn initialize(root: impl AsRef<Path>) -> Result<Self, AppError> {
        let root = root.as_ref().to_path_buf();
        let videos_dir = root.join("videos");
        let tmp_dir = root.join("tmp");
        let libs_dir = root.join("libs");

        ensure_dir(&videos_dir).await?;
        ensure_dir(&tmp_dir).await?;
        ensure_dir(&libs_dir).await?;

        Ok(Self {
            inner: Arc::new(StorageInner {
                root_dir: root,
                videos_dir,
                tmp_dir,
                libs_dir,
            }),
        })
    }

    pub fn incoming_path(&self, id: &uuid::Uuid) -> PathBuf {
        self.inner.tmp_dir.join(format!("{}.incoming", id.simple()))
    }

    pub fn video_dir(&self, id: &uuid::Uuid) -> PathBuf {
        self.inner.videos_dir.join(id.hyphenated().to_string())
    }

    pub fn download_path(&self, id: &uuid::Uuid) -> PathBuf {
        self.video_dir(id).join("download.webm")
    }

    pub fn hls_dir(&self, id: &uuid::Uuid) -> PathBuf {
        self.video_dir(id).join("hls")
    }

    pub fn dash_dir(&self, id: &uuid::Uuid) -> PathBuf {
        self.video_dir(id).join("dash")
    }

    pub fn tmp_dir(&self) -> PathBuf {
        self.inner.tmp_dir.clone()
    }

    pub fn libs_dir(&self) -> PathBuf {
        self.inner.libs_dir.clone()
    }

    pub fn root_dir(&self) -> PathBuf {
        self.inner.root_dir.clone()
    }

    pub async fn prune_transcodes(&self, id: &uuid::Uuid) -> Result<bool, AppError> {
        let mut pruned = false;
        let hls_dir = self.hls_dir(id);
        if hls_dir.exists() {
            match fs::remove_dir_all(&hls_dir).await {
                Ok(()) => pruned = true,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            }
        }

        let dash_dir = self.dash_dir(id);
        if dash_dir.exists() {
            match fs::remove_dir_all(&dash_dir).await {
                Ok(()) => pruned = true,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            }
        }

        Ok(pruned)
    }

    pub async fn prepare_video_dirs(
        &self,
        id: &uuid::Uuid,
        rendition_names: &[&str],
    ) -> Result<(), AppError> {
        let video_dir = self.video_dir(id);
        ensure_dir(&video_dir).await?;

        let hls_dir = self.hls_dir(id);
        ensure_dir(&hls_dir).await?;
        for name in rendition_names {
            ensure_dir(&hls_dir.join(name)).await?;
        }

        let dash_dir = self.dash_dir(id);
        ensure_dir(&dash_dir).await?;

        Ok(())
    }
}

pub async fn ensure_dir(dir: &Path) -> Result<(), AppError> {
    if !dir.exists() {
        fs::create_dir_all(dir).await?;
    }
    Ok(())
}

pub async fn ensure_parent(path: &Path) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use uuid::Uuid;

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

        let download = storage.download_path(&id);
        assert!(download.ends_with("download.webm"));

        assert!(storage.tmp_dir().exists());
        assert!(storage.libs_dir().exists());

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
}
