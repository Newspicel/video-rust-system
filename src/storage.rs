use std::{
    env,
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
    libs_dir: PathBuf,
    tmp_root: PathBuf,
    tmp_incoming_dir: PathBuf,
    tmp_hls_dir: PathBuf,
    tmp_dash_dir: PathBuf,
}

impl Storage {
    pub async fn initialize(root: impl AsRef<Path>) -> Result<Self, AppError> {
        let root = root.as_ref().to_path_buf();
        let videos_dir = root.join("videos");
        let libs_dir = root.join("libs");
        let tmp_root = env::temp_dir().join("vrs");
        let tmp_incoming_dir = tmp_root.join("incoming");
        let tmp_hls_dir = tmp_root.join("hls");
        let tmp_dash_dir = tmp_root.join("dash");

        ensure_dir(&videos_dir).await?;
        ensure_dir(&libs_dir).await?;
        ensure_dir(&tmp_root).await?;
        ensure_dir(&tmp_incoming_dir).await?;
        ensure_dir(&tmp_hls_dir).await?;
        ensure_dir(&tmp_dash_dir).await?;

        Ok(Self {
            inner: Arc::new(StorageInner {
                root_dir: root,
                videos_dir,
                libs_dir,
                tmp_root,
                tmp_incoming_dir,
                tmp_hls_dir,
                tmp_dash_dir,
            }),
        })
    }

    pub fn incoming_path(&self, id: &uuid::Uuid) -> PathBuf {
        self.inner
            .tmp_incoming_dir
            .join(format!("{}.incoming", id.simple()))
    }

    pub fn video_dir(&self, id: &uuid::Uuid) -> PathBuf {
        self.inner.videos_dir.join(id.hyphenated().to_string())
    }

    pub fn download_path(&self, id: &uuid::Uuid) -> PathBuf {
        self.video_dir(id).join("download.webm")
    }

    pub fn hls_dir(&self, id: &uuid::Uuid) -> PathBuf {
        self.inner.tmp_hls_dir.join(id.hyphenated().to_string())
    }

    pub fn dash_dir(&self, id: &uuid::Uuid) -> PathBuf {
        self.inner.tmp_dash_dir.join(id.hyphenated().to_string())
    }

    pub fn tmp_dir(&self) -> PathBuf {
        self.inner.tmp_root.clone()
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
        _rendition_names: &[&str],
    ) -> Result<(), AppError> {
        let video_dir = self.video_dir(id);
        ensure_dir(&video_dir).await?;

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
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/unit/storage_unit.rs"
    ));
}
