use std::{ffi::OsString, path::Path};

use tokio::fs;

use crate::{error::AppError, storage::ensure_parent};

pub(crate) async fn finalize_encoded_file(temp: &Path, final_path: &Path) -> Result<(), AppError> {
    ensure_parent(final_path).await?;

    if final_path.exists() {
        fs::remove_file(final_path).await.ok();
    }

    match fs::rename(temp, final_path).await {
        Ok(_) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::CrossesDevices => {
            fs::copy(temp, final_path).await.map_err(AppError::from)?;
            fs::remove_file(temp).await.ok();
            Ok(())
        }
        Err(err) => Err(err.into()),
    }
}

pub(crate) fn map_io_error(err: std::io::Error) -> AppError {
    match err.kind() {
        std::io::ErrorKind::NotFound => {
            AppError::dependency("required media tooling not found on PATH")
        }
        _ => AppError::Transcode(err.to_string()),
    }
}

pub(crate) fn os<S: Into<OsString>>(value: S) -> OsString {
    value.into()
}

pub(crate) fn os_path(path: &Path) -> OsString {
    path.as_os_str().to_os_string()
}
