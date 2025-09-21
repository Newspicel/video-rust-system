use std::{path::Path, time::Duration};

use tokio::process::Command;

use crate::error::AppError;

use super::util::map_io_error;

const FFPROBE_BIN: &str = "ffprobe";

pub(crate) async fn probe_has_audio(input: &Path) -> Result<bool, AppError> {
    let output = Command::new(FFPROBE_BIN)
        .arg("-v")
        .arg("error")
        .arg("-select_streams")
        .arg("a")
        .arg("-show_entries")
        .arg("stream=index")
        .arg("-of")
        .arg("csv=p=0")
        .arg(input)
        .output()
        .await
        .map_err(map_io_error)?;

    if !output.status.success() {
        return Err(AppError::transcode(format!(
            "ffprobe exited with status {}",
            output.status
        )));
    }

    Ok(!output.stdout.is_empty())
}

pub(crate) async fn probe_duration(input: &Path) -> Result<Option<Duration>, AppError> {
    let output = Command::new(FFPROBE_BIN)
        .arg("-v")
        .arg("error")
        .arg("-show_entries")
        .arg("format=duration")
        .arg("-of")
        .arg("default=noprint_wrappers=1:nokey=1")
        .arg(input)
        .output()
        .await
        .map_err(map_io_error)?;

    if !output.status.success() {
        tracing::warn!(status = %output.status, "ffprobe did not report duration");
        return Ok(None);
    }

    let text = String::from_utf8_lossy(&output.stdout);
    let duration_str = text
        .lines()
        .next()
        .map(str::trim)
        .filter(|line| !line.is_empty());

    if let Some(seconds) = duration_str
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|seconds| seconds.is_finite() && *seconds > 0.0)
    {
        return Ok(Some(Duration::from_secs_f64(seconds)));
    }

    tracing::warn!("ffprobe returned an unexpected duration value");
    Ok(None)
}
