use std::{
    ffi::OsString,
    process::Stdio,
    time::{Duration, Instant},
};

use tokio::{
    io::AsyncReadExt,
    process::{ChildStderr, Command},
};
use uuid::Uuid;

use crate::{error::AppError, jobs::DynJobStore};

use super::util::map_io_error;

const FFMPEG_BIN: &str = "ffmpeg";
const PROGRESS_EPSILON: f32 = 0.005;
const MAX_PROGRESS_UPDATE_INTERVAL: Duration = Duration::from_secs(3);
const PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(10);

pub(crate) async fn run_ffmpeg(args: Vec<OsString>) -> Result<(), AppError> {
    run_ffmpeg_inner(args, None).await
}

pub(crate) async fn run_ffmpeg_with_progress(
    args: Vec<OsString>,
    config: FfmpegProgressConfig,
) -> Result<(), AppError> {
    run_ffmpeg_inner(args, Some(config)).await
}

async fn run_ffmpeg_inner(
    args: Vec<OsString>,
    progress: Option<FfmpegProgressConfig>,
) -> Result<(), AppError> {
    let printable_args: Vec<String> = args
        .iter()
        .map(|arg| arg.to_string_lossy().to_string())
        .collect();
    tracing::debug!(command = %printable_args.join(" "), "spawning ffmpeg");

    let mut command = Command::new(FFMPEG_BIN);
    command.args(&args);
    command.stderr(Stdio::piped());
    command.stdout(Stdio::null());
    command.stdin(Stdio::null());

    let mut child = command.spawn().map_err(map_io_error)?;

    let mut progress_opt = progress;
    let stderr = child.stderr.take();
    let monitor_handle = if let Some(stderr) = stderr {
        if let Some(config) = progress_opt.take() {
            Some(tokio::spawn(monitor_ffmpeg(stderr, config)))
        } else {
            Some(tokio::spawn(
                async move { drain_ffmpeg(stderr, "ffmpeg").await },
            ))
        }
    } else {
        None
    };

    let status = child.wait().await.map_err(map_io_error)?;

    if let Some(handle) = monitor_handle {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err),
            Err(join_err) => {
                return Err(AppError::transcode(format!(
                    "ffmpeg stderr task failed: {join_err}"
                )));
            }
        }
    }

    if !status.success() {
        return Err(AppError::transcode(format!(
            "ffmpeg exited with status {status}"
        )));
    }

    tracing::debug!(command = %printable_args.join(" "), "ffmpeg finished successfully");

    Ok(())
}

async fn monitor_ffmpeg(
    mut stderr: ChildStderr,
    config: FfmpegProgressConfig,
) -> Result<(), AppError> {
    let FfmpegProgressConfig {
        total_duration,
        jobs,
        job_id,
        operation,
    } = config;

    let total_seconds = total_duration.as_secs_f64();
    if total_seconds <= f64::EPSILON {
        let mut drain = Vec::new();
        stderr.read_to_end(&mut drain).await.map_err(map_io_error)?;
        if !drain.is_empty() {
            let text = String::from_utf8_lossy(&drain);
            for line in text.split('\n') {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    log_ffmpeg_line(operation, trimmed);
                }
            }
        }
        return Ok(());
    }

    let mut buffer = Vec::with_capacity(8192);
    let mut chunk = [0u8; 4096];
    let mut last_reported = 0.0f32;
    let mut last_update = Instant::now();
    let mut last_log = Instant::now();

    loop {
        let read = stderr.read(&mut chunk).await.map_err(map_io_error)?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);

        while let Some(idx) = buffer
            .iter()
            .position(|byte| *byte == b'\r' || *byte == b'\n')
        {
            let mut line_bytes: Vec<u8> = buffer.drain(..=idx).collect();
            while matches!(buffer.first(), Some(b'\r' | b'\n')) {
                buffer.drain(..1);
            }
            while matches!(line_bytes.last(), Some(b'\r' | b'\n')) {
                line_bytes.pop();
            }
            if line_bytes.is_empty() {
                continue;
            }

            let line = String::from_utf8_lossy(&line_bytes);
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            log_ffmpeg_line(operation, trimmed);
            process_ffmpeg_line(
                trimmed,
                ProgressContext {
                    jobs: &jobs,
                    job_id,
                    total_seconds,
                    last_reported: &mut last_reported,
                    last_update: &mut last_update,
                    last_log: &mut last_log,
                    operation,
                },
            )
            .await?;
        }
    }

    if !buffer.is_empty() {
        let line = String::from_utf8_lossy(&buffer);
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            log_ffmpeg_line(operation, trimmed);
            process_ffmpeg_line(
                trimmed,
                ProgressContext {
                    jobs: &jobs,
                    job_id,
                    total_seconds,
                    last_reported: &mut last_reported,
                    last_update: &mut last_update,
                    last_log: &mut last_log,
                    operation,
                },
            )
            .await?;
        }
    }

    if last_reported < 1.0 - PROGRESS_EPSILON {
        jobs.update_progress(job_id, 1.0).await?;
    }
    jobs.update_stage_eta(job_id, Some(0.0)).await?;

    Ok(())
}

async fn drain_ffmpeg(mut stderr: ChildStderr, operation: &'static str) -> Result<(), AppError> {
    let mut buffer = Vec::with_capacity(8192);
    let mut chunk = [0u8; 4096];

    loop {
        let read = stderr.read(&mut chunk).await.map_err(map_io_error)?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);

        while let Some(idx) = buffer
            .iter()
            .position(|byte| *byte == b'\r' || *byte == b'\n')
        {
            let mut line_bytes: Vec<u8> = buffer.drain(..=idx).collect();
            while matches!(buffer.first(), Some(b'\r' | b'\n')) {
                buffer.drain(..1);
            }
            while matches!(line_bytes.last(), Some(b'\r' | b'\n')) {
                line_bytes.pop();
            }
            if line_bytes.is_empty() {
                continue;
            }

            let line = String::from_utf8_lossy(&line_bytes);
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            log_ffmpeg_line(operation, trimmed);
        }
    }

    if !buffer.is_empty() {
        let line = String::from_utf8_lossy(&buffer);
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            log_ffmpeg_line(operation, trimmed);
        }
    }

    Ok(())
}

struct ProgressContext<'a> {
    jobs: &'a DynJobStore,
    job_id: Uuid,
    total_seconds: f64,
    last_reported: &'a mut f32,
    last_update: &'a mut Instant,
    last_log: &'a mut Instant,
    operation: &'static str,
}

async fn process_ffmpeg_line(line: &str, ctx: ProgressContext<'_>) -> Result<(), AppError> {
    if line.is_empty() {
        return Ok(());
    }

    tracing::debug!(operation = %ctx.operation, message = %line, "ffmpeg stderr");

    if let Some(metrics) = parse_ffmpeg_metrics(line) {
        let ratio = (metrics.time_seconds / ctx.total_seconds).clamp(0.0, 1.0) as f32;
        if ratio < *ctx.last_reported {
            return Ok(());
        }

        if let Some(speed) = metrics.speed {
            let eta_seconds = if speed > 0.0 {
                (ctx.total_seconds - metrics.time_seconds).max(0.0) / speed
            } else {
                f64::INFINITY
            };
            ctx.jobs
                .update_stage_eta(ctx.job_id, Some(eta_seconds))
                .await?;
        } else {
            ctx.jobs.update_stage_eta(ctx.job_id, None).await?;
        }

        let delta = ratio - *ctx.last_reported;
        let now = Instant::now();

        if delta >= PROGRESS_EPSILON
            || now.duration_since(*ctx.last_update) >= MAX_PROGRESS_UPDATE_INTERVAL
        {
            ctx.jobs.update_progress(ctx.job_id, ratio).await?;
            *ctx.last_reported = ratio;
            *ctx.last_update = now;
        }

        if now.duration_since(*ctx.last_log) >= PROGRESS_LOG_INTERVAL
            || (1.0 - ratio) <= PROGRESS_EPSILON
        {
            if let Some(speed) = metrics.speed {
                let eta_seconds = if speed > 0.0 {
                    (ctx.total_seconds - metrics.time_seconds).max(0.0) / speed
                } else {
                    f64::INFINITY
                };
                let eta_str = format_eta(eta_seconds);
                tracing::info!(
                    operation = %ctx.operation,
                    progress = ratio,
                    speed = speed,
                    eta = %eta_str,
                    "ffmpeg progress"
                );
            } else {
                tracing::info!(
                    operation = %ctx.operation,
                    progress = ratio,
                    "ffmpeg progress"
                );
            }
            *ctx.last_log = now;
        }
    }

    Ok(())
}

fn parse_ffmpeg_metrics(line: &str) -> Option<FfmpegMetrics> {
    let time_value = extract_progress_token(line, "time=", |c| matches!(c, '0'..='9' | ':' | '.'));
    let time_seconds = time_value.and_then(parse_timecode)?;

    let speed = extract_progress_token(line, "speed=", |c| {
        matches!(c, '0'..='9' | '.' | 'x' | 'X' | 'N' | 'A' | '/')
    })
    .and_then(parse_speed);

    Some(FfmpegMetrics {
        time_seconds,
        speed,
    })
}

fn extract_progress_token<'a, F>(text: &'a str, needle: &str, allowed: F) -> Option<&'a str>
where
    F: Fn(char) -> bool,
{
    let start = text.find(needle)? + needle.len();
    let remainder = &text[start..];
    let end = remainder.find(|ch| !allowed(ch)).unwrap_or(remainder.len());
    let value = &remainder[..end];
    if value.is_empty() { None } else { Some(value) }
}

fn parse_timecode(value: &str) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut parts = trimmed.split(':');
    let hours = parts.next()?.parse::<f64>().ok()?;
    let minutes = parts.next()?.parse::<f64>().ok()?;
    let seconds = parts.next()?.parse::<f64>().ok()?;

    Some(hours * 3600.0 + minutes * 60.0 + seconds)
}

fn parse_speed(value: &str) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("N/A") {
        return None;
    }

    let numeric = trimmed.trim_end_matches(['x', 'X']);
    let parsed = numeric.parse::<f64>().ok()?;
    if parsed.is_finite() && parsed > 0.0 {
        Some(parsed)
    } else {
        None
    }
}

fn format_eta(seconds: f64) -> String {
    if !seconds.is_finite() {
        return "unknown".to_string();
    }

    let seconds = seconds.max(0.0);
    let total = seconds.round() as u64;

    if total >= 3600 {
        let hours = total / 3600;
        let minutes = (total % 3600) / 60;
        let secs = total % 60;
        format!("{hours}h {minutes:02}m {secs:02}s")
    } else if total >= 60 {
        let minutes = total / 60;
        let secs = total % 60;
        format!("{minutes}m {secs:02}s")
    } else {
        format!("{total}s")
    }
}

fn log_ffmpeg_line(operation: &str, line: &str) {
    let lowered = line.to_ascii_lowercase();

    if lowered.contains("error") || lowered.contains("failed") || lowered.contains("fatal") {
        tracing::error!(operation = %operation, message = %line, "ffmpeg message");
        return;
    }

    if lowered.contains("warning") || lowered.contains("deprecated") {
        tracing::warn!(operation = %operation, message = %line, "ffmpeg message");
        return;
    }

    if lowered.contains("speed=")
        || lowered.contains("muxing overhead")
        || lowered.contains("kb/s")
        || lowered.contains("encoded")
    {
        tracing::debug!(operation = %operation, message = %line, "ffmpeg message");
        return;
    }

    if lowered.contains("opening '") || lowered.contains("closing '") {
        return;
    }

    if lowered.starts_with("[dash") || lowered.starts_with("[hls") {
        return;
    }

    if lowered.starts_with("input #")
        || lowered.starts_with("output #")
        || lowered.contains("stream #")
        || lowered.contains("encoder")
    {
        tracing::debug!(operation = %operation, message = %line, "ffmpeg message");
    }
}

pub(crate) struct FfmpegProgressConfig {
    pub(crate) total_duration: Duration,
    pub(crate) jobs: DynJobStore,
    pub(crate) job_id: Uuid,
    pub(crate) operation: &'static str,
}

struct FfmpegMetrics {
    time_seconds: f64,
    speed: Option<f64>,
}

// Tests for ffmpeg helpers live under `tests/`.
