use std::{
    env,
    ffi::OsString,
    path::Path,
    process::Stdio,
    time::{Duration, Instant},
};

use tokio::{
    fs,
    io::AsyncReadExt,
    process::{ChildStderr, Command},
};
use uuid::Uuid;

use crate::{
    error::AppError,
    jobs::{DynJobStore, JobStage},
    storage::{Storage, ensure_dir, ensure_parent},
};

const FFMPEG_BIN: &str = "ffmpeg";
const FFPROBE_BIN: &str = "ffprobe";
const SEGMENT_SECONDS: &str = "4";
const PROGRESS_EPSILON: f32 = 0.005;
const MAX_PROGRESS_UPDATE_INTERVAL: Duration = Duration::from_secs(3);
const PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug)]
pub struct EncodeParams {
    pub crf: u8,
    pub cpu_used: u8,
    encoder: Option<EncoderKind>,
}

impl EncodeParams {
    pub fn sanitized(self) -> Self {
        Self {
            crf: self.crf.clamp(0, 63),
            cpu_used: self.cpu_used.clamp(0, 8),
            encoder: self.encoder,
        }
    }

    fn preferred_encoder(&self) -> Option<EncoderKind> {
        self.encoder
    }
}

impl Default for EncodeParams {
    fn default() -> Self {
        Self {
            crf: 24,
            cpu_used: 4,
            encoder: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum EncoderKind {
    VideoToolboxAv1,
    NvencAv1,
    QsvAv1,
    VaapiAv1,
    SoftwareAv1,
}

pub async fn process_video(
    storage: &Storage,
    jobs: &DynJobStore,
    id: &Uuid,
    input: &Path,
    encode: Option<EncodeParams>,
) -> Result<(), AppError> {
    storage.prepare_video_dirs(id, &[]).await?;

    let download_path = storage.download_path(id);
    ensure_parent(&download_path).await?;

    let params = encode.unwrap_or_default().sanitized();
    let has_audio = probe_has_audio(input).await?;
    let duration = match probe_duration(input).await {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(
                path = %input.display(),
                ?err,
                "failed to determine source duration; progress estimates will be coarse"
            );
            None
        }
    };

    encode_download(jobs, id, &download_path, input, has_audio, duration, params).await?;

    match fs::remove_file(input).await {
        Err(err) if err.kind() != std::io::ErrorKind::NotFound => {
            tracing::warn!(path = %input.display(), ?err, "failed to remove temporary input file");
        }
        _ => {}
    }

    jobs.update_progress(*id, 0.95).await?;
    jobs.update_stage(*id, JobStage::Finalizing).await?;

    let storage_for_hls = storage.clone();
    let storage_for_dash = storage.clone();
    let id_for_hls = *id;
    let id_for_dash = *id;
    let download_for_hls = download_path.clone();
    let download_for_dash = download_path.clone();

    tokio::try_join!(
        async move {
            generate_hls_stream(&storage_for_hls, &id_for_hls, &download_for_hls, has_audio).await
        },
        async move {
            generate_dash_stream(
                &storage_for_dash,
                &id_for_dash,
                &download_for_dash,
                has_audio,
            )
            .await
        },
    )?;

    jobs.update_progress(*id, 1.0).await?;
    jobs.update_stage_eta(*id, Some(0.0)).await?;

    Ok(())
}

pub async fn ensure_hls_ready(storage: &Storage, id: &Uuid) -> Result<(), AppError> {
    let source = storage.download_path(id);
    if !source.exists() {
        return Err(AppError::not_found(format!(
            "source video missing for HLS generation: {}",
            source.display()
        )));
    }

    let index = storage.hls_dir(id).join("index.m3u8");
    if index.exists() {
        return Ok(());
    }

    let has_audio = probe_has_audio(&source).await.unwrap_or(false);
    generate_hls_stream(storage, id, &source, has_audio).await
}

pub async fn ensure_dash_ready(storage: &Storage, id: &Uuid) -> Result<(), AppError> {
    let source = storage.download_path(id);
    if !source.exists() {
        return Err(AppError::not_found(format!(
            "source video missing for DASH generation: {}",
            source.display()
        )));
    }

    let manifest = storage.dash_dir(id).join("manifest.mpd");
    if manifest.exists() {
        return Ok(());
    }

    let has_audio = probe_has_audio(&source).await.unwrap_or(false);
    generate_dash_stream(storage, id, &source, has_audio).await
}

async fn encode_download(
    jobs: &DynJobStore,
    id: &Uuid,
    output: &Path,
    input: &Path,
    has_audio: bool,
    duration: Option<Duration>,
    params: EncodeParams,
) -> Result<(), AppError> {
    ensure_parent(output).await?;

    let candidates = encoder_candidates(params.preferred_encoder());
    let mut last_error: Option<AppError> = None;

    for encoder in candidates {
        let mut args = base_encode_args(input);
        apply_encoder_args(&mut args, encoder, params);
        apply_audio_args(&mut args, has_audio);
        args.push(os_path(output));

        tracing::info!(encoder = ?encoder, path = %output.display(), "starting encode" );

        let result = if let Some(total) = duration {
            run_ffmpeg_with_progress(
                args,
                FfmpegProgressConfig {
                    total_duration: total,
                    jobs: jobs.clone(),
                    job_id: *id,
                    operation: "encode_download",
                },
            )
            .await
        } else {
            run_ffmpeg(args).await
        };

        match result {
            Ok(()) => {
                jobs.update_stage_eta(*id, Some(0.0)).await?;
                return Ok(());
            }
            Err(err) => {
                tracing::warn!(
                    encoder = ?encoder,
                    error = %err,
                    "ffmpeg encode failed, attempting fallback"
                );
                last_error = Some(err);
                continue;
            }
        }
    }

    Err(last_error.unwrap_or_else(|| AppError::transcode("encode pipeline failed")))
}

fn base_encode_args(input: &Path) -> Vec<OsString> {
    vec![os("-y"), os("-i"), os_path(input)]
}

fn apply_encoder_args(args: &mut Vec<OsString>, encoder: EncoderKind, params: EncodeParams) {
    match encoder {
        EncoderKind::VideoToolboxAv1 => {
            args.extend([
                os("-c:v"),
                os("av1_videotoolbox"),
                os("-q:v"),
                os(params.crf.to_string()),
                os("-pix_fmt"),
                os("yuv420p"),
            ]);
        }
        EncoderKind::NvencAv1 => {
            let cq = params.crf.min(51);
            args.extend([
                os("-hwaccel"),
                os("cuda"),
                os("-hwaccel_output_format"),
                os("cuda"),
                os("-c:v"),
                os("av1_nvenc"),
                os("-preset"),
                os("p5"),
                os("-cq"),
                os(cq.to_string()),
                os("-pix_fmt"),
                os("yuv420p"),
            ]);
        }
        EncoderKind::QsvAv1 => {
            args.extend([
                os("-hwaccel"),
                os("qsv"),
                os("-c:v"),
                os("av1_qsv"),
                os("-global_quality"),
                os(params.crf.to_string()),
                os("-pix_fmt"),
                os("yuv420p"),
            ]);
        }
        EncoderKind::VaapiAv1 => {
            let device =
                env::var("VIDEO_VAAPI_DEVICE").unwrap_or_else(|_| "/dev/dri/renderD128".into());
            args.extend([
                os("-hwaccel"),
                os("vaapi"),
                os("-hwaccel_device"),
                os(device),
                os("-hwaccel_output_format"),
                os("vaapi"),
                os("-vf"),
                os("format=nv12,hwupload"),
                os("-c:v"),
                os("av1_vaapi"),
                os("-qp"),
                os(params.crf.to_string()),
            ]);
        }
        EncoderKind::SoftwareAv1 => {
            args.extend([
                os("-c:v"),
                os("libaom-av1"),
                os("-crf"),
                os(params.crf.to_string()),
                os("-b:v"),
                os("0"),
                os("-g"),
                os("120"),
                os("-cpu-used"),
                os(params.cpu_used.to_string()),
                os("-pix_fmt"),
                os("yuv420p"),
            ]);
        }
    }
}

fn apply_audio_args(args: &mut Vec<OsString>, has_audio: bool) {
    if has_audio {
        args.extend([os("-c:a"), os("libopus"), os("-b:a"), os("192k")]);
    } else {
        args.push(os("-an"));
    }
}

async fn generate_hls_stream(
    storage: &Storage,
    id: &Uuid,
    source: &Path,
    has_audio: bool,
) -> Result<(), AppError> {
    let hls_dir = storage.hls_dir(id);
    ensure_dir(&hls_dir).await?;

    let segment_pattern = hls_dir.join("segment_%05d.m4s");
    let mut args = vec![os("-y"), os("-i"), os_path(source), os("-c:v"), os("copy")];

    if has_audio {
        args.extend([os("-c:a"), os("copy")]);
    } else {
        args.push(os("-an"));
    }

    args.extend([
        os("-f"),
        os("hls"),
        os("-hls_time"),
        os(SEGMENT_SECONDS),
        os("-hls_playlist_type"),
        os("event"),
        os("-hls_flags"),
        os("independent_segments+append_list+omit_endlist"),
        os("-hls_segment_type"),
        os("fmp4"),
        os("-master_pl_name"),
        os("master.m3u8"),
        os("-hls_segment_filename"),
        os_path(&segment_pattern),
        os_path(&hls_dir.join("index.m3u8")),
    ]);

    run_ffmpeg(args).await
}

async fn generate_dash_stream(
    storage: &Storage,
    id: &Uuid,
    source: &Path,
    has_audio: bool,
) -> Result<(), AppError> {
    let dash_dir = storage.dash_dir(id);
    let manifest = dash_dir.join("manifest.mpd");
    ensure_parent(&manifest).await?;

    let mut args = vec![
        os("-y"),
        os("-i"),
        os_path(source),
        os("-map"),
        os("0"),
        os("-c:v"),
        os("copy"),
    ];

    if has_audio {
        args.extend([os("-c:a"), os("copy")]);
    } else {
        args.push(os("-an"));
    }

    let adaptation_sets = if has_audio {
        "id=0,streams=v id=1,streams=a"
    } else {
        "id=0,streams=v"
    };

    args.extend([
        os("-f"),
        os("dash"),
        os("-seg_duration"),
        os(SEGMENT_SECONDS),
        os("-use_template"),
        os("1"),
        os("-use_timeline"),
        os("1"),
        os("-streaming"),
        os("1"),
        os("-remove_at_exit"),
        os("0"),
        os("-adaptation_sets"),
        os(adaptation_sets),
        os("-init_seg_name"),
        os("init_$RepresentationID$.m4s"),
        os("-media_seg_name"),
        os("chunk_$RepresentationID$_$Number$.m4s"),
        os_path(&manifest),
    ]);

    run_ffmpeg(args).await
}

fn encoder_from_env() -> Option<EncoderKind> {
    env::var("VIDEO_SERVER_ENCODER").ok().and_then(|value| {
        match value.to_ascii_lowercase().as_str() {
            "videotoolbox" | "vt" => Some(EncoderKind::VideoToolboxAv1),
            "nvenc" | "cuda" => Some(EncoderKind::NvencAv1),
            "qsv" | "quicksync" => Some(EncoderKind::QsvAv1),
            "vaapi" => Some(EncoderKind::VaapiAv1),
            "software" | "cpu" => Some(EncoderKind::SoftwareAv1),
            _ => None,
        }
    })
}

fn encoder_candidates(explicit: Option<EncoderKind>) -> Vec<EncoderKind> {
    let mut order = Vec::new();
    if let Some(kind) = explicit.or_else(encoder_from_env) {
        order.push(kind);
    } else {
        #[cfg(target_os = "macos")]
        {
            order.push(EncoderKind::VideoToolboxAv1);
        }
        #[cfg(target_os = "windows")]
        {
            order.push(EncoderKind::NvencAv1);
            order.push(EncoderKind::QsvAv1);
        }
        #[cfg(target_os = "linux")]
        {
            order.push(EncoderKind::VaapiAv1);
            order.push(EncoderKind::NvencAv1);
        }
    }
    order.push(EncoderKind::SoftwareAv1);
    order.sort_unstable();
    order.dedup();
    order
}

#[derive(Clone)]
struct FfmpegProgressConfig {
    total_duration: Duration,
    jobs: DynJobStore,
    job_id: Uuid,
    operation: &'static str,
}

struct FfmpegMetrics {
    time_seconds: f64,
    speed: Option<f64>,
}

async fn probe_has_audio(input: &Path) -> Result<bool, AppError> {
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

async fn probe_duration(input: &Path) -> Result<Option<Duration>, AppError> {
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

async fn run_ffmpeg(args: Vec<OsString>) -> Result<(), AppError> {
    run_ffmpeg_inner(args, None).await
}

async fn run_ffmpeg_with_progress(
    args: Vec<OsString>,
    config: FfmpegProgressConfig,
) -> Result<(), AppError> {
    run_ffmpeg_inner(args, Some(config)).await
}

async fn run_ffmpeg_inner(
    args: Vec<OsString>,
    progress: Option<FfmpegProgressConfig>,
) -> Result<(), AppError> {
    let mut command = Command::new(FFMPEG_BIN);
    command.args(&args);

    let mut progress_opt = progress;
    if progress_opt.is_some() {
        command.stderr(Stdio::piped());
    }

    let mut child = command.spawn().map_err(map_io_error)?;
    let progress_handle = progress_opt.take().and_then(|config| {
        child
            .stderr
            .take()
            .map(|stderr| tokio::spawn(monitor_ffmpeg(stderr, config)))
    });

    let status = child.wait().await.map_err(map_io_error)?;

    if let Some(handle) = progress_handle {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err),
            Err(join_err) => {
                return Err(AppError::transcode(format!(
                    "ffmpeg progress task failed: {join_err}"
                )));
            }
        }
    }

    if !status.success() {
        return Err(AppError::transcode(format!(
            "ffmpeg exited with status {status}"
        )));
    }

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
                    tracing::debug!(
                        operation = %operation,
                        message = %trimmed,
                        "ffmpeg stderr"
                    );
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
            process_ffmpeg_line(
                line.trim(),
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
        process_ffmpeg_line(
            line.trim(),
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

    if last_reported < 1.0 - PROGRESS_EPSILON {
        jobs.update_progress(job_id, 1.0).await?;
    }
    jobs.update_stage_eta(job_id, Some(0.0)).await?;

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

fn map_io_error(err: std::io::Error) -> AppError {
    match err.kind() {
        std::io::ErrorKind::NotFound => {
            AppError::dependency("required media tooling not found on PATH")
        }
        _ => AppError::Transcode(err.to_string()),
    }
}

fn os<S: Into<OsString>>(value: S) -> OsString {
    value.into()
}

fn os_path(path: &Path) -> OsString {
    path.as_os_str().to_os_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_params_sanitized_clamps_values() {
        let params = EncodeParams {
            crf: 200,
            cpu_used: 99,
            encoder: None,
        }
        .sanitized();
        assert_eq!(params.crf, 63);
        assert_eq!(params.cpu_used, 8);

        let params = EncodeParams {
            crf: 0,
            cpu_used: 0,
            encoder: None,
        }
        .sanitized();
        assert_eq!(params.crf, 0);
        assert_eq!(params.cpu_used, 0);
    }

    #[test]
    fn encoder_candidates_include_software() {
        let list = encoder_candidates(None);
        assert!(list.contains(&EncoderKind::SoftwareAv1));
    }

    #[test]
    fn encoder_candidates_respect_explicit() {
        let list = encoder_candidates(Some(EncoderKind::SoftwareAv1));
        assert_eq!(list.first(), Some(&EncoderKind::SoftwareAv1));
    }

    #[test]
    fn parse_speed_handles_cases() {
        assert_eq!(parse_speed("2.5x"), Some(2.5));
        assert!(parse_speed("N/A").is_none());
    }
}
