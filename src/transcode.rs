use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};

use hlskit::{
    models::hls_video::HlsVideo,
    models::hls_video_processing_settings::{
        FfmpegVideoProcessingPreset, HlsVideoAudioBitrate, HlsVideoAudioCodec,
        HlsVideoProcessingSettings,
    },
    process_video_from_path,
};
use tokio::{
    fs,
    io::AsyncReadExt,
    process::{ChildStderr, Command},
};
use uuid::Uuid;

use crate::{
    error::AppError,
    jobs::DynJobStore,
    storage::{Storage, ensure_dir, ensure_parent},
};

const FFMPEG_BIN: &str = "ffmpeg";
const FFPROBE_BIN: &str = "ffprobe";

const GOP_LENGTH: &str = "120";
const SEGMENT_SECONDS: &str = "4";
const DEFAULT_DIMENSIONS: (u32, u32) = (1920, 1080);

#[derive(Clone, Copy, Debug)]
pub struct EncodeParams {
    pub crf: u8,
    pub cpu_used: u8,
}

impl EncodeParams {
    pub fn sanitized(self) -> Self {
        Self {
            crf: self.crf.clamp(0, 63),
            cpu_used: self.cpu_used.clamp(0, 8),
        }
    }
}

impl Default for EncodeParams {
    fn default() -> Self {
        Self {
            crf: 24,
            cpu_used: 4,
        }
    }
}

#[derive(Clone, Copy)]
struct Rendition {
    name: &'static str,
    height: u32,
    video_bitrate: &'static str,
    maxrate: &'static str,
    bufsize: &'static str,
    audio_bitrate: &'static str,
}

const RENDITIONS: &[Rendition] = &[
    Rendition {
        name: "2160p",
        height: 2160,
        video_bitrate: "8000k",
        maxrate: "12000k",
        bufsize: "16000k",
        audio_bitrate: "192k",
    },
    Rendition {
        name: "1080p",
        height: 1080,
        video_bitrate: "4000k",
        maxrate: "6000k",
        bufsize: "8000k",
        audio_bitrate: "160k",
    },
    Rendition {
        name: "720p",
        height: 720,
        video_bitrate: "2200k",
        maxrate: "3300k",
        bufsize: "4400k",
        audio_bitrate: "128k",
    },
];

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

    if duration.is_none() {
        jobs.update_progress(*id, 1.0).await?;
    }

    if let Err(err) = fs::remove_file(input).await {
        if err.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(path = %input.display(), ?err, "failed to remove temporary input file");
        }
    }

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

    let master = storage.hls_dir(id).join("master.m3u8");
    if master.exists() {
        return Ok(());
    }

    let dimensions = probe_dimensions(&source)
        .await
        .unwrap_or(DEFAULT_DIMENSIONS);
    generate_hls(storage, id, &source, dimensions).await
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

    let has_audio = match probe_has_audio(&source).await {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(
                ?err,
                "failed to probe audio stream; defaulting to video-only DASH"
            );
            false
        }
    };
    generate_dash(storage, id, &source, has_audio).await
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

    let mut args = vec![os("-y"), os("-i"), os_path(input)];

    args.extend([
        os("-c:v"),
        os("libaom-av1"),
        os("-crf"),
        os(params.crf.to_string()),
        os("-b:v"),
        os("0"),
        os("-g"),
        os(GOP_LENGTH),
        os("-cpu-used"),
        os(params.cpu_used.to_string()),
        os("-pix_fmt"),
        os("yuv420p"),
    ]);

    if has_audio {
        args.extend([
            os("-c:a"),
            os("libopus"),
            os("-b:a"),
            os(RENDITIONS[0].audio_bitrate),
        ]);
    } else {
        args.push(os("-an"));
    }

    args.push(os(output));

    let result = if let Some(total_duration) = duration {
        run_ffmpeg_with_progress(
            args,
            FfmpegProgressConfig {
                total_duration,
                jobs: jobs.clone(),
                job_id: *id,
                operation: "encode_download",
            },
        )
        .await
    } else {
        run_ffmpeg(args).await
    };

    if result.is_ok() {
        jobs.update_progress(*id, 1.0).await?;
    }

    result
}

async fn generate_hls(
    storage: &Storage,
    id: &Uuid,
    input: &Path,
    source_dimensions: (u32, u32),
) -> Result<(), AppError> {
    let (hlskit_input, is_temporary) = prepare_hlskit_input(storage, id, input).await?;
    let profiles = build_hls_profiles(source_dimensions);
    let settings: Vec<HlsVideoProcessingSettings> = profiles
        .iter()
        .map(|profile| profile.settings.clone())
        .collect();

    let input_str = hlskit_input
        .to_str()
        .ok_or_else(|| AppError::transcode("hlskit input path contains invalid UTF-8"))?;

    let video = process_video_from_path(input_str, settings)
        .await
        .map_err(|err| AppError::transcode(format!("hlskit processing failed: {err}")))?;

    write_hls_outputs(storage, id, &profiles, &video).await?;

    if is_temporary {
        if let Err(err) = fs::remove_file(&hlskit_input).await {
            if err.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(path = %hlskit_input.display(), ?err, "failed to remove temporary hlskit input");
            }
        }
    }

    Ok(())
}

struct HlsProfile {
    name: &'static str,
    settings: HlsVideoProcessingSettings,
}

async fn write_hls_outputs(
    storage: &Storage,
    id: &Uuid,
    profiles: &[HlsProfile],
    video: &HlsVideo,
) -> Result<(), AppError> {
    if video.resolutions.len() != profiles.len() {
        return Err(AppError::transcode(format!(
            "hlskit returned {} renditions but {} were requested",
            video.resolutions.len(),
            profiles.len()
        )));
    }

    let hls_dir = storage.hls_dir(id);
    ensure_dir(&hls_dir).await?;

    let mut master_playlist = String::from_utf8(video.master_m3u8_data.clone())
        .map_err(|_| AppError::transcode("hlskit master playlist is not valid UTF-8"))?;

    for (index, profile) in profiles.iter().enumerate() {
        let source_name = format!("playlist_{index}.m3u8");
        let replacement = format!("{}/index.m3u8", profile.name);
        master_playlist = master_playlist.replace(&source_name, &replacement);

        let rendition_dir = hls_dir.join(profile.name);
        if rendition_dir.exists() {
            if let Err(err) = fs::remove_dir_all(&rendition_dir).await {
                if err.kind() != std::io::ErrorKind::NotFound {
                    return Err(err.into());
                }
            }
        }
        ensure_dir(&rendition_dir).await?;

        let resolution = video
            .resolutions
            .get(index)
            .ok_or_else(|| AppError::transcode("missing HLS resolution payload"))?;

        fs::write(rendition_dir.join("index.m3u8"), &resolution.playlist_data).await?;
        for segment in &resolution.segments {
            fs::write(
                rendition_dir.join(&segment.segment_name),
                &segment.segment_data,
            )
            .await?;
        }
    }

    fs::write(hls_dir.join("master.m3u8"), master_playlist.into_bytes()).await?;

    Ok(())
}

async fn prepare_hlskit_input(
    storage: &Storage,
    id: &Uuid,
    input: &Path,
) -> Result<(PathBuf, bool), AppError> {
    const SUPPORTED: [&str; 4] = ["mp4", "mov", "mkv", "avi"];

    let requires_transcode = input
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| !SUPPORTED.contains(&ext.to_ascii_lowercase().as_str()))
        .unwrap_or(true);

    if !requires_transcode {
        return Ok((input.to_path_buf(), false));
    }

    let output = storage
        .tmp_dir()
        .join(format!("{}.hlskit.mp4", id.simple()));
    ensure_parent(&output).await?;
    if output.exists() {
        fs::remove_file(&output).await.ok();
    }

    let mut args = vec![os("-y"), os("-i"), os_path(input)];
    args.extend([
        os("-c:v"),
        os("libx264"),
        os("-preset"),
        os("medium"),
        os("-crf"),
        os("23"),
        os("-c:a"),
        os("aac"),
        os("-b:a"),
        os("160k"),
    ]);
    args.push(os_path(&output));

    run_ffmpeg(args).await?;

    Ok((output, true))
}

fn build_hls_profiles(source_dimensions: (u32, u32)) -> Vec<HlsProfile> {
    RENDITIONS
        .iter()
        .map(|rendition| {
            let dims = scaled_dimensions(source_dimensions, rendition.height);
            let settings = HlsVideoProcessingSettings::new(
                dims,
                28,
                Some(HlsVideoAudioCodec::Aac),
                Some(map_audio_bitrate(rendition.audio_bitrate)),
                FfmpegVideoProcessingPreset::Medium,
            );
            HlsProfile {
                name: rendition.name,
                settings,
            }
        })
        .collect()
}

fn scaled_dimensions(source: (u32, u32), target_height: u32) -> (i32, i32) {
    let (source_width, source_height) = source;
    if source_width == 0 || source_height == 0 {
        let width = (target_height as f64 * 16.0 / 9.0).round() as i32;
        return (ensure_even(width.max(2)), target_height as i32);
    }

    let ratio = source_width as f64 / source_height as f64;
    let mut width = (ratio * target_height as f64).round() as i32;
    if width <= 0 {
        width = (target_height as f64 * 16.0 / 9.0).round() as i32;
    }
    if width % 2 != 0 {
        width += 1;
    }
    (width.max(2), target_height as i32)
}

fn map_audio_bitrate(value: &str) -> HlsVideoAudioBitrate {
    match value {
        "192k" => HlsVideoAudioBitrate::High,
        "160k" => HlsVideoAudioBitrate::High,
        "128k" => HlsVideoAudioBitrate::Medium,
        _ => HlsVideoAudioBitrate::Low,
    }
}

fn ensure_even(value: i32) -> i32 {
    if value % 2 == 0 { value } else { value + 1 }
}

async fn generate_dash(
    storage: &Storage,
    id: &Uuid,
    input: &Path,
    has_audio: bool,
) -> Result<(), AppError> {
    let dash_dir = storage.dash_dir(id);
    let manifest = dash_dir.join("manifest.mpd");
    ensure_parent(&manifest).await?;

    let filter_complex = build_filter_complex();

    let mut args = vec![
        os("-y"),
        os("-i"),
        os_path(input),
        os("-filter_complex"),
        os(filter_complex),
    ];

    for (idx, rendition) in RENDITIONS.iter().enumerate() {
        let video_label = format!("[v{idx}out]");
        args.extend([os("-map"), os(video_label)]);

        args.extend([
            os(format!("-c:v:{idx}")),
            os("libaom-av1"),
            os(format!("-b:v:{idx}")),
            os(rendition.video_bitrate),
            os(format!("-maxrate:v:{idx}")),
            os(rendition.maxrate),
            os(format!("-bufsize:v:{idx}")),
            os(rendition.bufsize),
            os(format!("-g:v:{idx}")),
            os(GOP_LENGTH),
            os(format!("-keyint_min:v:{idx}")),
            os(GOP_LENGTH),
            os(format!("-cpu-used:v:{idx}")),
            os("6"),
            os(format!("-pix_fmt:v:{idx}")),
            os("yuv420p"),
        ]);

        if has_audio {
            args.extend([os("-map"), os("a:0")]);
            args.extend([
                os(format!("-c:a:{idx}")),
                os("libopus"),
                os(format!("-b:a:{idx}")),
                os(rendition.audio_bitrate),
            ]);
        }
    }

    args.extend([
        os("-f"),
        os("dash"),
        os("-seg_duration"),
        os(SEGMENT_SECONDS),
        os("-use_template"),
        os("1"),
        os("-use_timeline"),
        os("1"),
        os("-init_seg_name"),
        os("init_$RepresentationID$.m4s"),
        os("-media_seg_name"),
        os("chunk_$RepresentationID$_$Number$.m4s"),
        os("-dash_segment_type"),
        os("mp4"),
    ]);

    if has_audio {
        args.extend([os("-adaptation_sets"), os("id=0,streams=v id=1,streams=a")]);
    } else {
        args.extend([os("-adaptation_sets"), os("id=0,streams=v")]);
    }

    args.push(os(manifest));

    run_ffmpeg(args).await
}

fn build_filter_complex() -> String {
    let mut parts = Vec::new();
    let mut split = format!("[0:v]split={}", RENDITIONS.len());
    for idx in 0..RENDITIONS.len() {
        split.push_str(&format!("[v{idx}]"));
    }
    parts.push(split);

    for (idx, rendition) in RENDITIONS.iter().enumerate() {
        parts.push(format!("[v{idx}]scale=-2:{}[v{idx}out]", rendition.height));
    }

    parts.join(";")
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

async fn probe_dimensions(input: &Path) -> Result<(u32, u32), AppError> {
    let output = Command::new(FFPROBE_BIN)
        .arg("-v")
        .arg("error")
        .arg("-select_streams")
        .arg("v:0")
        .arg("-show_entries")
        .arg("stream=width,height")
        .arg("-of")
        .arg("csv=p=0:s=x")
        .arg(input)
        .output()
        .await
        .map_err(map_io_error)?;

    if !output.status.success() {
        tracing::warn!(
            status = %output.status,
            "ffprobe did not return video dimensions, using defaults"
        );
        return Ok(DEFAULT_DIMENSIONS);
    }

    let text = String::from_utf8_lossy(&output.stdout);
    let trimmed = text.trim();
    if trimmed.is_empty() {
        tracing::warn!("ffprobe returned empty dimensions, using defaults");
        return Ok(DEFAULT_DIMENSIONS);
    }

    let mut parts = trimmed.split('x');
    let width = parts
        .next()
        .and_then(|part| part.parse::<u32>().ok())
        .filter(|width| *width > 0)
        .unwrap_or(DEFAULT_DIMENSIONS.0);
    let height = parts
        .next()
        .and_then(|part| part.parse::<u32>().ok())
        .filter(|height| *height > 0)
        .unwrap_or(DEFAULT_DIMENSIONS.1);

    Ok((width, height))
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

    if let Some(value) = duration_str {
        if let Ok(seconds) = value.parse::<f64>() {
            if seconds.is_finite() && seconds > 0.0 {
                return Ok(Some(Duration::from_secs_f64(seconds)));
            }
        }
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

    if progress.is_some() {
        command.stderr(Stdio::piped());
    }

    let mut child = command.spawn().map_err(map_io_error)?;
    let mut progress_handle = None;

    if let Some(config) = progress {
        if let Some(stderr) = child.stderr.take() {
            progress_handle = Some(tokio::spawn(monitor_ffmpeg(stderr, config)));
        }
    }

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

const PROGRESS_EPSILON: f32 = 0.005;
const MAX_PROGRESS_UPDATE_INTERVAL: Duration = Duration::from_secs(3);
const PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(10);

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
                &jobs,
                job_id,
                total_seconds,
                &mut last_reported,
                &mut last_update,
                &mut last_log,
                operation,
            )
            .await?;
        }
    }

    if !buffer.is_empty() {
        let line = String::from_utf8_lossy(&buffer);
        process_ffmpeg_line(
            line.trim(),
            &jobs,
            job_id,
            total_seconds,
            &mut last_reported,
            &mut last_update,
            &mut last_log,
            operation,
        )
        .await?;
    }

    if last_reported < 1.0 - PROGRESS_EPSILON {
        jobs.update_progress(job_id, 1.0).await?;
    }

    Ok(())
}

async fn process_ffmpeg_line(
    line: &str,
    jobs: &DynJobStore,
    job_id: Uuid,
    total_seconds: f64,
    last_reported: &mut f32,
    last_update: &mut Instant,
    last_log: &mut Instant,
    operation: &'static str,
) -> Result<(), AppError> {
    if line.is_empty() {
        return Ok(());
    }

    tracing::debug!(operation = %operation, message = %line, "ffmpeg stderr");

    if let Some(metrics) = parse_ffmpeg_metrics(line) {
        let ratio = (metrics.time_seconds / total_seconds).clamp(0.0, 1.0) as f32;
        if ratio < *last_reported {
            return Ok(());
        }

        let delta = ratio - *last_reported;
        let now = Instant::now();

        if delta >= PROGRESS_EPSILON
            || now.duration_since(*last_update) >= MAX_PROGRESS_UPDATE_INTERVAL
        {
            jobs.update_progress(job_id, ratio).await?;
            *last_reported = ratio;
            *last_update = now;
        }

        if now.duration_since(*last_log) >= PROGRESS_LOG_INTERVAL
            || (1.0 - ratio) <= PROGRESS_EPSILON
        {
            if let Some(speed) = metrics.speed {
                let eta_seconds = if speed > 0.0 {
                    (total_seconds - metrics.time_seconds).max(0.0) / speed
                } else {
                    f64::INFINITY
                };
                let eta_str = format_eta(eta_seconds);
                tracing::info!(
                    operation = %operation,
                    progress_percent = (ratio * 100.0).clamp(0.0, 100.0),
                    speed = speed,
                    eta = %eta_str,
                    "ffmpeg progress"
                );
            } else {
                tracing::info!(
                    operation = %operation,
                    progress_percent = (ratio * 100.0).clamp(0.0, 100.0),
                    "ffmpeg progress"
                );
            }
            *last_log = now;
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
    let end = remainder
        .find(|ch| !allowed(ch))
        .unwrap_or_else(|| remainder.len());
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

    let numeric = trimmed.trim_end_matches(|c| c == 'x' || c == 'X');
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
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/unit/transcode_unit.rs"
    ));
}
