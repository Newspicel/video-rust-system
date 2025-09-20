use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

use hlskit::{
    models::hls_video::HlsVideo,
    models::hls_video_processing_settings::{
        FfmpegVideoProcessingPreset, HlsVideoAudioBitrate, HlsVideoAudioCodec,
        HlsVideoProcessingSettings,
    },
    process_video_from_path,
};
use tokio::{fs, process::Command};
use uuid::Uuid;

use crate::{
    error::AppError,
    jobs::{DynJobStore, JobStage},
    storage::{Storage, ensure_dir, ensure_parent},
};

const FFMPEG_BIN: &str = "ffmpeg";
const FFPROBE_BIN: &str = "ffprobe";

const GOP_LENGTH: &str = "120";
const SEGMENT_SECONDS: &str = "4";
const DEFAULT_DIMENSIONS: (u32, u32) = (1920, 1080);

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
) -> Result<(), AppError> {
    let rendition_names: Vec<&'static str> = RENDITIONS.iter().map(|r| r.name).collect();
    storage.prepare_video_dirs(id, &rendition_names).await?;

    let download_path = storage.download_path(id);
    ensure_parent(&download_path).await?;

    let has_audio = probe_has_audio(input).await?;
    let source_dimensions = probe_dimensions(input).await.unwrap_or(DEFAULT_DIMENSIONS);

    jobs.update_stage(*id, JobStage::Transcoding).await?;
    jobs.update_progress(*id, 0.45).await?;

    encode_download(&download_path, input, has_audio).await?;
    jobs.update_progress(*id, 0.6).await?;

    generate_hls(storage, id, input, source_dimensions).await?;
    jobs.update_progress(*id, 0.8).await?;

    generate_dash(storage, id, input, has_audio).await?;
    jobs.update_stage(*id, JobStage::Finalizing).await?;
    jobs.update_progress(*id, 0.95).await?;

    if let Err(err) = fs::remove_file(input).await {
        if err.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(path = %input.display(), ?err, "failed to remove temporary input file");
        }
    }

    Ok(())
}

async fn encode_download(output: &Path, input: &Path, has_audio: bool) -> Result<(), AppError> {
    ensure_parent(output).await?;

    let mut args = vec![os("-y"), os("-i"), os_path(input)];

    args.extend([
        os("-c:v"),
        os("libaom-av1"),
        os("-crf"),
        os("24"),
        os("-b:v"),
        os("0"),
        os("-g"),
        os(GOP_LENGTH),
        os("-cpu-used"),
        os("4"),
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

    run_ffmpeg(args).await
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
        os("veryfast"),
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
                FfmpegVideoProcessingPreset::VeryFast,
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

async fn run_ffmpeg(args: Vec<OsString>) -> Result<(), AppError> {
    let status = Command::new(FFMPEG_BIN)
        .args(&args)
        .status()
        .await
        .map_err(map_io_error)?;

    if !status.success() {
        return Err(AppError::transcode(format!(
            "ffmpeg exited with status {status}"
        )));
    }

    Ok(())
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
