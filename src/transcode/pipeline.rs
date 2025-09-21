use std::{env, ffi::OsString, path::Path, time::Duration};

use tokio::fs;
use uuid::Uuid;

use crate::{
    error::AppError,
    jobs::{DynJobStore, JobStage},
    storage::{Storage, ensure_parent},
};

use super::{
    config::{EncodeParams, EncoderKind, encoder_candidates},
    ffmpeg::{FfmpegProgressConfig, run_ffmpeg, run_ffmpeg_with_progress},
    probe::{probe_duration, probe_has_audio, probe_video_geometry},
    streams::{generate_dash_stream, generate_hls_stream, select_renditions},
    util::{finalize_encoded_file, os, os_path},
};

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

    let tmp_output = storage
        .tmp_dir()
        .join(format!("{}.encode.webm", id.simple()));
    ensure_parent(&tmp_output).await?;
    if tmp_output.exists() {
        fs::remove_file(&tmp_output).await.ok();
    }

    encode_download(jobs, id, &tmp_output, input, has_audio, duration, params).await?;

    finalize_encoded_file(&tmp_output, &download_path).await?;

    let geometry = probe_video_geometry(&download_path).await?;
    let renditions = select_renditions(geometry);
    let rendition_summary: Vec<String> = renditions
        .iter()
        .map(|r| format!("{}x{}@{}k", r.width, r.height, r.bitrate))
        .collect();
    tracing::debug!(
        video_id = %id,
        width = geometry.width,
        height = geometry.height,
        renditions = %rendition_summary.join(", "),
        "selected rendition ladder"
    );

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
        {
            let renditions = renditions.clone();
            async move {
                generate_hls_stream(
                    &storage_for_hls,
                    &id_for_hls,
                    &download_for_hls,
                    has_audio,
                    renditions,
                )
                .await
            }
        },
        {
            let renditions = renditions.clone();
            async move {
                generate_dash_stream(
                    &storage_for_dash,
                    &id_for_dash,
                    &download_for_dash,
                    has_audio,
                    renditions,
                )
                .await
            }
        },
    )?;

    tracing::debug!(video_id = %id, "segment generation finished");

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
    let geometry = probe_video_geometry(&source).await?;
    let renditions = select_renditions(geometry);
    generate_hls_stream(storage, id, &source, has_audio, renditions).await
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
    let geometry = probe_video_geometry(&source).await?;
    let renditions = select_renditions(geometry);
    generate_dash_stream(storage, id, &source, has_audio, renditions).await
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

        tracing::info!(encoder = ?encoder, path = %output.display(), "starting encode");

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
