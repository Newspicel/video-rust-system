use std::path::Path;

use crate::{
    error::AppError,
    storage::{Storage, ensure_dir, ensure_parent},
};

use super::{
    ffmpeg::run_ffmpeg,
    util::{os, os_path},
};

const SEGMENT_SECONDS: &str = "4";

pub(crate) async fn generate_hls_stream(
    storage: &Storage,
    id: &uuid::Uuid,
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

pub(crate) async fn generate_dash_stream(
    storage: &Storage,
    id: &uuid::Uuid,
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
