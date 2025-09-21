use std::{
    collections::{BTreeSet, HashSet},
    fmt::Write,
    path::Path,
};

use tokio::fs;

use crate::{
    error::AppError,
    storage::{Storage, ensure_dir, ensure_parent},
};

use super::{
    ffmpeg::run_ffmpeg,
    probe::VideoGeometry,
    util::{os, os_path},
};

const SEGMENT_SECONDS: &str = "4";
const MAX_RENDITIONS: usize = 5;
const BASE_BITRATE_1080P_KBPS: f64 = 4_500.0;
const MIN_BITRATE_KBPS: f64 = 320.0;
const MAX_BITRATE_KBPS: f64 = 22_000.0;
const AUDIO_BITRATE: &str = "192k";
const AUDIO_CHANNELS: &str = "2";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Rendition {
    pub name: String,
    pub width: u32,
    pub height: u32,
    pub bitrate: u32,
    pub maxrate: u32,
    pub bufsize: u32,
}

pub(crate) fn select_renditions(geometry: VideoGeometry) -> Vec<Rendition> {
    let mut height_candidates = BTreeSet::new();
    if geometry.height > 0 {
        height_candidates.insert(geometry.height);
    }
    for value in base_height_candidates(geometry) {
        if *value > 0 {
            height_candidates.insert(*value);
        }
    }

    let mut renditions = Vec::new();
    let mut seen = HashSet::new();

    let aspect_ratio = if geometry.height > 0 {
        geometry.width as f64 / geometry.height as f64
    } else {
        1.0
    };

    let mut sorted_candidates: Vec<u32> = height_candidates.into_iter().collect();
    sorted_candidates.sort_unstable();
    sorted_candidates.reverse();

    for raw_height in sorted_candidates {
        if raw_height == 0 || raw_height > geometry.height {
            continue;
        }

        let height = if raw_height % 2 == 0 {
            raw_height
        } else {
            raw_height.saturating_sub(1)
        };

        if height < 2 {
            continue;
        }

        let mut width = (aspect_ratio * height as f64).round() as u32;
        if width > geometry.width {
            width = geometry.width;
        }
        if !width.is_multiple_of(2) {
            width = width.saturating_sub(1);
        }
        if width < 2 {
            continue;
        }

        if !seen.insert((width, height)) {
            continue;
        }

        let (bitrate, maxrate, bufsize) = estimate_bitrates(width, height);
        renditions.push(Rendition {
            name: format!("{}p", height),
            width,
            height,
            bitrate,
            maxrate,
            bufsize,
        });

        if renditions.len() >= MAX_RENDITIONS {
            break;
        }
    }

    if renditions.is_empty() {
        let mut width = if geometry.width.is_multiple_of(2) {
            geometry.width
        } else {
            geometry.width.saturating_sub(1)
        };
        let mut height = if geometry.height.is_multiple_of(2) {
            geometry.height
        } else {
            geometry.height.saturating_sub(1)
        };

        width = width.max(2);
        height = height.max(2);

        let (bitrate, maxrate, bufsize) = estimate_bitrates(width, height);
        renditions.push(Rendition {
            name: format!("{}p", height),
            width,
            height,
            bitrate,
            maxrate,
            bufsize,
        });
    }

    renditions.sort_by(|a, b| b.height.cmp(&a.height));
    renditions
}

pub(crate) async fn generate_hls_stream(
    storage: &Storage,
    id: &uuid::Uuid,
    source: &Path,
    has_audio: bool,
    renditions: Vec<Rendition>,
) -> Result<(), AppError> {
    let hls_dir = storage.hls_dir(id);
    if hls_dir.exists() {
        match fs::remove_dir_all(&hls_dir).await {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
    }
    ensure_dir(&hls_dir).await?;

    let filter_complex = build_filter_complex(&renditions);
    let var_stream_map = build_var_stream_map(&renditions, has_audio);

    let mut args = vec![os("-y"), os("-i"), os_path(source)];
    if !filter_complex.is_empty() {
        args.extend([os("-filter_complex"), os(filter_complex)]);
    }

    for (index, _) in renditions.iter().enumerate() {
        args.extend([os("-map"), os(format!("[v{index}]"))]);
    }

    if has_audio {
        args.extend([os("-map"), os("0:a:0")]);
    }

    args.extend([
        os("-c:v"),
        os("libaom-av1"),
        os("-pix_fmt"),
        os("yuv420p"),
        os("-row-mt"),
        os("1"),
        os("-cpu-used"),
        os("6"),
        os("-g"),
        os("120"),
        os("-keyint_min"),
        os("120"),
        os("-sc_threshold"),
        os("0"),
    ]);

    for (idx, rendition) in renditions.iter().enumerate() {
        args.extend([
            os(format!("-b:v:{idx}")),
            os(format!("{}k", rendition.bitrate)),
            os(format!("-maxrate:v:{idx}")),
            os(format!("{}k", rendition.maxrate)),
            os(format!("-bufsize:v:{idx}")),
            os(format!("{}k", rendition.bufsize)),
            os(format!("-metadata:s:v:{idx}")),
            os(format!("variant={}", rendition.name)),
        ]);
    }

    if has_audio {
        args.extend([
            os("-c:a"),
            os("aac"),
            os("-b:a"),
            os(AUDIO_BITRATE),
            os("-ac"),
            os(AUDIO_CHANNELS),
        ]);
    } else {
        args.push(os("-an"));
    }

    let segment_pattern = hls_dir.join("segment_%v_%05d.m4s");
    let variant_index = hls_dir.join("stream_%v.m3u8");

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
        os("-hls_fmp4_init_filename"),
        os("init_%v.m4s"),
        os("-hls_segment_filename"),
        os_path(&segment_pattern),
        os("-master_pl_name"),
        os("index.m3u8"),
        os("-var_stream_map"),
        os(var_stream_map),
        os_path(&variant_index),
    ]);

    run_ffmpeg(args).await?;

    let index_playlist = hls_dir.join("index.m3u8");
    if !index_playlist.exists() {
        return Err(AppError::transcode("ffmpeg did not produce an HLS master playlist"));
    }

    let master_playlist = hls_dir.join("master.m3u8");
    fs::copy(&index_playlist, &master_playlist).await?;

    Ok(())
}

pub(crate) async fn generate_dash_stream(
    storage: &Storage,
    id: &uuid::Uuid,
    source: &Path,
    has_audio: bool,
    renditions: Vec<Rendition>,
) -> Result<(), AppError> {
    let dash_dir = storage.dash_dir(id);
    if dash_dir.exists() {
        match fs::remove_dir_all(&dash_dir).await {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
    }

    let manifest = dash_dir.join("manifest.mpd");
    ensure_parent(&manifest).await?;

    let filter_complex = build_filter_complex(&renditions);

    let mut args = vec![os("-y"), os("-i"), os_path(source)];
    if !filter_complex.is_empty() {
        args.extend([os("-filter_complex"), os(filter_complex)]);
    }

    for (index, _) in renditions.iter().enumerate() {
        args.extend([os("-map"), os(format!("[v{index}]"))]);
    }

    if has_audio {
        args.extend([os("-map"), os("0:a:0")]);
    }

    args.extend([
        os("-c:v"),
        os("libaom-av1"),
        os("-pix_fmt"),
        os("yuv420p"),
        os("-row-mt"),
        os("1"),
        os("-cpu-used"),
        os("6"),
        os("-g"),
        os("120"),
        os("-keyint_min"),
        os("120"),
        os("-sc_threshold"),
        os("0"),
    ]);

    for (idx, rendition) in renditions.iter().enumerate() {
        args.extend([
            os(format!("-b:v:{idx}")),
            os(format!("{}k", rendition.bitrate)),
            os(format!("-maxrate:v:{idx}")),
            os(format!("{}k", rendition.maxrate)),
            os(format!("-bufsize:v:{idx}")),
            os(format!("{}k", rendition.bufsize)),
            os(format!("-metadata:s:v:{idx}")),
            os(format!("variant={}", rendition.name)),
        ]);
    }

    if has_audio {
        args.extend([
            os("-c:a"),
            os("aac"),
            os("-b:a"),
            os(AUDIO_BITRATE),
            os("-ac"),
            os(AUDIO_CHANNELS),
        ]);
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

fn base_height_candidates(geometry: VideoGeometry) -> &'static [u32] {
    match classify_aspect(geometry) {
        AspectClass::Ultrawide => &[
            4320, 3200, 2560, 2160, 2000, 1600, 1440, 1080, 864, 720, 540, 432, 360,
        ],
        AspectClass::SixteenNine => &[
            4320, 2880, 2160, 1800, 1440, 1200, 1080, 900, 720, 540, 480, 360, 240,
        ],
        AspectClass::FourThree => &[
            2880, 2160, 1600, 1440, 1280, 1080, 960, 720, 540, 480, 360, 240,
        ],
        AspectClass::Tall => &[
            2160, 1920, 1600, 1440, 1200, 1080, 900, 720, 540, 480, 360, 240,
        ],
    }
}

fn classify_aspect(geometry: VideoGeometry) -> AspectClass {
    if geometry.width == 0 || geometry.height == 0 {
        return AspectClass::SixteenNine;
    }

    let ratio = geometry.width as f64 / geometry.height as f64;

    if ratio >= 2.1 {
        AspectClass::Ultrawide
    } else if ratio >= 1.55 {
        AspectClass::SixteenNine
    } else if ratio >= 1.3 {
        AspectClass::FourThree
    } else {
        AspectClass::Tall
    }
}

enum AspectClass {
    Ultrawide,
    SixteenNine,
    FourThree,
    Tall,
}

fn estimate_bitrates(width: u32, height: u32) -> (u32, u32, u32) {
    let pixels = (width as f64) * (height as f64);
    let reference = 1920.0 * 1080.0;
    let mut bitrate = BASE_BITRATE_1080P_KBPS * (pixels / reference);
    if !bitrate.is_finite() {
        bitrate = BASE_BITRATE_1080P_KBPS;
    }
    bitrate = bitrate.clamp(MIN_BITRATE_KBPS, MAX_BITRATE_KBPS);
    let maxrate = (bitrate * 1.3).ceil();
    let bufsize = (bitrate * 2.5).ceil();
    (bitrate.round() as u32, maxrate as u32, bufsize as u32)
}

fn build_filter_complex(renditions: &[Rendition]) -> String {
    let mut filter = String::new();
    for (idx, rendition) in renditions.iter().enumerate() {
        if idx > 0 {
            filter.push(';');
        }
        let _ = write!(
            &mut filter,
            "[0:v]scale=-2:{}:flags=lanczos[v{}]",
            rendition.height, idx
        );
    }
    filter
}

fn build_var_stream_map(renditions: &[Rendition], has_audio: bool) -> String {
    let mut entries = Vec::with_capacity(renditions.len());
    for (idx, rendition) in renditions.iter().enumerate() {
        if has_audio {
            entries.push(format!("v:{idx},a:0,name:{}", rendition.name));
        } else {
            entries.push(format!("v:{idx},name:{}", rendition.name));
        }
    }
    entries.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ladder_heights(renditions: &[Rendition]) -> Vec<u32> {
        renditions.iter().map(|rung| rung.height).collect()
    }

    fn sample_renditions() -> Vec<Rendition> {
        vec![
            Rendition {
                name: "1080p".into(),
                width: 1920,
                height: 1080,
                bitrate: 6000,
                maxrate: 6500,
                bufsize: 8000,
            },
            Rendition {
                name: "720p".into(),
                width: 1280,
                height: 720,
                bitrate: 3000,
                maxrate: 3500,
                bufsize: 4000,
            },
        ]
    }

    #[test]
    fn ultrawide_source_produces_descending_unique_even_rungs() {
        let geometry = VideoGeometry {
            width: 5120,
            height: 2160,
        };

        let renditions = select_renditions(geometry);
        assert!(!renditions.is_empty());
        assert!(renditions.len() <= MAX_RENDITIONS);
        assert_eq!(renditions[0].width, 5120);
        assert_eq!(renditions[0].height, 2160);

        let mut last_height = u32::MAX;
        let mut seen = std::collections::HashSet::new();
        for rung in renditions {
            assert!(rung.width <= 5120);
            assert!(rung.height <= 2160);
            assert!(rung.width.is_multiple_of(2));
            assert!(rung.height.is_multiple_of(2));
            assert!(rung.height <= last_height);
            assert!(seen.insert((rung.width, rung.height)));
            last_height = rung.height;
        }
    }

    #[test]
    fn sixteen_nine_source_matches_expected_ladder() {
        let geometry = VideoGeometry {
            width: 1920,
            height: 1080,
        };

        let renditions = select_renditions(geometry);
        assert_eq!(ladder_heights(&renditions), vec![1080, 900, 720, 540, 480]);
        for rung in renditions {
            assert!(rung.width <= 1920);
            assert!(rung.width.is_multiple_of(2));
        }
    }

    #[test]
    fn tall_video_keeps_vertical_ladder() {
        let geometry = VideoGeometry {
            width: 1080,
            height: 1920,
        };

        let renditions = select_renditions(geometry);
        assert_eq!(
            ladder_heights(&renditions),
            vec![1920, 1600, 1440, 1200, 1080]
        );
        for rung in renditions {
            assert!(rung.width <= 1080);
        }
    }

    #[test]
    fn filter_complex_matches_expected_layout() {
        let filter = build_filter_complex(&sample_renditions());
        assert_eq!(
            filter,
            "[0:v]scale=-2:1080:flags=lanczos[v0];[0:v]scale=-2:720:flags=lanczos[v1]"
        );
    }

    #[test]
    fn var_stream_map_handles_audio_and_video() {
        let renditions = sample_renditions();
        let with_audio = build_var_stream_map(&renditions, true);
        assert_eq!(with_audio, "v:0,a:0,name:1080p v:1,a:0,name:720p");

        let without_audio = build_var_stream_map(&renditions, false);
        assert_eq!(without_audio, "v:0,name:1080p v:1,name:720p");
    }

    #[test]
    fn bitrate_estimates_scale_with_resolution() {
        let high = estimate_bitrates(1920, 1080);
        let mid = estimate_bitrates(1280, 720);
        let low = estimate_bitrates(640, 360);

        assert!(high.0 > mid.0);
        assert!(high.1 > mid.1);
        assert!(high.2 > mid.2);
        assert!(mid.0 > low.0);
    }
}
