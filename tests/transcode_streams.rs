use std::collections::HashSet;

use vrs::transcode::streams::{Rendition, inspect};

fn heights(renditions: &[Rendition]) -> Vec<u32> {
    renditions.iter().map(|item| item.height).collect()
}

#[test]
fn ultrawide_source_produces_descending_unique_even_rungs() {
    let renditions = inspect::renditions(inspect::GeometryInput {
        width: 5120,
        height: 2160,
    });

    assert!(!renditions.is_empty());
    assert!(renditions.len() <= 5);
    assert_eq!(renditions[0].height, 2160);
    assert_eq!(renditions[0].width, 5120);

    let mut last_height = u32::MAX;
    let mut seen_pairs = HashSet::new();
    for rung in &renditions {
        assert!(rung.height <= 2160);
        assert!(rung.width <= 5120);
        assert_eq!(rung.height % 2, 0);
        assert_eq!(rung.width % 2, 0);
        assert!(rung.height <= last_height);
        assert!(seen_pairs.insert((rung.width, rung.height)));
        last_height = rung.height;
    }
}

#[test]
fn sixteen_nine_source_matches_expected_ladder() {
    let renditions = inspect::renditions(inspect::GeometryInput {
        width: 1920,
        height: 1080,
    });
    assert_eq!(heights(&renditions), vec![1080, 900, 720, 540, 480]);
    for rung in renditions {
        assert!(rung.width <= 1920);
        assert_eq!(rung.width % 2, 0);
    }
}

#[test]
fn tall_video_keeps_vertical_ladder() {
    let renditions = inspect::renditions(inspect::GeometryInput {
        width: 1080,
        height: 1920,
    });
    assert_eq!(heights(&renditions), vec![1920, 1600, 1440, 1200, 1080]);
    for rung in renditions {
        assert!(rung.width <= 1080);
    }
}

#[test]
fn filter_complex_matches_expected_layout() {
    let renditions = vec![
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
    ];

    let filter = inspect::filter_complex(&renditions);
    assert_eq!(
        filter,
        "[0:v]scale=-2:1080:flags=lanczos[v0];[0:v]scale=-2:720:flags=lanczos[v1]"
    );
}

#[test]
fn var_stream_map_handles_audio_and_video() {
    let renditions = vec![
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
    ];

    let with_audio = inspect::var_stream_map(&renditions, true);
    assert_eq!(with_audio, "v:0,a:0,name:1080p v:1,a:0,name:720p");

    let without_audio = inspect::var_stream_map(&renditions, false);
    assert_eq!(without_audio, "v:0,name:1080p v:1,name:720p");
}

#[test]
fn bitrate_estimates_scale_with_resolution() {
    let high = inspect::bitrates(1920, 1080);
    let mid = inspect::bitrates(1280, 720);

    assert!(high.0 > mid.0);
    assert!(high.1 > mid.1);
    assert!(high.2 > mid.2);

    let low = inspect::bitrates(640, 360);
    assert!(mid.0 > low.0);
}
