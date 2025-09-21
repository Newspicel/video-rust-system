#[test]
fn ensure_even_rounds_up() {
    assert_eq!(ensure_even(3), 4);
    assert_eq!(ensure_even(4), 4);
}

#[test]
fn scaled_dimensions_preserve_aspect_ratio() {
    let dims = scaled_dimensions((3840, 2160), 720);
    assert_eq!(dims, (1280, 720));

    let fallback = scaled_dimensions((0, 0), 720);
    assert_eq!(fallback.1, 720);
    assert_eq!(fallback.0 % 2, 0);
}

#[test]
fn map_audio_bitrate_matches_profiles() {
    assert_eq!(map_audio_bitrate("192k"), HlsVideoAudioBitrate::High);
    assert_eq!(map_audio_bitrate("128k"), HlsVideoAudioBitrate::Medium);
    assert_eq!(map_audio_bitrate("64k"), HlsVideoAudioBitrate::Low);
}

#[test]
fn build_hls_profiles_tracks_renditions() {
    let profiles = build_hls_profiles((3840, 2160));
    assert_eq!(profiles.len(), RENDITIONS.len());

    for (profile, rendition) in profiles.iter().zip(RENDITIONS.iter()) {
        assert_eq!(profile.name, rendition.name);
        assert_eq!(profile.settings.resolution.1, rendition.height as i32);
    }
}

#[test]
fn encode_params_sanitized_clamps_values() {
    let params = EncodeParams { crf: 200, cpu_used: 99 }.sanitized();
    assert_eq!(params.crf, 63);
    assert_eq!(params.cpu_used, 8);

    let params = EncodeParams { crf: 0, cpu_used: 0 }.sanitized();
    assert_eq!(params.crf, 0);
    assert_eq!(params.cpu_used, 0);
}

#[test]
fn parse_timecode_handles_invalid_values() {
    assert!(parse_timecode("").is_none());
    assert!(parse_timecode("foo").is_none());
    assert_eq!(parse_timecode("00:00:10"), Some(10.0));
}

#[test]
fn parse_speed_handles_na_and_suffix() {
    assert!(parse_speed("N/A").is_none());
    assert_eq!(parse_speed("2.5x"), Some(2.5));
    assert!(parse_speed("0.0x").is_none());
}

#[test]
fn format_eta_formats_reasonable_strings() {
    assert_eq!(format_eta(f64::INFINITY), "unknown");
    assert_eq!(format_eta(5.0), "5s");
    assert_eq!(format_eta(65.0), "1m 05s");
    assert_eq!(format_eta(3661.0), "1h 01m 01s");
}
