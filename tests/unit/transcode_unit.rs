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
