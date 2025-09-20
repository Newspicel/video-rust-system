use crate::error::AppError;
use uuid::Uuid;

#[test]
fn parse_range_handles_explicit_bounds() {
    let range = parse_range("bytes=10-19", 100).expect("range should parse");
    assert_eq!(range.start, 10);
    assert_eq!(range.end, 19);
    assert_eq!(range.length, 10);
}

#[test]
fn parse_range_handles_open_end() {
    let range = parse_range("bytes=100-", 200).expect("range should parse");
    assert_eq!(range.start, 100);
    assert_eq!(range.end, 199);
    assert_eq!(range.length, 100);
}

#[test]
fn parse_range_rejects_invalid_prefix() {
    let err = parse_range("items=0-10", 100).unwrap_err();
    assert!(matches!(err, AppError::Validation(_)));
}

#[test]
fn upload_response_uses_expected_urls() {
    let id = Uuid::new_v4();
    let response = build_upload_response(id);
    assert_eq!(response.id, id.to_string());
    assert!(response.status_url.ends_with(&response.id));
    assert!(response.download_url.contains(&response.id));
    assert!(response.hls_master_url.contains("hls"));
    assert!(response.dash_manifest_url.contains("dash"));
}

#[test]
fn validate_relative_path_rules() {
    assert!(validate_relative_path("segment.m4s").is_ok());
    assert!(validate_relative_path("subdir/segment.m4s").is_ok());
    assert!(validate_relative_path("../evil").is_err());
    assert!(validate_relative_path("/absolute").is_err());
}

#[test]
fn binary_name_reflects_platform() {
    let result = binary_name("yt-dlp");
    if cfg!(target_os = "windows") {
        assert_eq!(result, "yt-dlp.exe");
    } else {
        assert_eq!(result, "yt-dlp");
    }
}
