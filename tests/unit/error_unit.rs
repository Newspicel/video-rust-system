use crate::error::AppError;
use axum::http::StatusCode;

#[test]
fn into_response_sets_http_status() {
    let response = AppError::not_found("video").into_response();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[test]
fn validation_helper_formats_message() {
    let err = AppError::validation("bad value");
    assert_eq!(err.to_string(), "validation failed: bad value");
}
