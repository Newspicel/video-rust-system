use std::fmt::Display;

use axum::{Json, http::StatusCode, response::IntoResponse};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("resource not found: {0}")]
    NotFound(String),
    #[error("transcoding failed: {0}")]
    Transcode(String),
    #[error("external dependency missing: {0}")]
    Dependency(String),
    #[error(transparent)]
    Multipart(#[from] axum::extract::multipart::MultipartError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            AppError::Validation(_) => StatusCode::BAD_REQUEST,
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::Transcode(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Dependency(_) => StatusCode::SERVICE_UNAVAILABLE,
            AppError::Multipart(_) | AppError::Io(_) | AppError::Http(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };

        tracing::error!(?status, error = %self);

        (
            status,
            Json(ErrorBody {
                error: self.to_string(),
            }),
        )
            .into_response()
    }
}

impl AppError {
    pub fn not_found(resource: impl Display) -> Self {
        Self::NotFound(resource.to_string())
    }

    pub fn validation(message: impl Display) -> Self {
        Self::Validation(message.to_string())
    }

    pub fn dependency(message: impl Display) -> Self {
        Self::Dependency(message.to_string())
    }

    pub fn transcode(message: impl Display) -> Self {
        Self::Transcode(message.to_string())
    }
}
