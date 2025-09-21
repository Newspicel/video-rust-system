use axum::{
    Json,
    extract::{Path as AxumPath, State},
};
use uuid::Uuid;

use crate::{error::AppError, jobs::JobStatusResponse, state::AppState};

pub async fn job_status(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<JobStatusResponse>, AppError> {
    let job_id =
        Uuid::parse_str(&id).map_err(|_| AppError::validation("invalid job identifier"))?;
    match state.jobs.status(&job_id).await? {
        Some(status) => Ok(Json(status)),
        None => Err(AppError::not_found(format!("job {job_id} not found"))),
    }
}
