use axum::{Json, http::StatusCode, response::IntoResponse};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("invalid project name")]
    InvalidProject,
    #[error("session not found")]
    SessionNotFound,
    #[error("not found: {0}")]
    NotFound(String),
    #[error("session limit reached")]
    SessionLimitReached,
    #[error("session is not running")]
    SessionNotRunning,
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let status = match self {
            AppError::Unauthorized => StatusCode::UNAUTHORIZED,
            AppError::InvalidProject | AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::SessionNotFound | AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::SessionLimitReached | AppError::Conflict(_) => StatusCode::CONFLICT,
            AppError::SessionNotRunning => StatusCode::CONFLICT,
            AppError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let error = self.to_string();
        if status.is_server_error() {
            tracing::error!(status = status.as_u16(), error = %error, "request failed");
        } else {
            tracing::debug!(status = status.as_u16(), error = %error, "request rejected");
        }
        let body = Json(ErrorBody { error });
        (status, body).into_response()
    }
}

impl From<anyhow::Error> for AppError {
    fn from(value: anyhow::Error) -> Self {
        AppError::Internal(value.to_string())
    }
}
