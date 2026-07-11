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
    #[error("request body is too large")]
    PayloadTooLarge,
    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            AppError::Unauthorized => StatusCode::UNAUTHORIZED,
            AppError::InvalidProject | AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            AppError::SessionNotFound | AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::SessionLimitReached | AppError::Conflict(_) => StatusCode::CONFLICT,
            AppError::SessionNotRunning => StatusCode::CONFLICT,
            AppError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let detail = self.to_string();
        if status.is_server_error() {
            tracing::error!(status = status.as_u16(), error = %detail, "request failed");
        } else {
            tracing::debug!(status = status.as_u16(), error = %detail, "request rejected");
        }
        // Internal diagnostics can contain host paths and OS error details. Keep
        // those in structured logs and expose a stable message to API clients.
        let error = if status.is_server_error() {
            "internal server error".to_string()
        } else {
            detail
        };
        let body = Json(ErrorBody { error });
        (status, body).into_response()
    }
}

impl From<anyhow::Error> for AppError {
    fn from(value: anyhow::Error) -> Self {
        AppError::Internal(value.to_string())
    }
}
