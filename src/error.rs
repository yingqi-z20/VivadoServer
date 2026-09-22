use axum::{
    Json,
    http::{StatusCode, header},
    response::IntoResponse,
};
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;
use utoipa::ToSchema;

const MAX_CLIENT_MESSAGE_CHARS: usize = 1_024;

#[derive(Debug, Clone, Error)]
pub enum AppError {
    #[error("authentication is required")]
    Unauthorized,
    #[error("invalid project name")]
    InvalidProject,
    #[error("session not found")]
    SessionNotFound,
    #[error("not found: {0}")]
    NotFound(String),
    #[error("capacity reached: {0}")]
    Capacity(String),
    #[error("session is not running")]
    SessionNotRunning,
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("project requires a complete upload: {0}")]
    ProjectReuploadRequired(String),
    #[error("another workflow is active")]
    WorkflowBusy,
    #[error("invalid workflow phase: {0}")]
    WorkflowPhase(String),
    #[error("service is shutting down")]
    ShuttingDown,
    #[error("precondition failed: {0}")]
    PreconditionFailed(String),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("request body timed out")]
    RequestTimeout,
    #[error("request body is too large")]
    PayloadTooLarge,
    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ErrorEnvelope {
    pub error: ErrorBody,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
    pub request_id: String,
    #[schema(value_type = Object)]
    pub details: Value,
}

impl AppError {
    pub(crate) fn status(&self) -> StatusCode {
        match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::InvalidProject | Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::RequestTimeout => StatusCode::REQUEST_TIMEOUT,
            Self::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::SessionNotFound | Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Capacity(_) => StatusCode::TOO_MANY_REQUESTS,
            Self::SessionNotRunning
            | Self::Conflict(_)
            | Self::ProjectReuploadRequired(_)
            | Self::WorkflowBusy
            | Self::WorkflowPhase(_) => StatusCode::CONFLICT,
            Self::PreconditionFailed(_) => StatusCode::PRECONDITION_FAILED,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ShuttingDown => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Unauthorized => "unauthorized",
            Self::InvalidProject => "invalid_project",
            Self::SessionNotFound => "session_not_found",
            Self::NotFound(_) => "not_found",
            Self::Capacity(_) => "capacity_reached",
            Self::SessionNotRunning => "session_not_running",
            Self::Conflict(_) => "sync_conflict",
            Self::ProjectReuploadRequired(_) => "project_reupload_required",
            Self::WorkflowBusy => "workflow_busy",
            Self::WorkflowPhase(_) => "workflow_phase_conflict",
            Self::ShuttingDown => "service_shutting_down",
            Self::PreconditionFailed(_) => "precondition_failed",
            Self::BadRequest(_) => "invalid_request",
            Self::RequestTimeout => "request_timeout",
            Self::PayloadTooLarge => "payload_too_large",
            Self::Internal(_) => "internal_error",
        }
    }

    pub(crate) fn client_message(&self) -> String {
        if matches!(self, Self::Internal(_)) {
            return "internal server error".to_string();
        }
        truncate_message(&self.to_string())
    }

    pub(crate) fn envelope(&self, request_id: String) -> ErrorEnvelope {
        ErrorEnvelope {
            error: ErrorBody {
                code: self.code().to_string(),
                message: self.client_message(),
                request_id,
                details: serde_json::json!({}),
            },
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let status = self.status();
        let diagnostic = self.to_string();
        if status.is_server_error() {
            tracing::error!(status = status.as_u16(), error = %diagnostic, "request failed");
        } else {
            tracing::debug!(status = status.as_u16(), error = %diagnostic, "request rejected");
        }
        let mut response = (status, Json(self.envelope(String::new()))).into_response();
        if status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                axum::http::HeaderValue::from_static("Bearer"),
            );
        }
        response
    }
}

fn truncate_message(value: &str) -> String {
    if value.chars().count() <= MAX_CLIENT_MESSAGE_CHARS {
        return value.to_string();
    }
    let mut output: String = value.chars().take(MAX_CLIENT_MESSAGE_CHARS - 1).collect();
    output.push('…');
    output
}

impl From<anyhow::Error> for AppError {
    fn from(value: anyhow::Error) -> Self {
        Self::Internal(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_details_are_not_exposed() {
        let envelope =
            AppError::Internal("C:\\secret\\file".to_string()).envelope("request".to_string());
        assert_eq!(envelope.error.message, "internal server error");
        assert_eq!(envelope.error.code, "internal_error");
        assert!(envelope.error.details.is_object());
    }

    #[test]
    fn client_messages_are_bounded() {
        let envelope = AppError::Conflict("x".repeat(2_000)).envelope("request".to_string());
        assert!(envelope.error.message.chars().count() <= MAX_CLIENT_MESSAGE_CHARS);
    }
}
