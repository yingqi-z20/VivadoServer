use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateSessionRequest {
    pub project: String,
    #[serde(default)]
    pub args: Vec<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SendInputRequest {
    pub text: String,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct OutputQuery {
    #[serde(default)]
    pub cursor: u64,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Running,
    Stopping,
    Exited,
    Terminated,
    Failed,
}

impl SessionStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Exited | Self::Terminated | Self::Failed)
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TerminationReason {
    ProcessExited,
    ClientRequested,
    HeartbeatTimeout,
    ServiceShutdown,
    OutputReadFailed,
    ProcessWaitFailed,
    ProcessStartFailed,
    InputWriteFailed,
    WorkflowClosed,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SessionInfo {
    pub session_id: Uuid,
    pub project: String,
    pub status: SessionStatus,
    pub started_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<DateTime<Utc>>,
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub termination_reason: Option<TerminationReason>,
    pub output_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup_error: Option<String>,
}

impl SessionInfo {
    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct OutputChunk {
    pub seq: u64,
    pub timestamp: DateTime<Utc>,
    pub text: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OutputResponse {
    pub cursor: u64,
    pub chunks: Vec<OutputChunk>,
    pub status: SessionStatus,
    pub overrun: bool,
    pub output_truncated: bool,
}
