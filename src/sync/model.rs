use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Deserialize, ToSchema)]
pub struct ProjectPath {
    pub project: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct SyncPath {
    pub project: String,
    pub sync_id: Uuid,
    pub path: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct SyncIdPath {
    pub project: String,
    pub sync_id: Uuid,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct DownloadPath {
    pub project: String,
    pub path: String,
}

#[derive(Debug, Default, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ManifestRequest {
    #[serde(default)]
    pub include_globs: Vec<String>,
    #[serde(default)]
    pub exclude_globs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ManifestEntry {
    pub path: String,
    pub kind: ManifestEntryKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtime_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(default)]
    pub executable: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ManifestEntryKind {
    File,
    Dir,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ManifestResponse {
    pub entries: Vec<ManifestEntry>,
}

#[derive(Debug, Default, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PushPlanRequest {
    pub entries: Vec<ManifestEntry>,
    #[serde(default)]
    pub delete_extra: bool,
    #[serde(default)]
    pub include_globs: Vec<String>,
    #[serde(default)]
    pub exclude_globs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct PushPlanResponse {
    pub sync_id: Uuid,
    pub upload_files: Vec<FileTransfer>,
    pub create_dirs: Vec<String>,
    pub delete_files: Vec<String>,
    pub delete_dirs: Vec<String>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Default, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PullPlanRequest {
    pub entries: Vec<ManifestEntry>,
    #[serde(default)]
    pub delete_extra: bool,
    #[serde(default)]
    pub include_globs: Vec<String>,
    #[serde(default)]
    pub exclude_globs: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PullPlanResponse {
    pub download_files: Vec<FileTransfer>,
    pub create_dirs: Vec<String>,
    pub delete_files: Vec<String>,
    pub delete_dirs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct FileTransfer {
    pub path: String,
    pub size_bytes: u64,
    pub mtime_unix_ms: i64,
    pub sha256: String,
    pub executable: bool,
}

#[derive(Debug, Default, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CommitSyncRequest {}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CommitSyncResponse {
    pub sync_id: Uuid,
    pub status: &'static str,
    pub uploaded_files: Vec<String>,
    pub created_dirs: Vec<String>,
    pub deleted_files: Vec<String>,
    pub deleted_dirs: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct UploadResponse {
    pub path: String,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AbortSyncResponse {
    pub sync_id: Uuid,
    pub status: &'static str,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SyncSessionStatus {
    Open,
    Committing,
    Committed,
    Aborted,
    Expired,
    Failed,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SyncStatusResponse {
    pub sync_id: Uuid,
    pub project: String,
    pub status: SyncSessionStatus,
    pub cleanup_pending: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<CommitSyncResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}
