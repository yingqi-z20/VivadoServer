use crate::{AppConfig, error::AppError, paths::resolve_project_dir};
use axum::{
    body::Body,
    http::{
        HeaderValue, StatusCode,
        header::{CONTENT_LENGTH, CONTENT_TYPE},
    },
    response::Response,
};
use chrono::{DateTime, Utc};
use filetime::FileTime;
use globset::{Glob, GlobSet, GlobSetBuilder};
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufReader, Read},
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::AsyncWriteExt,
    sync::{Mutex as AsyncMutex, RwLock},
    time,
};
use tokio_util::io::ReaderStream;
use uuid::Uuid;
use walkdir::WalkDir;

const SYNC_DIR: &str = ".vivado-server-sync";
const REAPER_INTERVAL: Duration = Duration::from_secs(60);
const LOG_PATH_SAMPLE_LIMIT: usize = 20;

#[derive(Debug, Deserialize)]
pub struct ProjectPath {
    pub project: String,
}

#[derive(Debug, Deserialize)]
pub struct SyncPath {
    pub project: String,
    pub sync_id: Uuid,
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct SyncIdPath {
    pub project: String,
    pub sync_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct DownloadPath {
    pub project: String,
    pub path: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct ManifestRequest {
    #[serde(default)]
    pub include_globs: Vec<String>,
    #[serde(default)]
    pub exclude_globs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestEntry {
    pub path: String,
    pub kind: ManifestEntryKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtime_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ManifestEntryKind {
    File,
    Dir,
}

#[derive(Debug, Serialize)]
pub struct ManifestResponse {
    pub entries: Vec<ManifestEntry>,
}

#[derive(Debug, Default, Deserialize)]
pub struct PushPlanRequest {
    #[serde(default)]
    pub entries: Vec<ManifestEntry>,
    #[serde(default)]
    pub delete_extra: bool,
    #[serde(default)]
    pub include_globs: Vec<String>,
    #[serde(default)]
    pub exclude_globs: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct PushPlanResponse {
    pub sync_id: Uuid,
    pub upload_files: Vec<FileTransfer>,
    pub create_dirs: Vec<String>,
    pub delete_files: Vec<String>,
    pub delete_dirs: Vec<String>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Default, Deserialize)]
pub struct PullPlanRequest {
    #[serde(default)]
    pub entries: Vec<ManifestEntry>,
    #[serde(default)]
    pub delete_extra: bool,
    #[serde(default)]
    pub include_globs: Vec<String>,
    #[serde(default)]
    pub exclude_globs: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct PullPlanResponse {
    pub download_files: Vec<FileTransfer>,
    pub create_dirs: Vec<String>,
    pub delete_files: Vec<String>,
    pub delete_dirs: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FileTransfer {
    pub path: String,
    pub size_bytes: u64,
    pub mtime_unix_ms: i64,
    pub sha256: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct CommitSyncRequest {
    #[serde(default)]
    pub force: bool,
}

#[derive(Debug, Serialize)]
pub struct CommitSyncResponse {
    pub sync_id: Uuid,
    pub status: &'static str,
    pub uploaded_files: Vec<String>,
    pub created_dirs: Vec<String>,
    pub deleted_files: Vec<String>,
    pub deleted_dirs: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct UploadResponse {
    pub path: String,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Serialize)]
pub struct AbortSyncResponse {
    pub sync_id: Uuid,
    pub status: &'static str,
}

#[derive(Clone)]
pub struct SyncManager {
    config: Arc<AppConfig>,
    sessions: Arc<RwLock<HashMap<Uuid, Arc<AsyncMutex<PushSession>>>>>,
}

#[derive(Debug)]
struct PushSession {
    project: String,
    project_dir: PathBuf,
    staging_dir: PathBuf,
    expires_at: DateTime<Utc>,
    upload_files: HashMap<String, PlannedUpload>,
    uploaded_files: HashSet<String>,
    create_dirs: Vec<String>,
    delete_files: Vec<String>,
    delete_dirs: Vec<String>,
    baseline: HashMap<String, ManifestEntry>,
    delete_extra: bool,
}

#[derive(Debug, Clone)]
struct PlannedUpload {
    entry: ManifestEntry,
    transfer: FileTransfer,
}

#[derive(Debug)]
struct SyncFilters {
    include: GlobSet,
    include_active: bool,
    exclude: GlobSet,
}

impl SyncManager {
    pub fn new(config: AppConfig) -> Self {
        tracing::debug!(
            workspace_root = %config.workspace_root.display(),
            sync_max_file_bytes = config.sync_max_file_bytes,
            sync_max_manifest_entries = config.sync_max_manifest_entries,
            sync_session_ttl_secs = config.sync_session_ttl_secs,
            "initializing sync manager"
        );
        Self {
            config: Arc::new(config),
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn spawn_session_reaper(&self) {
        let manager = self.clone();
        tracing::debug!(
            interval_secs = REAPER_INTERVAL.as_secs(),
            "starting sync session reaper"
        );
        tokio::spawn(async move {
            let mut interval = time::interval(REAPER_INTERVAL);
            loop {
                interval.tick().await;
                manager.reap_expired_sessions().await;
            }
        });
    }

    pub async fn manifest(
        &self,
        project: String,
        request: ManifestRequest,
    ) -> Result<ManifestResponse, AppError> {
        let started = Instant::now();
        tracing::debug!(
            project = %project,
            include_globs = ?request.include_globs,
            exclude_globs = ?request.exclude_globs,
            "sync manifest requested"
        );
        let filters = SyncFilters::new(&request.include_globs, &request.exclude_globs)?;
        let project_dir = self.project_dir(&project).await?;
        let response = self.scan_manifest(project_dir, filters).await?;
        let stats = manifest_stats(&response.entries);
        tracing::debug!(
            project = %project,
            entries = response.entries.len(),
            files = stats.files,
            dirs = stats.dirs,
            bytes = stats.bytes,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "sync manifest completed"
        );
        Ok(response)
    }

    pub async fn push_plan(
        &self,
        project: String,
        request: PushPlanRequest,
    ) -> Result<PushPlanResponse, AppError> {
        let started = Instant::now();
        let PushPlanRequest {
            entries,
            delete_extra,
            include_globs,
            exclude_globs,
        } = request;
        tracing::debug!(
            project = %project,
            client_entries = entries.len(),
            delete_extra,
            include_globs = ?include_globs,
            exclude_globs = ?exclude_globs,
            "sync push plan requested"
        );
        let filters = SyncFilters::new(&include_globs, &exclude_globs)?;
        let project_dir = self.project_dir(&project).await?;
        let server_manifest = self
            .scan_manifest(project_dir.clone(), filters.clone())
            .await?;
        let server_stats = manifest_stats(&server_manifest.entries);
        let client_entries = validate_client_manifest(
            entries,
            &filters,
            self.config.sync_max_manifest_entries,
            self.config.sync_max_file_bytes,
        )?;
        let client_stats = manifest_stats(&client_entries);
        let client = manifest_map(client_entries);
        let server = manifest_map(server_manifest.entries);
        let client_entry_count = client.len();
        let server_entry_count = server.len();
        tracing::debug!(
            project = %project,
            client_entries = client_entry_count,
            client_files = client_stats.files,
            client_dirs = client_stats.dirs,
            client_bytes = client_stats.bytes,
            server_entries = server_entry_count,
            server_files = server_stats.files,
            server_dirs = server_stats.dirs,
            server_bytes = server_stats.bytes,
            "sync push manifests compared"
        );

        let mut upload_files = Vec::new();
        let mut create_dirs = Vec::new();
        let mut delete_files = Vec::new();
        let mut delete_dirs = Vec::new();
        let mut planned_uploads = HashMap::new();

        for entry in client.values() {
            match entry.kind {
                ManifestEntryKind::Dir => {
                    if server
                        .get(&entry.path)
                        .is_none_or(|server_entry| server_entry.kind != ManifestEntryKind::Dir)
                    {
                        create_dirs.push(entry.path.clone());
                    }
                    if delete_extra
                        && server.get(&entry.path).is_some_and(|server_entry| {
                            server_entry.kind == ManifestEntryKind::File
                        })
                    {
                        delete_files.push(entry.path.clone());
                    }
                }
                ManifestEntryKind::File => {
                    if server
                        .get(&entry.path)
                        .is_none_or(|server_entry| !same_file(entry, server_entry))
                    {
                        let transfer = file_transfer(entry)?;
                        upload_files.push(transfer.clone());
                        planned_uploads.insert(
                            entry.path.clone(),
                            PlannedUpload {
                                entry: entry.clone(),
                                transfer,
                            },
                        );
                    }
                    if delete_extra
                        && server
                            .get(&entry.path)
                            .is_some_and(|server_entry| server_entry.kind == ManifestEntryKind::Dir)
                    {
                        delete_dirs.push(entry.path.clone());
                    }
                }
            }
        }

        if delete_extra {
            for entry in server.values() {
                if !client.contains_key(&entry.path) {
                    match entry.kind {
                        ManifestEntryKind::File => delete_files.push(entry.path.clone()),
                        ManifestEntryKind::Dir => delete_dirs.push(entry.path.clone()),
                    }
                }
            }
        }

        sort_paths(&mut create_dirs);
        sort_paths(&mut delete_files);
        sort_paths_deepest_first(&mut delete_dirs);
        upload_files.sort_by(|left, right| left.path.cmp(&right.path));
        let upload_bytes: u64 = upload_files.iter().map(|file| file.size_bytes).sum();

        let sync_id = Uuid::new_v4();
        let staging_dir = self.staging_dir(&project_dir, sync_id);
        tokio::fs::create_dir_all(staging_dir.join("files"))
            .await
            .map_err(|err| AppError::Internal(format!("failed to create staging dir: {err}")))?;
        tracing::debug!(
            project = %project,
            sync_id = %sync_id,
            staging_dir = %staging_dir.display(),
            "created sync staging directory"
        );

        let expires_at = Utc::now()
            + chrono::Duration::from_std(self.config.sync_session_ttl())
                .map_err(|err| AppError::Internal(format!("invalid sync ttl: {err}")))?;
        let session = PushSession {
            project,
            project_dir,
            staging_dir,
            expires_at,
            upload_files: planned_uploads,
            uploaded_files: HashSet::new(),
            create_dirs: create_dirs.clone(),
            delete_files: delete_files.clone(),
            delete_dirs: delete_dirs.clone(),
            baseline: server,
            delete_extra,
        };
        self.sessions
            .write()
            .await
            .insert(sync_id, Arc::new(AsyncMutex::new(session)));
        tracing::debug!(
            sync_id = %sync_id,
            client_entries = client_entry_count,
            server_entries = server_entry_count,
            upload_files = upload_files.len(),
            upload_bytes,
            create_dirs = create_dirs.len(),
            delete_files = delete_files.len(),
            delete_dirs = delete_dirs.len(),
            delete_extra,
            expires_at = %expires_at,
            upload_sample = ?sample_transfer_paths(&upload_files),
            upload_omitted = omitted_count(upload_files.len()),
            create_dir_sample = ?sample_string_paths(&create_dirs),
            create_dir_omitted = omitted_count(create_dirs.len()),
            delete_file_sample = ?sample_string_paths(&delete_files),
            delete_file_omitted = omitted_count(delete_files.len()),
            delete_dir_sample = ?sample_string_paths(&delete_dirs),
            delete_dir_omitted = omitted_count(delete_dirs.len()),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "sync push plan created"
        );

        Ok(PushPlanResponse {
            sync_id,
            upload_files,
            create_dirs,
            delete_files,
            delete_dirs,
            expires_at,
        })
    }

    pub async fn pull_plan(
        &self,
        project: String,
        request: PullPlanRequest,
    ) -> Result<PullPlanResponse, AppError> {
        let started = Instant::now();
        let PullPlanRequest {
            entries,
            delete_extra,
            include_globs,
            exclude_globs,
        } = request;
        tracing::debug!(
            project = %project,
            client_entries = entries.len(),
            delete_extra,
            include_globs = ?include_globs,
            exclude_globs = ?exclude_globs,
            "sync pull plan requested"
        );
        let filters = SyncFilters::new(&include_globs, &exclude_globs)?;
        let project_dir = self.project_dir(&project).await?;
        let server_manifest = self.scan_manifest(project_dir, filters.clone()).await?;
        let server_stats = manifest_stats(&server_manifest.entries);
        let client_entries = validate_client_manifest(
            entries,
            &filters,
            self.config.sync_max_manifest_entries,
            self.config.sync_max_file_bytes,
        )?;
        let client_stats = manifest_stats(&client_entries);
        let server = manifest_map(server_manifest.entries);
        let client = manifest_map(client_entries);
        let server_entry_count = server.len();
        let client_entry_count = client.len();
        tracing::debug!(
            project = %project,
            server_entries = server_entry_count,
            server_files = server_stats.files,
            server_dirs = server_stats.dirs,
            server_bytes = server_stats.bytes,
            client_entries = client_entry_count,
            client_files = client_stats.files,
            client_dirs = client_stats.dirs,
            client_bytes = client_stats.bytes,
            "sync pull manifests compared"
        );

        let mut download_files = Vec::new();
        let mut create_dirs = Vec::new();
        let mut delete_files = Vec::new();
        let mut delete_dirs = Vec::new();

        for entry in server.values() {
            match entry.kind {
                ManifestEntryKind::Dir => {
                    if client
                        .get(&entry.path)
                        .is_none_or(|client_entry| client_entry.kind != ManifestEntryKind::Dir)
                    {
                        create_dirs.push(entry.path.clone());
                    }
                    if delete_extra
                        && client.get(&entry.path).is_some_and(|client_entry| {
                            client_entry.kind == ManifestEntryKind::File
                        })
                    {
                        delete_files.push(entry.path.clone());
                    }
                }
                ManifestEntryKind::File => {
                    if client
                        .get(&entry.path)
                        .is_none_or(|client_entry| !same_file(entry, client_entry))
                    {
                        download_files.push(file_transfer(entry)?);
                    }
                    if delete_extra
                        && client
                            .get(&entry.path)
                            .is_some_and(|client_entry| client_entry.kind == ManifestEntryKind::Dir)
                    {
                        delete_dirs.push(entry.path.clone());
                    }
                }
            }
        }

        if delete_extra {
            for entry in client.values() {
                if !server.contains_key(&entry.path) {
                    match entry.kind {
                        ManifestEntryKind::File => delete_files.push(entry.path.clone()),
                        ManifestEntryKind::Dir => delete_dirs.push(entry.path.clone()),
                    }
                }
            }
        }

        sort_paths(&mut create_dirs);
        sort_paths(&mut delete_files);
        sort_paths_deepest_first(&mut delete_dirs);
        download_files.sort_by(|left, right| left.path.cmp(&right.path));
        let download_bytes: u64 = download_files.iter().map(|file| file.size_bytes).sum();
        tracing::debug!(
            project = %project,
            server_entries = server_entry_count,
            client_entries = client_entry_count,
            download_files = download_files.len(),
            download_bytes,
            create_dirs = create_dirs.len(),
            delete_files = delete_files.len(),
            delete_dirs = delete_dirs.len(),
            delete_extra,
            download_sample = ?sample_transfer_paths(&download_files),
            download_omitted = omitted_count(download_files.len()),
            create_dir_sample = ?sample_string_paths(&create_dirs),
            create_dir_omitted = omitted_count(create_dirs.len()),
            delete_file_sample = ?sample_string_paths(&delete_files),
            delete_file_omitted = omitted_count(delete_files.len()),
            delete_dir_sample = ?sample_string_paths(&delete_dirs),
            delete_dir_omitted = omitted_count(delete_dirs.len()),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "sync pull plan created"
        );

        Ok(PullPlanResponse {
            download_files,
            create_dirs,
            delete_files,
            delete_dirs,
        })
    }

    pub async fn upload_file(
        &self,
        project: String,
        sync_id: Uuid,
        raw_path: String,
        body: Body,
    ) -> Result<UploadResponse, AppError> {
        let started = Instant::now();
        tracing::debug!(
            project = %project,
            sync_id = %sync_id,
            raw_path = %raw_path,
            "sync upload requested"
        );
        let path = normalize_sync_path(&raw_path)?;
        let session = self.session(sync_id).await?;
        let (expected, target_path, tmp_dir) = {
            let session = session.lock().await;
            if session.project != project {
                tracing::debug!(
                    project = %project,
                    sync_id = %sync_id,
                    session_project = %session.project,
                    "sync upload rejected because project does not match session"
                );
                return Err(AppError::NotFound("sync session not found".to_string()));
            }
            let Some(expected) = session.upload_files.get(&path).cloned() else {
                tracing::debug!(
                    project = %project,
                    sync_id = %sync_id,
                    path = %path,
                    "sync upload rejected because file is not in push plan"
                );
                return Err(AppError::BadRequest(
                    "file was not requested by push plan".to_string(),
                ));
            };
            (
                expected,
                resolve_relative_path(&session.staging_dir.join("files"), &path)?,
                session.staging_dir.join("tmp"),
            )
        };
        tracing::debug!(
            project = %project,
            sync_id = %sync_id,
            path = %path,
            expected_size = expected.transfer.size_bytes,
            expected_sha256 = %expected.transfer.sha256,
            staging_target = %target_path.display(),
            "receiving sync upload"
        );

        tokio::fs::create_dir_all(
            target_path
                .parent()
                .ok_or_else(|| AppError::BadRequest("invalid target path".to_string()))?,
        )
        .await
        .map_err(|err| AppError::Internal(format!("failed to create staging parent: {err}")))?;
        tokio::fs::create_dir_all(&tmp_dir).await.map_err(|err| {
            AppError::Internal(format!("failed to create staging tmp dir: {err}"))
        })?;
        let tmp_path = tmp_dir.join(format!("{}.part", Uuid::new_v4()));

        let result = write_body_to_file(
            body,
            &tmp_path,
            expected.transfer.size_bytes,
            &expected.transfer.sha256,
            self.config.sync_max_file_bytes,
        )
        .await;
        if let Err(err) = result {
            let _ = tokio::fs::remove_file(&tmp_path).await;
            tracing::debug!(
                project = %project,
                sync_id = %sync_id,
                path = %path,
                error = %err,
                "sync upload failed; temporary file removed"
            );
            return Err(err);
        }

        tokio::fs::rename(&tmp_path, &target_path)
            .await
            .map_err(|err| AppError::Internal(format!("failed to move staged upload: {err}")))?;

        let mut session = session.lock().await;
        session.uploaded_files.insert(path.clone());
        let uploaded_files = session.uploaded_files.len();
        tracing::debug!(
            project = %project,
            sync_id = %sync_id,
            path = %path,
            size_bytes = expected.transfer.size_bytes,
            sha256 = %expected.transfer.sha256,
            uploaded_files,
            planned_upload_files = session.upload_files.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "sync upload staged"
        );

        Ok(UploadResponse {
            path,
            size_bytes: expected.transfer.size_bytes,
            sha256: expected.transfer.sha256,
        })
    }

    pub async fn commit(
        &self,
        project: String,
        sync_id: Uuid,
        request: CommitSyncRequest,
    ) -> Result<CommitSyncResponse, AppError> {
        let started = Instant::now();
        tracing::debug!(
            project = %project,
            sync_id = %sync_id,
            force = request.force,
            "sync commit requested"
        );
        let session = self.session(sync_id).await?;
        let session = session.lock().await;
        if session.project != project {
            tracing::debug!(
                project = %project,
                sync_id = %sync_id,
                session_project = %session.project,
                "sync commit rejected because project does not match session"
            );
            return Err(AppError::NotFound("sync session not found".to_string()));
        }
        let missing: Vec<String> = session
            .upload_files
            .keys()
            .filter(|path| !session.uploaded_files.contains(*path))
            .cloned()
            .collect();
        if !missing.is_empty() {
            tracing::debug!(
                project = %project,
                sync_id = %sync_id,
                missing_files = missing.len(),
                missing_sample = ?sample_string_paths(&missing),
                missing_omitted = omitted_count(missing.len()),
                "sync commit rejected because planned uploads are missing"
            );
            return Err(AppError::Conflict(format!(
                "missing uploaded files: {}",
                missing.join(", ")
            )));
        }
        tracing::debug!(
            project = %project,
            sync_id = %sync_id,
            upload_files = session.upload_files.len(),
            create_dirs = session.create_dirs.len(),
            delete_files = session.delete_files.len(),
            delete_dirs = session.delete_dirs.len(),
            delete_extra = session.delete_extra,
            force = request.force,
            "checking sync commit baseline"
        );

        for dir in &session.create_dirs {
            tracing::trace!(
                project = %project,
                sync_id = %sync_id,
                path = %dir,
                "checking directory baseline"
            );
            ensure_baseline_matches(
                &resolve_relative_path(&session.project_dir, dir)?,
                session.baseline.get(dir),
                request.force,
            )
            .await?;
        }
        for path in session.upload_files.keys() {
            tracing::trace!(
                project = %project,
                sync_id = %sync_id,
                path = %path,
                "checking file baseline"
            );
            ensure_baseline_matches(
                &resolve_relative_path(&session.project_dir, path)?,
                session.baseline.get(path),
                request.force,
            )
            .await?;
        }
        if session.delete_extra {
            for path in session
                .delete_files
                .iter()
                .chain(session.delete_dirs.iter())
            {
                tracing::trace!(
                    project = %project,
                    sync_id = %sync_id,
                    path = %path,
                    "checking delete baseline"
                );
                ensure_baseline_matches(
                    &resolve_relative_path(&session.project_dir, path)?,
                    session.baseline.get(path),
                    request.force,
                )
                .await?;
            }
        }

        for dir in &session.create_dirs {
            let target = resolve_relative_path(&session.project_dir, dir)?;
            if target.is_file() {
                if request.force {
                    tracing::debug!(
                        project = %project,
                        sync_id = %sync_id,
                        path = %dir,
                        target = %target.display(),
                        "removing file before creating directory"
                    );
                    tokio::fs::remove_file(&target).await.map_err(|err| {
                        AppError::Internal(format!(
                            "failed to remove file before dir create: {err}"
                        ))
                    })?;
                } else {
                    return Err(AppError::Conflict(format!("target is a file: {dir}")));
                }
            }
            tracing::debug!(
                project = %project,
                sync_id = %sync_id,
                path = %dir,
                target = %target.display(),
                "creating synced directory"
            );
            tokio::fs::create_dir_all(&target)
                .await
                .map_err(|err| AppError::Internal(format!("failed to create dir {dir}: {err}")))?;
        }

        for upload in session.upload_files.values() {
            let target = resolve_relative_path(&session.project_dir, &upload.entry.path)?;
            let staged =
                resolve_relative_path(&session.staging_dir.join("files"), &upload.entry.path)?;
            tracing::debug!(
                project = %project,
                sync_id = %sync_id,
                path = %upload.entry.path,
                size_bytes = upload.transfer.size_bytes,
                sha256 = %upload.transfer.sha256,
                staged = %staged.display(),
                target = %target.display(),
                "committing staged sync file"
            );
            tokio::fs::create_dir_all(
                target
                    .parent()
                    .ok_or_else(|| AppError::BadRequest("invalid target path".to_string()))?,
            )
            .await
            .map_err(|err| AppError::Internal(format!("failed to create target parent: {err}")))?;
            if target.is_dir() {
                return Err(AppError::Conflict(format!(
                    "target is a directory: {}",
                    upload.entry.path
                )));
            }
            if target.is_file() {
                tracing::trace!(
                    project = %project,
                    sync_id = %sync_id,
                    path = %upload.entry.path,
                    target = %target.display(),
                    "removing existing target file before replace"
                );
                tokio::fs::remove_file(&target).await.map_err(|err| {
                    AppError::Internal(format!("failed to replace target file: {err}"))
                })?;
            }
            tokio::fs::rename(&staged, &target).await.map_err(|err| {
                AppError::Internal(format!("failed to commit staged file: {err}"))
            })?;
            if let Some(mtime) = upload.entry.mtime_unix_ms {
                tracing::trace!(
                    project = %project,
                    sync_id = %sync_id,
                    path = %upload.entry.path,
                    mtime_unix_ms = mtime,
                    "restoring synced file mtime"
                );
                set_file_mtime(target, mtime).await?;
            }
        }

        let mut deleted_files = Vec::new();
        let mut deleted_dirs = Vec::new();
        if session.delete_extra {
            for path in &session.delete_files {
                let target = resolve_relative_path(&session.project_dir, path)?;
                if target.is_file() {
                    tracing::debug!(
                        project = %project,
                        sync_id = %sync_id,
                        path = %path,
                        target = %target.display(),
                        "deleting extra synced file"
                    );
                    tokio::fs::remove_file(&target).await.map_err(|err| {
                        AppError::Internal(format!("failed to delete file: {err}"))
                    })?;
                    deleted_files.push(path.clone());
                }
            }
            for path in &session.delete_dirs {
                let target = resolve_relative_path(&session.project_dir, path)?;
                if target.is_dir() {
                    tracing::debug!(
                        project = %project,
                        sync_id = %sync_id,
                        path = %path,
                        target = %target.display(),
                        "deleting extra synced directory"
                    );
                    tokio::fs::remove_dir(&target).await.map_err(|err| {
                        AppError::Conflict(format!("failed to delete dir {path}: {err}"))
                    })?;
                    deleted_dirs.push(path.clone());
                }
            }
        }

        let created_dirs = session.create_dirs.clone();
        let uploaded_files: Vec<String> = session.upload_files.keys().cloned().collect();
        let staging_dir = session.staging_dir.clone();
        drop(session);
        self.sessions.write().await.remove(&sync_id);
        let _ = tokio::fs::remove_dir_all(staging_dir).await;
        tracing::info!(
            project = %project,
            sync_id = %sync_id,
            uploaded_files = uploaded_files.len(),
            created_dirs = created_dirs.len(),
            deleted_files = deleted_files.len(),
            deleted_dirs = deleted_dirs.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "sync commit completed"
        );

        Ok(CommitSyncResponse {
            sync_id,
            status: "committed",
            uploaded_files,
            created_dirs,
            deleted_files,
            deleted_dirs,
        })
    }

    pub async fn abort(
        &self,
        project: String,
        sync_id: Uuid,
    ) -> Result<AbortSyncResponse, AppError> {
        tracing::debug!(
            project = %project,
            sync_id = %sync_id,
            "sync abort requested"
        );
        let session = self.session(sync_id).await?;
        {
            let session = session.lock().await;
            if session.project != project {
                tracing::debug!(
                    project = %project,
                    sync_id = %sync_id,
                    session_project = %session.project,
                    "sync abort rejected because project does not match session"
                );
                return Err(AppError::NotFound("sync session not found".to_string()));
            }
        }
        let session = self
            .sessions
            .write()
            .await
            .remove(&sync_id)
            .ok_or_else(|| AppError::NotFound("sync session not found".to_string()))?;
        let session = session.lock().await;
        let staging_dir = session.staging_dir.clone();
        drop(session);
        let _ = tokio::fs::remove_dir_all(staging_dir).await;
        tracing::info!(
            project = %project,
            sync_id = %sync_id,
            "sync session aborted"
        );
        Ok(AbortSyncResponse {
            sync_id,
            status: "aborted",
        })
    }

    pub async fn download_file(
        &self,
        project: String,
        raw_path: String,
    ) -> Result<Response, AppError> {
        let started = Instant::now();
        tracing::debug!(
            project = %project,
            raw_path = %raw_path,
            "sync download requested"
        );
        let path = normalize_sync_path(&raw_path)?;
        let project_dir = self.project_dir(&project).await?;
        let target = resolve_relative_path(&project_dir, &path)?;
        let entry = file_manifest_entry(&target, path.clone()).await?;
        let transfer = file_transfer(&entry)?;
        tracing::debug!(
            project = %project,
            path = %path,
            target = %target.display(),
            size_bytes = transfer.size_bytes,
            sha256 = %transfer.sha256,
            "opening sync download file"
        );
        let file = tokio::fs::File::open(&target)
            .await
            .map_err(|err| AppError::NotFound(format!("file not found: {err}")))?;
        let body = Body::from_stream(ReaderStream::new(file));
        let mut response = Response::new(body);
        *response.status_mut() = StatusCode::OK;
        let headers = response.headers_mut();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        headers.insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&transfer.size_bytes.to_string())
                .map_err(|err| AppError::Internal(format!("invalid content length: {err}")))?,
        );
        headers.insert(
            "x-sync-size-bytes",
            HeaderValue::from_str(&transfer.size_bytes.to_string())
                .map_err(|err| AppError::Internal(format!("invalid size header: {err}")))?,
        );
        headers.insert(
            "x-sync-mtime-unix-ms",
            HeaderValue::from_str(&transfer.mtime_unix_ms.to_string())
                .map_err(|err| AppError::Internal(format!("invalid mtime header: {err}")))?,
        );
        headers.insert(
            "x-sync-sha256",
            HeaderValue::from_str(&transfer.sha256)
                .map_err(|err| AppError::Internal(format!("invalid sha header: {err}")))?,
        );
        tracing::debug!(
            project = %project,
            path = %path,
            size_bytes = transfer.size_bytes,
            mtime_unix_ms = transfer.mtime_unix_ms,
            sha256 = %transfer.sha256,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "sync download response ready"
        );
        Ok(response)
    }

    async fn project_dir(&self, project: &str) -> Result<PathBuf, AppError> {
        let project_dir = resolve_project_dir(&self.config.workspace_root, project)?;
        tokio::fs::create_dir_all(&project_dir)
            .await
            .map_err(|err| AppError::Internal(format!("failed to create project dir: {err}")))?;
        tracing::debug!(
            project = %project,
            project_dir = %project_dir.display(),
            "sync project directory ready"
        );
        Ok(project_dir)
    }

    async fn scan_manifest(
        &self,
        project_dir: PathBuf,
        filters: SyncFilters,
    ) -> Result<ManifestResponse, AppError> {
        let max_entries = self.config.sync_max_manifest_entries;
        let max_file_bytes = self.config.sync_max_file_bytes;
        tracing::debug!(
            project_dir = %project_dir.display(),
            max_entries,
            max_file_bytes,
            "starting sync manifest scan"
        );
        tokio::task::spawn_blocking(move || {
            scan_manifest_blocking(project_dir, filters, max_entries, max_file_bytes)
        })
        .await
        .map_err(|err| AppError::Internal(format!("manifest task failed: {err}")))?
    }

    async fn session(&self, sync_id: Uuid) -> Result<Arc<AsyncMutex<PushSession>>, AppError> {
        let session = self.sessions.read().await.get(&sync_id).cloned();
        if session.is_none() {
            tracing::debug!(sync_id = %sync_id, "sync session not found");
        }
        session.ok_or_else(|| AppError::NotFound("sync session not found".to_string()))
    }

    fn staging_dir(&self, project_dir: &Path, sync_id: Uuid) -> PathBuf {
        project_dir.join(SYNC_DIR).join(sync_id.to_string())
    }

    async fn reap_expired_sessions(&self) {
        let now = Utc::now();
        let sessions: Vec<(Uuid, Arc<AsyncMutex<PushSession>>)> = self
            .sessions
            .read()
            .await
            .iter()
            .map(|(id, session)| (*id, session.clone()))
            .collect();
        tracing::trace!(
            sync_session_count = sessions.len(),
            "checking sync sessions for expiration"
        );

        for (sync_id, session) in sessions {
            let expired = session.lock().await.expires_at <= now;
            if expired && let Some(session) = self.sessions.write().await.remove(&sync_id) {
                let staging_dir = session.lock().await.staging_dir.clone();
                let _ = tokio::fs::remove_dir_all(&staging_dir).await;
                tracing::info!(
                    sync_id = %sync_id,
                    staging_dir = %staging_dir.display(),
                    "expired sync session removed"
                );
            }
        }
    }
}

impl Clone for SyncFilters {
    fn clone(&self) -> Self {
        Self {
            include: self.include.clone(),
            include_active: self.include_active,
            exclude: self.exclude.clone(),
        }
    }
}

impl SyncFilters {
    fn new(include_globs: &[String], exclude_globs: &[String]) -> Result<Self, AppError> {
        tracing::debug!(
            include_globs = ?include_globs,
            exclude_globs = ?exclude_globs,
            "building sync filters"
        );
        let mut include = GlobSetBuilder::new();
        for pattern in include_globs {
            include.add(Glob::new(pattern).map_err(|err| {
                AppError::BadRequest(format!("invalid include glob {pattern}: {err}"))
            })?);
        }

        let mut exclude = GlobSetBuilder::new();
        for pattern in exclude_globs {
            exclude.add(Glob::new(pattern).map_err(|err| {
                AppError::BadRequest(format!("invalid exclude glob {pattern}: {err}"))
            })?);
        }
        exclude.add(
            Glob::new(SYNC_DIR)
                .map_err(|err| AppError::Internal(format!("invalid internal glob: {err}")))?,
        );
        exclude.add(
            Glob::new(&format!("{SYNC_DIR}/**"))
                .map_err(|err| AppError::Internal(format!("invalid internal glob: {err}")))?,
        );

        Ok(Self {
            include: include
                .build()
                .map_err(|err| AppError::BadRequest(format!("invalid include globs: {err}")))?,
            include_active: !include_globs.is_empty(),
            exclude: exclude
                .build()
                .map_err(|err| AppError::BadRequest(format!("invalid exclude globs: {err}")))?,
        })
    }

    fn matches_entry(&self, path: &str) -> bool {
        !is_internal_path(path)
            && !self.exclude.is_match(path)
            && (!self.include_active || self.include.is_match(path))
    }

    fn should_descend(&self, path: &str) -> bool {
        !is_internal_path(path) && !self.exclude.is_match(path)
    }
}

fn scan_manifest_blocking(
    project_dir: PathBuf,
    filters: SyncFilters,
    max_entries: usize,
    max_file_bytes: u64,
) -> Result<ManifestResponse, AppError> {
    let started = Instant::now();
    let mut entries = Vec::new();
    let mut files = 0_usize;
    let mut dirs = 0_usize;
    let mut bytes = 0_u64;
    let mut skipped_by_filter = 0_usize;
    let walker = WalkDir::new(&project_dir)
        .follow_links(false)
        .sort_by_file_name()
        .into_iter()
        .filter_entry(|entry| {
            if entry.depth() == 0 {
                return true;
            }
            relative_path_from_disk(entry.path(), &project_dir)
                .map(|path| filters.should_descend(&path))
                .unwrap_or(true)
        });

    for entry in walker {
        let entry =
            entry.map_err(|err| AppError::Internal(format!("failed to walk project: {err}")))?;
        if entry.depth() == 0 {
            continue;
        }
        let path = relative_path_from_disk(entry.path(), &project_dir)?;
        if !filters.matches_entry(&path) {
            skipped_by_filter += 1;
            tracing::trace!(
                project_dir = %project_dir.display(),
                path = %path,
                "manifest entry skipped by filters"
            );
            continue;
        }
        if entries.len() >= max_entries {
            return Err(AppError::BadRequest(format!(
                "manifest exceeds sync_max_manifest_entries ({max_entries})"
            )));
        }
        let file_type = entry.file_type();
        if file_type.is_symlink() {
            return Err(AppError::BadRequest(format!(
                "symlink is not supported: {path}"
            )));
        }
        if file_type.is_dir() {
            let metadata = entry
                .metadata()
                .map_err(|err| AppError::Internal(format!("failed to read dir metadata: {err}")))?;
            dirs += 1;
            tracing::trace!(
                project_dir = %project_dir.display(),
                path = %path,
                "manifest directory accepted"
            );
            entries.push(ManifestEntry {
                path,
                kind: ManifestEntryKind::Dir,
                size_bytes: None,
                mtime_unix_ms: Some(system_time_to_ms(metadata.modified().unwrap_or(UNIX_EPOCH))),
                sha256: None,
            });
        } else if file_type.is_file() {
            let metadata = entry.metadata().map_err(|err| {
                AppError::Internal(format!("failed to read file metadata: {err}"))
            })?;
            let size = metadata.len();
            if size > max_file_bytes {
                return Err(AppError::BadRequest(format!(
                    "file exceeds sync_max_file_bytes: {path}"
                )));
            }
            files += 1;
            bytes = bytes.saturating_add(size);
            tracing::trace!(
                project_dir = %project_dir.display(),
                path = %path,
                size_bytes = size,
                "manifest file accepted"
            );
            entries.push(ManifestEntry {
                path: path.clone(),
                kind: ManifestEntryKind::File,
                size_bytes: Some(size),
                mtime_unix_ms: Some(system_time_to_ms(metadata.modified().unwrap_or(UNIX_EPOCH))),
                sha256: Some(hash_file_blocking(entry.path())?),
            });
        } else {
            return Err(AppError::BadRequest(format!(
                "unsupported filesystem entry: {path}"
            )));
        }
    }

    entries.sort_by(|left, right| left.path.cmp(&right.path));
    tracing::debug!(
        project_dir = %project_dir.display(),
        entries = entries.len(),
        files,
        dirs,
        bytes,
        skipped_by_filter,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "sync manifest scan completed"
    );
    Ok(ManifestResponse { entries })
}

fn validate_client_manifest(
    entries: Vec<ManifestEntry>,
    filters: &SyncFilters,
    max_entries: usize,
    max_file_bytes: u64,
) -> Result<Vec<ManifestEntry>, AppError> {
    let input_entries = entries.len();
    tracing::debug!(
        input_entries,
        max_entries,
        max_file_bytes,
        "validating client sync manifest"
    );
    if entries.len() > max_entries {
        return Err(AppError::BadRequest(format!(
            "manifest exceeds sync_max_manifest_entries ({max_entries})"
        )));
    }

    let mut seen = HashSet::new();
    let mut validated = Vec::new();
    let mut skipped_by_filter = 0_usize;
    let mut files = 0_usize;
    let mut dirs = 0_usize;
    let mut bytes = 0_u64;
    for mut entry in entries {
        entry.path = normalize_sync_path(&entry.path)?;
        if !seen.insert(entry.path.clone()) {
            return Err(AppError::BadRequest(format!(
                "duplicate manifest path: {}",
                entry.path
            )));
        }
        if !filters.matches_entry(&entry.path) {
            skipped_by_filter += 1;
            tracing::trace!(
                path = %entry.path,
                "client manifest entry skipped by filters"
            );
            continue;
        }
        match entry.kind {
            ManifestEntryKind::File => {
                let size = entry.size_bytes.ok_or_else(|| {
                    AppError::BadRequest(format!("file entry missing size: {}", entry.path))
                })?;
                if size > max_file_bytes {
                    return Err(AppError::BadRequest(format!(
                        "file exceeds sync_max_file_bytes: {}",
                        entry.path
                    )));
                }
                files += 1;
                bytes = bytes.saturating_add(size);
                entry.mtime_unix_ms.ok_or_else(|| {
                    AppError::BadRequest(format!("file entry missing mtime: {}", entry.path))
                })?;
                let sha = entry.sha256.as_ref().ok_or_else(|| {
                    AppError::BadRequest(format!("file entry missing sha256: {}", entry.path))
                })?;
                if !is_sha256_hex(sha) {
                    return Err(AppError::BadRequest(format!(
                        "file entry has invalid sha256: {}",
                        entry.path
                    )));
                }
            }
            ManifestEntryKind::Dir => {
                entry.size_bytes = None;
                entry.sha256 = None;
                dirs += 1;
            }
        }
        validated.push(entry);
    }
    validated.sort_by(|left, right| left.path.cmp(&right.path));
    tracing::debug!(
        input_entries,
        accepted_entries = validated.len(),
        skipped_by_filter,
        files,
        dirs,
        bytes,
        "client sync manifest validated"
    );
    Ok(validated)
}

fn manifest_map(entries: Vec<ManifestEntry>) -> HashMap<String, ManifestEntry> {
    entries
        .into_iter()
        .map(|entry| (entry.path.clone(), entry))
        .collect()
}

fn same_file(left: &ManifestEntry, right: &ManifestEntry) -> bool {
    left.kind == ManifestEntryKind::File
        && right.kind == ManifestEntryKind::File
        && left.size_bytes == right.size_bytes
        && left.mtime_unix_ms == right.mtime_unix_ms
        && left.sha256 == right.sha256
}

fn file_transfer(entry: &ManifestEntry) -> Result<FileTransfer, AppError> {
    if entry.kind != ManifestEntryKind::File {
        return Err(AppError::BadRequest(format!("not a file: {}", entry.path)));
    }
    Ok(FileTransfer {
        path: entry.path.clone(),
        size_bytes: entry.size_bytes.ok_or_else(|| {
            AppError::BadRequest(format!("file entry missing size: {}", entry.path))
        })?,
        mtime_unix_ms: entry.mtime_unix_ms.ok_or_else(|| {
            AppError::BadRequest(format!("file entry missing mtime: {}", entry.path))
        })?,
        sha256: entry.sha256.clone().ok_or_else(|| {
            AppError::BadRequest(format!("file entry missing sha256: {}", entry.path))
        })?,
    })
}

async fn write_body_to_file(
    mut body: Body,
    tmp_path: &Path,
    expected_size: u64,
    expected_sha256: &str,
    max_file_bytes: u64,
) -> Result<(), AppError> {
    let started = Instant::now();
    tracing::debug!(
        tmp_path = %tmp_path.display(),
        expected_size,
        expected_sha256,
        max_file_bytes,
        "streaming sync upload body to staging file"
    );
    if expected_size > max_file_bytes {
        return Err(AppError::BadRequest(
            "file exceeds sync_max_file_bytes".to_string(),
        ));
    }
    let mut file = tokio::fs::File::create(tmp_path)
        .await
        .map_err(|err| AppError::Internal(format!("failed to create staged file: {err}")))?;
    let mut hasher = Sha256::new();
    let mut written = 0_u64;
    let mut chunks = 0_u64;

    while let Some(frame) = body.frame().await {
        let frame = frame
            .map_err(|err| AppError::BadRequest(format!("failed to read request body: {err}")))?;
        let Some(data) = frame.data_ref() else {
            continue;
        };
        chunks += 1;
        written += data.len() as u64;
        tracing::trace!(
            tmp_path = %tmp_path.display(),
            chunk_bytes = data.len(),
            written,
            chunks,
            "received sync upload body chunk"
        );
        if written > max_file_bytes {
            return Err(AppError::BadRequest(
                "file exceeds sync_max_file_bytes".to_string(),
            ));
        }
        hasher.update(data);
        file.write_all(data)
            .await
            .map_err(|err| AppError::Internal(format!("failed to write staged file: {err}")))?;
    }
    file.flush()
        .await
        .map_err(|err| AppError::Internal(format!("failed to flush staged file: {err}")))?;

    if written != expected_size {
        tracing::debug!(
            tmp_path = %tmp_path.display(),
            expected_size,
            actual_size = written,
            "sync upload size mismatch"
        );
        return Err(AppError::BadRequest(format!(
            "size mismatch: expected {expected_size}, got {written}"
        )));
    }
    let digest = hasher.finalize();
    let actual = hex_digest(&digest);
    if actual != expected_sha256 {
        tracing::debug!(
            tmp_path = %tmp_path.display(),
            expected_sha256,
            actual_sha256 = %actual,
            "sync upload sha256 mismatch"
        );
        return Err(AppError::BadRequest(format!(
            "sha256 mismatch: expected {expected_sha256}, got {actual}"
        )));
    }
    tracing::debug!(
        tmp_path = %tmp_path.display(),
        bytes = written,
        chunks,
        sha256 = %actual,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "sync upload body staged"
    );
    Ok(())
}

async fn file_manifest_entry(path: &Path, rel_path: String) -> Result<ManifestEntry, AppError> {
    let path = path.to_path_buf();
    tracing::debug!(
        path = %path.display(),
        rel_path = %rel_path,
        "reading sync file metadata"
    );
    tokio::task::spawn_blocking(move || {
        let metadata = std::fs::metadata(&path)
            .map_err(|err| AppError::NotFound(format!("file not found: {err}")))?;
        if !metadata.is_file() {
            return Err(AppError::BadRequest("path is not a file".to_string()));
        }
        Ok(ManifestEntry {
            path: rel_path,
            kind: ManifestEntryKind::File,
            size_bytes: Some(metadata.len()),
            mtime_unix_ms: Some(system_time_to_ms(metadata.modified().unwrap_or(UNIX_EPOCH))),
            sha256: Some(hash_file_blocking(&path)?),
        })
    })
    .await
    .map_err(|err| AppError::Internal(format!("file metadata task failed: {err}")))?
}

async fn ensure_baseline_matches(
    target: &Path,
    baseline: Option<&ManifestEntry>,
    force: bool,
) -> Result<(), AppError> {
    let baseline_path = baseline.map(|entry| entry.path.as_str()).unwrap_or("<new>");
    tracing::debug!(
        target = %target.display(),
        baseline_path,
        force,
        "checking sync baseline"
    );
    if force {
        tracing::debug!(
            target = %target.display(),
            baseline_path,
            "sync baseline check skipped because force=true"
        );
        return Ok(());
    }
    let target = target.to_path_buf();
    let baseline = baseline.cloned();
    tokio::task::spawn_blocking(move || match baseline {
        None => {
            if target.exists() {
                Err(AppError::Conflict(format!(
                    "target changed since plan: {}",
                    target.display()
                )))
            } else {
                Ok(())
            }
        }
        Some(entry) => match entry.kind {
            ManifestEntryKind::Dir => {
                if target.is_dir() {
                    Ok(())
                } else {
                    Err(AppError::Conflict(format!(
                        "target changed since plan: {}",
                        entry.path
                    )))
                }
            }
            ManifestEntryKind::File => {
                if !target.is_file() {
                    return Err(AppError::Conflict(format!(
                        "target changed since plan: {}",
                        entry.path
                    )));
                }
                let current_sha = hash_file_blocking(&target)?;
                if Some(current_sha) == entry.sha256 {
                    Ok(())
                } else {
                    Err(AppError::Conflict(format!(
                        "target changed since plan: {}",
                        entry.path
                    )))
                }
            }
        },
    })
    .await
    .map_err(|err| AppError::Internal(format!("baseline check task failed: {err}")))?
}

async fn set_file_mtime(path: PathBuf, mtime_unix_ms: i64) -> Result<(), AppError> {
    tracing::trace!(
        path = %path.display(),
        mtime_unix_ms,
        "setting synced file mtime"
    );
    tokio::task::spawn_blocking(move || {
        let seconds = mtime_unix_ms.div_euclid(1_000);
        let nanos = (mtime_unix_ms.rem_euclid(1_000) * 1_000_000) as u32;
        let mtime = FileTime::from_unix_time(seconds, nanos);
        filetime::set_file_mtime(path, mtime)
            .map_err(|err| AppError::Internal(format!("failed to set file mtime: {err}")))
    })
    .await
    .map_err(|err| AppError::Internal(format!("mtime task failed: {err}")))?
}

fn normalize_sync_path(raw: &str) -> Result<String, AppError> {
    let normalized = normalize_sync_path_allow_internal(raw)?;
    if is_internal_path(&normalized) {
        return Err(AppError::BadRequest(
            "internal sync path is reserved".to_string(),
        ));
    }
    Ok(normalized)
}

fn normalize_sync_path_allow_internal(raw: &str) -> Result<String, AppError> {
    if raw.is_empty()
        || raw == "."
        || raw == ".."
        || raw.starts_with('/')
        || raw.starts_with('\\')
        || raw.contains('\\')
        || raw.contains(':')
        || raw.contains('\0')
    {
        return Err(AppError::BadRequest("invalid sync path".to_string()));
    }

    let mut parts = Vec::new();
    for part in raw.split('/') {
        if part.is_empty() || part == "." || part == ".." {
            return Err(AppError::BadRequest("invalid sync path".to_string()));
        }
        parts.push(part);
    }
    Ok(parts.join("/"))
}

fn resolve_relative_path(root: &Path, normalized_path: &str) -> Result<PathBuf, AppError> {
    let normalized_path = normalize_sync_path(normalized_path)?;
    let mut path = root.to_path_buf();
    for part in normalized_path.split('/') {
        path.push(part);
    }
    Ok(path)
}

fn relative_path_from_disk(path: &Path, root: &Path) -> Result<String, AppError> {
    let rel = path.strip_prefix(root).map_err(|err| {
        AppError::Internal(format!("failed to derive relative manifest path: {err}"))
    })?;
    let mut parts = Vec::new();
    for component in rel.components() {
        match component {
            Component::Normal(part) => {
                let Some(part) = part.to_str() else {
                    return Err(AppError::BadRequest(
                        "non-UTF-8 filesystem path is not supported".to_string(),
                    ));
                };
                parts.push(part.to_string());
            }
            _ => {
                return Err(AppError::BadRequest(
                    "invalid filesystem path in project".to_string(),
                ));
            }
        }
    }
    normalize_sync_path_allow_internal(&parts.join("/"))
}

fn is_internal_path(path: &str) -> bool {
    path == SYNC_DIR || path.starts_with(&format!("{SYNC_DIR}/"))
}

fn hash_file_blocking(path: &Path) -> Result<String, AppError> {
    let file = File::open(path)
        .map_err(|err| AppError::Internal(format!("failed to open file for hashing: {err}")))?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let len = reader
            .read(&mut buffer)
            .map_err(|err| AppError::Internal(format!("failed to hash file: {err}")))?;
        if len == 0 {
            break;
        }
        hasher.update(&buffer[..len]);
    }
    let digest = hasher.finalize();
    Ok(hex_digest(&digest))
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn system_time_to_ms(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().min(i64::MAX as u128) as i64,
        Err(_) => 0,
    }
}

fn sort_paths(paths: &mut Vec<String>) {
    paths.sort();
    paths.dedup();
}

fn sort_paths_deepest_first(paths: &mut Vec<String>) {
    paths.sort_by(|left, right| {
        right
            .matches('/')
            .count()
            .cmp(&left.matches('/').count())
            .then_with(|| left.cmp(right))
    });
    paths.dedup();
}

#[derive(Debug, Default)]
struct ManifestStats {
    files: usize,
    dirs: usize,
    bytes: u64,
}

fn manifest_stats(entries: &[ManifestEntry]) -> ManifestStats {
    let mut stats = ManifestStats::default();
    for entry in entries {
        match entry.kind {
            ManifestEntryKind::File => {
                stats.files += 1;
                stats.bytes = stats.bytes.saturating_add(entry.size_bytes.unwrap_or(0));
            }
            ManifestEntryKind::Dir => {
                stats.dirs += 1;
            }
        }
    }
    stats
}

fn sample_string_paths(paths: &[String]) -> Vec<&str> {
    paths
        .iter()
        .take(LOG_PATH_SAMPLE_LIMIT)
        .map(String::as_str)
        .collect()
}

fn sample_transfer_paths(transfers: &[FileTransfer]) -> Vec<&str> {
    transfers
        .iter()
        .take(LOG_PATH_SAMPLE_LIMIT)
        .map(|transfer| transfer.path.as_str())
        .collect()
}

fn omitted_count(len: usize) -> usize {
    len.saturating_sub(LOG_PATH_SAMPLE_LIMIT)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, size: u64, sha: &str) -> ManifestEntry {
        ManifestEntry {
            path: path.to_string(),
            kind: ManifestEntryKind::File,
            size_bytes: Some(size),
            mtime_unix_ms: Some(1),
            sha256: Some(sha.to_string()),
        }
    }

    #[test]
    fn rejects_unsafe_sync_paths() {
        for path in [
            "",
            ".",
            "..",
            "../x",
            "/x",
            r"a\b",
            "a:b",
            "a//b",
            ".vivado-server-sync/file",
        ] {
            assert!(normalize_sync_path(path).is_err(), "{path}");
        }
        assert_eq!(normalize_sync_path("src/top.tcl").unwrap(), "src/top.tcl");
    }

    #[test]
    fn filters_entries_with_include_and_exclude_globs() {
        let filters = SyncFilters::new(&["src/**".to_string()], &["**/*.tmp".to_string()]).unwrap();
        assert!(filters.matches_entry("src/top.tcl"));
        assert!(!filters.matches_entry("src/cache.tmp"));
        assert!(!filters.matches_entry("docs/readme.md"));
        assert!(!filters.matches_entry(".vivado-server-sync/state"));
    }

    #[test]
    fn validates_client_manifest() {
        let filters = SyncFilters::new(&[], &[]).unwrap();
        let entries = validate_client_manifest(
            vec![file(
                "a.txt",
                3,
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            )],
            &filters,
            10,
            10,
        )
        .unwrap();
        assert_eq!(entries.len(), 1);
    }
}
