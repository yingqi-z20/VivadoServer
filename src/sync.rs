//! Linux sync operations. Workflows own project exclusivity and dirty markers;
//! this manager owns accepted filesystem operations until they settle.
mod files;
mod model;
mod transaction;
use crate::{
    config::RuntimeConfig,
    error::AppError,
    workspace::{
        ensure_path_has_no_links, ensure_safe_project_root, metadata_is_link, resolve_project_dir,
        validate_portable_segment,
    },
};
use axum::{
    body::Body,
    http::{
        HeaderValue, StatusCode,
        header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG},
    },
    response::Response,
};
use chrono::Utc;
use files::*;
use filetime::FileTime;
use globset::{Glob, GlobSet, GlobSetBuilder};
use http_body_util::BodyExt;
pub use model::*;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{File, Metadata},
    future::Future,
    io::{BufReader, Read, Seek, SeekFrom},
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::AsyncWriteExt,
    sync::{Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, RwLock, Semaphore},
    time,
};
use tokio_util::{io::ReaderStream, sync::CancellationToken, task::TaskTracker};
use transaction::CommitTransaction;
use uuid::Uuid;
use walkdir::WalkDir;
const REAPER_INTERVAL: Duration = Duration::from_secs(60);
const MAX_GLOBS: usize = 256;
const MAX_GLOB_BYTES: usize = 4 * 1024;
pub(crate) const MAX_RETAINED_SYNC_RESULTS: usize = 128;

#[derive(Clone)]
pub struct SyncManager {
    config: Arc<RuntimeConfig>,
    sessions: Arc<RwLock<HashMap<Uuid, Arc<SyncSession>>>>,
    tasks: TaskTracker,
    admission: Arc<AsyncMutex<()>>,
    creating: Arc<AsyncMutex<()>>,
    closed: Arc<AtomicBool>,
    slot: Arc<Semaphore>,
}
struct SyncSession {
    data: AsyncMutex<PushSession>,
    activity: RwLock<()>,
    cancel_uploads: CancellationToken,
    notify: Notify,
    cleanup: AsyncMutex<()>,
}
struct PushSession {
    state: SyncSessionStatus,
    project: String,
    plan: CommitPlan,
    expires_deadline: Instant,
    finished_at: Option<Instant>,
    uploaded_files: HashSet<String>,
    result: Option<CommitSyncResponse>,
    error: Option<AppError>,
    cleanup_pending: bool,
    active_permit: Option<OwnedSemaphorePermit>,
}
impl PushSession {
    fn release_plan(&mut self) {
        // Retained results need the outcome and cleanup path, not copies of
        // every input/server manifest for the entire result-retention window.
        self.plan.upload_files.clear();
        self.plan.baseline.clear();
        self.plan.create_dirs = Vec::new();
        self.plan.delete_files = Vec::new();
        self.plan.delete_dirs = Vec::new();
        self.uploaded_files = HashSet::new();
        self.active_permit.take();
    }
}
#[derive(Clone)]
struct PlannedUpload {
    entry: ManifestEntry,
    transfer: FileTransfer,
}
#[derive(Clone)]
struct CommitPlan {
    project_dir: PathBuf,
    staging_dir: PathBuf,
    reset: bool,
    upload_files: BTreeMap<String, PlannedUpload>,
    create_dirs: Vec<String>,
    delete_files: Vec<String>,
    delete_dirs: Vec<String>,
    baseline: BTreeMap<String, ManifestEntry>,
}
fn ensure_open(data: &PushSession) -> Result<(), AppError> {
    if data.state == SyncSessionStatus::Open {
        Ok(())
    } else {
        Err(AppError::Conflict(format!(
            "sync session is {:?}",
            data.state
        )))
    }
}
fn commit_result(data: &PushSession) -> Result<CommitSyncResponse, AppError> {
    if let Some(result) = &data.result {
        return Ok(result.clone());
    }
    if let Some(error) = &data.error {
        return Err(error.clone());
    }
    Err(AppError::Conflict(format!(
        "sync session is {:?}",
        data.state
    )))
}
impl SyncManager {
    pub(crate) fn new(config: RuntimeConfig) -> Self {
        Self {
            config: Arc::new(config),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            tasks: TaskTracker::new(),
            admission: Arc::new(AsyncMutex::new(())),
            creating: Arc::new(AsyncMutex::new(())),
            closed: Arc::new(AtomicBool::new(false)),
            slot: Arc::new(Semaphore::new(1)),
        }
    }
    /// A disconnected observer never cancels accepted filesystem work. The gate
    /// prevents shutdown from missing a task accepted concurrently with draining.
    async fn tracked<T: Send + 'static>(
        &self,
        work: impl Future<Output = Result<T, AppError>> + Send + 'static,
    ) -> Result<T, AppError> {
        let gate = self.admission.lock().await;
        if self.closed.load(Ordering::Acquire) {
            return Err(AppError::Conflict(
                "sync manager is shutting down".to_string(),
            ));
        }
        let task = self.tasks.spawn(work);
        drop(gate);
        task.await.map_err(|error| {
            AppError::ProjectReuploadRequired(format!("sync task did not settle: {error}"))
        })?
    }
    pub(crate) fn spawn_session_reaper(
        &self,
        cancellation: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let manager = self.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(REAPER_INTERVAL);
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    _ = interval.tick() => {
                        let worker = manager.clone();
                        let _ = manager.tracked(async move { worker.reap().await; Ok(()) }).await;
                    }
                }
            }
        })
    }
    pub async fn shutdown(&self) {
        {
            let _gate = self.admission.lock().await;
            self.closed.store(true, Ordering::Release);
            self.tasks.close();
        }
        let sessions: Vec<_> = self.sessions.read().await.values().cloned().collect();
        for session in sessions {
            session.cancel_uploads.cancel();
        }
        self.tasks.wait().await;
        // Re-snapshot after creators finish, including those whose HTTP caller left.
        let sessions: Vec<_> = self.sessions.read().await.values().cloned().collect();
        for session in sessions {
            let _ = self
                .abort_session(session, SyncSessionStatus::Aborted)
                .await;
        }
    }
    /// Startup discards scratch data without replaying or restoring a transaction.
    pub async fn cleanup_orphans(&self) -> Result<(), AppError> {
        let manager = self.clone();
        self.tracked(async move {
            let _creating = manager.creating.lock().await;
            if manager.slot.available_permits() == 0 {
                return Err(AppError::Conflict(
                    "an active plan owns staging".to_string(),
                ));
            }
            let root = private_staging_root(&manager.config.workspace_root).await?;
            let mut entries = tokio::fs::read_dir(&root).await.map_err(internal_io)?;
            while let Some(entry) = entries.next_entry().await.map_err(internal_io)? {
                remove_private_entry(&root, &entry.path()).await?;
            }
            Ok(())
        })
        .await
    }
    pub async fn manifest(
        &self,
        project: String,
        request: ManifestRequest,
    ) -> Result<ManifestResponse, AppError> {
        self.scan_manifest(
            resolve_project_dir(&self.config.workspace_root, &project)?,
            SyncFilters::new(&request.include_globs, &request.exclude_globs)?,
        )
        .await
    }
    pub async fn push_plan(
        &self,
        project: String,
        request: PushPlanRequest,
    ) -> Result<PushPlanResponse, AppError> {
        let manager = self.clone();
        self.tracked(async move { manager.create_plan(project, request, false).await })
            .await
    }
    pub async fn push_reset_plan(
        &self,
        project: String,
        request: PushPlanRequest,
    ) -> Result<PushPlanResponse, AppError> {
        let manager = self.clone();
        self.tracked(async move { manager.create_plan(project, request, true).await })
            .await
    }
    async fn create_plan(
        &self,
        project: String,
        request: PushPlanRequest,
        reset: bool,
    ) -> Result<PushPlanResponse, AppError> {
        let _creating = self.creating.lock().await;
        self.prune_results().await;
        let permit = self
            .slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| AppError::Conflict("an open sync plan already exists".to_string()))?;
        if reset && (!request.include_globs.is_empty() || !request.exclude_globs.is_empty()) {
            return Err(AppError::BadRequest(
                "full replacement does not accept filters".to_string(),
            ));
        }
        let filters = SyncFilters::new(&request.include_globs, &request.exclude_globs)?;
        let client = manifest_map(validate_client_manifest(
            request.entries,
            &filters,
            self.config.sync_max_manifest_entries,
            self.config.sync_max_file_bytes,
        )?);
        let project_dir = resolve_project_dir(&self.config.workspace_root, &project)?;
        let server = if reset {
            BTreeMap::new()
        } else {
            manifest_map(
                self.scan_manifest(project_dir.clone(), filters)
                    .await?
                    .entries,
            )
        };
        let mut plan = CommitPlan {
            project_dir,
            staging_dir: PathBuf::new(),
            reset,
            upload_files: BTreeMap::new(),
            create_dirs: Vec::new(),
            delete_files: Vec::new(),
            delete_dirs: Vec::new(),
            baseline: server,
        };
        for entry in client.values() {
            let old = plan.baseline.get(&entry.path);
            match entry.kind {
                ManifestEntryKind::Dir => {
                    if old.is_none_or(|old| old.kind != ManifestEntryKind::Dir) {
                        plan.create_dirs.push(entry.path.clone());
                    }
                    if old.is_some_and(|old| old.kind == ManifestEntryKind::File) {
                        plan.delete_files.push(entry.path.clone());
                    }
                }
                ManifestEntryKind::File => {
                    if old.is_none_or(|old| !same_file(entry, old)) {
                        plan.upload_files.insert(
                            entry.path.clone(),
                            PlannedUpload {
                                entry: entry.clone(),
                                transfer: file_transfer(entry)?,
                            },
                        );
                    }
                    if old.is_some_and(|old| old.kind == ManifestEntryKind::Dir) {
                        let prefix = format!("{}/", entry.path);
                        // Removing this subtree is necessary for the explicit
                        // type replacement, independent of unrelated extra files.
                        for child in plan
                            .baseline
                            .values()
                            .filter(|child| child.path.starts_with(&prefix))
                        {
                            match child.kind {
                                ManifestEntryKind::File => {
                                    plan.delete_files.push(child.path.clone())
                                }
                                ManifestEntryKind::Dir => plan.delete_dirs.push(child.path.clone()),
                            }
                        }
                        plan.delete_dirs.push(entry.path.clone());
                    }
                }
            }
        }
        if request.delete_extra && !reset {
            for old in plan
                .baseline
                .values()
                .filter(|old| !client.contains_key(&old.path))
            {
                match old.kind {
                    ManifestEntryKind::File => plan.delete_files.push(old.path.clone()),
                    ManifestEntryKind::Dir => plan.delete_dirs.push(old.path.clone()),
                }
            }
        }
        sort_paths(&mut plan.create_dirs);
        sort_paths(&mut plan.delete_files);
        sort_paths_deepest_first(&mut plan.delete_dirs);
        let sync_id = Uuid::new_v4();
        let expires_at = Utc::now()
            + chrono::Duration::from_std(self.config.sync_session_ttl())
                .map_err(|error| AppError::Internal(format!("invalid sync ttl: {error}")))?;
        plan.staging_dir = create_staging_dir(&self.config.workspace_root, sync_id).await?;
        if let Err(error) = ensure_same_filesystem(
            &self.config.workspace_root,
            &plan.staging_dir,
            &plan.project_dir,
        )
        .await
        {
            let _ = remove_staging_dir(&self.config.workspace_root, &plan.staging_dir).await;
            return Err(error);
        }
        let response = PushPlanResponse {
            sync_id,
            upload_files: plan
                .upload_files
                .values()
                .map(|u| u.transfer.clone())
                .collect(),
            create_dirs: plan.create_dirs.clone(),
            delete_files: plan.delete_files.clone(),
            delete_dirs: plan.delete_dirs.clone(),
            expires_at,
        };
        self.sessions.write().await.insert(
            sync_id,
            Arc::new(SyncSession {
                data: AsyncMutex::new(PushSession {
                    state: SyncSessionStatus::Open,
                    project,
                    plan,
                    expires_deadline: Instant::now() + self.config.sync_session_ttl(),
                    finished_at: None,
                    uploaded_files: HashSet::new(),
                    result: None,
                    error: None,
                    cleanup_pending: false,
                    active_permit: Some(permit),
                }),
                activity: RwLock::new(()),
                cancel_uploads: CancellationToken::new(),
                notify: Notify::new(),
                cleanup: AsyncMutex::new(()),
            }),
        );
        Ok(response)
    }
    pub async fn pull_plan(
        &self,
        project: String,
        request: PullPlanRequest,
    ) -> Result<PullPlanResponse, AppError> {
        let filters = SyncFilters::new(&request.include_globs, &request.exclude_globs)?;
        let client = manifest_map(validate_client_manifest(
            request.entries,
            &filters,
            self.config.sync_max_manifest_entries,
            self.config.sync_max_file_bytes,
        )?);
        let server = manifest_map(
            self.scan_manifest(
                resolve_project_dir(&self.config.workspace_root, &project)?,
                filters,
            )
            .await?
            .entries,
        );
        let mut response = PullPlanResponse {
            download_files: Vec::new(),
            create_dirs: Vec::new(),
            delete_files: Vec::new(),
            delete_dirs: Vec::new(),
        };
        for entry in server.values() {
            let old = client.get(&entry.path);
            match entry.kind {
                ManifestEntryKind::Dir => {
                    if old.is_none_or(|old| old.kind != ManifestEntryKind::Dir) {
                        response.create_dirs.push(entry.path.clone());
                    }
                    if old.is_some_and(|old| old.kind == ManifestEntryKind::File) {
                        response.delete_files.push(entry.path.clone());
                    }
                }
                ManifestEntryKind::File => {
                    if old.is_none_or(|old| !same_file(entry, old)) {
                        response.download_files.push(file_transfer(entry)?);
                    }
                    if old.is_some_and(|old| old.kind == ManifestEntryKind::Dir) {
                        let prefix = format!("{}/", entry.path);
                        // Removing this subtree is necessary for the explicit
                        // type replacement, independent of unrelated extra files.
                        for child in client
                            .values()
                            .filter(|child| child.path.starts_with(&prefix))
                        {
                            match child.kind {
                                ManifestEntryKind::File => {
                                    response.delete_files.push(child.path.clone())
                                }
                                ManifestEntryKind::Dir => {
                                    response.delete_dirs.push(child.path.clone())
                                }
                            }
                        }
                        response.delete_dirs.push(entry.path.clone());
                    }
                }
            }
        }
        if request.delete_extra {
            for old in client
                .values()
                .filter(|old| !server.contains_key(&old.path))
            {
                match old.kind {
                    ManifestEntryKind::File => response.delete_files.push(old.path.clone()),
                    ManifestEntryKind::Dir => response.delete_dirs.push(old.path.clone()),
                }
            }
        }
        sort_paths(&mut response.create_dirs);
        sort_paths(&mut response.delete_files);
        sort_paths_deepest_first(&mut response.delete_dirs);
        response.download_files.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(response)
    }
    pub async fn upload_file(
        &self,
        project: String,
        sync_id: Uuid,
        raw_path: String,
        content_length: Option<u64>,
        body: Body,
    ) -> Result<UploadResponse, AppError> {
        let manager = self.clone();
        self.tracked(async move {
            manager
                .receive_upload(project, sync_id, raw_path, content_length, body)
                .await
        })
        .await
    }
    async fn receive_upload(
        &self,
        project: String,
        sync_id: Uuid,
        raw_path: String,
        content_length: Option<u64>,
        body: Body,
    ) -> Result<UploadResponse, AppError> {
        let path = normalize_sync_path(&raw_path)?;
        let session = self.session(sync_id, &project).await?;
        let _activity = session.activity.read().await;
        let (expected, staging_dir) = {
            let data = session.data.lock().await;
            ensure_open(&data)?;
            if Instant::now() >= data.expires_deadline {
                return Err(AppError::NotFound("sync session expired".to_string()));
            }
            let expected = data.plan.upload_files.get(&path).cloned().ok_or_else(|| {
                AppError::BadRequest("file was not requested by the push plan".to_string())
            })?;
            (expected, data.plan.staging_dir.clone())
        };
        if let Some(length) = content_length {
            if length > expected.transfer.size_bytes {
                return Err(AppError::PayloadTooLarge);
            }
            if length != expected.transfer.size_bytes {
                return Err(AppError::BadRequest(
                    "Content-Length does not match plan".to_string(),
                ));
            }
        }
        ensure_path_has_no_links(staging_dir.clone(), format!("files/{path}")).await?;
        let target = resolve_relative_path(&staging_dir.join("files"), &path)?;
        tokio::fs::create_dir_all(
            target
                .parent()
                .ok_or_else(|| AppError::BadRequest("invalid upload path".to_string()))?,
        )
        .await
        .map_err(internal_io)?;
        let tmp = staging_dir
            .join("tmp")
            .join(format!("{}.part", Uuid::new_v4()));
        let mut guard = TempPathGuard::new(tmp.clone());
        tokio::select! {
            biased;
            _ = session.cancel_uploads.cancelled() => return Err(AppError::Conflict("sync upload was cancelled".to_string())),
            result = write_body_to_file(body, &tmp, expected.transfer.size_bytes, &expected.transfer.sha256,
                self.config.sync_max_file_bytes, self.config.upload_idle_timeout(), self.config.upload_deadline()) => result?,
        }
        ensure_path_has_no_links(staging_dir, format!("files/{path}")).await?;
        atomic_replace(tmp, target).await?;
        guard.disarm();
        let mut data = session.data.lock().await;
        ensure_open(&data)?;
        data.uploaded_files.insert(path.clone());
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
        _request: CommitSyncRequest,
    ) -> Result<CommitSyncResponse, AppError> {
        let manager = self.clone();
        self.tracked(async move { manager.commit_session(project, sync_id).await })
            .await
    }
    async fn commit_session(
        &self,
        project: String,
        sync_id: Uuid,
    ) -> Result<CommitSyncResponse, AppError> {
        let session = self.session(sync_id, &project).await?;
        let plan = {
            let activity = session.activity.write().await;
            let mut data = session.data.lock().await;
            match data.state {
                SyncSessionStatus::Committing => None,
                SyncSessionStatus::Open => {
                    if Instant::now() >= data.expires_deadline {
                        drop(data);
                        drop(activity);
                        self.abort_session(session.clone(), SyncSessionStatus::Expired)
                            .await?;
                        return Err(AppError::NotFound("sync session expired".to_string()));
                    }
                    if data
                        .plan
                        .upload_files
                        .keys()
                        .any(|path| !data.uploaded_files.contains(path))
                    {
                        return Err(AppError::Conflict(
                            "planned uploads are missing".to_string(),
                        ));
                    }
                    data.state = SyncSessionStatus::Committing;
                    Some(data.plan.clone())
                }
                _ => return commit_result(&data),
            }
        };
        if let Some(plan) = plan {
            let worker = self.clone();
            let owned_plan = plan.clone();
            let result =
                tokio::spawn(async move { worker.apply_commit(sync_id, &owned_plan).await })
                    .await
                    .unwrap_or_else(|error| {
                        Err(AppError::ProjectReuploadRequired(format!(
                            "commit task did not settle: {error}"
                        )))
                    });
            {
                let mut data = session.data.lock().await;
                match &result {
                    Ok(response) => {
                        data.state = SyncSessionStatus::Committed;
                        data.result = Some(response.clone());
                    }
                    Err(error) => {
                        data.state = SyncSessionStatus::Failed;
                        data.error = Some(error.clone());
                    }
                }
                data.finished_at = Some(Instant::now());
                data.cleanup_pending = true;
                data.release_plan();
            }
            self.cleanup_session(&session).await;
            session.notify.notify_waiters();
            return result;
        }
        loop {
            let notified = session.notify.notified();
            let data = session.data.lock().await;
            if data.state != SyncSessionStatus::Committing {
                return commit_result(&data);
            }
            drop(data);
            notified.await;
        }
    }
    async fn apply_commit(
        &self,
        sync_id: Uuid,
        plan: &CommitPlan,
    ) -> Result<CommitSyncResponse, AppError> {
        self.check_project_root(&plan.project_dir).await?;
        ensure_same_filesystem(
            &self.config.workspace_root,
            &plan.staging_dir,
            &plan.project_dir,
        )
        .await?;
        // A prior upload acknowledgement does not prove staging is still intact.
        for upload in plan.upload_files.values() {
            ensure_path_has_no_links(
                plan.staging_dir.clone(),
                format!("files/{}", upload.entry.path),
            )
            .await?;
            let staged =
                resolve_relative_path(&plan.staging_dir.join("files"), &upload.entry.path)?;
            let (_, transfer) = open_file_for_transfer(
                staged,
                upload.entry.path.clone(),
                self.config.sync_max_file_bytes,
            )
            .await?;
            if transfer.size_bytes != upload.transfer.size_bytes
                || transfer.sha256 != upload.transfer.sha256
            {
                return Err(AppError::Conflict(format!(
                    "staged file does not match plan: {}",
                    upload.entry.path
                )));
            }
        }
        if plan.reset {
            return self.apply_reset(sync_id, plan).await;
        }
        for path in plan
            .create_dirs
            .iter()
            .chain(plan.upload_files.keys())
            .chain(plan.delete_files.iter())
            .chain(plan.delete_dirs.iter())
        {
            let project = plan
                .project_dir
                .file_name()
                .and_then(|p| p.to_str())
                .unwrap_or_default();
            ensure_path_has_no_links(
                self.config.workspace_root.clone(),
                format!("{project}/{path}"),
            )
            .await?;
            ensure_baseline_matches(
                &resolve_relative_path(&plan.project_dir, path)?,
                plan.baseline.get(path),
            )
            .await?;
        }
        let mut transaction = CommitTransaction::new(plan.staging_dir.join("rollback")).await?;
        let operations: Result<CommitSyncResponse, AppError> = async {
            transaction
                .create_dir(plan.project_dir.clone(), "project root")
                .await?;
            let mut deleted_files = Vec::new();
            let mut deleted_dirs = Vec::new();
            for path in &plan.delete_files {
                if transaction
                    .delete_file(resolve_relative_path(&plan.project_dir, path)?, path)
                    .await?
                {
                    deleted_files.push(path.clone());
                }
            }
            for path in &plan.delete_dirs {
                if transaction
                    .delete_empty_dir(resolve_relative_path(&plan.project_dir, path)?, path)
                    .await?
                {
                    deleted_dirs.push(path.clone());
                }
            }
            for path in &plan.create_dirs {
                transaction
                    .create_dir(resolve_relative_path(&plan.project_dir, path)?, path)
                    .await?;
            }
            for upload in plan.upload_files.values() {
                let target = resolve_relative_path(&plan.project_dir, &upload.entry.path)?;
                transaction
                    .install_file(
                        resolve_relative_path(&plan.staging_dir.join("files"), &upload.entry.path)?,
                        target.clone(),
                        &upload.entry.path,
                    )
                    .await?;
                set_file_attributes(target, upload.entry.mtime_unix_ms, upload.entry.executable)
                    .await?;
            }
            Ok(CommitSyncResponse {
                sync_id,
                status: "committed",
                uploaded_files: plan.upload_files.keys().cloned().collect(),
                created_dirs: plan.create_dirs.clone(),
                deleted_files,
                deleted_dirs,
            })
        }
        .await;
        settle_transaction(operations, &mut transaction).await
    }
    async fn apply_reset(
        &self,
        sync_id: Uuid,
        plan: &CommitPlan,
    ) -> Result<CommitSyncResponse, AppError> {
        let files = plan.staging_dir.join("files");
        for path in &plan.create_dirs {
            ensure_path_has_no_links(files.clone(), path.clone()).await?;
            tokio::fs::create_dir_all(resolve_relative_path(&files, path)?)
                .await
                .map_err(internal_io)?;
        }
        for upload in plan.upload_files.values() {
            set_file_attributes(
                resolve_relative_path(&files, &upload.entry.path)?,
                upload.entry.mtime_unix_ms,
                upload.entry.executable,
            )
            .await?;
        }
        let config = self.config.clone();
        let snapshot = files.clone();
        let actual = tokio::task::spawn_blocking(move || {
            scan_manifest_blocking(
                snapshot,
                SyncFilters::new(&[], &[])?,
                config.sync_max_manifest_entries,
                config.sync_max_file_bytes,
            )
        })
        .await
        .map_err(|error| AppError::Internal(format!("reset validation task failed: {error}")))??;
        let expected_dirs: HashSet<_> = plan.create_dirs.iter().map(String::as_str).collect();
        let valid = actual.entries.len() == expected_dirs.len() + plan.upload_files.len()
            && actual.entries.iter().all(|entry| match entry.kind {
                ManifestEntryKind::Dir => expected_dirs.contains(entry.path.as_str()),
                ManifestEntryKind::File => plan
                    .upload_files
                    .get(&entry.path)
                    .is_some_and(|upload| same_file(entry, &upload.entry)),
            });
        if !valid {
            return Err(AppError::Conflict(
                "replacement tree differs from the complete manifest".to_string(),
            ));
        }
        let mut transaction = CommitTransaction::new(plan.staging_dir.join("rollback")).await?;
        let result = transaction
            .replace_project(files, plan.project_dir.clone())
            .await
            .map(|()| CommitSyncResponse {
                sync_id,
                status: "committed",
                uploaded_files: plan.upload_files.keys().cloned().collect(),
                created_dirs: plan.create_dirs.clone(),
                deleted_files: Vec::new(),
                deleted_dirs: Vec::new(),
            });
        settle_transaction(result, &mut transaction).await
    }
    pub async fn abort(
        &self,
        project: String,
        sync_id: Uuid,
    ) -> Result<AbortSyncResponse, AppError> {
        let manager = self.clone();
        self.tracked(async move {
            let session = manager.session(sync_id, &project).await?;
            manager
                .abort_session(session.clone(), SyncSessionStatus::Aborted)
                .await?;
            if session.data.lock().await.state != SyncSessionStatus::Aborted {
                return Err(AppError::Conflict(
                    "only an open sync plan can be aborted".to_string(),
                ));
            }
            Ok(AbortSyncResponse {
                sync_id,
                status: "aborted",
            })
        })
        .await
    }
    /// Ok means every plan for this project has settled and private cleanup is
    /// complete. Workflow shutdown may retry an error while retaining its slot.
    pub async fn abort_open(&self, project: String) -> Result<(), AppError> {
        let manager = self.clone();
        self.tracked(async move {
            let _creating = manager.creating.lock().await;
            let sessions: Vec<_> = manager.sessions.read().await.values().cloned().collect();
            for session in sessions {
                if session.data.lock().await.project == project {
                    manager
                        .abort_session(session, SyncSessionStatus::Aborted)
                        .await?;
                }
            }
            Ok(())
        })
        .await
    }
    async fn abort_session(
        &self,
        session: Arc<SyncSession>,
        terminal: SyncSessionStatus,
    ) -> Result<(), AppError> {
        session.cancel_uploads.cancel();
        loop {
            let notified = session.notify.notified();
            let activity = session.activity.write().await;
            let mut data = session.data.lock().await;
            if data.state == SyncSessionStatus::Committing {
                drop(data);
                drop(activity);
                notified.await;
                continue;
            }
            if data.state == SyncSessionStatus::Open {
                // Freeze business state before cleanup suspends. A disconnected
                // caller cannot reopen a partially removed staging directory.
                data.state = terminal;
                data.finished_at = Some(Instant::now());
                data.cleanup_pending = true;
                data.release_plan();
            }
            drop(data);
            self.cleanup_session(&session).await;
            session.notify.notify_waiters();
            if session.data.lock().await.cleanup_pending {
                return Err(AppError::Internal(
                    "sync staging cleanup is still pending".to_string(),
                ));
            }
            return Ok(());
        }
    }
    async fn cleanup_session(&self, session: &SyncSession) {
        let _cleanup = session.cleanup.lock().await;
        let path = {
            let data = session.data.lock().await;
            if !data.cleanup_pending {
                return;
            }
            data.plan.staging_dir.clone()
        };
        match remove_staging_dir(&self.config.workspace_root, &path).await {
            Ok(()) => session.data.lock().await.cleanup_pending = false,
            Err(error) => {
                tracing::warn!(%error, staging = %path.display(), "private sync cleanup will be retried")
            }
        }
        self.prune_results().await;
    }
    pub async fn status(
        &self,
        project: String,
        sync_id: Uuid,
    ) -> Result<SyncStatusResponse, AppError> {
        let session = self.session(sync_id, &project).await?;
        let data = session.data.lock().await;
        Ok(SyncStatusResponse {
            sync_id,
            project,
            status: data.state,
            cleanup_pending: data.cleanup_pending,
            result: data.result.clone(),
            error_code: data.error.as_ref().map(|e| e.code().to_string()),
            error_message: data.error.as_ref().map(AppError::client_message),
        })
    }
    pub async fn download_file(
        &self,
        project: String,
        raw_path: String,
        if_match: Option<String>,
    ) -> Result<Response, AppError> {
        let path = normalize_sync_path(&raw_path)?;
        let dir = resolve_project_dir(&self.config.workspace_root, &project)?;
        self.check_project_root(&dir).await?;
        ensure_path_has_no_links(dir.clone(), path.clone()).await?;
        let (file, transfer) = open_file_for_transfer(
            resolve_relative_path(&dir, &path)?,
            path,
            self.config.sync_max_file_bytes,
        )
        .await?;
        let etag = format!("\"sha256:{}\"", transfer.sha256);
        if let Some(expected) = if_match
            && !expected
                .split(',')
                .any(|candidate| candidate.trim() == etag || candidate.trim() == "*")
        {
            return Err(AppError::PreconditionFailed(
                "file changed after the pull plan".to_string(),
            ));
        }
        // A workflow freezes writes during pull. The same open handle still
        // cannot prevent external writers; clients verify the streamed body.
        let mut response = Response::new(Body::from_stream(ReaderStream::new(
            tokio::fs::File::from_std(file),
        )));
        *response.status_mut() = StatusCode::OK;
        let headers = response.headers_mut();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        for (name, value) in [
            (CONTENT_LENGTH, transfer.size_bytes.to_string()),
            (ETAG, etag),
            (
                "x-sync-size-bytes".parse().unwrap(),
                transfer.size_bytes.to_string(),
            ),
            (
                "x-sync-mtime-unix-ms".parse().unwrap(),
                transfer.mtime_unix_ms.to_string(),
            ),
            ("x-sync-sha256".parse().unwrap(), transfer.sha256),
            (
                "x-sync-executable".parse().unwrap(),
                transfer.executable.to_string(),
            ),
        ] {
            headers.insert(
                name,
                HeaderValue::from_str(&value).map_err(|error| {
                    AppError::Internal(format!("invalid transfer header: {error}"))
                })?,
            );
        }
        Ok(response)
    }
    async fn session(&self, sync_id: Uuid, project: &str) -> Result<Arc<SyncSession>, AppError> {
        let session = self
            .sessions
            .read()
            .await
            .get(&sync_id)
            .cloned()
            .ok_or_else(|| AppError::NotFound("sync session not found".to_string()))?;
        if session.data.lock().await.project != project {
            return Err(AppError::NotFound("sync session not found".to_string()));
        }
        Ok(session)
    }
    async fn check_project_root(&self, dir: &Path) -> Result<(), AppError> {
        match tokio::fs::symlink_metadata(dir).await {
            Ok(_) => {
                ensure_safe_project_root(self.config.workspace_root.clone(), dir.to_path_buf())
                    .await
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(internal_io(error)),
        }
    }
    async fn scan_manifest(
        &self,
        dir: PathBuf,
        filters: SyncFilters,
    ) -> Result<ManifestResponse, AppError> {
        self.check_project_root(&dir).await?;
        if !tokio::fs::try_exists(&dir).await.map_err(internal_io)? {
            return Ok(ManifestResponse {
                entries: Vec::new(),
            });
        }
        let config = self.config.clone();
        tokio::task::spawn_blocking(move || {
            scan_manifest_blocking(
                dir,
                filters,
                config.sync_max_manifest_entries,
                config.sync_max_file_bytes,
            )
        })
        .await
        .map_err(|error| AppError::Internal(format!("manifest task failed: {error}")))?
    }
    async fn reap(&self) {
        let sessions: Vec<_> = self.sessions.read().await.values().cloned().collect();
        for session in sessions {
            let data = session.data.lock().await;
            let expired =
                data.state == SyncSessionStatus::Open && Instant::now() >= data.expires_deadline;
            let terminal = !matches!(
                data.state,
                SyncSessionStatus::Open | SyncSessionStatus::Committing
            );
            drop(data);
            if expired {
                let _ = self
                    .abort_session(session.clone(), SyncSessionStatus::Expired)
                    .await;
            } else if terminal {
                self.cleanup_session(&session).await;
            }
        }
        self.prune_results().await;
    }

    async fn prune_results(&self) {
        let snapshot: Vec<_> = self
            .sessions
            .read()
            .await
            .iter()
            .map(|(id, session)| (*id, session.clone()))
            .collect();
        let mut finished = Vec::new();
        // Never hold the registry lock while examining session metadata. These
        // guards cover state only, not filesystem work or upload bodies.
        for (id, session) in snapshot {
            let data = session.data.lock().await;
            if !data.cleanup_pending
                && !matches!(
                    data.state,
                    SyncSessionStatus::Open | SyncSessionStatus::Committing
                )
                && let Some(at) = data.finished_at
            {
                finished.push((id, at));
            }
        }
        finished.sort_unstable_by_key(|(id, at)| (*at, *id));
        let excess = finished.len().saturating_sub(MAX_RETAINED_SYNC_RESULTS);
        let retention = self.config.sync_result_retention();
        let mut sessions = self.sessions.write().await;
        for (index, (id, at)) in finished.into_iter().enumerate() {
            if index < excess || at.elapsed() >= retention {
                sessions.remove(&id);
            }
        }
    }
}
async fn settle_transaction<T>(
    result: Result<T, AppError>,
    transaction: &mut CommitTransaction,
) -> Result<T, AppError> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => match transaction.rollback().await {
            Ok(()) => Err(error),
            Err(rollback) => Err(AppError::ProjectReuploadRequired(format!(
                "sync failed ({error}); rollback failed ({rollback})"
            ))),
        },
    }
}
fn internal_io(error: std::io::Error) -> AppError {
    AppError::Internal(format!("sync filesystem operation failed: {error}"))
}
async fn private_staging_root(workspace: &Path) -> Result<PathBuf, AppError> {
    let metadata = tokio::fs::symlink_metadata(workspace)
        .await
        .map_err(internal_io)?;
    if !metadata.is_dir() || metadata_is_link(&metadata) {
        return Err(AppError::BadRequest(
            "workspace is not a real directory".to_string(),
        ));
    }
    let mut current = workspace.to_path_buf();
    for name in [".vivado-server", "staging"] {
        current.push(name);
        match tokio::fs::symlink_metadata(&current).await {
            Ok(metadata) if metadata.is_dir() && !metadata_is_link(&metadata) => {}
            Ok(_) => {
                return Err(AppError::BadRequest(
                    "private staging root is not a real directory".to_string(),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match tokio::fs::create_dir(&current).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(internal_io(error)),
                }
                let metadata = tokio::fs::symlink_metadata(&current)
                    .await
                    .map_err(internal_io)?;
                if !metadata.is_dir() || metadata_is_link(&metadata) {
                    return Err(AppError::BadRequest(
                        "private staging root is not a real directory".to_string(),
                    ));
                }
            }
            Err(error) => return Err(internal_io(error)),
        }
    }
    Ok(current)
}
async fn create_staging_dir(workspace: &Path, sync_id: Uuid) -> Result<PathBuf, AppError> {
    let root = private_staging_root(workspace).await?;
    let dir = root.join(sync_id.to_string());
    tokio::fs::create_dir(&dir).await.map_err(internal_io)?;
    let create = async {
        tokio::fs::create_dir(dir.join("files"))
            .await
            .map_err(internal_io)?;
        tokio::fs::create_dir(dir.join("tmp"))
            .await
            .map_err(internal_io)
    }
    .await;
    if let Err(error) = create {
        let _ = remove_private_entry(&root, &dir).await;
        return Err(error);
    }
    Ok(dir)
}
async fn remove_staging_dir(workspace: &Path, path: &Path) -> Result<(), AppError> {
    let root = private_staging_root(workspace).await?;
    remove_private_entry(&root, path).await
}
async fn remove_private_entry(root: &Path, path: &Path) -> Result<(), AppError> {
    if path.parent() != Some(root) {
        return Err(AppError::BadRequest(
            "cleanup path escapes private staging".to_string(),
        ));
    }
    let valid_id = path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| Uuid::parse_str(name).is_ok());
    if !valid_id {
        return Err(AppError::BadRequest(
            "unexpected entry in private staging".to_string(),
        ));
    }
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.is_dir() && !metadata_is_link(&metadata) => {
            // Linux remove_dir_all unlinks nested links without following them.
            // Only this validated service-owned directory is traversed.
            tokio::fs::remove_dir_all(path).await.map_err(internal_io)
        }
        Ok(_) => Err(AppError::BadRequest(
            "private staging entry is not a real directory".to_string(),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(internal_io(error)),
    }
}
async fn ensure_same_filesystem(
    workspace: &Path,
    staging: &Path,
    project: &Path,
) -> Result<(), AppError> {
    use std::os::unix::fs::MetadataExt;
    let device = tokio::fs::metadata(workspace)
        .await
        .map_err(internal_io)?
        .dev();
    let stage_device = tokio::fs::metadata(staging)
        .await
        .map_err(internal_io)?
        .dev();
    let project_device = match tokio::fs::symlink_metadata(project).await {
        Ok(metadata) => metadata.dev(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => device,
        Err(error) => return Err(internal_io(error)),
    };
    if device != stage_device || device != project_device {
        return Err(AppError::BadRequest(
            "project and staging must share the workspace filesystem".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
