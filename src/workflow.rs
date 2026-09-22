//! The workflow owns the project from the first push until the last pull.
//!
//! Metadata locks never span I/O. The activity lock excludes project transitions
//! from transfers; cancellation first changes metadata and wakes those transfers,
//! then waits for them. HTTP futures only wait for manager-owned mutations.
use crate::{
    RuntimeConfig, SessionManager,
    error::AppError,
    project::ProjectStore,
    session::{
        CreateSessionRequest, OutputQuery, OutputResponse, SendInputRequest, SessionInfo,
        TerminationReason,
    },
    sync::{
        AbortSyncResponse, CommitSyncRequest, CommitSyncResponse, ManifestRequest,
        ManifestResponse, PullPlanRequest, PullPlanResponse, PushPlanRequest, PushPlanResponse,
        SyncManager, SyncSessionStatus, SyncStatusResponse, UploadResponse,
    },
};
use axum::{body::Body, response::Response};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{Notify, OwnedRwLockReadGuard, RwLock, RwLockWriteGuard};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateWorkflowRequest {
    pub project: String,
    #[serde(default)]
    pub reset_project: bool,
}

#[derive(Debug, Default, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct StartSessionRequest {
    #[serde(default)]
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    Preparing,
    Running,
    Pulling,
    Stopping,
    Completed,
    Cancelled,
    Failed,
}

impl WorkflowStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Failed)
    }
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct WorkflowInfo {
    pub workflow_id: Uuid,
    pub project: String,
    pub status: WorkflowStatus,
    pub session_id: Option<Uuid>,
    pub requires_full_upload: bool,
    pub cleanup_pending: bool,
    pub started_at: DateTime<Utc>,
    pub last_heartbeat_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

struct WorkflowData {
    info: WorkflowInfo,
    request: CreateWorkflowRequest,
    heartbeat: Instant,
    ended: Option<Instant>,
    open_sync: Option<Uuid>,
    open_sync_deadline: Option<Instant>,
    sync_ids: VecDeque<Uuid>,
    baseline_clean: bool,
    transfers: usize,
    initializing: bool,
    initialization_error: Option<AppError>,
    cleanup_started: bool,
    cleanup_target: WorkflowStatus,
    terminal_error: Option<AppError>,
    stop_reason: TerminationReason,
    invalidate_project: bool,
}

struct Workflow {
    data: Mutex<WorkflowData>,
    activity: Arc<RwLock<()>>,
    cancel: CancellationToken,
    initialized: Notify,
}

#[derive(Default)]
struct Registry {
    active: Option<Uuid>,
    entries: HashMap<Uuid, Arc<Workflow>>,
    stopping: bool,
}

struct Inner {
    config: RuntimeConfig,
    sessions: SessionManager,
    sync: SyncManager,
    projects: ProjectStore,
    registry: Mutex<Registry>,
    tasks: TaskTracker,
}

#[derive(Clone)]
pub(crate) struct WorkflowManager(Arc<Inner>);

struct TransferLease {
    workflow: Arc<Workflow>,
    _activity: OwnedRwLockReadGuard<()>,
}

impl Drop for TransferLease {
    fn drop(&mut self) {
        self.workflow
            .data
            .lock()
            .expect("workflow metadata poisoned")
            .transfers -= 1;
    }
}

impl WorkflowManager {
    pub(crate) fn new(
        config: RuntimeConfig,
        sessions: SessionManager,
        sync: SyncManager,
        projects: ProjectStore,
    ) -> Self {
        Self(Arc::new(Inner {
            config,
            sessions,
            sync,
            projects,
            registry: Mutex::new(Registry::default()),
            tasks: TaskTracker::new(),
        }))
    }

    // Register under the same gate used by shutdown. TaskTracker::close alone
    // does not reject new tasks, so it cannot serve as the admission barrier.
    async fn managed<T, F>(&self, future: F) -> Result<T, AppError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, AppError>> + Send + 'static,
    {
        let task = {
            let registry = self.0.registry.lock().expect("workflow registry poisoned");
            if registry.stopping {
                return Err(AppError::ShuttingDown);
            }
            self.0.tasks.spawn(future)
        };
        task.await
            .map_err(|error| AppError::Internal(format!("workflow task failed: {error}")))?
    }

    fn workflow(&self, id: Uuid) -> Result<Arc<Workflow>, AppError> {
        self.0
            .registry
            .lock()
            .expect("workflow registry poisoned")
            .entries
            .get(&id)
            .cloned()
            .ok_or_else(|| AppError::NotFound("workflow not found".into()))
    }

    fn snapshot(workflow: &Workflow) -> WorkflowInfo {
        workflow
            .data
            .lock()
            .expect("workflow metadata poisoned")
            .info
            .clone()
    }

    fn phase(workflow: &Workflow, allowed: &[WorkflowStatus]) -> Result<(), AppError> {
        let data = workflow.data.lock().expect("workflow metadata poisoned");
        if !data.initializing && allowed.contains(&data.info.status) {
            Ok(())
        } else {
            Err(AppError::WorkflowPhase(
                "operation is not allowed in the current phase".into(),
            ))
        }
    }

    fn admit(&self) -> Result<(), AppError> {
        if self
            .0
            .registry
            .lock()
            .expect("workflow registry poisoned")
            .stopping
        {
            Err(AppError::ShuttingDown)
        } else {
            Ok(())
        }
    }

    pub(crate) async fn create(
        &self,
        id: Uuid,
        request: CreateWorkflowRequest,
    ) -> Result<WorkflowInfo, AppError> {
        crate::workspace::validate_project_name(&request.project)?;
        let manager = self.clone();
        self.managed(async move {
            let now = Utc::now();
            let workflow = Arc::new(Workflow {
                data: Mutex::new(WorkflowData {
                    info: WorkflowInfo {
                        workflow_id: id,
                        project: request.project.clone(),
                        status: WorkflowStatus::Preparing,
                        session_id: None,
                        requires_full_upload: true,
                        cleanup_pending: false,
                        started_at: now,
                        last_heartbeat_at: now,
                        ended_at: None,
                        error_code: None,
                        error_message: None,
                    },
                    request: request.clone(),
                    heartbeat: Instant::now(),
                    ended: None,
                    open_sync: None,
                    open_sync_deadline: None,
                    sync_ids: VecDeque::new(),
                    baseline_clean: false,
                    transfers: 0,
                    initializing: true,
                    initialization_error: None,
                    cleanup_started: false,
                    cleanup_target: WorkflowStatus::Cancelled,
                    terminal_error: None,
                    stop_reason: TerminationReason::WorkflowClosed,
                    invalidate_project: false,
                }),
                activity: Arc::new(RwLock::new(())),
                cancel: CancellationToken::new(),
                initialized: Notify::new(),
            });
            // Reserve before reading filesystem state. Otherwise an entire other
            // workflow could run between the clean check and claiming this slot.
            let activity = workflow.activity.write().await;
            let existing = {
                let mut registry = manager
                    .0
                    .registry
                    .lock()
                    .expect("workflow registry poisoned");
                if registry.stopping {
                    return Err(AppError::ShuttingDown);
                }
                if let Some(existing) = registry.entries.get(&id) {
                    if existing
                        .data
                        .lock()
                        .expect("workflow metadata poisoned")
                        .request
                        != request
                    {
                        return Err(AppError::Conflict(
                            "workflow ID was already used with different parameters".into(),
                        ));
                    }
                    Some(existing.clone())
                } else {
                    if registry.active.is_some() {
                        return Err(AppError::WorkflowBusy);
                    }
                    registry.active = Some(id);
                    registry.entries.insert(id, workflow.clone());
                    None
                }
            };
            if let Some(existing) = existing {
                drop(activity);
                return Self::wait_initialized(&existing).await;
            }
            let result = async {
                manager.0.projects.project_path(&request.project).await?;
                let clean = manager.0.projects.is_clean(&request.project).await?;
                if !clean && !request.reset_project {
                    return Err(AppError::ProjectReuploadRequired(request.project.clone()));
                }
                Ok(clean)
            }
            .await;
            {
                let mut data = workflow.data.lock().expect("workflow metadata poisoned");
                data.initializing = false;
                match &result {
                    Ok(clean) => {
                        data.baseline_clean = *clean;
                        data.info.requires_full_upload = request.reset_project || !clean;
                    }
                    Err(error) => data.initialization_error = Some(error.clone()),
                }
            }
            if let Err(error) = &result {
                // No business operation can pass the activity lock while this
                // reservation is being initialized, so there is nothing to undo.
                manager.terminal(
                    &workflow,
                    &activity,
                    WorkflowStatus::Failed,
                    Some(error),
                    true,
                );
            }
            workflow.initialized.notify_waiters();
            result.map(|_| Self::snapshot(&workflow))
        })
        .await
    }

    async fn wait_initialized(workflow: &Workflow) -> Result<WorkflowInfo, AppError> {
        loop {
            let initialized = workflow.initialized.notified();
            tokio::pin!(initialized);
            initialized.as_mut().enable();
            {
                let data = workflow.data.lock().expect("workflow metadata poisoned");
                if !data.initializing {
                    return match &data.initialization_error {
                        Some(error) => Err(error.clone()),
                        None => Ok(data.info.clone()),
                    };
                }
            }
            initialized.await;
        }
    }

    pub(crate) async fn get(&self, id: Uuid) -> Result<WorkflowInfo, AppError> {
        let workflow = self.workflow(id)?;
        Ok(Self::snapshot(&workflow))
    }

    pub(crate) async fn heartbeat(&self, id: Uuid) -> Result<WorkflowInfo, AppError> {
        // Share the registry gate with expiry's check-and-stop transition.
        let registry = self.0.registry.lock().expect("workflow registry poisoned");
        if registry.stopping {
            return Err(AppError::ShuttingDown);
        }
        let workflow = registry
            .entries
            .get(&id)
            .ok_or_else(|| AppError::NotFound("workflow not found".into()))?;
        let mut data = workflow.data.lock().expect("workflow metadata poisoned");
        if data.info.status.is_terminal() || data.info.status == WorkflowStatus::Stopping {
            return Err(AppError::WorkflowPhase(
                "workflow no longer accepts heartbeats".into(),
            ));
        }
        data.heartbeat = Instant::now();
        data.info.last_heartbeat_at = Utc::now();
        Ok(data.info.clone())
    }

    async fn lease(&self, id: Uuid, allowed: &[WorkflowStatus]) -> Result<TransferLease, AppError> {
        self.admit()?;
        let workflow = self.workflow(id)?;
        let activity = tokio::select! {
            guard = workflow.activity.clone().read_owned() => guard,
            _ = workflow.cancel.cancelled() => return Err(AppError::WorkflowPhase("workflow cancelled".into())),
        };
        {
            let mut data = workflow.data.lock().expect("workflow metadata poisoned");
            if data.initializing
                || !allowed.contains(&data.info.status)
                || workflow.cancel.is_cancelled()
            {
                return Err(AppError::WorkflowPhase(
                    "operation is not allowed in the current phase".into(),
                ));
            }
            data.transfers += 1;
        }
        Ok(TransferLease {
            workflow,
            _activity: activity,
        })
    }

    pub(crate) async fn manifest(
        &self,
        id: Uuid,
        request: ManifestRequest,
    ) -> Result<ManifestResponse, AppError> {
        let lease = self
            .lease(id, &[WorkflowStatus::Preparing, WorkflowStatus::Pulling])
            .await?;
        tokio::select! {
            result = self.0.sync.manifest(Self::snapshot(&lease.workflow).project, request) => result,
            _ = lease.workflow.cancel.cancelled() => Err(AppError::WorkflowPhase("workflow cancelled".into())),
        }
    }

    pub(crate) async fn push_plan(
        &self,
        id: Uuid,
        request: PushPlanRequest,
    ) -> Result<PushPlanResponse, AppError> {
        let manager = self.clone();
        self.managed(async move {
            let workflow = manager.workflow(id)?;
            let _activity = workflow.activity.write().await;
            Self::phase(&workflow, &[WorkflowStatus::Preparing])?;
            manager.refresh_open_sync(&workflow).await?;
            if workflow
                .data
                .lock()
                .expect("workflow metadata poisoned")
                .open_sync
                .is_some()
            {
                return Err(AppError::Conflict("an upload plan is already open".into()));
            }
            let info = Self::snapshot(&workflow);
            let plan = if info.requires_full_upload {
                manager
                    .0
                    .sync
                    .push_reset_plan(info.project, request)
                    .await?
            } else {
                manager.0.sync.push_plan(info.project, request).await?
            };
            let mut data = workflow.data.lock().expect("workflow metadata poisoned");
            data.open_sync = Some(plan.sync_id);
            data.open_sync_deadline = Some(Instant::now() + manager.0.config.sync_session_ttl());
            data.sync_ids.push_back(plan.sync_id);
            while data.sync_ids.len() > crate::sync::MAX_RETAINED_SYNC_RESULTS {
                data.sync_ids.pop_front();
            }
            Ok(plan)
        })
        .await
    }

    fn check_sync(workflow: &Workflow, sync_id: Uuid) -> Result<(), AppError> {
        if workflow
            .data
            .lock()
            .expect("workflow metadata poisoned")
            .sync_ids
            .contains(&sync_id)
        {
            Ok(())
        } else {
            Err(AppError::NotFound(
                "sync plan does not belong to this workflow".into(),
            ))
        }
    }

    // Caller owns the activity write lock. An expired or aborted plan must not
    // leave a stale workflow-level flag preventing every later push or start.
    async fn refresh_open_sync(&self, workflow: &Workflow) -> Result<(), AppError> {
        let (project, open, expired) = {
            let data = workflow.data.lock().expect("workflow metadata poisoned");
            (
                data.info.project.clone(),
                data.open_sync,
                data.open_sync_deadline
                    .is_some_and(|deadline| Instant::now() >= deadline),
            )
        };
        let Some(sync_id) = open else {
            return Ok(());
        };
        let status = self.0.sync.status(project.clone(), sync_id).await;
        let terminal = match status {
            Ok(status) if status.status == SyncSessionStatus::Open && expired => {
                self.0.sync.abort(project, sync_id).await?;
                true
            }
            Ok(status) => !matches!(
                status.status,
                SyncSessionStatus::Open | SyncSessionStatus::Committing
            ),
            Err(AppError::NotFound(_)) => true,
            Err(error) => return Err(error),
        };
        if terminal {
            let mut data = workflow.data.lock().expect("workflow metadata poisoned");
            data.open_sync = None;
            data.open_sync_deadline = None;
        }
        Ok(())
    }

    pub(crate) async fn upload(
        &self,
        id: Uuid,
        sync_id: Uuid,
        path: String,
        length: Option<u64>,
        body: Body,
    ) -> Result<UploadResponse, AppError> {
        let lease = self.lease(id, &[WorkflowStatus::Preparing]).await?;
        Self::check_sync(&lease.workflow, sync_id)?;
        let project = Self::snapshot(&lease.workflow).project;
        tokio::select! {
            result = self.0.sync.upload_file(project, sync_id, path, length, body) => result,
            _ = lease.workflow.cancel.cancelled() => Err(AppError::WorkflowPhase("workflow cancelled".into())),
        }
    }

    pub(crate) async fn commit(
        &self,
        id: Uuid,
        sync_id: Uuid,
        request: CommitSyncRequest,
    ) -> Result<CommitSyncResponse, AppError> {
        let manager = self.clone();
        self.managed(async move {
            let workflow = manager.workflow(id)?;
            let _activity = workflow.activity.write().await;
            Self::phase(&workflow, &[WorkflowStatus::Preparing])?;
            Self::check_sync(&workflow, sync_id)?;
            let info = Self::snapshot(&workflow);
            // A repeated committed request must only observe its stored result.
            let open = workflow
                .data
                .lock()
                .expect("workflow metadata poisoned")
                .open_sync
                == Some(sync_id);
            if !open {
                return manager.0.sync.commit(info.project, sync_id, request).await;
            }
            let was_clean = manager.0.projects.is_clean(&info.project).await?;
            if let Err(error) = manager.0.projects.mark_dirty(&info.project).await {
                manager.fail(&workflow, &error, true);
                return Err(error);
            }
            let result = manager
                .0
                .sync
                .commit(info.project.clone(), sync_id, request)
                .await;
            match &result {
                Ok(_) => {
                    let mut data = workflow.data.lock().expect("workflow metadata poisoned");
                    data.open_sync = None;
                    data.open_sync_deadline = None;
                    data.info.requires_full_upload = false;
                    data.baseline_clean = true;
                }
                Err(error) => {
                    let baseline_clean = workflow
                        .data
                        .lock()
                        .expect("workflow metadata poisoned")
                        .baseline_clean;
                    let reupload = matches!(error, AppError::ProjectReuploadRequired(_));
                    let status = manager
                        .0
                        .sync
                        .status(info.project.clone(), sync_id)
                        .await
                        .ok();
                    let still_open = status
                        .as_ref()
                        .is_some_and(|status| status.status == SyncSessionStatus::Open);
                    // Missing uploads are retryable and have not touched the
                    // project. Other ordinary errors guarantee no mutation or
                    // complete in-process rollback; failed rollback is explicit.
                    if !reupload
                        && ((still_open && was_clean) || (!still_open && baseline_clean))
                        && let Err(mark_error) = manager.0.projects.mark_clean(&info.project).await
                    {
                        manager.fail(&workflow, &mark_error, true);
                        return Err(mark_error);
                    }
                    if still_open && !reupload {
                        return result;
                    }
                    manager.fail(&workflow, error, reupload);
                }
            }
            result
        })
        .await
    }

    pub(crate) async fn abort_sync(
        &self,
        id: Uuid,
        sync_id: Uuid,
    ) -> Result<AbortSyncResponse, AppError> {
        let manager = self.clone();
        self.managed(async move {
            let workflow = manager.workflow(id)?;
            let _activity = workflow.activity.write().await;
            Self::phase(&workflow, &[WorkflowStatus::Preparing])?;
            Self::check_sync(&workflow, sync_id)?;
            let result = manager
                .0
                .sync
                .abort(Self::snapshot(&workflow).project, sync_id)
                .await?;
            let mut data = workflow.data.lock().expect("workflow metadata poisoned");
            if data.open_sync == Some(sync_id) {
                data.open_sync = None;
                data.open_sync_deadline = None;
            }
            Ok(result)
        })
        .await
    }

    pub(crate) async fn sync_status(
        &self,
        id: Uuid,
        sync_id: Uuid,
    ) -> Result<SyncStatusResponse, AppError> {
        let workflow = self.workflow(id)?;
        Self::check_sync(&workflow, sync_id)?;
        self.0
            .sync
            .status(Self::snapshot(&workflow).project, sync_id)
            .await
    }

    pub(crate) async fn start_session(
        &self,
        id: Uuid,
        request: StartSessionRequest,
    ) -> Result<SessionInfo, AppError> {
        crate::session::validate_vivado_args(&request.args)?;
        let manager = self.clone();
        self.managed(async move {
            let workflow = manager.workflow(id)?;
            let _activity = workflow.activity.write().await;
            Self::phase(&workflow, &[WorkflowStatus::Preparing])?;
            manager.refresh_open_sync(&workflow).await?;
            let info = Self::snapshot(&workflow);
            if info.requires_full_upload {
                return Err(AppError::ProjectReuploadRequired(info.project));
            }
            if workflow
                .data
                .lock()
                .expect("workflow metadata poisoned")
                .open_sync
                .is_some()
            {
                return Err(AppError::WorkflowPhase(
                    "commit or abort the open plan first".into(),
                ));
            }
            if let Err(error) = manager.0.projects.mark_dirty(&info.project).await {
                manager.fail(&workflow, &error, true);
                return Err(error);
            }
            Self::phase(&workflow, &[WorkflowStatus::Preparing])?;
            let result = manager
                .0
                .sessions
                .create_session(CreateSessionRequest {
                    project: info.project,
                    args: request.args,
                })
                .await;
            match result {
                Ok(session) => {
                    {
                        let mut data = workflow.data.lock().expect("workflow metadata poisoned");
                        data.info.session_id = Some(session.session_id);
                        if data.info.status != WorkflowStatus::Stopping {
                            data.info.status = WorkflowStatus::Running;
                        }
                    }
                    let observer = manager.clone();
                    let observed = workflow.clone();
                    let session_id = session.session_id;
                    manager.0.tasks.spawn(async move {
                        observer.observe_session(observed, session_id).await;
                    });
                    Ok(session)
                }
                Err(error) => {
                    manager.fail(&workflow, &error, true);
                    Err(error)
                }
            }
        })
        .await
    }

    async fn observe_session(&self, workflow: Arc<Workflow>, session_id: Uuid) {
        loop {
            if Self::snapshot(&workflow).status != WorkflowStatus::Running {
                return;
            }
            let session = match self.0.sessions.get_session(session_id).await {
                Ok(session) => session,
                Err(error) => {
                    self.fail(&workflow, &error, true);
                    return;
                }
            };
            if session.status.is_terminal() {
                let _activity = workflow.activity.write().await;
                if Self::snapshot(&workflow).status != WorkflowStatus::Running {
                    return;
                }
                if session.termination_reason == Some(TerminationReason::ProcessExited)
                    && session.exit_code == Some(0)
                {
                    let project = Self::snapshot(&workflow).project;
                    match self.0.projects.mark_clean(&project).await {
                        Ok(()) => {
                            let mut data =
                                workflow.data.lock().expect("workflow metadata poisoned");
                            if data.info.status == WorkflowStatus::Running {
                                data.info.status = WorkflowStatus::Pulling;
                                data.info.requires_full_upload = false;
                            }
                        }
                        Err(error) => self.fail(&workflow, &error, true),
                    }
                } else {
                    self.fail(
                        &workflow,
                        &AppError::ProjectReuploadRequired(
                            "Vivado did not exit normally with status zero".into(),
                        ),
                        true,
                    );
                }
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    fn session_id(workflow: &Workflow) -> Result<Uuid, AppError> {
        Self::snapshot(workflow)
            .session_id
            .ok_or(AppError::SessionNotFound)
    }

    pub(crate) async fn session(&self, id: Uuid) -> Result<SessionInfo, AppError> {
        let workflow = self.workflow(id)?;
        self.0
            .sessions
            .get_session(Self::session_id(&workflow)?)
            .await
    }

    pub(crate) async fn stdin(
        &self,
        id: Uuid,
        request: SendInputRequest,
    ) -> Result<SessionInfo, AppError> {
        self.admit()?;
        let workflow = self.workflow(id)?;
        Self::phase(&workflow, &[WorkflowStatus::Running])?;
        self.0
            .sessions
            .send_input(Self::session_id(&workflow)?, request)
            .await
    }

    pub(crate) async fn output(
        &self,
        id: Uuid,
        query: OutputQuery,
    ) -> Result<OutputResponse, AppError> {
        let workflow = self.workflow(id)?;
        self.0
            .sessions
            .read_output(Self::session_id(&workflow)?, query)
            .await
    }

    pub(crate) async fn stop_session(&self, id: Uuid) -> Result<SessionInfo, AppError> {
        let manager = self.clone();
        self.managed(async move {
            let workflow = manager.workflow(id)?;
            let session_id = Self::session_id(&workflow)?;
            let mut session = manager.0.sessions.get_session(session_id).await?;
            if session.status.is_terminal() {
                // A repeated stop after natural exit does not invalidate pull,
                // nor race its observer's durable clean-marker publication.
                return Ok(session);
            }
            manager.request_cleanup(
                &workflow,
                TerminationReason::ClientRequested,
                WorkflowStatus::Failed,
                Some(AppError::ProjectReuploadRequired(
                    "Vivado was stopped by the client".into(),
                )),
                true,
            );
            session.status = crate::session::SessionStatus::Stopping;
            session
                .termination_reason
                .get_or_insert(TerminationReason::ClientRequested);
            Ok(session)
        })
        .await
    }

    pub(crate) async fn pull_plan(
        &self,
        id: Uuid,
        request: PullPlanRequest,
    ) -> Result<PullPlanResponse, AppError> {
        let lease = self.lease(id, &[WorkflowStatus::Pulling]).await?;
        tokio::select! {
            result = self.0.sync.pull_plan(Self::snapshot(&lease.workflow).project, request) => result,
            _ = lease.workflow.cancel.cancelled() => Err(AppError::WorkflowPhase("workflow cancelled".into())),
        }
    }

    pub(crate) async fn download(
        &self,
        id: Uuid,
        path: String,
        if_match: Option<String>,
    ) -> Result<Response, AppError> {
        let lease = self.lease(id, &[WorkflowStatus::Pulling]).await?;
        let response = tokio::select! {
            result = self.0.sync.download_file(Self::snapshot(&lease.workflow).project, path, if_match) => result?,
            _ = lease.workflow.cancel.cancelled() => return Err(AppError::WorkflowPhase("workflow cancelled".into())),
        };
        let cancel = lease.workflow.cancel.clone();
        let (mut parts, body) = response.into_parts();
        parts.extensions.insert(cancel.clone());
        Ok(Response::from_parts(
            parts,
            crate::body::hold(body, lease, Some(cancel)),
        ))
    }

    pub(crate) async fn finish(&self, id: Uuid) -> Result<WorkflowInfo, AppError> {
        let manager = self.clone();
        self.managed(async move {
            let workflow = manager.workflow(id)?;
            if Self::snapshot(&workflow).status == WorkflowStatus::Completed {
                return Ok(Self::snapshot(&workflow));
            }
            let activity = workflow
                .activity
                .try_write()
                .map_err(|_| AppError::WorkflowPhase("transfers are still active".into()))?;
            Self::phase(&workflow, &[WorkflowStatus::Pulling])?;
            if let Err(error) = manager
                .0
                .sync
                .abort_open(Self::snapshot(&workflow).project)
                .await
            {
                workflow
                    .data
                    .lock()
                    .expect("workflow metadata poisoned")
                    .info
                    .cleanup_pending = true;
                return Err(error);
            }
            if !manager.terminal(&workflow, &activity, WorkflowStatus::Completed, None, false) {
                return Err(AppError::WorkflowPhase("workflow is stopping".into()));
            }
            Ok(Self::snapshot(&workflow))
        })
        .await
    }

    pub(crate) async fn cancel(&self, id: Uuid) -> Result<WorkflowInfo, AppError> {
        let manager = self.clone();
        self.managed(async move {
            let workflow = manager.workflow(id)?;
            manager.request_cleanup(
                &workflow,
                TerminationReason::WorkflowClosed,
                WorkflowStatus::Cancelled,
                None,
                false,
            );
            Ok(Self::snapshot(&workflow))
        })
        .await
    }

    fn fail(&self, workflow: &Arc<Workflow>, error: &AppError, invalidate: bool) {
        self.request_cleanup(
            workflow,
            TerminationReason::WorkflowClosed,
            WorkflowStatus::Failed,
            Some(error.clone()),
            invalidate,
        );
    }

    fn request_cleanup(
        &self,
        workflow: &Arc<Workflow>,
        reason: TerminationReason,
        target: WorkflowStatus,
        error: Option<AppError>,
        invalidate: bool,
    ) {
        let registry = self.0.registry.lock().expect("workflow registry poisoned");
        self.request_cleanup_locked(&registry, workflow, reason, target, error, invalidate);
    }

    // Registration and the transition share shutdown's admission lock. One
    // worker owns cleanup until it actually succeeds, independent of HTTP waits.
    fn request_cleanup_locked(
        &self,
        registry: &Registry,
        workflow: &Arc<Workflow>,
        reason: TerminationReason,
        target: WorkflowStatus,
        error: Option<AppError>,
        invalidate: bool,
    ) {
        let mut data = workflow.data.lock().expect("workflow metadata poisoned");
        if data.info.status.is_terminal() || registry.active != Some(data.info.workflow_id) {
            return;
        }
        data.info.status = WorkflowStatus::Stopping;
        data.info.cleanup_pending = true;
        data.invalidate_project |= invalidate;
        if invalidate {
            data.info.requires_full_upload = true;
        }
        if target == WorkflowStatus::Failed {
            data.cleanup_target = target;
        }
        if let Some(error) = error {
            data.info.error_code = Some(error.code().into());
            data.info.error_message = Some(error.client_message());
            data.terminal_error = Some(error);
        }
        workflow.cancel.cancel();
        if data.cleanup_started {
            return;
        }
        data.cleanup_started = true;
        data.stop_reason = reason;
        let manager = self.clone();
        let workflow = workflow.clone();
        self.0.tasks.spawn(async move {
            manager.cleanup_workflow(workflow).await;
        });
    }

    async fn cleanup_workflow(&self, workflow: Arc<Workflow>) {
        let activity = workflow.activity.write().await;
        loop {
            let info = Self::snapshot(&workflow);
            if info.status.is_terminal() {
                return;
            }
            let (reason, invalidate) = {
                let data = workflow.data.lock().expect("workflow metadata poisoned");
                (data.stop_reason, data.invalidate_project)
            };
            let cleanup = async {
                if let Some(session_id) = info.session_id {
                    let session = self.0.sessions.stop_session(session_id, reason).await?;
                    if !session.status.is_terminal() {
                        return Err(AppError::Internal(
                            "waiting for confirmed Vivado process cleanup".into(),
                        ));
                    }
                } else if self.0.sessions.is_degraded() {
                    return Err(AppError::Internal(
                        "Vivado startup cleanup is not confirmed".into(),
                    ));
                }
                if invalidate {
                    self.0.projects.mark_dirty(&info.project).await?;
                }
                self.0.sync.abort_open(info.project.clone()).await?;
                self.0.projects.is_clean(&info.project).await
            }
            .await;
            match cleanup {
                Ok(clean) => {
                    let (target, error, now_invalidated) = {
                        let data = workflow.data.lock().expect("workflow metadata poisoned");
                        (
                            data.cleanup_target,
                            data.terminal_error.clone(),
                            data.invalidate_project,
                        )
                    };
                    // A failure discovered during an awaited cleanup can add a
                    // dirty-marker requirement; satisfy it before releasing ownership.
                    if now_invalidated && !invalidate {
                        continue;
                    }
                    if self.terminal(&workflow, &activity, target, error.as_ref(), !clean) {
                        return;
                    }
                }
                Err(error) => {
                    let mut data = workflow.data.lock().expect("workflow metadata poisoned");
                    data.info.error_code = Some(error.code().into());
                    data.info.error_message = Some(error.client_message());
                    tracing::warn!(workflow_id = %data.info.workflow_id, %error, "workflow cleanup will be retried");
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    fn terminal(
        &self,
        workflow: &Workflow,
        _activity: &RwLockWriteGuard<'_, ()>,
        status: WorkflowStatus,
        error: Option<&AppError>,
        requires_full_upload: bool,
    ) -> bool {
        // Keep lock order registry -> workflow consistent with idempotent create.
        let mut registry = self.0.registry.lock().expect("workflow registry poisoned");
        let mut data = workflow.data.lock().expect("workflow metadata poisoned");
        if data.info.status.is_terminal() {
            return data.info.status == status;
        }
        if status == WorkflowStatus::Completed && data.info.status != WorkflowStatus::Pulling {
            return false;
        }
        if data.invalidate_project && !requires_full_upload {
            return false;
        }
        data.info.status = status;
        data.info.cleanup_pending = false;
        data.info.requires_full_upload = requires_full_upload;
        data.open_sync = None;
        data.open_sync_deadline = None;
        data.info.ended_at = Some(Utc::now());
        data.ended = Some(Instant::now());
        if let Some(error) = error {
            data.info.error_code = Some(error.code().into());
            data.info.error_message = Some(error.client_message());
        } else {
            data.info.error_code = None;
            data.info.error_message = None;
        }
        if let Some(session_id) = data.info.session_id
            && let Err(error) = self.0.sessions.release_session(session_id)
        {
            tracing::warn!(%session_id, %error, "could not begin terminal session retention");
        }
        if registry.active == Some(data.info.workflow_id) {
            registry.active = None;
        }
        true
    }

    pub(crate) fn spawn_reaper(&self, cancel: CancellationToken) -> tokio::task::JoinHandle<()> {
        let manager = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(250));
            loop {
                tokio::select! { _ = cancel.cancelled() => break, _ = tick.tick() => {} }
                manager.expire_active();
                manager.prune();
            }
        })
    }

    fn expire_active(&self) {
        let registry = self.0.registry.lock().expect("workflow registry poisoned");
        if registry.stopping {
            return;
        }
        let Some(workflow) = registry.active.and_then(|id| registry.entries.get(&id)) else {
            return;
        };
        let expired = {
            let data = workflow.data.lock().expect("workflow metadata poisoned");
            !data.info.status.is_terminal()
                && data.info.status != WorkflowStatus::Stopping
                && data.heartbeat.elapsed() >= self.0.config.heartbeat_timeout()
        };
        if expired {
            // Heartbeat, completion, and new reservations all need this registry
            // gate, so a stale expiry observation cannot resurrect an old workflow.
            self.request_cleanup_locked(
                &registry,
                workflow,
                TerminationReason::HeartbeatTimeout,
                WorkflowStatus::Cancelled,
                None,
                false,
            );
        }
    }

    fn prune(&self) {
        let mut registry = self.0.registry.lock().expect("workflow registry poisoned");
        let retention = self.0.config.workflow_retention();
        registry.entries.retain(|_, workflow| {
            let data = workflow.data.lock().expect("workflow metadata poisoned");
            data.ended.is_none_or(|ended| ended.elapsed() < retention)
        });
        let mut terminal: Vec<_> = registry
            .entries
            .iter()
            .filter_map(|(id, workflow)| {
                workflow
                    .data
                    .lock()
                    .expect("workflow metadata poisoned")
                    .ended
                    .map(|ended| (*id, ended))
            })
            .collect();
        terminal.sort_by_key(|(_, ended)| *ended);
        let excess = terminal
            .len()
            .saturating_sub(self.0.config.max_retained_workflows);
        for (id, _) in terminal.into_iter().take(excess) {
            registry.entries.remove(&id);
        }
    }

    pub(crate) fn is_degraded(&self) -> bool {
        self.0.sessions.is_degraded() || {
            let registry = self.0.registry.lock().expect("workflow registry poisoned");
            registry.entries.values().any(|workflow| {
                let data = workflow.data.lock().expect("workflow metadata poisoned");
                data.info.status == WorkflowStatus::Stopping && data.info.error_code.is_some()
            })
        }
    }

    pub(crate) async fn shutdown(&self) {
        {
            let mut registry = self.0.registry.lock().expect("workflow registry poisoned");
            registry.stopping = true;
            if let Some(workflow) = registry.active.and_then(|id| registry.entries.get(&id)) {
                self.request_cleanup_locked(
                    &registry,
                    workflow,
                    TerminationReason::ServiceShutdown,
                    WorkflowStatus::Cancelled,
                    None,
                    false,
                );
            }
        }
        // Lower managers remain open until accepted workflow commits and cleanup
        // finish; closing sync admission earlier would reject abort_open forever.
        self.0.tasks.close();
        self.0.tasks.wait().await;
        self.0.sessions.shutdown().await;
        self.0.sync.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AppConfig,
        sync::{ManifestEntry, ManifestEntryKind},
    };
    use std::{fs, os::unix::fs::symlink};

    async fn manager(temp: &tempfile::TempDir) -> WorkflowManager {
        let config = AppConfig {
            vivado_path: "/bin/true".into(),
            workspace_root: temp.path().to_path_buf(),
            auth_tokens: vec!["workflow-test-token-at-least-32-bytes".into()],
            allow_run_as_root: true,
            ..AppConfig::default()
        }
        .into_runtime()
        .unwrap()
        .0;
        let projects = ProjectStore::initialize(temp.path().to_path_buf())
            .await
            .unwrap();
        WorkflowManager::new(
            config.clone(),
            SessionManager::new(config.clone()),
            SyncManager::new(config),
            projects,
        )
    }

    fn create_request(reset_project: bool) -> CreateWorkflowRequest {
        CreateWorkflowRequest {
            project: "demo".into(),
            reset_project,
        }
    }

    async fn clean_project(manager: &WorkflowManager, temp: &tempfile::TempDir) {
        fs::create_dir(temp.path().join("demo")).unwrap();
        manager.0.projects.mark_clean("demo").await.unwrap();
    }

    async fn wait_terminal(manager: &WorkflowManager, id: Uuid) -> WorkflowInfo {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let info = manager.get(id).await.unwrap();
                if info.status.is_terminal() {
                    return info;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("workflow cleanup did not finish")
    }

    #[tokio::test]
    async fn concurrent_identical_creation_is_idempotent_and_reserves_one_slot() {
        let temp = tempfile::tempdir().unwrap();
        let manager = manager(&temp).await;
        let id = Uuid::new_v4();
        let mut requests = Vec::new();
        for _ in 0..12 {
            let manager = manager.clone();
            requests.push(tokio::spawn(async move {
                manager.create(id, create_request(true)).await
            }));
        }
        for request in requests {
            let result = request.await.unwrap().unwrap();
            assert_eq!(result.workflow_id, id);
            assert_eq!(result.status, WorkflowStatus::Preparing);
        }
        assert!(matches!(
            manager.create(Uuid::new_v4(), create_request(true)).await,
            Err(AppError::WorkflowBusy)
        ));
        assert!(matches!(
            manager.create(id, create_request(false)).await,
            Err(AppError::Conflict(_))
        ));
        manager.cancel(id).await.unwrap();
        wait_terminal(&manager, id).await;
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn cancellation_returns_promptly_but_owns_slot_until_transfer_releases() {
        let temp = tempfile::tempdir().unwrap();
        let manager = manager(&temp).await;
        let id = Uuid::new_v4();
        manager.create(id, create_request(true)).await.unwrap();
        let transfer = manager
            .lease(id, &[WorkflowStatus::Preparing])
            .await
            .unwrap();
        let stopping = tokio::time::timeout(Duration::from_secs(1), manager.cancel(id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stopping.status, WorkflowStatus::Stopping);
        assert!(stopping.cleanup_pending);
        assert!(matches!(
            manager.create(Uuid::new_v4(), create_request(true)).await,
            Err(AppError::WorkflowBusy)
        ));
        drop(transfer);
        assert_eq!(
            wait_terminal(&manager, id).await.status,
            WorkflowStatus::Cancelled
        );
        let replacement = Uuid::new_v4();
        manager
            .create(replacement, create_request(true))
            .await
            .unwrap();
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn dirty_marker_failure_keeps_slot_and_cleanup_retries_until_repaired() {
        let temp = tempfile::tempdir().unwrap();
        let manager = manager(&temp).await;
        clean_project(&manager, &temp).await;
        let id = Uuid::new_v4();
        manager.create(id, create_request(false)).await.unwrap();
        let state = temp.path().join(".vivado-server/projects/demo/state");
        let outside = temp.path().join("untouched");
        fs::write(&outside, "clean\n").unwrap();
        fs::remove_file(&state).unwrap();
        symlink(&outside, &state).unwrap();
        manager.fail(
            &manager.workflow(id).unwrap(),
            &AppError::Internal("marker failed".into()),
            true,
        );
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            manager.get(id).await.unwrap().status,
            WorkflowStatus::Stopping
        );
        assert!(manager.is_degraded());
        assert!(matches!(
            manager.create(Uuid::new_v4(), create_request(true)).await,
            Err(AppError::WorkflowBusy)
        ));
        fs::remove_file(&state).unwrap();
        let failed = wait_terminal(&manager, id).await;
        assert_eq!(failed.status, WorkflowStatus::Failed);
        assert!(failed.requires_full_upload);
        assert!(!failed.cleanup_pending);
        assert_eq!(fs::read_to_string(outside).unwrap(), "clean\n");
        assert!(!manager.0.projects.is_clean("demo").await.unwrap());
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn missing_upload_commit_is_retryable_and_preserves_clean_baseline() {
        let temp = tempfile::tempdir().unwrap();
        let manager = manager(&temp).await;
        clean_project(&manager, &temp).await;
        let id = Uuid::new_v4();
        manager.create(id, create_request(false)).await.unwrap();
        let plan = manager
            .push_plan(
                id,
                PushPlanRequest {
                    entries: vec![ManifestEntry {
                        path: "top.tcl".into(),
                        kind: ManifestEntryKind::File,
                        size_bytes: Some(1),
                        mtime_unix_ms: Some(1),
                        sha256: Some(
                            "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"
                                .into(),
                        ),
                        executable: false,
                    }],
                    ..PushPlanRequest::default()
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            manager
                .commit(id, plan.sync_id, CommitSyncRequest::default())
                .await,
            Err(AppError::Conflict(_))
        ));
        assert_eq!(
            manager.get(id).await.unwrap().status,
            WorkflowStatus::Preparing
        );
        assert!(manager.0.projects.is_clean("demo").await.unwrap());
        manager
            .upload(id, plan.sync_id, "top.tcl".into(), Some(1), Body::from("x"))
            .await
            .unwrap();
        manager
            .commit(id, plan.sync_id, CommitSyncRequest::default())
            .await
            .unwrap();
        assert_eq!(fs::read(temp.path().join("demo/top.tcl")).unwrap(), b"x");
        assert!(!manager.0.projects.is_clean("demo").await.unwrap());
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn expired_plan_does_not_permanently_block_later_pushes() {
        let temp = tempfile::tempdir().unwrap();
        let manager = manager(&temp).await;
        let id = Uuid::new_v4();
        manager.create(id, create_request(true)).await.unwrap();
        let first = manager
            .push_plan(id, PushPlanRequest::default())
            .await
            .unwrap();
        manager
            .workflow(id)
            .unwrap()
            .data
            .lock()
            .unwrap()
            .open_sync_deadline = Some(Instant::now());
        let second = manager
            .push_plan(id, PushPlanRequest::default())
            .await
            .unwrap();
        assert_ne!(first.sync_id, second.sync_id);
        assert_eq!(
            manager.sync_status(id, first.sync_id).await.unwrap().status,
            SyncSessionStatus::Aborted
        );
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn rejected_session_arguments_do_not_dirty_the_project() {
        let temp = tempfile::tempdir().unwrap();
        let manager = manager(&temp).await;
        clean_project(&manager, &temp).await;
        let id = Uuid::new_v4();
        manager.create(id, create_request(false)).await.unwrap();
        assert!(matches!(
            manager
                .start_session(
                    id,
                    StartSessionRequest {
                        args: vec!["-mode".into()]
                    }
                )
                .await,
            Err(AppError::BadRequest(_))
        ));
        assert!(manager.0.projects.is_clean("demo").await.unwrap());
        assert_eq!(
            manager.get(id).await.unwrap().status,
            WorkflowStatus::Preparing
        );
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn heartbeat_and_shutdown_share_the_expiry_and_admission_gate() {
        let temp = tempfile::tempdir().unwrap();
        let manager = manager(&temp).await;
        let id = Uuid::new_v4();
        manager.create(id, create_request(true)).await.unwrap();
        manager.workflow(id).unwrap().data.lock().unwrap().heartbeat =
            Instant::now() - Duration::from_secs(121);
        manager.heartbeat(id).await.unwrap();
        manager.expire_active();
        assert_eq!(
            manager.get(id).await.unwrap().status,
            WorkflowStatus::Preparing
        );
        let transfer = manager
            .lease(id, &[WorkflowStatus::Preparing])
            .await
            .unwrap();
        let shutting_down = manager.clone();
        let shutdown = tokio::spawn(async move {
            shutting_down.shutdown().await;
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while manager.get(id).await.unwrap().status != WorkflowStatus::Stopping {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            manager.create(Uuid::new_v4(), create_request(true)).await,
            Err(AppError::ShuttingDown)
        ));
        assert!(!shutdown.is_finished());
        drop(transfer);
        tokio::time::timeout(Duration::from_secs(5), shutdown)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            manager.get(id).await.unwrap().status,
            WorkflowStatus::Cancelled
        );
    }
}
