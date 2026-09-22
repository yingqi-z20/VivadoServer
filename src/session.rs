//! Manager-owned Linux PTY sessions. Dropping an HTTP future never drops a child.

mod model;
mod output;
mod process;
mod supervisor;

use crate::{config::RuntimeConfig, error::AppError};
use chrono::Utc;
pub use model::*;
use output::OutputBuffer;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use uuid::Uuid;

const MAX_VIVADO_ARGS: usize = 128;
const MAX_VIVADO_ARG_BYTES: usize = 32 * 1024;
const INPUT_QUEUE_LENGTH: usize = 8;

#[derive(Clone)]
pub struct SessionManager {
    shared: Arc<Manager>,
}

struct Manager {
    config: RuntimeConfig,
    state: Mutex<ManagerState>,
    tasks: TaskTracker,
    degraded: AtomicBool,
}

#[derive(Default)]
struct ManagerState {
    sessions: HashMap<Uuid, Arc<Session>>,
    active: Option<Uuid>,
    shutting_down: bool,
}

struct Session {
    data: AsyncMutex<SessionData>,
    inputs: mpsc::Sender<Input>,
    input_budget: Arc<Semaphore>,
    stop: CancellationToken,
    stop_reason: Mutex<Option<TerminationReason>>,
    changed: Notify,
    terminal: AtomicBool,
    retained_since: Mutex<Option<Instant>>,
}

struct SessionData {
    info: SessionInfo,
    output: OutputBuffer,
}

struct Input {
    bytes: Vec<u8>,
    offset: usize,
    completion: oneshot::Sender<Result<(), AppError>>,
    _budget: OwnedSemaphorePermit,
}

impl SessionManager {
    pub(crate) fn new(config: RuntimeConfig) -> Self {
        Self {
            shared: Arc::new(Manager {
                config,
                state: Mutex::new(ManagerState::default()),
                tasks: TaskTracker::new(),
                degraded: AtomicBool::new(false),
            }),
        }
    }

    pub fn is_degraded(&self) -> bool {
        self.shared.degraded.load(Ordering::Acquire)
    }

    pub async fn create_session(
        &self,
        request: CreateSessionRequest,
    ) -> Result<SessionInfo, AppError> {
        validate_vivado_args(&request.args)?;
        let id = Uuid::new_v4();
        let (inputs, receiver) = mpsc::channel(INPUT_QUEUE_LENGTH);
        let session = Arc::new(Session {
            data: AsyncMutex::new(SessionData {
                info: SessionInfo {
                    session_id: id,
                    project: request.project.clone(),
                    status: SessionStatus::Running,
                    started_at: Utc::now(),
                    ended_at: None,
                    exit_code: None,
                    termination_reason: None,
                    output_truncated: false,
                    cleanup_error: None,
                },
                output: OutputBuffer::new(self.shared.config.output_buffer_bytes),
            }),
            inputs,
            input_budget: Arc::new(Semaphore::new(self.shared.config.stdin_max_bytes)),
            stop: CancellationToken::new(),
            stop_reason: Mutex::new(None),
            changed: Notify::new(),
            terminal: AtomicBool::new(false),
            retained_since: Mutex::new(None),
        });
        let (started, result) = oneshot::channel();
        {
            // Registration shares shutdown's lock; no await can orphan the slot
            // between its reservation and the owned supervisor being installed.
            let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
            self.shared.prune(&mut state);
            if state.shutting_down || self.is_degraded() {
                return Err(AppError::Capacity(
                    "session service is stopping or requires cleanup".into(),
                ));
            }
            if state.active.is_some() {
                return Err(AppError::Capacity(
                    "a Vivado session is already active".into(),
                ));
            }
            state.active = Some(id);
            state.sessions.insert(id, session.clone());
            let manager = self.shared.clone();
            self.shared.tasks.spawn(async move {
                supervisor::run(manager, session, receiver, request, started).await;
            });
        }
        result
            .await
            .map_err(|_| AppError::Internal("session supervisor stopped during startup".into()))?
    }

    pub async fn get_session(&self, id: Uuid) -> Result<SessionInfo, AppError> {
        Ok(self.session(id)?.snapshot().await)
    }

    /// Start retention only when the enclosing workflow has released its slot.
    /// A workflow may still need sealed session output throughout its pull phase.
    pub(crate) fn release_session(&self, id: Uuid) -> Result<(), AppError> {
        let session = self.session(id)?;
        if !session.terminal.load(Ordering::Acquire) {
            return Err(AppError::SessionNotRunning);
        }
        session.begin_retention();
        let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        self.shared.prune(&mut state);
        Ok(())
    }

    pub async fn send_input(
        &self,
        id: Uuid,
        request: SendInputRequest,
    ) -> Result<SessionInfo, AppError> {
        let session = self.session(id)?;
        if session.stop.is_cancelled() || session.snapshot().await.status != SessionStatus::Running
        {
            return Err(AppError::SessionNotRunning);
        }
        if request.text.len() > self.shared.config.stdin_max_bytes {
            return Err(AppError::BadRequest("stdin exceeds stdin_max_bytes".into()));
        }
        if request.text.is_empty() {
            return Ok(session.snapshot().await);
        }
        let count = u32::try_from(request.text.len())
            .map_err(|_| AppError::BadRequest("stdin is too large".into()))?;
        let budget = session
            .input_budget
            .clone()
            .try_acquire_many_owned(count)
            .map_err(|_| AppError::Capacity("stdin queue byte budget is full".into()))?;
        let (completion, result) = oneshot::channel();
        session
            .inputs
            .try_send(Input {
                bytes: request.text.into_bytes(),
                offset: 0,
                completion,
                _budget: budget,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    AppError::Capacity("stdin queue is full".into())
                }
                mpsc::error::TrySendError::Closed(_) => AppError::SessionNotRunning,
            })?;
        result.await.map_err(|_| AppError::SessionNotRunning)??;
        Ok(session.snapshot().await)
    }

    pub async fn read_output(
        &self,
        id: Uuid,
        query: OutputQuery,
    ) -> Result<OutputResponse, AppError> {
        let session = self.session(id)?;
        let timeout = Duration::from_millis(query.timeout_ms.unwrap_or(30_000).min(60_000));
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let changed = session.changed.notified();
            let data = session.data.lock().await;
            let mut response = data.output.since(query.cursor, data.info.status);
            response.output_truncated = data.info.output_truncated;
            drop(data);
            if !response.chunks.is_empty()
                || response.status.is_terminal()
                || tokio::time::Instant::now() >= deadline
            {
                return Ok(response);
            }
            let _ = tokio::time::timeout_at(deadline, changed).await;
        }
    }

    pub async fn terminate_session(&self, id: Uuid) -> Result<SessionInfo, AppError> {
        self.stop_session(id, TerminationReason::ClientRequested)
            .await
    }

    pub async fn stop_session(
        &self,
        id: Uuid,
        reason: TerminationReason,
    ) -> Result<SessionInfo, AppError> {
        let session = self.session(id)?;
        session.request_stop(reason);
        // Cleanup outlives this waiter. A timed-out stopping result must never
        // be interpreted as evidence that the child is dead.
        let _ = tokio::time::timeout(Duration::from_secs(10), session.wait_terminal()).await;
        Ok(session.snapshot().await)
    }

    pub async fn shutdown(&self) {
        {
            let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
            state.shutting_down = true;
            for session in state.sessions.values() {
                if !session.terminal.load(Ordering::Acquire) {
                    session.request_stop(TerminationReason::ServiceShutdown);
                }
            }
            self.shared.tasks.close();
        }
        if tokio::time::timeout(
            Duration::from_secs(self.shared.config.shutdown_grace_secs),
            self.shared.tasks.wait(),
        )
        .await
        .is_err()
        {
            self.shared.degraded.store(true, Ordering::Release);
            tracing::error!("Vivado cleanup remains active after shutdown deadline");
        }
    }

    fn session(&self, id: Uuid) -> Result<Arc<Session>, AppError> {
        let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        self.shared.prune(&mut state);
        state
            .sessions
            .get(&id)
            .cloned()
            .ok_or(AppError::SessionNotFound)
    }
}

impl Manager {
    fn prune(&self, state: &mut ManagerState) {
        let retention = Duration::from_secs(self.config.workflow_retention_secs);
        state.sessions.retain(|_, session| {
            session
                .retained_since
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_none_or(|ended| ended.elapsed() < retention)
        });
        let mut terminal: Vec<_> = state
            .sessions
            .iter()
            .filter_map(|(id, session)| {
                session
                    .retained_since
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .map(|ended| (*id, ended))
            })
            .collect();
        terminal.sort_by_key(|(_, ended)| *ended);
        let remove_count = terminal
            .len()
            .saturating_sub(self.config.max_retained_workflows);
        for (id, _) in terminal.into_iter().take(remove_count) {
            state.sessions.remove(&id);
        }
    }

    fn finished(&self, id: Uuid) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.active == Some(id) {
            state.active = None;
        }
        self.degraded.store(false, Ordering::Release);
        self.prune(&mut state);
    }
}

impl Session {
    fn begin_retention(&self) {
        self.retained_since
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_insert_with(Instant::now);
    }

    async fn snapshot(&self) -> SessionInfo {
        self.data.lock().await.info.clone()
    }

    fn request_stop(&self, reason: TerminationReason) {
        self.stop_reason
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_insert(reason);
        self.stop.cancel();
    }

    fn reason(&self) -> TerminationReason {
        self.stop_reason
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .unwrap_or(TerminationReason::ClientRequested)
    }

    async fn wait_terminal(&self) {
        loop {
            let changed = self.changed.notified();
            if self.terminal.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }

    async fn push_output(&self, text: String) {
        if text.is_empty() {
            return;
        }
        let mut data = self.data.lock().await;
        debug_assert!(!data.info.status.is_terminal());
        data.output.push(text);
        drop(data);
        self.changed.notify_waiters();
    }

    async fn stopping(&self, reason: TerminationReason) {
        let mut data = self.data.lock().await;
        data.info.status = SessionStatus::Stopping;
        data.info.termination_reason.get_or_insert(reason);
        drop(data);
        self.changed.notify_waiters();
    }

    async fn finish(&self, reason: TerminationReason, code: Option<i32>, truncated: bool) {
        let mut data = self.data.lock().await;
        data.info.status = match reason {
            TerminationReason::ProcessExited => SessionStatus::Exited,
            TerminationReason::ProcessStartFailed
            | TerminationReason::ProcessWaitFailed
            | TerminationReason::InputWriteFailed
            | TerminationReason::OutputReadFailed => SessionStatus::Failed,
            _ => SessionStatus::Terminated,
        };
        data.info.termination_reason = Some(reason);
        data.info.exit_code = code;
        data.info.ended_at = Some(Utc::now());
        data.info.output_truncated = truncated;
        data.info.cleanup_error = None;
        self.terminal.store(true, Ordering::Release);
        drop(data);
        self.changed.notify_waiters();
    }
}

pub(crate) fn validate_vivado_args(args: &[String]) -> Result<(), AppError> {
    if args.len() > MAX_VIVADO_ARGS
        || args.iter().map(String::len).sum::<usize>() > MAX_VIVADO_ARG_BYTES
    {
        return Err(AppError::BadRequest(
            "Vivado arguments exceed the count or byte limit".into(),
        ));
    }
    for arg in args {
        let lower = arg.to_ascii_lowercase();
        if arg.contains('\0')
            || matches!(lower.as_str(), "-mode" | "-gui" | "-batch" | "-tcl")
            || lower.starts_with("-mode=")
        {
            return Err(AppError::BadRequest(
                "invalid Vivado argument; execution mode is owned by the server".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stopping_is_not_terminal() {
        assert!(!SessionStatus::Running.is_terminal());
        assert!(!SessionStatus::Stopping.is_terminal());
        assert!(SessionStatus::Exited.is_terminal());
        assert!(SessionStatus::Terminated.is_terminal());
        assert!(SessionStatus::Failed.is_terminal());
    }
    #[test]
    fn strict_requests_and_server_owned_mode() {
        assert!(
            serde_json::from_str::<SendInputRequest>(r#"{"text":"puts ok","extra":true}"#).is_err()
        );
        assert!(validate_vivado_args(&["-nolog".into()]).is_ok());
        assert!(validate_vivado_args(&["-mode=batch".into()]).is_err());
        assert!(validate_vivado_args(&["-MODE".into()]).is_err());
    }

    #[tokio::test]
    async fn raw_long_line_exit_code_and_sealed_output_survive_until_workflow_release() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("fake-vivado");
        std::fs::write(
            &executable,
            "#!/bin/sh\nIFS= read -r line\nprintf 'FINAL:%s\\n' \"${#line}\"\nexit 23\n",
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        let (config, _) = crate::config::AppConfig {
            vivado_path: executable,
            workspace_root: temp.path().to_path_buf(),
            auth_tokens: vec!["0123456789abcdef0123456789abcdef".into()],
            allow_run_as_root: true,
            ..crate::config::AppConfig::default()
        }
        .into_runtime()
        .unwrap();
        let manager = SessionManager::new(config);
        let info = manager
            .create_session(CreateSessionRequest {
                project: "long-line".into(),
                args: vec![],
            })
            .await
            .unwrap();
        manager
            .send_input(
                info.session_id,
                SendInputRequest {
                    text: format!("{}\n", "x".repeat(16 * 1024)),
                },
            )
            .await
            .unwrap();
        let session = manager.session(info.session_id).unwrap();
        tokio::time::timeout(Duration::from_secs(8), session.wait_terminal())
            .await
            .unwrap();
        let info = manager.get_session(info.session_id).await.unwrap();
        assert_eq!(info.status, SessionStatus::Exited);
        assert_eq!(info.exit_code, Some(23));
        assert!(!info.output_truncated);
        let output = manager
            .read_output(
                info.session_id,
                OutputQuery {
                    cursor: 0,
                    timeout_ms: Some(0),
                },
            )
            .await
            .unwrap();
        let text: String = output
            .chunks
            .iter()
            .map(|chunk| chunk.text.as_str())
            .collect();
        assert_eq!(text, "FINAL:16384\n");
        let last = manager
            .read_output(
                info.session_id,
                OutputQuery {
                    cursor: output.cursor,
                    timeout_ms: Some(0),
                },
            )
            .await
            .unwrap();
        assert_eq!(last.cursor, output.cursor);
        assert!(last.chunks.is_empty());
        assert!(session.retained_since.lock().unwrap().is_none());
        manager.release_session(info.session_id).unwrap();
        assert!(session.retained_since.lock().unwrap().is_some());
        manager.shutdown().await;
    }
}
