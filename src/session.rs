use crate::{AppConfig, error::AppError, paths::resolve_project_dir};
use anyhow::Context;
use chrono::{DateTime, Utc};
use portable_pty::{Child, ChildKiller, CommandBuilder, MasterPty, PtySize, native_pty_system};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    io::{Read, Write},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify, RwLock, Semaphore},
    time,
};
use uuid::Uuid;

const DEFAULT_LONG_POLL_MS: u64 = 30_000;
const MAX_LONG_POLL_MS: u64 = 60_000;
const TERMINATE_GRACE_MS: u64 = 1_500;

#[derive(Debug, Deserialize)]
pub struct CreateSessionRequest {
    pub project: String,
    #[serde(default)]
    pub args: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct SendInputRequest {
    pub text: String,
}

#[derive(Debug, Deserialize)]
pub struct OutputQuery {
    #[serde(default)]
    pub cursor: u64,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Running,
    Exited,
    Terminated,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionInfo {
    pub session_id: Uuid,
    pub project: String,
    pub status: SessionStatus,
    pub started_at: DateTime<Utc>,
    pub last_heartbeat_at: DateTime<Utc>,
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OutputChunk {
    pub seq: u64,
    pub timestamp: DateTime<Utc>,
    pub text: String,
}

#[derive(Debug, Serialize)]
pub struct OutputResponse {
    pub cursor: u64,
    pub chunks: Vec<OutputChunk>,
    pub status: SessionStatus,
    pub overrun: bool,
}

#[derive(Clone)]
pub struct SessionManager {
    config: Arc<AppConfig>,
    inner: Arc<RwLock<HashMap<Uuid, Arc<Session>>>>,
    active_slots: Arc<Semaphore>,
}

struct Session {
    info: AsyncMutex<SessionInfo>,
    process: Arc<Mutex<Option<Box<dyn Child + Send + Sync>>>>,
    _master: Arc<Mutex<Option<Box<dyn MasterPty + Send>>>>,
    killer: Arc<Mutex<Box<dyn ChildKiller + Send + Sync>>>,
    reader: Arc<Mutex<Option<Box<dyn Read + Send>>>>,
    writer: Arc<Mutex<Box<dyn Write + Send>>>,
    output: AsyncMutex<OutputBuffer>,
    notify_output: Notify,
    slot_released: AtomicBool,
}

struct OutputBuffer {
    chunks: VecDeque<OutputChunk>,
    bytes: usize,
    max_bytes: usize,
    next_seq: u64,
}

impl SessionManager {
    pub fn new(config: AppConfig) -> Self {
        let max_active_sessions = config.max_active_sessions;
        tracing::debug!(
            max_active_sessions,
            heartbeat_timeout_secs = config.heartbeat_timeout_secs,
            output_buffer_bytes = config.output_buffer_bytes,
            "initializing session manager"
        );
        Self {
            config: Arc::new(config),
            inner: Arc::new(RwLock::new(HashMap::new())),
            active_slots: Arc::new(Semaphore::new(max_active_sessions)),
        }
    }

    pub fn spawn_heartbeat_reaper(&self) {
        let manager = self.clone();
        tracing::debug!("starting session heartbeat reaper");
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                manager.reap_expired_sessions().await;
            }
        });
    }

    pub async fn create_session(
        &self,
        request: CreateSessionRequest,
    ) -> Result<SessionInfo, AppError> {
        tracing::debug!(
            project = %request.project,
            args = ?request.args,
            available_slots = self.active_slots.available_permits(),
            "creating vivado session"
        );
        let slot = match self.active_slots.clone().try_acquire_owned() {
            Ok(slot) => slot,
            Err(_) => {
                tracing::warn!(
                    project = %request.project,
                    max_active_sessions = self.config.max_active_sessions,
                    "session limit reached"
                );
                return Err(AppError::SessionLimitReached);
            }
        };

        let project_dir = resolve_project_dir(&self.config.workspace_root, &request.project)?;
        tokio::fs::create_dir_all(&project_dir)
            .await
            .map_err(|err| AppError::Internal(format!("failed to create project dir: {err}")))?;
        tracing::debug!(
            project = %request.project,
            project_dir = %project_dir.display(),
            "project directory ready"
        );

        let project_name = request.project.clone();
        let started = spawn_vivado(
            self.config.vivado_path.clone(),
            request.args,
            project_dir,
            self.config.output_buffer_bytes,
            request.project,
        )
        .await?;

        let session_id = started.snapshot().await.session_id;
        let session = Arc::new(started);
        self.inner.write().await.insert(session_id, session.clone());
        self.spawn_output_reader(session.clone());
        self.spawn_exit_watcher(session.clone(), slot);
        tracing::info!(
            session_id = %session_id,
            project = %project_name,
            "vivado session created"
        );

        Ok(session.snapshot().await)
    }

    pub async fn get_session(&self, session_id: Uuid) -> Result<SessionInfo, AppError> {
        let info = self.session(session_id).await?.snapshot().await;
        tracing::debug!(
            session_id = %session_id,
            status = ?info.status,
            exit_code = ?info.exit_code,
            "read session state"
        );
        Ok(info)
    }

    pub async fn send_input(
        &self,
        session_id: Uuid,
        request: SendInputRequest,
    ) -> Result<SessionInfo, AppError> {
        let session = self.session(session_id).await?;
        if session.snapshot().await.status != SessionStatus::Running {
            return Err(AppError::SessionNotRunning);
        }

        let writer = session.writer.clone();
        let text = request.text;
        tracing::debug!(
            session_id = %session_id,
            bytes = text.len(),
            "writing to vivado stdin"
        );
        tokio::task::spawn_blocking(move || -> Result<(), AppError> {
            let mut writer = writer
                .lock()
                .map_err(|_| AppError::Internal("stdin writer lock poisoned".to_string()))?;
            writer
                .write_all(text.as_bytes())
                .map_err(|err| AppError::Internal(format!("failed to write stdin: {err}")))?;
            writer
                .flush()
                .map_err(|err| AppError::Internal(format!("failed to flush stdin: {err}")))?;
            Ok(())
        })
        .await
        .map_err(|err| AppError::Internal(format!("stdin task failed: {err}")))??;

        let info = session.snapshot().await;
        tracing::debug!(
            session_id = %session_id,
            status = ?info.status,
            "stdin write completed"
        );
        Ok(info)
    }

    pub async fn read_output(
        &self,
        session_id: Uuid,
        query: OutputQuery,
    ) -> Result<OutputResponse, AppError> {
        let session = self.session(session_id).await?;
        let timeout = Duration::from_millis(
            query
                .timeout_ms
                .unwrap_or(DEFAULT_LONG_POLL_MS)
                .min(MAX_LONG_POLL_MS),
        );
        tracing::debug!(
            session_id = %session_id,
            cursor = query.cursor,
            timeout_ms = timeout.as_millis() as u64,
            "reading vivado output"
        );
        let deadline = Instant::now() + timeout;

        loop {
            let response = session.output_since(query.cursor).await;
            if !response.chunks.is_empty()
                || response.status != SessionStatus::Running
                || Instant::now() >= deadline
            {
                let byte_count: usize = response.chunks.iter().map(|chunk| chunk.text.len()).sum();
                tracing::debug!(
                    session_id = %session_id,
                    request_cursor = query.cursor,
                    response_cursor = response.cursor,
                    chunks = response.chunks.len(),
                    bytes = byte_count,
                    status = ?response.status,
                    overrun = response.overrun,
                    "returning vivado output"
                );
                return Ok(response);
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            tokio::select! {
                _ = session.notify_output.notified() => {},
                _ = time::sleep(remaining) => {},
            }
        }
    }

    pub async fn heartbeat(&self, session_id: Uuid) -> Result<SessionInfo, AppError> {
        let session = self.session(session_id).await?;
        let mut info = session.info.lock().await;
        info.last_heartbeat_at = Utc::now();
        tracing::debug!(
            session_id = %session_id,
            status = ?info.status,
            last_heartbeat_at = %info.last_heartbeat_at,
            "session heartbeat updated"
        );
        Ok(info.clone())
    }

    pub async fn terminate_session(&self, session_id: Uuid) -> Result<SessionInfo, AppError> {
        let session = self.session(session_id).await?;
        tracing::debug!(session_id = %session_id, "terminating session by request");
        terminate_process(session.clone(), SessionStatus::Terminated).await;
        Ok(session.snapshot().await)
    }

    async fn session(&self, session_id: Uuid) -> Result<Arc<Session>, AppError> {
        let session = self.inner.read().await.get(&session_id).cloned();
        if session.is_none() {
            tracing::debug!(session_id = %session_id, "session not found");
        }
        session.ok_or(AppError::SessionNotFound)
    }

    async fn reap_expired_sessions(&self) {
        let sessions: Vec<Arc<Session>> = self.inner.read().await.values().cloned().collect();
        tracing::trace!(
            session_count = sessions.len(),
            "checking sessions for heartbeat expiration"
        );
        for session in sessions {
            let expired = {
                let info = session.info.lock().await;
                info.status == SessionStatus::Running
                    && (Utc::now() - info.last_heartbeat_at)
                        .to_std()
                        .unwrap_or_default()
                        > self.config.heartbeat_timeout()
            };
            if expired {
                let id = session.snapshot().await.session_id;
                tracing::warn!(session_id = %id, "terminating session after heartbeat timeout");
                terminate_process(session, SessionStatus::Terminated).await;
            }
        }
    }

    fn spawn_output_reader(&self, session: Arc<Session>) {
        let session_for_error = session.clone();
        tokio::spawn(async move {
            let session_id = session.snapshot().await.session_id;
            tracing::debug!(session_id = %session_id, "starting vivado output reader");
            let reader = {
                let reader = session.reader.clone();
                tokio::task::spawn_blocking(move || -> anyhow::Result<Box<dyn Read + Send>> {
                    let mut guard = reader
                        .lock()
                        .map_err(|_| anyhow::anyhow!("reader lock poisoned"))?;
                    guard.take().context("pty reader is missing")
                })
                .await
                .map_err(|err| anyhow::anyhow!("reader task failed: {err}"))
                .and_then(|result| result)
            };

            match reader {
                Ok(mut reader) => {
                    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
                    let output_session = session.clone();
                    tokio::spawn(async move {
                        while let Some(text) = rx.recv().await {
                            output_session.push_output(text).await;
                        }
                    });

                    let read_result = tokio::task::spawn_blocking(move || {
                        let mut buffer = [0_u8; 4096];
                        loop {
                            match reader.read(&mut buffer) {
                                Ok(0) => break,
                                Ok(len) => {
                                    let text = String::from_utf8_lossy(&buffer[..len]).to_string();
                                    tracing::trace!(
                                        session_id = %session_id,
                                        bytes = len,
                                        "read vivado output chunk"
                                    );
                                    if tx.send(text).is_err() {
                                        break;
                                    }
                                }
                                Err(err) => {
                                    tracing::debug!(
                                        session_id = %session_id,
                                        "pty reader stopped: {err}"
                                    );
                                    break;
                                }
                            }
                        }
                    })
                    .await;
                    if let Err(err) = read_result {
                        tracing::warn!(session_id = %session_id, "output reader task failed: {err}");
                    }
                    tracing::debug!(session_id = %session_id, "vivado output reader stopped");
                }
                Err(err) => {
                    tracing::warn!(
                        session_id = %session_id,
                        "failed to start output reader: {err}"
                    );
                    session_for_error.mark_failed().await;
                }
            }
        });
    }

    fn spawn_exit_watcher(&self, session: Arc<Session>, slot: tokio::sync::OwnedSemaphorePermit) {
        tokio::spawn(async move {
            let session_id = session.snapshot().await.session_id;
            tracing::debug!(session_id = %session_id, "waiting for vivado process exit");
            let process = session.process.clone();
            let status = tokio::task::spawn_blocking(move || {
                let mut process = process
                    .lock()
                    .map_err(|_| anyhow::anyhow!("process lock poisoned"))?;
                if let Some(child) = process.as_mut() {
                    child.wait().context("failed waiting for vivado process")
                } else {
                    Err(anyhow::anyhow!("process already removed"))
                }
            })
            .await;

            let exit_code = match status {
                Ok(Ok(status)) => Some(status.exit_code() as i32),
                Ok(Err(err)) => {
                    tracing::warn!(session_id = %session_id, "process wait failed: {err}");
                    None
                }
                Err(err) => {
                    tracing::warn!(session_id = %session_id, "process wait task failed: {err}");
                    None
                }
            };

            let mut info = session.info.lock().await;
            if info.status == SessionStatus::Running {
                info.status = SessionStatus::Exited;
            }
            info.exit_code = exit_code;
            session.notify_output.notify_waiters();
            if !session.slot_released.swap(true, Ordering::SeqCst) {
                drop(slot);
            }
            tracing::info!(
                session_id = %session_id,
                status = ?info.status,
                exit_code = ?info.exit_code,
                "vivado session finished"
            );
        });
    }
}

async fn spawn_vivado(
    vivado_path: PathBuf,
    args: Vec<String>,
    project_dir: PathBuf,
    output_buffer_bytes: usize,
    project: String,
) -> Result<Session, AppError> {
    tracing::debug!(
        vivado_path = %vivado_path.display(),
        project_dir = %project_dir.display(),
        project = %project,
        args = ?args,
        "spawning vivado process"
    );
    tokio::task::spawn_blocking(move || -> Result<Session, AppError> {
        let pty_system = native_pty_system();
        let pair = pty_system
            .openpty(PtySize {
                rows: 40,
                cols: 120,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|err| AppError::Internal(format!("failed to open pty: {err}")))?;

        let mut cmd = CommandBuilder::new(vivado_path);
        cmd.cwd(project_dir);
        for arg in args {
            cmd.arg(arg);
        }

        let child = pair
            .slave
            .spawn_command(cmd)
            .map_err(|err| AppError::Internal(format!("failed to spawn vivado: {err}")))?;
        let killer = child.clone_killer();
        drop(pair.slave);
        let reader = pair
            .master
            .try_clone_reader()
            .map_err(|err| AppError::Internal(format!("failed to open pty reader: {err}")))?;
        let writer = pair
            .master
            .take_writer()
            .map_err(|err| AppError::Internal(format!("failed to open pty writer: {err}")))?;
        let now = Utc::now();
        let session_id = Uuid::new_v4();
        tracing::debug!(
            session_id = %session_id,
            project = %project,
            "vivado process spawned"
        );

        Ok(Session {
            info: AsyncMutex::new(SessionInfo {
                session_id,
                project,
                status: SessionStatus::Running,
                started_at: now,
                last_heartbeat_at: now,
                exit_code: None,
            }),
            process: Arc::new(Mutex::new(Some(child))),
            _master: Arc::new(Mutex::new(Some(pair.master))),
            killer: Arc::new(Mutex::new(killer)),
            reader: Arc::new(Mutex::new(Some(reader))),
            writer: Arc::new(Mutex::new(writer)),
            output: AsyncMutex::new(OutputBuffer::new(output_buffer_bytes)),
            notify_output: Notify::new(),
            slot_released: AtomicBool::new(false),
        })
    })
    .await
    .map_err(|err| AppError::Internal(format!("spawn task failed: {err}")))?
}

async fn terminate_process(session: Arc<Session>, final_status: SessionStatus) {
    let session_id = session.snapshot().await.session_id;
    {
        let mut info = session.info.lock().await;
        if info.status != SessionStatus::Running {
            tracing::debug!(
                session_id = %session_id,
                status = ?info.status,
                "terminate skipped because session is not running"
            );
            return;
        }
        tracing::debug!(
            session_id = %session_id,
            final_status = ?final_status,
            "marking session for termination"
        );
        info.status = final_status;
    }

    let writer = session.writer.clone();
    let _ = tokio::task::spawn_blocking(move || {
        if let Ok(mut writer) = writer.lock() {
            let _ = writer.write_all(b"exit\n");
            let _ = writer.flush();
        }
    })
    .await;
    tracing::debug!(
        session_id = %session_id,
        grace_ms = TERMINATE_GRACE_MS,
        "sent graceful exit to vivado"
    );

    time::sleep(Duration::from_millis(TERMINATE_GRACE_MS)).await;

    let killer = session.killer.clone();
    let _ = tokio::task::spawn_blocking(move || {
        if let Ok(mut killer) = killer.lock() {
            let _ = killer.kill();
        }
    })
    .await;
    tracing::debug!(session_id = %session_id, "sent kill signal to vivado process");
    session.notify_output.notify_waiters();
}

impl Session {
    async fn snapshot(&self) -> SessionInfo {
        self.info.lock().await.clone()
    }

    async fn push_output(&self, text: String) {
        let session_id = self.info.lock().await.session_id;
        let text_len = text.len();
        let mut output = self.output.lock().await;
        let (seq, buffer_bytes, evicted_chunks) = output.push(text);
        tracing::trace!(
            session_id = %session_id,
            seq,
            bytes = text_len,
            buffer_bytes,
            evicted_chunks,
            "buffered vivado output chunk"
        );
        drop(output);
        self.notify_output.notify_waiters();
    }

    async fn output_since(&self, cursor: u64) -> OutputResponse {
        let status = self.snapshot().await.status;
        let output = self.output.lock().await;
        output.since(cursor, status)
    }

    async fn mark_failed(&self) {
        let mut info = self.info.lock().await;
        if info.status == SessionStatus::Running {
            info.status = SessionStatus::Failed;
            tracing::debug!(
                session_id = %info.session_id,
                "session marked failed"
            );
        }
        self.notify_output.notify_waiters();
    }
}

impl OutputBuffer {
    fn new(max_bytes: usize) -> Self {
        Self {
            chunks: VecDeque::new(),
            bytes: 0,
            max_bytes,
            next_seq: 0,
        }
    }

    fn push(&mut self, text: String) -> (u64, usize, usize) {
        let len = text.len();
        let seq = self.next_seq;
        let chunk = OutputChunk {
            seq,
            timestamp: Utc::now(),
            text,
        };
        self.next_seq += 1;
        self.bytes += len;
        self.chunks.push_back(chunk);

        let mut evicted_chunks = 0;
        while self.bytes > self.max_bytes {
            let Some(front) = self.chunks.pop_front() else {
                self.bytes = 0;
                break;
            };
            self.bytes = self.bytes.saturating_sub(front.text.len());
            evicted_chunks += 1;
        }
        (seq, self.bytes, evicted_chunks)
    }

    fn since(&self, cursor: u64, status: SessionStatus) -> OutputResponse {
        let first_seq = self
            .chunks
            .front()
            .map(|chunk| chunk.seq)
            .unwrap_or(self.next_seq);
        let overrun = cursor < first_seq;
        let chunks: Vec<OutputChunk> = self
            .chunks
            .iter()
            .filter(|chunk| chunk.seq >= cursor)
            .cloned()
            .collect();
        OutputResponse {
            cursor: self.next_seq,
            chunks,
            status,
            overrun,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_buffer_tracks_cursor_and_overrun() {
        let mut buffer = OutputBuffer::new(5);
        buffer.push("abc".to_string());
        buffer.push("def".to_string());

        let response = buffer.since(0, SessionStatus::Running);
        assert!(response.overrun);
        assert_eq!(response.cursor, 2);
        assert_eq!(response.chunks.len(), 1);
        assert_eq!(response.chunks[0].text, "def");
    }
}
