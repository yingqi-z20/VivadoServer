//! Best-effort local diagnostics. A single bounded writer never blocks the PTY.
//!
//! Files contain decoded PTY output (including potentially sensitive tool output),
//! not service log messages. A missing `finished` record means an interrupted
//! archive; even a finished record must be checked for `archive_truncated`.

use crate::{
    config::{ObservabilityConfig, RuntimeConfig},
    observability::Observability,
    project::ProjectStore,
    session::SessionInfo,
    workspace::{INTERNAL_DIR, ensure_real_directory_chain},
};
use anyhow::Context;
use chrono::Utc;
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::Notify;
use uuid::Uuid;

const QUEUE_LENGTH: usize = 64;
const CHUNK_BYTES: usize = 4096;
const FOOTER_RESERVE: u64 = 2048;
const FORMAT: &str = "vivado-server-pty-v1";
const QUEUE_FULL: u32 = 1;
const SESSION_LIMIT: u32 = 2;
const TOTAL_LIMIT: u32 = 4;
const DISK_ERROR: u32 = 8;
const WORKER_STOPPED: u32 = 16;
const DRAIN_TIMEOUT: u32 = 32;

#[derive(Clone, Default)]
pub(crate) struct Diagnostics {
    inner: Option<Arc<DiagnosticsInner>>,
}

struct DiagnosticsInner {
    sender: SyncSender<Command>,
    telemetry: Observability,
    stopping: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    changed: Arc<Notify>,
}

enum Command {
    Start(Arc<ArchiveState>, SessionInfo),
    Output(Arc<ArchiveState>, String),
    Wake,
}

pub(crate) struct OutputArchive {
    sender: SyncSender<Command>,
    state: Arc<ArchiveState>,
}

struct ArchiveState {
    id: Uuid,
    project: String,
    telemetry: Observability,
    disabled: AtomicBool,
    issues: AtomicU32,
    observed: AtomicU64,
    dropped: AtomicU64,
    pending: AtomicUsize,
    final_info: Mutex<Option<SessionInfo>>,
    completed: AtomicBool,
    changed: Notify,
}

impl ArchiveState {
    fn issue(&self, flag: u32, outcome: &'static str) {
        self.disabled.store(true, Ordering::Release);
        if self.issues.fetch_or(flag, Ordering::AcqRel) & flag == 0 {
            self.telemetry.event("archive", outcome);
            tracing::warn!(session_id = %self.id, project = %self.project,
                reason = outcome, "PTY diagnostics archive is incomplete");
        }
    }

    fn complete(&self) {
        self.completed.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
}

impl Diagnostics {
    pub(crate) fn disabled() -> Self {
        Self::default()
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.inner.as_ref().is_some_and(|inner| {
            !inner.stopping.load(Ordering::Acquire) && !inner.stopped.load(Ordering::Acquire)
        })
    }

    #[cfg(test)]
    pub(crate) fn stalled_for_test(telemetry: Observability) -> (Self, Box<dyn Send>) {
        let (sender, receiver) = mpsc::sync_channel(QUEUE_LENGTH);
        let diagnostics = Self {
            inner: Some(Arc::new(DiagnosticsInner {
                sender,
                telemetry,
                stopping: Arc::new(AtomicBool::new(false)),
                stopped: Arc::new(AtomicBool::new(false)),
                changed: Arc::new(Notify::new()),
            })),
        };
        (diagnostics, Box::new(receiver))
    }

    /// Initialize only after acquiring ProjectStore's workspace instance lock.
    /// Filesystem failures disable this optional facility, never process cleanup.
    pub(crate) async fn initialize(
        config: &RuntimeConfig,
        workspace: ProjectStore,
    ) -> anyhow::Result<Self> {
        if !config.observability.archive_output {
            return Ok(Self::disabled());
        }
        let root = config.workspace_root.join(INTERNAL_DIR).join("diagnostics");
        let options = config.observability.clone();
        let telemetry = config.telemetry.clone();
        let span = tracing::Span::current();
        let initialized = tokio::task::spawn_blocking(move || {
            let _entered = span.enter();
            create_directory(&root)?;
            let mut writer = Writer {
                _workspace: workspace,
                root,
                options,
                telemetry,
                archives: HashMap::new(),
                total_bytes: 0,
            };
            writer.prune(0, 0)?;
            Ok::<_, anyhow::Error>(writer)
        })
        .await;
        let writer = match initialized {
            Ok(Ok(writer)) => writer,
            Ok(Err(error)) => {
                config.telemetry.event("archive", "initialization_failed");
                tracing::error!(%error, "PTY diagnostics disabled: initialization failed");
                return Ok(Self::disabled());
            }
            Err(error) => {
                config.telemetry.event("archive", "initialization_failed");
                tracing::error!(%error, "PTY diagnostics initialization task failed");
                return Ok(Self::disabled());
            }
        };
        let (sender, receiver) = mpsc::sync_channel(QUEUE_LENGTH);
        let stopping = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let changed = Arc::new(Notify::new());
        let worker_stopping = stopping.clone();
        let worker_stopped = stopped.clone();
        let worker_changed = changed.clone();
        let worker_telemetry = config.telemetry.clone();
        if let Err(error) = std::thread::Builder::new()
            .name("pty-diagnostics".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    writer.run(receiver, worker_stopping);
                }));
                if result.is_err() {
                    worker_telemetry.event("archive", "worker_panicked");
                    tracing::error!("PTY diagnostics writer panicked");
                }
                worker_stopped.store(true, Ordering::Release);
                worker_changed.notify_waiters();
            })
        {
            config.telemetry.event("archive", "initialization_failed");
            tracing::error!(%error, "PTY diagnostics disabled: writer startup failed");
            return Ok(Self::disabled());
        }
        Ok(Self {
            inner: Some(Arc::new(DiagnosticsInner {
                sender,
                telemetry: config.telemetry.clone(),
                stopping,
                stopped,
                changed,
            })),
        })
    }

    pub(crate) fn start(&self, info: &SessionInfo) -> Option<OutputArchive> {
        let inner = self.inner.as_ref()?;
        let state = Arc::new(ArchiveState {
            id: info.session_id,
            project: info.project.clone(),
            telemetry: inner.telemetry.clone(),
            disabled: AtomicBool::new(false),
            issues: AtomicU32::new(0),
            observed: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            pending: AtomicUsize::new(0),
            final_info: Mutex::new(None),
            completed: AtomicBool::new(false),
            changed: Notify::new(),
        });
        if inner.stopping.load(Ordering::Acquire) {
            state.issue(WORKER_STOPPED, "worker_stopped");
            return None;
        }
        if let Err(error) = inner
            .sender
            .try_send(Command::Start(state.clone(), info.clone()))
        {
            let (flag, reason) = match error {
                mpsc::TrySendError::Full(_) => (QUEUE_FULL, "queue_full"),
                mpsc::TrySendError::Disconnected(_) => (WORKER_STOPPED, "worker_stopped"),
            };
            state.issue(flag, reason);
            return None;
        }
        Some(OutputArchive {
            sender: inner.sender.clone(),
            state,
        })
    }

    pub(crate) async fn shutdown(&self) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        inner.stopping.store(true, Ordering::Release);
        let _ = inner.sender.try_send(Command::Wake);
        let wait = async {
            loop {
                let changed = inner.changed.notified();
                if inner.stopped.load(Ordering::Acquire) {
                    break;
                }
                changed.await;
            }
        };
        if tokio::time::timeout(Duration::from_secs(2), wait)
            .await
            .is_err()
        {
            inner.telemetry.event("archive", "shutdown_timeout");
            tracing::error!("PTY diagnostics writer did not finish within shutdown deadline");
        }
    }
}

impl OutputArchive {
    pub(crate) fn push(&self, text: &str) {
        self.state
            .observed
            .fetch_add(text.len() as u64, Ordering::Relaxed);
        let mut remaining = text;
        while !remaining.is_empty() {
            if self.state.disabled.load(Ordering::Acquire) {
                self.state
                    .dropped
                    .fetch_add(remaining.len() as u64, Ordering::Relaxed);
                return;
            }
            let mut end = remaining.len().min(CHUNK_BYTES);
            while !remaining.is_char_boundary(end) {
                end -= 1;
            }
            let chunk = remaining[..end].to_owned();
            self.state.pending.fetch_add(1, Ordering::AcqRel);
            if let Err(error) = self
                .sender
                .try_send(Command::Output(self.state.clone(), chunk))
            {
                self.state.pending.fetch_sub(1, Ordering::AcqRel);
                let (flag, reason) = match error {
                    mpsc::TrySendError::Full(_) => (QUEUE_FULL, "queue_full"),
                    mpsc::TrySendError::Disconnected(_) => (WORKER_STOPPED, "worker_stopped"),
                };
                self.state.issue(flag, reason);
                self.state
                    .dropped
                    .fetch_add(remaining.len() as u64, Ordering::Relaxed);
                return;
            }
            remaining = &remaining[end..];
        }
    }

    pub(crate) fn seal(&self, info: SessionInfo) {
        *self
            .state
            .final_info
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(info);
        let _ = self.sender.try_send(Command::Wake);
    }

    #[cfg(test)]
    pub(crate) async fn finish(self, info: SessionInfo) {
        self.seal(info);
        self.wait().await;
    }

    pub(crate) async fn wait(self) {
        let wait = async {
            loop {
                let changed = self.state.changed.notified();
                if self.state.completed.load(Ordering::Acquire) {
                    break;
                }
                changed.await;
            }
        };
        if tokio::time::timeout(Duration::from_millis(500), wait)
            .await
            .is_err()
        {
            self.state.issue(DRAIN_TIMEOUT, "drain_timeout");
        }
    }
}

struct OpenArchive {
    file: File,
    state: Arc<ArchiveState>,
    bytes: u64,
    output_bytes: u64,
}

struct Writer {
    // The writer can outlive a timed-out shutdown. Keep exclusive workspace
    // ownership until every filesystem operation on this thread has stopped.
    _workspace: ProjectStore,
    root: PathBuf,
    options: ObservabilityConfig,
    telemetry: Observability,
    archives: HashMap<Uuid, OpenArchive>,
    total_bytes: u64,
}

impl Writer {
    fn run(mut self, receiver: Receiver<Command>, stopping: Arc<AtomicBool>) {
        let mut last_prune = Instant::now();
        loop {
            match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(Command::Start(state, info)) => self.start(state, &info),
                Ok(Command::Output(state, text)) => {
                    self.output(&state, &text);
                    state.pending.fetch_sub(1, Ordering::AcqRel);
                }
                Ok(Command::Wake) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) if stopping.load(Ordering::Acquire) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }
            self.finish_ready();
            if last_prune.elapsed() >= Duration::from_secs(60) {
                if let Err(error) = self.prune(0, 0) {
                    self.telemetry.event("archive", "retention_failed");
                    tracing::error!(%error, "PTY diagnostics retention failed");
                }
                last_prune = Instant::now();
            }
        }
        self.finish_ready();
        for archive in self.archives.values() {
            archive.state.issue(WORKER_STOPPED, "worker_stopped");
            // No fabricated terminal status: an absent footer means interrupted.
            archive.state.complete();
        }
    }

    fn start(&mut self, state: Arc<ArchiveState>, info: &SessionInfo) {
        self.finish_ready();
        let record = record_bytes(json!({
            "format": FORMAT, "event": "started", "session_id": info.session_id,
            "project": info.project, "started_at": info.started_at,
        }));
        if let Err(error) = self.prune(1, record.len() as u64) {
            tracing::error!(session_id = %state.id, %error, "cannot prepare PTY diagnostics retention");
            state.issue(DISK_ERROR, "disk_error");
            state.complete();
            return;
        }
        let reserved = FOOTER_RESERVE.saturating_mul(self.archives.len() as u64 + 1);
        if record.len() as u64 + FOOTER_RESERVE > self.options.archive_max_bytes_per_session {
            state.issue(SESSION_LIMIT, "session_limit");
            state.complete();
            return;
        }
        if self
            .total_bytes
            .saturating_add(record.len() as u64)
            .saturating_add(reserved)
            > self.options.archive_max_total_bytes
            || self.archives.len() >= self.options.archive_max_sessions
        {
            state.issue(TOTAL_LIMIT, "total_limit");
            state.complete();
            return;
        }
        let path = self.root.join(format!("{}.jsonl", state.id));
        let opened = (|| -> io::Result<File> {
            ensure_real_directory_chain(&self.root).map_err(io::Error::other)?;
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
                .open(&path)?;
            if let Err(error) = file.write_all(&record) {
                // We exclusively created this path; do not leave an unrecognizable
                // empty/partial header that retention could never reclaim.
                if let Err(cleanup_error) = fs::remove_file(&path) {
                    tracing::error!(session_id = %state.id, %cleanup_error,
                        "cannot remove failed PTY diagnostics header");
                }
                return Err(error);
            }
            Ok(file)
        })();
        match opened {
            Ok(file) => {
                self.total_bytes = self.total_bytes.saturating_add(record.len() as u64);
                self.archives.insert(
                    state.id,
                    OpenArchive {
                        file,
                        state,
                        bytes: record.len() as u64,
                        output_bytes: 0,
                    },
                );
                self.telemetry.event("archive", "started");
            }
            Err(error) => {
                tracing::error!(session_id = %state.id, %error, "cannot create PTY diagnostics archive");
                state.issue(DISK_ERROR, "disk_error");
                state.complete();
            }
        }
    }

    fn output(&mut self, state: &Arc<ArchiveState>, text: &str) {
        let reserved = FOOTER_RESERVE.saturating_mul(self.archives.len() as u64);
        let Some(archive) = self.archives.get(&state.id) else {
            state
                .dropped
                .fetch_add(text.len() as u64, Ordering::Relaxed);
            return;
        };
        // Queue-full disables new enqueueing but preserves the already queued prefix.
        if state.issues.load(Ordering::Acquire) & (SESSION_LIMIT | TOTAL_LIMIT | DISK_ERROR) != 0 {
            state
                .dropped
                .fetch_add(text.len() as u64, Ordering::Relaxed);
            return;
        }
        let record =
            record_bytes(json!({ "event": "output", "timestamp": Utc::now(), "text": text }));
        let bytes = record.len() as u64;
        if archive
            .bytes
            .saturating_add(bytes)
            .saturating_add(FOOTER_RESERVE)
            > self.options.archive_max_bytes_per_session
        {
            state.issue(SESSION_LIMIT, "session_limit");
        } else {
            if self
                .total_bytes
                .saturating_add(bytes)
                .saturating_add(reserved)
                > self.options.archive_max_total_bytes
                && let Err(error) = self.prune(0, bytes)
            {
                tracing::error!(session_id = %state.id, %error, "cannot rotate PTY diagnostics archives");
                state.issue(DISK_ERROR, "disk_error");
                state
                    .dropped
                    .fetch_add(text.len() as u64, Ordering::Relaxed);
                return;
            }
            if self
                .total_bytes
                .saturating_add(bytes)
                .saturating_add(reserved)
                > self.options.archive_max_total_bytes
            {
                state.issue(TOTAL_LIMIT, "total_limit");
                state
                    .dropped
                    .fetch_add(text.len() as u64, Ordering::Relaxed);
                return;
            }
            let archive = self
                .archives
                .get_mut(&state.id)
                .expect("active archive remains owned during retention");
            // Charge before writing; a partial write must not hide disk consumption.
            self.total_bytes = self.total_bytes.saturating_add(bytes);
            archive.bytes += bytes;
            match archive.file.write_all(&record) {
                Ok(()) => {
                    archive.output_bytes += text.len() as u64;
                    self.telemetry
                        .add_bytes("archive_output", text.len() as u64);
                    return;
                }
                Err(error) => {
                    tracing::error!(session_id = %state.id, %error, "PTY diagnostics write failed");
                    state.issue(DISK_ERROR, "disk_error");
                    // A partial JSON record must not poison a later footer if
                    // disk access recovers. Roll back only our own open file.
                    let previous = archive.bytes.saturating_sub(bytes);
                    match archive
                        .file
                        .set_len(previous)
                        .and_then(|()| archive.file.seek(SeekFrom::Start(previous)).map(|_| ()))
                    {
                        Ok(()) => {
                            archive.bytes = previous;
                            self.total_bytes = self.total_bytes.saturating_sub(bytes);
                        }
                        Err(error) => {
                            tracing::error!(session_id = %state.id, %error,
                                "cannot roll back partial PTY diagnostics record");
                        }
                    }
                }
            }
        }
        state
            .dropped
            .fetch_add(text.len() as u64, Ordering::Relaxed);
    }

    fn finish_ready(&mut self) {
        let finished: Vec<_> = self
            .archives
            .iter()
            .filter_map(|(id, archive)| {
                // Seeing final_info first establishes that the producer has finished
                // enqueueing. Reading pending first could race the final output send.
                let info = archive
                    .state
                    .final_info
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone()?;
                if archive.state.pending.load(Ordering::Acquire) != 0 {
                    return None;
                }
                Some((*id, info))
            })
            .collect();
        for (id, info) in finished {
            let Some(mut archive) = self.archives.remove(&id) else {
                continue;
            };
            let state = &archive.state;
            let issues = state.issues.load(Ordering::Acquire);
            let record = record_bytes(json!({
                "event": "finished", "session_id": id, "project": info.project,
                "ended_at": info.ended_at, "exit_code": info.exit_code,
                "termination_reason": info.termination_reason,
                "output_truncated": info.output_truncated,
                "archive_truncated": issues != 0,
                "archive_issues": issue_names(issues),
                "observed_bytes": state.observed.load(Ordering::Relaxed),
                "archived_output_bytes": archive.output_bytes,
                "dropped_bytes": state.dropped.load(Ordering::Relaxed),
            }));
            let result = if record.len() as u64 <= FOOTER_RESERVE {
                self.total_bytes = self.total_bytes.saturating_add(record.len() as u64);
                archive
                    .file
                    .write_all(&record)
                    .and_then(|()| archive.file.sync_data())
            } else {
                Err(io::Error::other("archive footer exceeds reserved space"))
            };
            if let Err(error) = result {
                tracing::error!(session_id = %id, %error, "PTY diagnostics finalization failed");
                state.issue(DISK_ERROR, "disk_error");
            } else {
                self.telemetry.event(
                    "archive",
                    if issues == 0 { "complete" } else { "truncated" },
                );
                tracing::info!(session_id = %id, project = %state.project,
                    archive_bytes = archive.bytes + record.len() as u64,
                    archive_truncated = issues != 0, "PTY diagnostics archive finalized");
            }
            state.complete();
        }
    }

    /// Delete only our regular, single-link UUID files with a matching format header.
    fn prune(&mut self, incoming: usize, additional_bytes: u64) -> anyhow::Result<()> {
        ensure_real_directory_chain(&self.root)?;
        let active: HashSet<_> = self.archives.keys().copied().collect();
        let mut owned = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let path = entry.path();
            let Some(id) = archive_id(&path) else {
                continue;
            };
            let metadata = fs::symlink_metadata(&path)?;
            if !metadata.is_file() || metadata.nlink() != 1 || !has_header(&path, id)? {
                continue;
            }
            owned.push((path, id, metadata.len(), metadata.modified()?));
        }
        owned.sort_by_key(|(_, _, _, modified)| *modified);
        let mut count = owned.len();
        self.total_bytes = owned.iter().map(|(_, _, bytes, _)| *bytes).sum();
        for (path, id, bytes, modified) in owned {
            if active.contains(&id) {
                continue;
            }
            let expired = SystemTime::now()
                .duration_since(modified)
                .unwrap_or_default()
                >= Duration::from_secs(self.options.archive_retention_secs);
            let over_count = count.saturating_add(incoming) > self.options.archive_max_sessions;
            let reserve =
                FOOTER_RESERVE.saturating_mul(self.archives.len() as u64 + incoming as u64);
            let over_bytes = self
                .total_bytes
                .saturating_add(reserve)
                .saturating_add(additional_bytes)
                > self.options.archive_max_total_bytes;
            if expired || over_count || over_bytes {
                ensure_real_directory_chain(&self.root)?;
                let metadata = fs::symlink_metadata(&path)?;
                if !metadata.is_file() || metadata.nlink() != 1 || !has_header(&path, id)? {
                    continue;
                }
                fs::remove_file(&path)?;
                self.total_bytes = self.total_bytes.saturating_sub(bytes);
                count -= 1;
                self.telemetry.event("archive", "pruned");
                self.telemetry.add_bytes("archive_pruned", bytes);
                tracing::info!(session_id = %id, archive_bytes = bytes, "PTY diagnostics archive pruned");
            }
        }
        Ok(())
    }
}

fn record_bytes(value: serde_json::Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(&value).expect("JSON values are serializable");
    bytes.push(b'\n');
    bytes
}

fn issue_names(issues: u32) -> Vec<&'static str> {
    [
        (QUEUE_FULL, "queue_full"),
        (SESSION_LIMIT, "session_limit"),
        (TOTAL_LIMIT, "total_limit"),
        (DISK_ERROR, "disk_error"),
        (WORKER_STOPPED, "worker_stopped"),
        (DRAIN_TIMEOUT, "drain_timeout"),
    ]
    .into_iter()
    .filter_map(|(flag, name)| (issues & flag != 0).then_some(name))
    .collect()
}

fn archive_id(path: &Path) -> Option<Uuid> {
    if path.extension()?.to_str()? != "jsonl" {
        return None;
    }
    let name = path.file_stem()?.to_str()?;
    let id = Uuid::parse_str(name).ok()?;
    (id.to_string() == name).then_some(id)
}

fn has_header(path: &Path, id: Uuid) -> io::Result<bool> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Ok(false);
    }
    let mut line = String::new();
    if let Err(error) = BufReader::new(file.take(4096)).read_line(&mut line) {
        if error.kind() == io::ErrorKind::InvalidData {
            return Ok(false);
        }
        return Err(error);
    }
    let Ok(header) = serde_json::from_str::<serde_json::Value>(&line) else {
        return Ok(false);
    };
    Ok(header["format"] == FORMAT
        && header["event"] == "started"
        && header["session_id"] == id.to_string())
}

fn create_directory(root: &Path) -> anyhow::Result<()> {
    ensure_real_directory_chain(
        root.parent()
            .context("diagnostics directory has no parent")?,
    )?;
    match fs::DirBuilder::new().mode(0o700).create(root) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    ensure_real_directory_chain(root)?;
    fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(test)]
mod tests;
