//! Durable, bounded history, independent of clients polling the live PTY ring.
use crate::{error::AppError, project::ProjectStore};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};
use uuid::Uuid;

const FORMAT: &str = "vivado-history-v1";
const EVENT_LIMIT: usize = 32 * 1024;
const PAGE_LIMIT: usize = 2 * 1024 * 1024;
const GC_BYTES: u64 = 32 * 1024 * 1024;
const MAX_EVENTS: usize = 100_000;
const TEXT_CHUNK: usize = 4096;
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HistoryConfig {
    pub enabled: bool,
    pub max_bytes: u64,
    pub retention_secs: u64,
    pub session_head_bytes: u64,
    pub session_tail_bytes: u64,
}
impl Default for HistoryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_bytes: 512 * 1024 * 1024,
            retention_secs: 7 * 86400,
            session_head_bytes: 8 * 1024 * 1024,
            session_tail_bytes: 56 * 1024 * 1024,
        }
    }
}
impl HistoryConfig {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (64 * 1024..=512 * 1024 * 1024).contains(&self.max_bytes),
            "history.max_bytes must be 64 KiB..512 MiB"
        );
        anyhow::ensure!(
            (1..=7 * 86400).contains(&self.retention_secs),
            "history retention must be 1 second..7 days"
        );
        anyhow::ensure!(
            self.session_head_bytes <= 8 * 1024 * 1024
                && self.session_tail_bytes > 0
                && self.session_tail_bytes <= 56 * 1024 * 1024,
            "invalid history session byte bounds"
        );
        Ok(())
    }
}
#[derive(Clone, Debug, Default)]
pub(crate) struct Context {
    pub request_id: Option<Uuid>,
    pub parent_request_id: Option<Uuid>,
    pub workflow_id: Option<Uuid>,
    pub sync_id: Option<Uuid>,
}
tokio::task_local! { static CONTEXT: Context; }
pub(crate) fn context() -> Context {
    CONTEXT.try_with(Clone::clone).unwrap_or_default()
}
pub(crate) async fn scoped<F: std::future::Future>(context: Context, future: F) -> F::Output {
    CONTEXT.scope(context, future).await
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Event {
    pub seq: u64,
    #[serde(default)]
    pub instance_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<Uuid>,
    pub data: Value,
}
impl Event {
    pub(crate) fn new(
        event: &str,
        workflow: Option<Uuid>,
        session: Option<Uuid>,
        project: Option<&str>,
        mut data: Value,
    ) -> Self {
        let ctx = context();
        if let Some(id) = ctx.sync_id {
            data["sync_id"] = json!(id);
        }
        if let Some(parent) = ctx.parent_request_id {
            data["parent_request_id"] = json!(parent);
        }
        Self {
            seq: 0,
            instance_id: Uuid::nil(),
            timestamp: Utc::now(),
            event: event.into(),
            workflow_id: workflow.or(ctx.workflow_id),
            session_id: session,
            project: project.map(str::to_owned),
            request_id: ctx.request_id,
            data,
        }
    }
    pub(crate) fn correlated(mut self, ctx: &Context) -> Self {
        self.request_id = ctx.request_id;
        if let Some(data) = self.data.as_object_mut() {
            data.remove("parent_request_id");
        }
        if let Some(parent) = ctx.parent_request_id {
            self.data["parent_request_id"] = json!(parent);
        }
        self
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Gap {
    pub after_seq: u64,
    pub through_seq: u64,
    pub reason: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct State {
    format: String,
    journal_id: Uuid,
    instance_id: Uuid,
    next_cursor: u64,
    earliest_cursor: u64,
    recording_state: String,
    gaps: Vec<Gap>,
    #[serde(default)]
    session_windows: HashMap<Uuid, u64>,
    #[serde(default)]
    session_head_bytes: u64,
    #[serde(default)]
    open_sessions: HashMap<Uuid, Event>,
    #[serde(default)]
    open_workflows: HashMap<Uuid, Event>,
}
#[derive(Serialize)]
pub(crate) struct Page {
    pub journal_id: Uuid,
    pub instance_id: Uuid,
    pub events: Vec<Event>,
    pub next_cursor: u64,
    pub earliest_cursor: u64,
    pub head_cursor: u64,
    pub has_more: bool,
    pub gaps: Vec<Gap>,
    pub recording_state: String,
}
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Query {
    #[serde(default)]
    pub after: u64,
    pub limit: Option<usize>,
}
#[derive(Clone, Default)]
pub(crate) struct History {
    inner: Option<Arc<Inner>>,
}
impl std::fmt::Debug for History {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("History")
            .field("enabled", &self.inner.is_some())
            .finish()
    }
}
struct Inner {
    instance_id: Uuid,
    sender: mpsc::SyncSender<Message>,
    producer: Mutex<u64>,
    pending: Mutex<Vec<Gap>>,
    reader: Mutex<Reader>,
    healthy: AtomicBool,
    stopped: AtomicBool,
    queued_bytes: AtomicUsize,
    finished: AtomicBool,
}
struct Message {
    events: Vec<Event>,
    bytes: usize,
    ack: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
}
struct Reader {
    state: State,
    events: BTreeMap<u64, (Event, usize)>,
    bytes: u64,
}
// Retained byte count and ordered (sequence, byte offset, byte length) entries.
type TailWindow = (u64, VecDeque<(u64, u64, u64)>);
struct Writer {
    root: PathBuf,
    _root_fd: File,
    file: File,
    physical: u64,
    options: HistoryConfig,
    inner: Arc<Inner>,
    _owner: Option<ProjectStore>,
    tails: HashMap<Uuid, TailWindow>,
    #[cfg(test)]
    fail_write_after: Option<usize>,
    #[cfg(test)]
    fail_compact_dirsync: bool,
}
fn error(e: impl std::fmt::Display) -> AppError {
    AppError::Internal(format!("history I/O failed: {e}"))
}
fn safe_file(path: &Path, write: bool, create: bool) -> std::io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(write)
        .create(create)
        .truncate(false)
        .mode(0o600)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file() || meta.nlink() != 1 {
        return Err(std::io::Error::other(
            "history file must be a regular single-link file",
        ));
    }
    Ok(file)
}
fn gap(gaps: &mut Vec<Gap>, first: u64, last: u64, reason: &str) {
    if first > last {
        return;
    }
    if let Some(previous) = gaps.last_mut()
        && previous.reason == reason
        && previous.through_seq.saturating_add(1) >= first
    {
        previous.after_seq = previous.after_seq.min(first.saturating_sub(1));
        previous.through_seq = previous.through_seq.max(last);
        return;
    }
    gaps.push(Gap {
        after_seq: first.saturating_sub(1),
        through_seq: last,
        reason: reason.into(),
    });
    if gaps.len() > 128 {
        let b = gaps.remove(1);
        let a = &mut gaps[0];
        a.after_seq = a.after_seq.min(b.after_seq);
        a.through_seq = a.through_seq.max(b.through_seq);
        a.reason = "older_history_gaps".into();
    }
}
impl History {
    pub(crate) fn enabled(&self) -> bool {
        self.inner.is_some()
    }
    pub(crate) async fn initialize(
        root: PathBuf,
        options: HistoryConfig,
        owner: ProjectStore,
    ) -> anyhow::Result<Self> {
        if !options.enabled {
            return Ok(Self::default());
        }
        tokio::task::spawn_blocking(move || {
            let parent = crate::workspace_read::open_root(&root.join(".vivado-server"))?;
            let result =
                unsafe { nix::libc::mkdirat(parent.as_raw_fd(), c"history-v1".as_ptr(), 0o700) };
            if result < 0
                && std::io::Error::last_os_error().kind() != std::io::ErrorKind::AlreadyExists
            {
                return Err(std::io::Error::last_os_error().into());
            }
            let root_fd = crate::workspace_read::open_at(&parent, "history-v1", true)?;
            root_fd.set_permissions(std::fs::Permissions::from_mode(0o700))?;
            // A pinned directory descriptor anchors every later child open and rename,
            // even if untrusted Tcl renames the directory or replaces it with a link.
            let root = PathBuf::from(format!("/proc/self/fd/{}", root_fd.as_raw_fd()));
            let mut state = match safe_file(&root.join("state.json"), false, false) {
                Ok(file) => {
                    anyhow::ensure!(
                        file.metadata()?.len() <= 64 * 1024,
                        "history state too large"
                    );
                    serde_json::from_reader::<_, State>(file)?
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => State {
                    format: FORMAT.into(),
                    journal_id: Uuid::new_v4(),
                    instance_id: Uuid::new_v4(),
                    next_cursor: 0,
                    earliest_cursor: 0,
                    recording_state: "recording".into(),
                    gaps: vec![],
                    session_windows: HashMap::new(),
                    session_head_bytes: options.session_head_bytes,
                    open_sessions: HashMap::new(),
                    open_workflows: HashMap::new(),
                },
                Err(e) => return Err(e.into()),
            };
            anyhow::ensure!(state.format == FORMAT, "unknown history format");
            state.session_head_bytes = options.session_head_bytes;
            for name in ["state.next", "events.next"] {
                let path = root.join(name);
                match safe_file(&path, false, false) {
                    Ok(_) => std::fs::remove_file(path)?,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
            let persisted_cursor = state.next_cursor;
            state.instance_id = Uuid::new_v4();
            let mut file = safe_file(&root.join("events.jsonl"), true, true)?;
            anyhow::ensure!(
                file.metadata()?.len() <= options.max_bytes + GC_BYTES,
                "history spool exceeds physical bound"
            );
            let mut reader = Reader {
                state,
                events: BTreeMap::new(),
                bytes: 0,
            };
            let mut disk = BufReader::new(file.try_clone()?);
            let mut position = 0;
            let mut line = Vec::new();
            let mut damaged_tail = false;
            loop {
                line.clear();
                let count = disk
                    .by_ref()
                    .take((EVENT_LIMIT + 1) as u64)
                    .read_until(b'\n', &mut line)?;
                if count == 0 {
                    break;
                }
                if count > EVENT_LIMIT || line.last() != Some(&b'\n') {
                    file.set_len(position)?;
                    reader.state.recording_state = "partial".into();
                    damaged_tail = true;
                    break;
                }
                let event: Event = match serde_json::from_slice(&line) {
                    Ok(e) => e,
                    Err(_) => {
                        file.set_len(position)?;
                        reader.state.recording_state = "partial".into();
                        damaged_tail = true;
                        break;
                    }
                };
                anyhow::ensure!(
                    event.seq > 0 && event.data.is_object(),
                    "invalid history event"
                );
                reader.state.next_cursor = reader.state.next_cursor.max(event.seq);
                if event.seq > persisted_cursor {
                    apply_lifecycle(&mut reader.state, &event);
                }
                let retired = event.seq <= reader.state.earliest_cursor
                    || (event.event == "session.output"
                        && event
                            .session_id
                            .and_then(|id| reader.state.session_windows.get(&id))
                            .is_some_and(|start| {
                                event.data["byte_offset"].as_u64().is_some_and(|offset| {
                                    offset >= reader.state.session_head_bytes && offset < *start
                                })
                            }));
                if !retired {
                    reader.bytes += count as u64;
                    reader.events.insert(event.seq, (event, count));
                }
                position += count as u64;
            }
            // Pending losses may have been durably described while older valid
            // events were still queued. Never reuse their reserved sequences.
            let previous = reader.state.next_cursor;
            let gap_head = reader
                .state
                .gaps
                .iter()
                .map(|g| g.through_seq)
                .max()
                .unwrap_or(previous);
            if gap_head > previous {
                gap(
                    &mut reader.state.gaps,
                    previous + 1,
                    gap_head,
                    "core_restart_uncommitted",
                );
                reader.state.next_cursor = gap_head;
            }
            if damaged_tail {
                let lost = reader
                    .state
                    .next_cursor
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("history sequence exhausted"))?;
                gap(
                    &mut reader.state.gaps,
                    lost,
                    lost,
                    "incomplete_journal_tail",
                );
                reader.state.next_cursor = lost;
            }
            anyhow::ensure!(
                reader.state.next_cursor < u64::MAX - 1024,
                "history sequence exhausted"
            );
            let interrupted_sessions = reader.state.open_sessions.clone();
            let interrupted_workflows = reader.state.open_workflows.clone();
            file.seek(SeekFrom::End(0))?;
            let (sender, receiver) = mpsc::sync_channel(2048);
            let inner = Arc::new(Inner {
                instance_id: reader.state.instance_id,
                sender,
                producer: Mutex::new(reader.state.next_cursor),
                pending: Mutex::new(vec![]),
                reader: Mutex::new(reader),
                healthy: AtomicBool::new(true),
                stopped: AtomicBool::new(false),
                queued_bytes: AtomicUsize::new(0),
                finished: AtomicBool::new(false),
            });
            let mut writer = Writer {
                root,
                _root_fd: root_fd,
                file,
                physical: position,
                options,
                inner: inner.clone(),
                _owner: Some(owner),
                tails: HashMap::new(),
                #[cfg(test)]
                fail_write_after: None,
                #[cfg(test)]
                fail_compact_dirsync: false,
            };
            // Reconstruct the bounded tail index, then apply retention before publishing.
            let existing: Vec<_> = inner
                .reader
                .lock()
                .unwrap()
                .events
                .values()
                .map(|(e, _)| e.clone())
                .collect();
            for event in existing {
                writer.track_tail(&event);
            }
            writer.prune();
            writer.compact()?;
            // Commit the stable journal identity before the first event can land.
            writer.persist()?;
            let mut startup = vec![];
            for (_, old) in interrupted_sessions {
                let mut event = Event::new(
                    "session.interrupted",
                    old.workflow_id,
                    old.session_id,
                    old.project.as_deref(),
                    json!({"outcome":"unknown","reason":"core_restart"}),
                );
                event.instance_id = old.instance_id;
                startup.push(event);
            }
            for (_, old) in interrupted_workflows {
                let mut event = Event::new(
                    "workflow.interrupted",
                    old.workflow_id,
                    None,
                    old.project.as_deref(),
                    json!({"outcome":"unknown","reason":"core_restart"}),
                );
                event.instance_id = old.instance_id;
                startup.push(event);
            }
            startup.push(Event::new(
                "service.started",
                None,
                None,
                None,
                json!({"instance_id":inner.instance_id}),
            ));
            {
                let mut next = inner.producer.lock().unwrap();
                for event in &mut startup {
                    *next += 1;
                    event.seq = *next;
                    if event.instance_id.is_nil() {
                        event.instance_id = inner.instance_id;
                    }
                }
            }
            // Seal abandoned lifecycles before publishing the new runtime or clearing
            // their recovery metadata. A crash at any point can replay this batch.
            writer.append(startup)?;
            std::thread::Builder::new()
                .name("vivado-history".into())
                .spawn(move || writer.run(receiver))?;
            let history = Self { inner: Some(inner) };
            Ok(history)
        })
        .await?
    }
    fn enqueue(
        &self,
        mut events: Vec<Event>,
        ack: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
    ) -> Result<(), AppError> {
        let Some(inner) = &self.inner else {
            if let Some(ack) = ack {
                let _ = ack.send(Ok(()));
            }
            return Ok(());
        };
        if inner.stopped.load(Ordering::Acquire) {
            return Err(AppError::Capacity("history recording unavailable".into()));
        }
        let mut next = inner.producer.lock().unwrap();
        let first = *next + 1;
        for event in &mut events {
            *next += 1;
            event.seq = *next;
            if event.instance_id.is_nil() {
                event.instance_id = inner.instance_id;
            }
            if serde_json::to_vec(event).map_err(error)?.len() + 1 > EVENT_LIMIT {
                gap(
                    &mut inner.pending.lock().unwrap(),
                    first,
                    *next,
                    "event_too_large",
                );
                return Err(AppError::Capacity("history event too large".into()));
            }
        }
        let bytes: usize = events
            .iter()
            .map(|e| serde_json::to_vec(e).map_or(EVENT_LIMIT, |b| b.len() + 1))
            .sum();
        if inner
            .queued_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(bytes).filter(|v| *v <= 8 * 1024 * 1024)
            })
            .is_err()
        {
            gap(
                &mut inner.pending.lock().unwrap(),
                first,
                *next,
                "queue_full",
            );
            inner.healthy.store(false, Ordering::Release);
            return Err(AppError::Capacity("history recording unavailable".into()));
        }
        match inner.sender.try_send(Message { events, bytes, ack }) {
            Ok(()) => Ok(()),
            Err(_) => {
                inner.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
                gap(
                    &mut inner.pending.lock().unwrap(),
                    first,
                    *next,
                    "queue_full",
                );
                inner.healthy.store(false, Ordering::Release);
                Err(AppError::Capacity("history recording unavailable".into()))
            }
        }
    }
    pub(crate) fn record(&self, event: Event) {
        let _ = self.enqueue(vec![event], None);
    }
    pub(crate) async fn critical(&self, events: Vec<Event>) -> Result<(), AppError> {
        if !self
            .inner
            .as_ref()
            .is_none_or(|inner| inner.healthy.load(Ordering::Acquire))
        {
            return Err(AppError::Capacity("history recording unavailable".into()));
        }
        // stdin may legitimately approach the configured native request limit.
        // Persist it in bounded batches before any bytes reach the PTY.
        let mut events = events.into_iter();
        loop {
            let batch: Vec<_> = events.by_ref().take(64).collect();
            if batch.is_empty() {
                return Ok(());
            }
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.enqueue(batch, Some(tx))?;
            match tokio::time::timeout(Duration::from_secs(5), rx).await {
                Ok(Ok(Ok(()))) => {}
                _ => return Err(AppError::Capacity("history recording unavailable".into())),
            }
        }
    }
    pub(crate) fn text_events(
        event: &str,
        workflow: Option<Uuid>,
        session: Uuid,
        project: &str,
        text: &str,
        mut data: Value,
    ) -> Vec<Event> {
        let mut result = vec![];
        let mut offset = data.get("byte_offset").and_then(Value::as_u64).unwrap_or(0);
        let total = text.len();
        let mut remaining = text;
        if remaining.is_empty() {
            data["text"] = json!("");
            data["byte_length"] = json!(0);
            return vec![Event::new(
                event,
                workflow,
                Some(session),
                Some(project),
                data,
            )];
        }
        while !remaining.is_empty() {
            let mut n = remaining.len().min(TEXT_CHUNK);
            while !remaining.is_char_boundary(n) {
                n -= 1;
            }
            let chunk = &remaining[..n];
            let mut fields = data.clone();
            fields["text"] = json!(chunk);
            fields["byte_offset"] = json!(offset);
            fields["byte_length"] = json!(n);
            fields["total_bytes"] = json!(total);
            let kind = if event == "session.stdin_intent" && !result.is_empty() {
                "session.stdin_chunk"
            } else {
                event
            };
            result.push(Event::new(
                kind,
                workflow,
                Some(session),
                Some(project),
                fields,
            ));
            remaining = &remaining[n..];
            offset += n as u64;
        }
        result
    }
    pub(crate) fn output(
        &self,
        workflow: Option<Uuid>,
        session: Uuid,
        project: &str,
        text: &str,
        offset: u64,
    ) {
        if !self.enabled() {
            return;
        }
        for event in Self::text_events(
            "session.output",
            workflow,
            session,
            project,
            text,
            json!({"byte_offset":offset}),
        ) {
            // Output can contain asynchronous tool messages and spans multiple
            // commands; do not falsely attribute it to the session-start request.
            self.record(event.correlated(&Context::default()));
        }
    }
    pub(crate) fn page(&self, after: u64, limit: usize) -> Result<Page, AppError> {
        if limit == 0 || limit > 256 {
            return Err(AppError::BadRequest("history limit must be 1..256".into()));
        }
        let inner = self
            .inner
            .as_ref()
            .ok_or_else(|| AppError::NotFound("history is disabled".into()))?;
        let reader = inner.reader.lock().unwrap();
        let mut events = vec![];
        let mut bytes = 0;
        let mut next = after;
        for (&seq, (event, size)) in reader
            .events
            .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
        {
            if events.len() >= limit || bytes + size > PAGE_LIMIT - 64 * 1024 {
                break;
            }
            events.push(event.clone());
            bytes += size;
            next = seq;
        }
        if events.len() < limit
            && reader
               .events
               .range((std::ops::Bound::Excluded(next), std::ops::Bound::Unbounded))
               .next()
               .is_none()
        {
            next = next.max(reader.state.next_cursor);
        }
        Ok(Page {
            journal_id: reader.state.journal_id,
            instance_id: reader.state.instance_id,
            events,
            next_cursor: next,
            earliest_cursor: reader.state.earliest_cursor,
            head_cursor: reader.state.next_cursor,
            has_more: next < reader.state.next_cursor,
            gaps: reader.state.gaps.clone(),
            recording_state: if inner.healthy.load(Ordering::Acquire) {
                reader.state.recording_state.clone()
            } else {
                "unavailable".into()
            },
        })
    }
    pub(crate) async fn shutdown(&self) {
        if let Some(inner) = &self.inner {
            let _ = self
                .critical(vec![Event::new(
                    "service.stopped",
                    None,
                    None,
                    None,
                    json!({}),
                )])
                .await;
            inner.stopped.store(true, Ordering::Release);
            for _ in 0..500 {
                if inner.finished.load(Ordering::Acquire) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}
impl Writer {
    fn persist(&self) -> std::io::Result<()> {
        let reader = self.inner.reader.lock().unwrap();
        let bytes = serde_json::to_vec(&reader.state)?;
        drop(reader);
        if bytes.len() > 64 * 1024 {
            return Err(std::io::Error::other("history state too large"));
        }
        let path = self.root.join("state.next");
        let mut file = safe_file(&path, true, true)?;
        file.set_len(0)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(path, self.root.join("state.json"))?;
        File::open(&self.root)?.sync_all()
    }
    fn track_tail(&mut self, event: &Event) {
        if event.event != "session.output" {
            return;
        }
        let Some(id) = event.session_id else {
            return;
        };
        let offset = event.data["byte_offset"].as_u64().unwrap_or(0);
        let len = event.data["byte_length"].as_u64().unwrap_or(0);
        if offset < self.options.session_head_bytes {
            return;
        }
        let tail = self.tails.entry(id).or_default();
        tail.0 += len;
        tail.1.push_back((event.seq, offset, len));
        while tail.0 > self.options.session_tail_bytes {
            let Some((seq, _, len)) = tail.1.pop_front() else {
                break;
            };
            tail.0 -= len;
            let mut reader = self.inner.reader.lock().unwrap();
            if let Some((_, size)) = reader.events.remove(&seq) {
                reader.bytes -= size as u64;
                reader.state.session_windows.insert(
                    id,
                    tail.1
                        .front()
                        .map_or(offset + len, |(_, offset, _)| *offset),
                );
                gap(&mut reader.state.gaps, seq, seq, "session_middle_trimmed");
                reader.state.recording_state = "partial".into();
            }
        }
    }
    fn prune(&mut self) {
        let cutoff = Utc::now() - chrono::Duration::seconds(self.options.retention_secs as i64);
        let mut reader = self.inner.reader.lock().unwrap();
        while let Some((&seq, (event, _))) = reader.events.first_key_value() {
            let expired = event.timestamp < cutoff;
            if !expired
                && reader.bytes <= self.options.max_bytes
                && reader.events.len() <= MAX_EVENTS
            {
                break;
            }
            let (_, size) = reader.events.remove(&seq).unwrap();
            reader.bytes -= size as u64;
            gap(
                &mut reader.state.gaps,
                seq,
                seq,
                if expired {
                    "retention_expired"
                } else {
                    "spool_limit"
                },
            );
            reader.state.recording_state = "partial".into();
        }
        let retained_sessions: std::collections::HashSet<_> = reader
            .events
            .values()
            .filter_map(|(event, _)| event.session_id)
            .collect();
        reader
            .state
            .session_windows
            .retain(|id, _| retained_sessions.contains(id));
        self.tails.retain(|id, _| retained_sessions.contains(id));
        // Global retention removes a sequence prefix. Keep tail indexes bounded
        // by retained events even for a process emitting millions of tiny writes.
        let first_retained = reader
            .events
            .first_key_value()
            .map_or(u64::MAX, |(&seq, _)| seq);
        for (bytes, items) in self.tails.values_mut() {
            while items
                .front()
                .is_some_and(|(seq, _, _)| *seq < first_retained)
            {
                if let Some((_, _, len)) = items.pop_front() {
                    *bytes = bytes.saturating_sub(len);
                }
            }
        }
        reader.state.earliest_cursor = reader
            .events
            .first_key_value()
            .map_or(reader.state.next_cursor, |(&seq, _)| seq.saturating_sub(1));
    }
    fn compact(&mut self) -> std::io::Result<()> {
        let path = self.root.join("events.next");
        let mut file = safe_file(&path, true, true)?;
        file.set_len(0)?;
        let reader = self.inner.reader.lock().unwrap();
        let mut length = 0;
        for (event, _) in reader.events.values() {
            let mut bytes = serde_json::to_vec(event)?;
            bytes.push(b'\n');
            file.write_all(&bytes)?;
            length += bytes.len() as u64;
        }
        drop(reader);
        file.sync_all()?;
        std::fs::rename(path, self.root.join("events.jsonl"))?;
        // After rename this inode is authoritative even if directory fsync fails.
        self.file = file;
        self.physical = length;
        #[cfg(test)]
        if std::mem::take(&mut self.fail_compact_dirsync) {
            return Err(std::io::Error::other("injected directory fsync failure"));
        }
        File::open(&self.root)?.sync_all()
    }
    fn append(&mut self, events: Vec<Event>) -> std::io::Result<()> {
        let mut batch = Vec::new();
        let start = self.physical;
        let mut encoded = Vec::new();
        for event in events {
            let mut bytes = serde_json::to_vec(&event)?;
            bytes.push(b'\n');
            if bytes.len() > EVENT_LIMIT {
                return Err(std::io::Error::other("history event too large"));
            }
            encoded.extend_from_slice(&bytes);
            batch.push((event, bytes.len()));
        }
        #[cfg(test)]
        let write_result = if let Some(bytes) = self.fail_write_after.take() {
            self.file
                .write_all(&encoded[..bytes.min(encoded.len())])
                .and_then(|()| Err(std::io::Error::other("injected partial write")))
        } else {
            self.file
                .write_all(&encoded)
                .and_then(|()| self.file.sync_data())
        };
        #[cfg(not(test))]
        let write_result = self
            .file
            .write_all(&encoded)
            .and_then(|()| self.file.sync_data());
        if let Err(error) = write_result {
            if self
                .file
                .set_len(start)
                .and_then(|()| self.file.seek(SeekFrom::Start(start)).map(|_| ()))
                .is_err()
            {
                self.inner.stopped.store(true, Ordering::Release);
            }
            return Err(error);
        }
        self.physical += encoded.len() as u64;
        for (event, size) in batch {
            {
                let mut reader = self.inner.reader.lock().unwrap();
                reader.state.next_cursor = reader.state.next_cursor.max(event.seq);
                apply_lifecycle(&mut reader.state, &event);
                reader.bytes += size as u64;
                reader.events.insert(event.seq, (event.clone(), size));
            }
            self.track_tail(&event);
        }
        let pending = std::mem::take(&mut *self.inner.pending.lock().unwrap());
        {
            let mut reader = self.inner.reader.lock().unwrap();
            for item in pending {
                gap(
                    &mut reader.state.gaps,
                    item.after_seq + 1,
                    item.through_seq,
                    &item.reason,
                );
                reader.state.recording_state = "partial".into();
            }
        }
        self.prune();
        let retained = self.inner.reader.lock().unwrap().bytes;
        if self.physical > self.options.max_bytes + GC_BYTES / 2
            || self.physical.saturating_sub(retained) >= GC_BYTES
        {
            self.compact()?;
        }
        self.persist()
    }
    fn run(mut self, receiver: mpsc::Receiver<Message>) {
        let mut last_prune = Instant::now();
        loop {
            match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(message) => {
                    let mut events = message.events;
                    let mut acks = message.ack.into_iter().collect::<Vec<_>>();
                    self.inner
                        .queued_bytes
                        .fetch_sub(message.bytes, Ordering::AcqRel);
                    while events.len() < 256 {
                        match receiver.try_recv() {
                            Ok(next) => {
                                self.inner
                                    .queued_bytes
                                    .fetch_sub(next.bytes, Ordering::AcqRel);
                                events.extend(next.events);
                                acks.extend(next.ack);
                            }
                            Err(_) => break,
                        }
                    }
                    let range = events
                        .first()
                        .zip(events.last())
                        .map(|(a, b)| (a.seq, b.seq));
                    let result = self.append(events);
                    self.inner.healthy.store(result.is_ok(), Ordering::Release);
                    if result.is_err()
                        && let Some((first, last)) = range
                    {
                        gap(
                            &mut self.inner.pending.lock().unwrap(),
                            first,
                            last,
                            "disk_error",
                        );
                    }
                    let result = result.map_err(|e| e.to_string());
                    for ack in acks {
                        let _ = ack.send(result.clone());
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    let producer = self.inner.producer.lock().unwrap();
                    let drained = self.inner.queued_bytes.load(Ordering::Acquire) == 0;
                    let gaps = std::mem::take(&mut *self.inner.pending.lock().unwrap());
                    let mut reader = self.inner.reader.lock().unwrap();
                    let previous = reader.state.next_cursor;
                    let new_gaps = !gaps.is_empty();
                    for item in gaps {
                        gap(
                            &mut reader.state.gaps,
                            item.after_seq + 1,
                            item.through_seq,
                            &item.reason,
                        );
                        reader.state.recording_state = "partial".into();
                    }
                    // Only an empty queue permits crossing dropped high sequence
                    // numbers: earlier queued events must never be skipped by a
                    // collector whose cursor advances through the gap.
                    if drained
                        && let Some(high) = reader.state.gaps.iter().map(|g| g.through_seq).max()
                    {
                        reader.state.next_cursor = reader.state.next_cursor.max(high);
                    }
                    let changed = new_gaps || previous != reader.state.next_cursor;
                    drop(reader);
                    drop(producer);
                    if changed {
                        let ok = self.persist().is_ok();
                        self.inner.healthy.store(ok, Ordering::Release);
                    }
                    if !self.inner.healthy.load(Ordering::Acquire)
                        && !self.inner.stopped.load(Ordering::Acquire)
                    {
                        let ok = self.file.sync_data().and_then(|()| self.persist()).is_ok();
                        self.inner.healthy.store(ok, Ordering::Release);
                    }
                    if last_prune.elapsed() > Duration::from_secs(60) {
                        let before = self.inner.reader.lock().unwrap().bytes;
                        self.prune();
                        if self.inner.reader.lock().unwrap().bytes != before {
                            let _ = self.compact();
                            let _ = self.persist();
                        }
                        last_prune = Instant::now();
                    }
                    if self.inner.stopped.load(Ordering::Acquire) {
                        break;
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        // Release the ownership guard before shutdown reports writer completion.
        drop(self._owner.take());
        self.inner.finished.store(true, Ordering::Release);
    }
}

fn apply_lifecycle(state: &mut State, event: &Event) {
    match event.event.as_str() {
        "session.started" => {
            if let Some(id) = event.session_id {
                state.open_sessions.insert(id, event.clone());
            }
        }
        "session.finished" | "session.interrupted" => {
            if let Some(id) = event.session_id {
                state.open_sessions.remove(&id);
            }
        }
        "workflow.created" => {
            if let Some(id) = event.workflow_id {
                state.open_workflows.insert(id, event.clone());
            }
        }
        "workflow.completed" | "workflow.interrupted" => {
            if let Some(id) = event.workflow_id {
                state.open_workflows.remove(&id);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn writer(root: &Path, head: u64, tail: u64) -> (Writer, mpsc::Receiver<Message>) {
        let owner = ProjectStore::initialize(root.to_owned()).await.unwrap();
        let directory = root.join(".vivado-server/history-v1");
        std::fs::create_dir(&directory).unwrap();
        let instance_id = Uuid::new_v4();
        let state = State {
            format: FORMAT.into(),
            journal_id: Uuid::new_v4(),
            instance_id,
            next_cursor: 0,
            earliest_cursor: 0,
            recording_state: "recording".into(),
            gaps: vec![],
            session_windows: HashMap::new(),
            session_head_bytes: head,
            open_sessions: HashMap::new(),
            open_workflows: HashMap::new(),
        };
        let (sender, receiver) = mpsc::sync_channel(2);
        let inner = Arc::new(Inner {
            instance_id,
            sender,
            producer: Mutex::new(0),
            pending: Mutex::new(vec![]),
            reader: Mutex::new(Reader {
                state,
                events: BTreeMap::new(),
                bytes: 0,
            }),
            healthy: AtomicBool::new(true),
            stopped: AtomicBool::new(false),
            queued_bytes: AtomicUsize::new(0),
            finished: AtomicBool::new(false),
        });
        let root_fd = File::open(&directory).unwrap();
        let root = PathBuf::from(format!("/proc/self/fd/{}", root_fd.as_raw_fd()));
        let file = safe_file(&root.join("events.jsonl"), true, true).unwrap();
        (
            Writer {
                root,
                _root_fd: root_fd,
                file,
                physical: 0,
                options: HistoryConfig {
                    enabled: true,
                    session_head_bytes: head,
                    session_tail_bytes: tail,
                    ..HistoryConfig::default()
                },
                inner,
                _owner: Some(owner),
                tails: HashMap::new(),
                fail_write_after: None,
                fail_compact_dirsync: false,
            },
            receiver,
        )
    }
    fn event(writer: &Writer, seq: u64, kind: &str, session: Option<Uuid>, data: Value) -> Event {
        let mut event = Event::new(kind, None, session, Some("test"), data);
        event.seq = seq;
        event.instance_id = writer.inner.instance_id;
        event
    }
    async fn reopen(root: &Path, head: u64, tail: u64) -> History {
        let owner = ProjectStore::initialize(root.to_owned()).await.unwrap();
        History::initialize(
            root.to_owned(),
            HistoryConfig {
                enabled: true,
                session_head_bytes: head,
                session_tail_bytes: tail,
                ..HistoryConfig::default()
            },
            owner,
        )
        .await
        .unwrap()
    }
    #[tokio::test]
    async fn partial_append_rolls_back_before_later_success_and_restart() {
        let temp = tempfile::tempdir().unwrap();
        let (mut writer, _) = writer(temp.path(), 4, 8).await;
        writer
            .append(vec![event(&writer, 1, "first", None, json!({}))])
            .unwrap();
        let before = writer.physical;
        writer.fail_write_after = Some(17);
        assert!(
            writer
                .append(vec![event(&writer, 2, "failed", None, json!({}))])
                .is_err()
        );
        assert_eq!(writer.file.metadata().unwrap().len(), before);
        writer
            .append(vec![event(&writer, 3, "third", None, json!({}))])
            .unwrap();
        drop(writer);
        let history = reopen(temp.path(), 4, 8).await;
        let events = history.page(0, 256).unwrap().events;
        assert!(events.iter().any(|e| e.event == "third"));
        assert!(!events.iter().any(|e| e.event == "failed"));
        history.shutdown().await;
    }
    #[tokio::test]
    async fn compaction_rename_failure_keeps_new_inode_authoritative() {
        let temp = tempfile::tempdir().unwrap();
        let (mut writer, _) = writer(temp.path(), 4, 8).await;
        writer
            .append(vec![event(&writer, 1, "first", None, json!({}))])
            .unwrap();
        writer.fail_compact_dirsync = true;
        assert!(writer.compact().is_err());
        writer
            .append(vec![event(&writer, 2, "after_rename", None, json!({}))])
            .unwrap();
        drop(writer);
        let history = reopen(temp.path(), 4, 8).await;
        assert!(
            history
                .page(0, 256)
                .unwrap()
                .events
                .iter()
                .any(|e| e.event == "after_rename")
        );
        history.shutdown().await;
    }
    #[tokio::test]
    async fn replay_durable_lifecycle_past_stale_state_and_preserve_instance() {
        for finished in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let (mut writer, _) = writer(temp.path(), 4, 8).await;
            let id = Uuid::new_v4();
            let old_instance = writer.inner.instance_id;
            writer
                .append(vec![event(
                    &writer,
                    1,
                    "session.started",
                    Some(id),
                    json!({}),
                )])
                .unwrap();
            let old_state = std::fs::read(writer.root.join("state.json")).unwrap();
            if finished {
                writer
                    .append(vec![event(
                        &writer,
                        2,
                        "session.finished",
                        Some(id),
                        json!({}),
                    )])
                    .unwrap();
            }
            let mut state: State = serde_json::from_slice(&old_state).unwrap();
            if !finished {
                state.next_cursor = 0;
                state.open_sessions.clear();
            }
            std::fs::write(
                writer.root.join("state.json"),
                serde_json::to_vec(&state).unwrap(),
            )
            .unwrap();
            drop(writer);
            let history = reopen(temp.path(), 4, 8).await;
            let page = history.page(0, 256).unwrap();
            assert_ne!(page.instance_id, old_instance);
            let interrupted = page
                .events
                .iter()
                .find(|e| e.event == "session.interrupted");
            assert_eq!(interrupted.is_some(), !finished);
            if let Some(event) = interrupted {
                assert_eq!(event.instance_id, old_instance);
                assert_eq!(event.data["outcome"], "unknown");
            }
            let journal = page.journal_id;
            history.shutdown().await;
            drop(history);
            let history = reopen(temp.path(), 4, 8).await;
            assert_eq!(history.page(0, 256).unwrap().journal_id, journal);
            assert_eq!(
                history
                    .page(0, 256)
                    .unwrap()
                    .events
                    .iter()
                    .filter(|e| e.event == "session.interrupted")
                    .count(),
                usize::from(!finished)
            );
            history.shutdown().await;
        }
    }
    #[tokio::test]
    async fn first_and_tail_survive_noncontiguous_output_offsets_and_restart() {
        let temp = tempfile::tempdir().unwrap();
        let (mut writer, _) = writer(temp.path(), 4, 8).await;
        let id = Uuid::new_v4();
        for (i, offset) in [0, 100, 108, 116].into_iter().enumerate() {
            writer
                .append(vec![event(
                    &writer,
                    i as u64 + 1,
                    "session.output",
                    Some(id),
                    json!({"byte_offset":offset,"byte_length":4,"text":"abcd"}),
                )])
                .unwrap();
        }
        assert_eq!(
            writer.inner.reader.lock().unwrap().state.session_windows[&id],
            108
        );
        let expected: Vec<_> = writer
            .inner
            .reader
            .lock()
            .unwrap()
            .events
            .keys()
            .copied()
            .collect();
        drop(writer);
        let history = reopen(temp.path(), 4, 8).await;
        let actual: Vec<_> = history
            .page(0, 256)
            .unwrap()
            .events
            .iter()
            .filter(|e| e.event == "session.output")
            .map(|e| e.seq)
            .collect();
        assert_eq!(actual, expected);
        history.shutdown().await;
    }
    #[tokio::test]
    async fn pending_gap_never_skips_valid_queued_sequence_for_collector() {
        let temp = tempfile::tempdir().unwrap();
        let (mut writer, receiver) = writer(temp.path(), 4, 8).await;
        let history = History {
            inner: Some(writer.inner.clone()),
        };
        for _ in 0..3 {
            history.record(Event::new("accepted", None, None, None, json!({})));
        }
        let first = receiver.recv().unwrap();
        writer
            .inner
            .queued_bytes
            .fetch_sub(first.bytes, Ordering::AcqRel);
        writer.append(first.events).unwrap();
        history.record(Event::new("after_gap", None, None, None, json!({})));
        let first_page = history.page(0, 256).unwrap();
        assert_eq!(first_page.next_cursor, 1);
        let second = receiver.recv().unwrap();
        writer
            .inner
            .queued_bytes
            .fetch_sub(second.bytes, Ordering::AcqRel);
        writer.append(second.events).unwrap();
        let second_page = history.page(first_page.next_cursor, 256).unwrap();
        assert_eq!(second_page.next_cursor, 2);
        assert_eq!(second_page.events.len(), 1);
        let last = receiver.recv().unwrap();
        writer
            .inner
            .queued_bytes
            .fetch_sub(last.bytes, Ordering::AcqRel);
        writer.append(last.events).unwrap();
        let final_page = history.page(second_page.next_cursor, 256).unwrap();
        assert_eq!(final_page.next_cursor, 4);
        assert_eq!(final_page.events[0].event, "after_gap");
        assert!(
            final_page
                .gaps
                .iter()
                .any(|g| g.after_seq == 2 && g.through_seq == 3)
        );
    }
    #[tokio::test]
    async fn restart_never_reuses_reserved_gap_sequences_or_torn_tail() {
        let temp = tempfile::tempdir().unwrap();
        let (mut writer, _) = writer(temp.path(), 4, 8).await;
        writer
            .append(vec![event(&writer, 1, "first", None, json!({}))])
            .unwrap();
        gap(
            &mut writer.inner.reader.lock().unwrap().state.gaps,
            3,
            3,
            "queue_full",
        );
        writer.persist().unwrap();
        writer.file.write_all(b"{\"seq\":4,").unwrap();
        writer.file.sync_all().unwrap();
        drop(writer);
        let history = reopen(temp.path(), 4, 8).await;
        let page = history.page(1, 256).unwrap();
        assert!(page.events.iter().all(|event| event.seq > 4));
        assert!(
            page.gaps
                .iter()
                .any(|gap| gap.after_seq < 2 && gap.through_seq >= 3)
        );
        assert!(
            page.gaps
                .iter()
                .any(|gap| gap.reason == "incomplete_journal_tail")
        );
        assert_eq!(page.recording_state, "partial");
        history.shutdown().await;
    }
    #[tokio::test]
    async fn replaced_parent_directory_cannot_redirect_history_writes() {
        use std::os::unix::fs::symlink;
        let temp = tempfile::tempdir().unwrap();
        let owner = ProjectStore::initialize(temp.path().to_owned())
            .await
            .unwrap();
        let history = History::initialize(
            temp.path().to_owned(),
            HistoryConfig {
                enabled: true,
                ..HistoryConfig::default()
            },
            owner,
        )
        .await
        .unwrap();
        let old = temp.path().join(".vivado-server/history-v1");
        let moved = temp.path().join(".vivado-server/history-moved");
        let outside = tempfile::tempdir().unwrap();
        std::fs::rename(&old, &moved).unwrap();
        symlink(outside.path(), &old).unwrap();
        history
            .critical(vec![Event::new("anchored", None, None, None, json!({}))])
            .await
            .unwrap();
        assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);
        assert!(
            std::fs::read_to_string(moved.join("events.jsonl"))
                .unwrap()
                .contains("anchored")
        );
        history.shutdown().await;
    }
}
