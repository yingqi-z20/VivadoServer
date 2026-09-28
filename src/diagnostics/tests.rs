use super::*;
use crate::{
    config::AppConfig,
    project::ProjectStore,
    session::{SessionStatus, TerminationReason},
};
use filetime::{FileTime, set_file_mtime};
use serde_json::Value;
use std::os::unix::fs::symlink;

struct Fixture {
    config: RuntimeConfig,
    root: PathBuf,
    _store: ProjectStore,
    _temporary: tempfile::TempDir,
}

impl Fixture {
    async fn new(options: ObservabilityConfig) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let (config, _) = AppConfig {
            vivado_path: PathBuf::from("/bin/sh"),
            workspace_root: temporary.path().to_path_buf(),
            auth_tokens: vec!["0123456789abcdef0123456789abcdef".into()],
            allow_run_as_root: true,
            observability: options,
            ..AppConfig::default()
        }
        .into_runtime()
        .unwrap();
        let store = ProjectStore::initialize(config.workspace_root.clone())
            .await
            .unwrap();
        let root = config.workspace_root.join(INTERNAL_DIR).join("diagnostics");
        Self {
            config,
            root,
            _store: store,
            _temporary: temporary,
        }
    }

    fn path(&self, id: Uuid) -> PathBuf {
        self.root.join(format!("{id}.jsonl"))
    }
}

fn session_info() -> SessionInfo {
    SessionInfo {
        session_id: Uuid::new_v4(),
        project: "diagnostic-project".into(),
        status: SessionStatus::Running,
        started_at: Utc::now(),
        ended_at: None,
        exit_code: None,
        termination_reason: None,
        output_truncated: false,
        cleanup_error: None,
    }
}

fn terminal_info(mut info: SessionInfo) -> SessionInfo {
    info.status = SessionStatus::Exited;
    info.ended_at = Some(Utc::now());
    info.exit_code = Some(23);
    info.termination_reason = Some(TerminationReason::ProcessExited);
    info
}

fn records(path: &Path) -> Vec<Value> {
    let bytes = fs::read(path).unwrap();
    assert!(bytes.ends_with(b"\n"), "records must end with a newline");
    std::str::from_utf8(&bytes)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn output_text(records: &[Value]) -> String {
    records
        .iter()
        .filter(|record| record["event"] == "output")
        .map(|record| record["text"].as_str().unwrap())
        .collect()
}

fn seed_archive(root: &Path, age: Duration) -> PathBuf {
    let info = session_info();
    let path = root.join(format!("{}.jsonl", info.session_id));
    fs::write(
        &path,
        record_bytes(json!({
            "format": FORMAT,
            "event": "started",
            "session_id": info.session_id,
            "project": info.project,
            "started_at": info.started_at,
        })),
    )
    .unwrap();
    set_file_mtime(&path, FileTime::from_system_time(SystemTime::now() - age)).unwrap();
    path
}

async fn capture(diagnostics: &Diagnostics, info: SessionInfo, text: &str) {
    let archive = diagnostics.start(&info).unwrap();
    archive.push(text);
    archive.finish(terminal_info(info)).await;
}

#[tokio::test]
async fn archive_preserves_utf8_and_terminal_metadata_in_private_jsonl() {
    let fixture = Fixture::new(ObservabilityConfig::default()).await;
    let diagnostics = Diagnostics::initialize(&fixture.config, fixture._store.clone())
        .await
        .unwrap();
    assert!(diagnostics.inner.is_some());
    let info = session_info();
    let path = fixture.path(info.session_id);
    let text = "开始🙂\"\n\tend\r\n".repeat(1100);
    let archive = diagnostics.start(&info).unwrap();
    archive.push(&text);
    let mut final_info = terminal_info(info.clone());
    final_info.output_truncated = true;
    archive.finish(final_info.clone()).await;
    diagnostics.shutdown().await;

    // A new diagnostics service can retain and read archives from the previous
    // writer without any in-memory session state.
    let restarted = Diagnostics::initialize(&fixture.config, fixture._store.clone())
        .await
        .unwrap();
    restarted.shutdown().await;

    let rows = records(&path);
    assert_eq!(rows[0]["format"], FORMAT);
    assert_eq!(rows[0]["session_id"], info.session_id.to_string());
    assert_eq!(output_text(&rows), text);
    let final_record = rows.last().unwrap();
    assert_eq!(final_record["event"], "finished");
    assert_eq!(final_record["session_id"], info.session_id.to_string());
    assert_eq!(final_record["project"], info.project);
    assert_eq!(final_record["exit_code"], 23);
    assert_eq!(final_record["termination_reason"], "process_exited");
    assert_eq!(final_record["ended_at"], json!(final_info.ended_at));
    assert_eq!(final_record["output_truncated"], true);
    assert_eq!(final_record["archive_truncated"], false);
    assert_eq!(final_record["archive_issues"], json!([]));
    assert_eq!(final_record["observed_bytes"], text.len() as u64);
    assert_eq!(final_record["archived_output_bytes"], text.len() as u64);
    assert_eq!(final_record["dropped_bytes"], 0);
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(&fixture.root).unwrap().permissions().mode() & 0o777,
        0o700
    );
}

#[tokio::test]
async fn session_byte_limit_preserves_prefix_and_reports_every_dropped_byte() {
    let options = ObservabilityConfig {
        archive_max_bytes_per_session: 8192,
        ..ObservabilityConfig::default()
    };
    let fixture = Fixture::new(options).await;
    let diagnostics = Diagnostics::initialize(&fixture.config, fixture._store.clone())
        .await
        .unwrap();
    let info = session_info();
    let path = fixture.path(info.session_id);
    let text = "x".repeat(CHUNK_BYTES * 4);
    capture(&diagnostics, info, &text).await;
    diagnostics.shutdown().await;

    let rows = records(&path);
    let saved = output_text(&rows);
    assert!(!saved.is_empty());
    assert!(saved.len() < text.len());
    assert!(text.starts_with(&saved));
    let footer = rows.last().unwrap();
    assert_eq!(footer["archive_truncated"], true);
    assert!(
        footer["archive_issues"]
            .as_array()
            .unwrap()
            .contains(&json!("session_limit"))
    );
    assert_eq!(footer["observed_bytes"], text.len() as u64);
    assert_eq!(footer["archived_output_bytes"], saved.len() as u64);
    assert_eq!(footer["dropped_bytes"], (text.len() - saved.len()) as u64);
    assert!(
        fs::metadata(&path).unwrap().len()
            <= fixture.config.observability.archive_max_bytes_per_session
    );
}

#[tokio::test]
async fn total_limit_reclaims_finished_history_before_losing_current_output() {
    let fixture = Fixture::new(ObservabilityConfig {
        archive_max_bytes_per_session: 8192,
        archive_max_total_bytes: 8192,
        ..ObservabilityConfig::default()
    })
    .await;
    let diagnostics = Diagnostics::initialize(&fixture.config, fixture._store.clone())
        .await
        .unwrap();
    let first = session_info();
    let first_path = fixture.path(first.session_id);
    capture(&diagnostics, first, &"a".repeat(3000)).await;
    assert!(first_path.exists());
    let second = session_info();
    let second_path = fixture.path(second.session_id);
    let text = "b".repeat(3500);
    capture(&diagnostics, second, &text).await;
    diagnostics.shutdown().await;

    assert!(
        !first_path.exists(),
        "old completed output should yield capacity"
    );
    let rows = records(&second_path);
    assert_eq!(output_text(&rows), text);
    assert_eq!(rows.last().unwrap()["archive_truncated"], false);
    let total: u64 = fs::read_dir(&fixture.root)
        .unwrap()
        .map(|entry| entry.unwrap().metadata().unwrap().len())
        .sum();
    assert!(total <= fixture.config.observability.archive_max_total_bytes);
}

#[tokio::test]
async fn initialization_removes_expired_archives_and_keeps_newest_within_count() {
    let fixture = Fixture::new(ObservabilityConfig {
        archive_retention_secs: 3600,
        archive_max_sessions: 2,
        ..ObservabilityConfig::default()
    })
    .await;
    create_directory(&fixture.root).unwrap();
    let expired = seed_archive(&fixture.root, Duration::from_secs(7200));
    let oldest = seed_archive(&fixture.root, Duration::from_secs(300));
    let middle = seed_archive(&fixture.root, Duration::from_secs(200));
    let newest = seed_archive(&fixture.root, Duration::from_secs(100));

    let diagnostics = Diagnostics::initialize(&fixture.config, fixture._store.clone())
        .await
        .unwrap();
    diagnostics.shutdown().await;
    assert!(!expired.exists());
    assert!(!oldest.exists());
    assert!(middle.exists());
    assert!(newest.exists());
    assert_eq!(fs::read_dir(&fixture.root).unwrap().count(), 2);
}

#[tokio::test]
async fn retention_leaves_unknown_files_symlinks_and_hardlinks_untouched() {
    let fixture = Fixture::new(ObservabilityConfig {
        archive_retention_secs: 1,
        archive_max_sessions: 1,
        ..ObservabilityConfig::default()
    })
    .await;
    create_directory(&fixture.root).unwrap();
    let expired = seed_archive(&fixture.root, Duration::from_secs(60));
    let unknown = fixture.path(Uuid::new_v4());
    fs::write(&unknown, b"user-owned contents\n").unwrap();
    let note = fixture.root.join("keep.txt");
    fs::write(&note, b"operator notes\n").unwrap();
    let outside = fixture.config.workspace_root.join("outside.txt");
    fs::write(&outside, b"external sentinel\n").unwrap();
    let linked = fixture.path(Uuid::new_v4());
    symlink(&outside, &linked).unwrap();
    let hardlinked = seed_archive(&fixture.root, Duration::from_secs(60));
    let hardlink_copy = fixture.config.workspace_root.join("archive-hardlink");
    fs::hard_link(&hardlinked, &hardlink_copy).unwrap();

    let diagnostics = Diagnostics::initialize(&fixture.config, fixture._store.clone())
        .await
        .unwrap();
    diagnostics.shutdown().await;
    assert!(!expired.exists());
    assert_eq!(fs::read(&unknown).unwrap(), b"user-owned contents\n");
    assert_eq!(fs::read(&note).unwrap(), b"operator notes\n");
    assert_eq!(fs::read(&outside).unwrap(), b"external sentinel\n");
    assert!(
        fs::symlink_metadata(&linked)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert!(hardlinked.exists());
    assert_eq!(
        fs::read(&hardlinked).unwrap(),
        fs::read(&hardlink_copy).unwrap()
    );
}

#[tokio::test]
async fn linked_diagnostics_directory_disables_archiving_without_touching_target() {
    let fixture = Fixture::new(ObservabilityConfig::default()).await;
    let outside = fixture.config.workspace_root.join("outside-diagnostics");
    fs::create_dir(&outside).unwrap();
    fs::set_permissions(&outside, fs::Permissions::from_mode(0o750)).unwrap();
    let sentinel = outside.join("sentinel");
    fs::write(&sentinel, b"do not touch").unwrap();
    symlink(&outside, &fixture.root).unwrap();

    let diagnostics = Diagnostics::initialize(&fixture.config, fixture._store.clone())
        .await
        .unwrap();
    assert!(diagnostics.inner.is_none());
    assert!(diagnostics.start(&session_info()).is_none());
    diagnostics.shutdown().await;
    assert_eq!(fs::read(&sentinel).unwrap(), b"do not touch");
    assert_eq!(
        fs::metadata(&outside).unwrap().permissions().mode() & 0o777,
        0o750
    );
}

#[test]
fn full_queue_keeps_a_bounded_utf8_prefix_and_counts_later_output_as_dropped() {
    let (sender, receiver) = mpsc::sync_channel(1);
    let state = Arc::new(ArchiveState {
        id: Uuid::new_v4(),
        project: "queue-test".into(),
        telemetry: Observability::new(),
        disabled: AtomicBool::new(false),
        issues: AtomicU32::new(0),
        observed: AtomicU64::new(0),
        dropped: AtomicU64::new(0),
        pending: AtomicUsize::new(0),
        final_info: Mutex::new(None),
        completed: AtomicBool::new(false),
        changed: Notify::new(),
    });
    let archive = OutputArchive {
        sender,
        state: state.clone(),
    };
    let text = "你🙂".repeat(CHUNK_BYTES);
    archive.push(&text);
    archive.push("later output");
    assert!(state.disabled.load(Ordering::Acquire));
    assert_eq!(state.issues.load(Ordering::Acquire), QUEUE_FULL);
    assert_eq!(state.pending.load(Ordering::Acquire), 1);
    let Command::Output(queued_state, prefix) = receiver.try_recv().unwrap() else {
        panic!("expected one output chunk");
    };
    assert!(Arc::ptr_eq(&queued_state, &state));
    assert!(prefix.len() <= CHUNK_BYTES);
    assert!(text.starts_with(&prefix));
    assert!(receiver.try_recv().is_err());
    let total = text.len() + "later output".len();
    assert_eq!(state.observed.load(Ordering::Acquire), total as u64);
    assert_eq!(
        state.dropped.load(Ordering::Acquire),
        (total - prefix.len()) as u64
    );
    assert!(
        state
            .telemetry
            .render()
            .unwrap()
            .contains("vivado_server_events_total{kind=\"archive\",outcome=\"queue_full\"} 1")
    );
}

#[tokio::test]
async fn existing_archive_path_is_preserved_and_failure_does_not_stop_the_writer() {
    let fixture = Fixture::new(ObservabilityConfig::default()).await;
    create_directory(&fixture.root).unwrap();
    let info = session_info();
    let path = fixture.path(info.session_id);
    fs::write(&path, b"operator-owned file\n").unwrap();
    let diagnostics = Diagnostics::initialize(&fixture.config, fixture._store.clone())
        .await
        .unwrap();
    capture(&diagnostics, info, "will not overwrite").await;
    assert_eq!(fs::read(&path).unwrap(), b"operator-owned file\n");

    let next = session_info();
    let next_path = fixture.path(next.session_id);
    capture(&diagnostics, next, "writer remains available").await;
    diagnostics.shutdown().await;
    assert_eq!(
        output_text(&records(&next_path)),
        "writer remains available"
    );
    assert!(
        fixture
            .config
            .telemetry
            .render()
            .unwrap()
            .contains("vivado_server_events_total{kind=\"archive\",outcome=\"disk_error\"} 1")
    );
}
