use super::*;

fn metric_value(manager: &SyncManager, name: &str, labels: &[&str]) -> f64 {
    manager
        .config
        .telemetry
        .render()
        .unwrap()
        .lines()
        .find(|line| {
            line.starts_with(&format!("vivado_server_{name}{{"))
                && labels.iter().all(|label| line.contains(label))
        })
        .map(|line| line.rsplit_once(' ').unwrap().1.parse().unwrap())
        .unwrap_or(0.0)
}

#[tokio::test]
async fn sync_metrics_count_valid_uploads_and_transactions_once() {
    let temp = tempfile::tempdir().unwrap();
    let manager = SyncManager::new(test_config(temp.path()));
    let plan = manager
        .push_reset_plan(
            "project".into(),
            PushPlanRequest {
                entries: vec![file_entry("top.v", b"good")],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        metric_value(&manager, "active", &["kind=\"sync_session\""]),
        1.0
    );
    let invalid = manager
        .upload_file(
            "project".into(),
            plan.sync_id,
            "top.v".into(),
            Some(4),
            Body::from("oops"),
        )
        .await;
    assert!(matches!(invalid, Err(AppError::BadRequest(_))));
    assert_eq!(
        metric_value(&manager, "bytes_total", &["kind=\"sync_upload\""]),
        0.0
    );
    upload(&manager, plan.sync_id, "top.v", b"good").await;
    for _ in 0..2 {
        manager
            .commit("project".into(), plan.sync_id, CommitSyncRequest::default())
            .await
            .unwrap();
    }
    assert_eq!(
        metric_value(&manager, "bytes_total", &["kind=\"sync_upload\""]),
        4.0
    );
    assert_eq!(
        metric_value(
            &manager,
            "operations_total",
            &["kind=\"sync_upload\"", "outcome=\"error\""]
        ),
        1.0
    );
    assert_eq!(
        metric_value(
            &manager,
            "operations_total",
            &["kind=\"sync_commit\"", "outcome=\"success\""]
        ),
        1.0
    );
    assert_eq!(
        metric_value(&manager, "active", &["kind=\"sync_upload\""]),
        0.0
    );
    assert_eq!(
        metric_value(&manager, "active", &["kind=\"sync_session\""]),
        0.0
    );
    assert!(!manager.is_degraded());
    manager.shutdown().await;
}

#[tokio::test]
async fn sync_download_metrics_follow_body_consumption_and_cancellation() {
    let temp = tempfile::tempdir().unwrap();
    let manager = SyncManager::new(test_config(temp.path()));
    tokio::fs::create_dir(temp.path().join("project"))
        .await
        .unwrap();
    tokio::fs::write(temp.path().join("project/top.v"), b"data")
        .await
        .unwrap();
    let unread = manager
        .download_file("project".into(), "top.v".into(), None)
        .await
        .unwrap();
    assert_eq!(
        metric_value(&manager, "bytes_total", &["kind=\"sync_download\""]),
        0.0
    );
    drop(unread);
    assert_eq!(
        metric_value(
            &manager,
            "operations_total",
            &["kind=\"sync_download\"", "outcome=\"cancelled\""]
        ),
        1.0
    );
    let response = manager
        .download_file("project".into(), "top.v".into(), None)
        .await
        .unwrap();
    assert_eq!(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .as_ref(),
        b"data"
    );
    assert_eq!(
        metric_value(&manager, "bytes_total", &["kind=\"sync_download\""]),
        4.0
    );
    assert_eq!(
        metric_value(
            &manager,
            "operations_total",
            &["kind=\"sync_download\"", "outcome=\"success\""]
        ),
        1.0
    );
    assert_eq!(
        metric_value(&manager, "active", &["kind=\"sync_download\""]),
        0.0
    );
    let head = manager
        .head_file("project".into(), "top.v".into(), None)
        .await
        .unwrap();
    assert_eq!(head.headers()[CONTENT_LENGTH], "4");
    assert!(
        head.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty()
    );
    assert_eq!(
        metric_value(&manager, "bytes_total", &["kind=\"sync_download\""]),
        4.0
    );
    assert_eq!(
        metric_value(
            &manager,
            "operations_total",
            &["kind=\"sync_download_metadata\"", "outcome=\"success\""]
        ),
        1.0
    );
    manager.shutdown().await;
}

#[tokio::test]
async fn cleanup_failure_is_degraded_until_all_pending_sessions_recover() {
    let temp = tempfile::tempdir().unwrap();
    let manager = SyncManager::new(test_config(temp.path()));
    // Replacing a private staging directory with a link safely forces cleanup
    // to fail even under root, without making the test depend on permissions.
    let external = tempfile::tempdir().unwrap();
    let mut broken = Vec::new();
    for _ in 0..2 {
        let plan = manager
            .push_plan("project".into(), PushPlanRequest::default())
            .await
            .unwrap();
        let session = manager.session(plan.sync_id, "project").await.unwrap();
        let staging = session.data.lock().await.plan.staging_dir.clone();
        tokio::fs::remove_dir_all(&staging).await.unwrap();
        std::os::unix::fs::symlink(external.path(), &staging).unwrap();
        assert!(manager.abort("project".into(), plan.sync_id).await.is_err());
        assert!(manager.is_degraded());
        broken.push((session, staging));
    }
    assert_eq!(
        metric_value(&manager, "active", &["kind=\"sync_cleanup_pending\""]),
        2.0
    );
    for (index, (session, staging)) in broken.into_iter().enumerate() {
        tokio::fs::remove_file(staging).await.unwrap();
        manager.cleanup_session(&session).await.unwrap();
        assert_eq!(manager.is_degraded(), index == 0);
    }
    assert_eq!(
        metric_value(&manager, "active", &["kind=\"sync_cleanup_pending\""]),
        0.0
    );
    assert_eq!(
        metric_value(
            &manager,
            "events_total",
            &["kind=\"sync_cleanup\"", "outcome=\"recovered\""]
        ),
        2.0
    );
    manager.shutdown().await;
}

#[tokio::test]
async fn detached_accepted_task_panic_marks_sync_unhealthy() {
    let temp = tempfile::tempdir().unwrap();
    let manager = SyncManager::new(test_config(temp.path()));
    let worker = manager.clone();
    let entered = Arc::new(Notify::new());
    let entered_worker = entered.clone();
    let release = Arc::new(Notify::new());
    let release_worker = release.clone();
    let observer = tokio::spawn(async move {
        worker
            .tracked::<()>("sync_plan", async move {
                entered_worker.notify_one();
                release_worker.notified().await;
                panic!("test accepted sync worker panic");
            })
            .await
    });
    entered.notified().await;
    observer.abort();
    let _ = observer.await;
    release.notify_one();
    time::timeout(Duration::from_secs(5), manager.shutdown())
        .await
        .unwrap();
    assert!(manager.is_degraded());
    assert_eq!(
        metric_value(&manager, "active", &["kind=\"sync_plan\""]),
        0.0
    );
    assert_eq!(
        metric_value(
            &manager,
            "background_task_failures_total",
            &["task=\"sync_operation\""]
        ),
        1.0
    );
}

fn test_config(root: &Path) -> RuntimeConfig {
    crate::config::AppConfig {
        vivado_path: "/bin/true".into(),
        workspace_root: root.into(),
        auth_tokens: vec!["0123456789abcdef0123456789abcdef".into()],
        allow_run_as_root: true,
        ..Default::default()
    }
    .into_runtime()
    .unwrap()
    .0
}

fn file_entry(path: &str, contents: &[u8]) -> ManifestEntry {
    ManifestEntry {
        path: path.into(),
        kind: ManifestEntryKind::File,
        size_bytes: Some(contents.len() as u64),
        mtime_unix_ms: Some(1_700_000_000_000),
        sha256: Some(format!("{:x}", Sha256::digest(contents))),
        executable: false,
    }
}

async fn upload(manager: &SyncManager, sync_id: Uuid, path: &str, contents: &'static [u8]) {
    manager
        .upload_file(
            "project".into(),
            sync_id,
            path.into(),
            Some(contents.len() as u64),
            Body::from(contents),
        )
        .await
        .unwrap();
}

async fn wait_for_status(
    manager: &SyncManager,
    sync_id: Uuid,
    expected: SyncSessionStatus,
) -> SyncStatusResponse {
    time::timeout(Duration::from_secs(5), async {
        loop {
            let status = manager.status("project".into(), sync_id).await.unwrap();
            if status.status == expected && !status.cleanup_pending {
                return status;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("accepted sync work did not settle")
}

#[tokio::test]
async fn reset_requires_every_upload_and_replaces_the_complete_project_tree() {
    let temp = tempfile::tempdir().unwrap();
    let project = temp.path().join("project");
    tokio::fs::create_dir_all(project.join("extra/cache"))
        .await
        .unwrap();
    tokio::fs::write(project.join("design.v"), b"same design")
        .await
        .unwrap();
    tokio::fs::write(project.join("extra/cache/old.bit"), b"old output")
        .await
        .unwrap();
    let manager = SyncManager::new(test_config(temp.path()));
    let same_file = manager
        .manifest("project".into(), ManifestRequest::default())
        .await
        .unwrap()
        .entries
        .into_iter()
        .find(|entry| entry.path == "design.v")
        .unwrap();
    let plan = manager
        .push_reset_plan(
            "project".into(),
            PushPlanRequest {
                entries: vec![same_file, file_entry("rtl/core.v", b"new core")],
                // Full replacement discards old extras even with this default.
                delete_extra: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        plan.upload_files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>(),
        ["design.v", "rtl/core.v"]
    );
    let session = manager.session(plan.sync_id, "project").await.unwrap();
    let staging = session.data.lock().await.plan.staging_dir.clone();
    let private_root = manager.config.workspace_root.join(".vivado-server/staging");
    assert_eq!(staging.parent(), Some(private_root.as_path()));
    assert!(!staging.starts_with(&project));

    upload(&manager, plan.sync_id, "rtl/core.v", b"new core").await;
    let error = manager
        .commit("project".into(), plan.sync_id, CommitSyncRequest::default())
        .await
        .unwrap_err();
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(
        manager
            .status("project".into(), plan.sync_id)
            .await
            .unwrap()
            .status,
        SyncSessionStatus::Open
    );
    assert_eq!(
        tokio::fs::read(project.join("extra/cache/old.bit"))
            .await
            .unwrap(),
        b"old output"
    );

    upload(&manager, plan.sync_id, "design.v", b"same design").await;
    let result = manager
        .commit("project".into(), plan.sync_id, CommitSyncRequest::default())
        .await
        .unwrap();
    assert_eq!(result.uploaded_files, ["design.v", "rtl/core.v"]);
    assert_eq!(
        tokio::fs::read(project.join("design.v")).await.unwrap(),
        b"same design"
    );
    assert_eq!(
        tokio::fs::read(project.join("rtl/core.v")).await.unwrap(),
        b"new core"
    );
    assert!(!project.join("extra").exists());
    assert!(!staging.exists());
    let status = wait_for_status(&manager, plan.sync_id, SyncSessionStatus::Committed).await;
    assert!(status.result.is_some());
    manager.shutdown().await;
}

#[tokio::test]
async fn incremental_commit_conflict_preserves_the_current_project_files() {
    let temp = tempfile::tempdir().unwrap();
    let project = temp.path().join("project");
    tokio::fs::create_dir(&project).await.unwrap();
    tokio::fs::write(project.join("design.v"), b"original")
        .await
        .unwrap();
    tokio::fs::write(project.join("keep.txt"), b"keep")
        .await
        .unwrap();
    let manager = SyncManager::new(test_config(temp.path()));
    let plan = manager
        .push_plan(
            "project".into(),
            PushPlanRequest {
                entries: vec![file_entry("design.v", b"upload")],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    upload(&manager, plan.sync_id, "design.v", b"upload").await;
    tokio::fs::write(project.join("design.v"), b"changed after planning")
        .await
        .unwrap();

    let error = manager
        .commit("project".into(), plan.sync_id, CommitSyncRequest::default())
        .await
        .unwrap_err();

    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(
        tokio::fs::read(project.join("design.v")).await.unwrap(),
        b"changed after planning"
    );
    assert_eq!(
        tokio::fs::read(project.join("keep.txt")).await.unwrap(),
        b"keep"
    );
    let status = wait_for_status(&manager, plan.sync_id, SyncSessionStatus::Failed).await;
    assert!(status.error_code.is_some());
    manager.shutdown().await;
}

#[tokio::test]
async fn excluding_an_ancestor_hides_its_children_in_both_directions() {
    let temp = tempfile::tempdir().unwrap();
    let project = temp.path().join("project");
    tokio::fs::create_dir_all(project.join("cache"))
        .await
        .unwrap();
    tokio::fs::write(project.join("cache/server.bin"), b"server cache")
        .await
        .unwrap();
    let manager = SyncManager::new(test_config(temp.path()));
    let manifest = manager
        .manifest(
            "project".into(),
            ManifestRequest {
                exclude_globs: vec!["cache".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(manifest.entries.is_empty());
    let plan = manager
        .push_plan(
            "project".into(),
            PushPlanRequest {
                entries: vec![file_entry("cache/client.bin", b"client cache")],
                delete_extra: true,
                exclude_globs: vec!["cache".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(plan.upload_files.is_empty());
    assert!(plan.create_dirs.is_empty());
    assert!(plan.delete_files.is_empty());
    assert!(plan.delete_dirs.is_empty());
    manager
        .commit("project".into(), plan.sync_id, CommitSyncRequest::default())
        .await
        .unwrap();
    assert_eq!(
        tokio::fs::read(project.join("cache/server.bin"))
            .await
            .unwrap(),
        b"server cache"
    );
    assert!(!project.join("cache/client.bin").exists());

    let pull = manager
        .pull_plan(
            "project".into(),
            PullPlanRequest {
                entries: vec![file_entry("cache/client.bin", b"client cache")],
                delete_extra: true,
                exclude_globs: vec!["cache".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(pull.download_files.is_empty());
    assert!(pull.create_dirs.is_empty());
    assert!(pull.delete_files.is_empty());
    assert!(pull.delete_dirs.is_empty());
    manager.shutdown().await;
}

#[test]
fn sync_requests_require_explicit_manifests_and_reject_unknown_fields() {
    for json in [r#"{}"#, r#"{"delete_extra":true}"#] {
        assert!(serde_json::from_str::<PushPlanRequest>(json).is_err());
        assert!(serde_json::from_str::<PullPlanRequest>(json).is_err());
    }
    for json in [
        r#"{"entries":[],"force":true}"#,
        r#"{"entries":[],"unknown":true}"#,
    ] {
        assert!(serde_json::from_str::<PushPlanRequest>(json).is_err());
        assert!(serde_json::from_str::<PullPlanRequest>(json).is_err());
    }
    for json in [r#"{"force":true}"#, r#"{"unknown":true}"#] {
        assert!(serde_json::from_str::<CommitSyncRequest>(json).is_err());
        assert!(serde_json::from_str::<ManifestRequest>(json).is_err());
    }
    let mut entry_json = serde_json::to_value(file_entry("design.v", b"design")).unwrap();
    entry_json["unknown"] = serde_json::json!(true);
    assert!(serde_json::from_value::<ManifestEntry>(entry_json).is_err());
    assert!(serde_json::from_str::<PushPlanRequest>(r#"{"entries":[]}"#).is_ok());
    assert!(serde_json::from_str::<PullPlanRequest>(r#"{"entries":[]}"#).is_ok());
    assert!(serde_json::from_str::<CommitSyncRequest>(r#"{}"#).is_ok());
}

#[tokio::test]
async fn cancelling_the_plan_observer_does_not_cancel_accepted_creation() {
    let temp = tempfile::tempdir().unwrap();
    let manager = SyncManager::new(test_config(temp.path()));
    let creating = manager.creating.lock().await;
    let worker = manager.clone();
    let observer = tokio::spawn(async move {
        worker
            .push_plan("project".into(), PushPlanRequest::default())
            .await
    });
    // Observe acceptance while the filesystem worker is blocked on our lock.
    time::timeout(Duration::from_secs(5), async {
        while manager.tasks.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("plan operation was not accepted");
    observer.abort();
    assert!(observer.await.unwrap_err().is_cancelled());
    drop(creating);

    let sync_id = time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(sync_id) = manager.sessions.read().await.keys().next().copied() {
                return sync_id;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("accepted plan creation stopped when its observer disconnected");
    let status = wait_for_status(&manager, sync_id, SyncSessionStatus::Open).await;
    assert_eq!(status.project, "project");
    let session = manager.session(sync_id, "project").await.unwrap();
    let staging = session.data.lock().await.plan.staging_dir.clone();
    assert!(staging.join("files").is_dir());
    manager.abort("project".into(), sync_id).await.unwrap();
    assert!(!staging.exists());
    manager.shutdown().await;
}

#[tokio::test]
async fn cancelling_the_abort_observer_does_not_cancel_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let manager = SyncManager::new(test_config(temp.path()));
    let plan = manager
        .push_plan("project".into(), PushPlanRequest::default())
        .await
        .unwrap();
    let session = manager.session(plan.sync_id, "project").await.unwrap();
    let staging = session.data.lock().await.plan.staging_dir.clone();
    let activity = session.activity.read().await;
    let worker = manager.clone();
    let observer = tokio::spawn(async move { worker.abort("project".into(), plan.sync_id).await });
    // Cancellation is signalled before abort waits for outstanding upload readers.
    time::timeout(Duration::from_secs(5), session.cancel_uploads.cancelled())
        .await
        .expect("abort operation was not accepted");
    observer.abort();
    assert!(observer.await.unwrap_err().is_cancelled());
    drop(activity);

    wait_for_status(&manager, plan.sync_id, SyncSessionStatus::Aborted).await;
    assert!(!staging.exists());
    let retry = manager.abort("project".into(), plan.sync_id).await.unwrap();
    assert_eq!(retry.status, "aborted");
    let replacement = manager
        .push_plan("project".into(), PushPlanRequest::default())
        .await
        .unwrap();
    manager
        .abort("project".into(), replacement.sync_id)
        .await
        .unwrap();
    manager.shutdown().await;
}

#[tokio::test]
async fn cancelling_the_commit_observer_preserves_commit_and_its_idempotent_result() {
    let temp = tempfile::tempdir().unwrap();
    let manager = SyncManager::new(test_config(temp.path()));
    let plan = manager
        .push_plan(
            "project".into(),
            PushPlanRequest {
                entries: vec![file_entry("design.v", b"committed design")],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    upload(&manager, plan.sync_id, "design.v", b"committed design").await;
    time::timeout(Duration::from_secs(5), async {
        while !manager.tasks.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("preceding upload task did not settle");
    let session = manager.session(plan.sync_id, "project").await.unwrap();
    let staging = session.data.lock().await.plan.staging_dir.clone();
    let activity = session.activity.read().await;
    let worker = manager.clone();
    let observer = tokio::spawn(async move {
        worker
            .commit("project".into(), plan.sync_id, CommitSyncRequest::default())
            .await
    });
    time::timeout(Duration::from_secs(5), async {
        while manager.tasks.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("commit operation was not accepted");
    observer.abort();
    assert!(observer.await.unwrap_err().is_cancelled());
    drop(activity);

    let status = wait_for_status(&manager, plan.sync_id, SyncSessionStatus::Committed).await;
    assert_eq!(
        tokio::fs::read(temp.path().join("project/design.v"))
            .await
            .unwrap(),
        b"committed design"
    );
    assert!(!staging.exists());
    let retry = manager
        .commit("project".into(), plan.sync_id, CommitSyncRequest::default())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(retry).unwrap(),
        serde_json::to_value(status.result.unwrap()).unwrap()
    );
    manager.shutdown().await;
}

#[tokio::test]
async fn orphan_cleanup_removes_only_private_staging_and_preserves_project_data() {
    let temp = tempfile::tempdir().unwrap();
    let manager = SyncManager::new(test_config(temp.path()));
    let project = temp.path().join("project");
    let legacy_scratch = project.join(".vivado-server/staging");
    tokio::fs::create_dir_all(&legacy_scratch).await.unwrap();
    tokio::fs::write(project.join("design.v"), b"project design")
        .await
        .unwrap();
    tokio::fs::write(legacy_scratch.join("do-not-replay"), b"project data")
        .await
        .unwrap();
    let staging_root = private_staging_root(temp.path()).await.unwrap();
    let orphan = staging_root.join(Uuid::new_v4().to_string());
    tokio::fs::create_dir_all(orphan.join("rollback"))
        .await
        .unwrap();
    tokio::fs::write(orphan.join("rollback/old-design.v"), b"stale backup")
        .await
        .unwrap();
    let unfinished = staging_root.join(Uuid::new_v4().to_string());
    tokio::fs::create_dir(&unfinished).await.unwrap();

    manager.cleanup_orphans().await.unwrap();

    assert!(!orphan.exists());
    assert!(!unfinished.exists());
    assert_eq!(
        tokio::fs::read(project.join("design.v")).await.unwrap(),
        b"project design"
    );
    assert_eq!(
        tokio::fs::read(legacy_scratch.join("do-not-replay"))
            .await
            .unwrap(),
        b"project data"
    );
    assert!(staging_root.is_dir());
    manager.shutdown().await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn orphan_cleanup_rejects_links_at_each_private_directory_boundary() {
    for relative_link in [
        PathBuf::from(".vivado-server"),
        PathBuf::from(".vivado-server/staging"),
        PathBuf::from(".vivado-server/staging").join(Uuid::new_v4().to_string()),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let external = tempfile::tempdir().unwrap();
        let manager = SyncManager::new(test_config(temp.path()));
        // A UUID-named external tree would otherwise look eligible for cleanup.
        let external_tree = external.path().join(Uuid::new_v4().to_string());
        tokio::fs::create_dir_all(external_tree.join("staging"))
            .await
            .unwrap();
        tokio::fs::write(external_tree.join("sentinel"), b"external data")
            .await
            .unwrap();
        let link = temp.path().join(&relative_link);
        tokio::fs::create_dir_all(link.parent().unwrap())
            .await
            .unwrap();
        std::os::unix::fs::symlink(external.path(), &link).unwrap();

        let error = manager.cleanup_orphans().await.unwrap_err();

        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(tokio::fs::read_link(&link).await.unwrap(), external.path());
        assert_eq!(
            tokio::fs::read(external_tree.join("sentinel"))
                .await
                .unwrap(),
            b"external data",
            "cleanup followed {}",
            relative_link.display()
        );
        manager.shutdown().await;
    }
}

#[tokio::test]
async fn completed_sync_history_is_bounded_without_waiting_for_the_reaper() {
    let temp = tempfile::tempdir().unwrap();
    let manager = SyncManager::new(test_config(temp.path()));
    let mut ids = Vec::new();
    for _ in 0..MAX_RETAINED_SYNC_RESULTS + 5 {
        let plan = manager
            .push_plan("project".into(), PushPlanRequest::default())
            .await
            .unwrap();
        ids.push(plan.sync_id);
        manager.abort("project".into(), plan.sync_id).await.unwrap();
        assert!(manager.sessions.read().await.len() <= MAX_RETAINED_SYNC_RESULTS);
    }
    assert_eq!(
        manager.sessions.read().await.len(),
        MAX_RETAINED_SYNC_RESULTS
    );
    assert!(matches!(
        manager.status("project".into(), ids[0]).await,
        Err(AppError::NotFound(_))
    ));
    assert_eq!(
        manager
            .status("project".into(), *ids.last().unwrap())
            .await
            .unwrap()
            .status,
        SyncSessionStatus::Aborted
    );

    let active = manager
        .push_plan("project".into(), PushPlanRequest::default())
        .await
        .unwrap();
    manager.prune_results().await;
    assert_eq!(
        manager
            .status("project".into(), active.sync_id)
            .await
            .unwrap()
            .status,
        SyncSessionStatus::Open
    );
    assert_eq!(
        manager.sessions.read().await.len(),
        MAX_RETAINED_SYNC_RESULTS + 1
    );
    manager
        .abort("project".into(), active.sync_id)
        .await
        .unwrap();
    assert_eq!(
        manager.sessions.read().await.len(),
        MAX_RETAINED_SYNC_RESULTS
    );
    manager.shutdown().await;
}

#[tokio::test]
async fn history_pruning_preserves_active_and_pending_cleanup_sessions() {
    let temp = tempfile::tempdir().unwrap();
    let manager = SyncManager::new(test_config(temp.path()));
    let first = manager
        .push_plan("project".into(), PushPlanRequest::default())
        .await
        .unwrap();
    manager
        .abort("project".into(), first.sync_id)
        .await
        .unwrap();
    let pending = manager.session(first.sync_id, "project").await.unwrap();
    {
        let mut data = pending.data.lock().await;
        data.cleanup_pending = true;
        data.finished_at =
            Some(Instant::now() - manager.config.sync_result_retention() - Duration::from_secs(1));
    }
    let active = manager
        .push_plan("project".into(), PushPlanRequest::default())
        .await
        .unwrap();
    let active_session = manager.session(active.sync_id, "project").await.unwrap();
    // A nonterminal session must not be removed even if stale timestamp metadata
    // exists; status and cleanup eligibility are checked independently of age.
    for state in [SyncSessionStatus::Open, SyncSessionStatus::Committing] {
        {
            let mut data = active_session.data.lock().await;
            data.state = state;
            data.finished_at = Some(
                Instant::now() - manager.config.sync_result_retention() - Duration::from_secs(1),
            );
        }
        manager.prune_results().await;
        assert!(manager.sessions.read().await.contains_key(&active.sync_id));
        assert!(manager.sessions.read().await.contains_key(&first.sync_id));
    }
    {
        let mut data = active_session.data.lock().await;
        data.state = SyncSessionStatus::Open;
        data.finished_at = None;
    }
    manager
        .abort("project".into(), active.sync_id)
        .await
        .unwrap();
    // Startup-style absence of the old scratch directory is a successful retry.
    manager.abort_open("project".into()).await.unwrap();
    assert!(matches!(
        manager.status("project".into(), first.sync_id).await,
        Err(AppError::NotFound(_))
    ));
    manager.shutdown().await;
}

#[test]
fn manifest_filters_skip_unselected_linux_names_and_large_generated_files() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(temp.path().join("src")).unwrap();
    std::fs::write(temp.path().join("src/top.v"), b"module top; endmodule").unwrap();
    std::fs::create_dir_all(temp.path().join("build/run:1")).unwrap();
    std::fs::write(temp.path().join("build/run:1/illegal?.rpt"), b"generated").unwrap();
    std::fs::write(temp.path().join("bad|name"), b"generated").unwrap();
    // A sparse oversized output must be skipped before metadata size limits or
    // hashing. Otherwise this tiny source-only scan fails (or hashes 1 TiB).
    std::fs::File::create(temp.path().join("build/huge.bit"))
        .unwrap()
        .set_len(1 << 40)
        .unwrap();
    std::os::unix::fs::symlink("/etc/passwd", temp.path().join("build/link")).unwrap();
    let manifest = scan_manifest_blocking(
        temp.path().into(),
        SyncFilters::new(&["src/top.v".into()], &[]).unwrap(),
        10,
        64,
    )
    .unwrap();
    let paths: Vec<_> = manifest
        .entries
        .iter()
        .map(|entry| entry.path.as_str())
        .collect();
    assert_eq!(paths, ["src", "src/top.v"]);
    assert_eq!(
        manifest.entries[1].sha256.as_deref(),
        Some(format!("{:x}", Sha256::digest(b"module top; endmodule")).as_str())
    );
}

#[test]
fn manifest_selection_still_rejects_nonportable_names_and_selected_links() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("safe.v"), b"source").unwrap();
    std::fs::write(temp.path().join("run:1.rpt"), b"generated").unwrap();
    std::os::unix::fs::symlink("/etc/passwd", temp.path().join("linked.v")).unwrap();
    for include in [vec![], vec!["run:1.rpt".into()], vec!["linked.v".into()]] {
        let result = scan_manifest_blocking(
            temp.path().into(),
            SyncFilters::new(&include, &[]).unwrap(),
            10,
            64,
        );
        assert!(
            matches!(result, Err(AppError::BadRequest(_))),
            "{include:?}"
        );
    }
    let manifest = scan_manifest_blocking(
        temp.path().into(),
        SyncFilters::new(&[], &["run:1.rpt".into(), "linked.v".into()]).unwrap(),
        10,
        64,
    )
    .unwrap();
    assert_eq!(manifest.entries.len(), 1);
    assert_eq!(manifest.entries[0].path, "safe.v");
}

#[test]
fn manifest_filters_prune_reserved_metadata_and_explicitly_excluded_directories() {
    let temp = tempfile::tempdir().unwrap();
    for name in [".vivado-server", "cache:generated"] {
        std::fs::create_dir(temp.path().join(name)).unwrap();
        std::fs::write(temp.path().join(name).join("bad?.bin"), b"generated").unwrap();
    }
    std::fs::write(temp.path().join("top.v"), b"source").unwrap();
    let manifest = scan_manifest_blocking(
        temp.path().into(),
        SyncFilters::new(&[], &["cache:generated".into()]).unwrap(),
        10,
        64,
    )
    .unwrap();
    assert_eq!(manifest.entries.len(), 1);
    assert_eq!(manifest.entries[0].path, "top.v");
}

#[test]
fn raw_manifest_paths_enforce_root_boundary_components_and_utf8() {
    use std::os::unix::ffi::OsStrExt;
    let root = Path::new("/workspace/project");
    for path in [
        "/workspace/other/top.v",
        "/workspace/project/../secret",
        "/workspace/project",
    ] {
        assert!(
            raw_relative_path_from_disk(Path::new(path), root).is_err(),
            "{path}"
        );
    }
    let non_utf8 = root.join(std::ffi::OsStr::from_bytes(b"bad\xff"));
    assert!(raw_relative_path_from_disk(&non_utf8, root).is_err());
    assert_eq!(
        raw_relative_path_from_disk(&root.join("build/run:1.rpt"), root).unwrap(),
        "build/run:1.rpt"
    );
}
