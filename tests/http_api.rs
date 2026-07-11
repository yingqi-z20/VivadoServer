use reqwest::StatusCode;
use std::{net::SocketAddr, path::PathBuf, process::Command, time::Duration};
use tempfile::TempDir;
use tokio::time;
use vivado_server::{AppConfig, SessionManager, build_router};

fn build_fake_vivado(temp: &TempDir) -> PathBuf {
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/support/fake_vivado.rs");
    let exe = temp
        .path()
        .join(format!("fake-vivado{}", std::env::consts::EXE_SUFFIX));
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    let status = Command::new(rustc)
        .arg(source)
        .arg("-o")
        .arg(&exe)
        .status()
        .unwrap();
    assert!(status.success(), "failed to compile fake Vivado");
    exe
}

async fn spawn_test_server(
    max_active_sessions: usize,
    heartbeat_timeout_secs: u64,
) -> (String, TempDir, SessionManager) {
    let temp = tempfile::tempdir().unwrap();
    let vivado_path = build_fake_vivado(&temp);
    let config = AppConfig {
        listen_addr: "127.0.0.1:0".to_string(),
        vivado_path,
        workspace_root: temp.path().to_path_buf(),
        auth_tokens: vec!["secret".to_string()],
        max_active_sessions,
        heartbeat_timeout_secs,
        output_buffer_bytes: 16 * 1024,
        api_json_body_limit_bytes: 64 * 1024 * 1024,
        session_retention_secs: 60,
        stdin_max_bytes: 1024 * 1024,
        sync_max_file_bytes: 1024 * 1024,
        sync_max_manifest_entries: 10_000,
        sync_session_ttl_secs: 60,
        tls: None,
    };
    let manager = SessionManager::new(config.clone());
    manager.spawn_heartbeat_reaper();
    let app = build_router(config, manager.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), temp, manager)
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

#[tokio::test]
async fn rejects_missing_auth() {
    let (base, _temp, _manager) = spawn_test_server(1, 120).await;
    let client = http_client();

    let response = client
        .post(format!("{base}/v1/sessions"))
        .json(&serde_json::json!({"project":"demo","args":[]}))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn creates_interactive_session_and_reads_output() {
    let (base, _temp, _manager) = spawn_test_server(1, 120).await;
    let client = http_client();

    let created: serde_json::Value = client
        .post(format!("{base}/v1/sessions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"project":"demo","args":[]}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let session_id = created["session_id"].as_str().unwrap();

    client
        .post(format!("{base}/v1/sessions/{session_id}/stdin"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"text":"puts hello\n"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let mut cursor = 0;
    let mut text = String::new();
    for _ in 0..10 {
        let output: serde_json::Value = client
            .get(format!(
                "{base}/v1/sessions/{session_id}/output?cursor={cursor}&timeout_ms=1000"
            ))
            .bearer_auth("secret")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        cursor = output["cursor"].as_u64().unwrap();
        text.push_str(
            &output["chunks"]
                .as_array()
                .unwrap()
                .iter()
                .map(|chunk| chunk["text"].as_str().unwrap())
                .collect::<String>(),
        );

        if text.contains("fake Vivado ready") && text.contains("puts hello") {
            break;
        }
    }

    client
        .delete(format!("{base}/v1/sessions/{session_id}"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    assert!(
        text.contains("fake Vivado ready"),
        "unexpected output: {text:?}"
    );
    assert!(text.contains("puts hello"), "unexpected output: {text:?}");
}

#[tokio::test]
async fn enforces_session_limit() {
    let (base, _temp, _manager) = spawn_test_server(1, 120).await;
    let client = http_client();

    let created: serde_json::Value = client
        .post(format!("{base}/v1/sessions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"project":"one","args":[]}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let session_id = created["session_id"].as_str().unwrap();

    let second = client
        .post(format!("{base}/v1/sessions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"project":"two","args":[]}))
        .send()
        .await
        .unwrap();

    assert_eq!(second.status(), StatusCode::CONFLICT);

    client
        .delete(format!("{base}/v1/sessions/{session_id}"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

#[tokio::test]
async fn rejects_invalid_project_name() {
    let (base, _temp, _manager) = spawn_test_server(1, 120).await;
    let client = http_client();

    let response = client
        .post(format!("{base}/v1/sessions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"project":"../escape","args":[]}))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn heartbeat_timeout_terminates_session() {
    let (base, _temp, _manager) = spawn_test_server(1, 1).await;
    let client = http_client();

    let created: serde_json::Value = client
        .post(format!("{base}/v1/sessions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"project":"demo","args":[]}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let session_id = created["session_id"].as_str().unwrap();

    let mut status = String::new();
    for _ in 0..8 {
        time::sleep(Duration::from_secs(1)).await;
        let info: serde_json::Value = client
            .get(format!("{base}/v1/sessions/{session_id}"))
            .bearer_auth("secret")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        status = info["status"].as_str().unwrap().to_string();
        if status == "terminated" {
            break;
        }
    }

    assert_eq!(status, "terminated");
}

#[tokio::test]
async fn api_rejections_use_authenticated_json_errors() {
    let (base, _temp, _manager) = spawn_test_server(1, 120).await;
    let client = http_client();

    let unauthenticated = client
        .get(format!("{base}/v1/does-not-exist"))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

    let missing = client
        .get(format!("{base}/v1/does-not-exist"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert!(
        missing
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("application/json")
    );

    let malformed = client
        .post(format!("{base}/v1/sessions"))
        .bearer_auth("secret")
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body("{")
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = malformed.json().await.unwrap();
    assert!(body["error"].as_str().is_some());
}

#[tokio::test]
async fn accepts_manifest_json_larger_than_axum_default_limit() {
    let (base, _temp, _manager) = spawn_test_server(1, 120).await;
    let client = http_client();
    let padding = "x".repeat(3 * 1024 * 1024);

    let response = client
        .post(format!("{base}/v1/projects/demo/sync/manifest"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"padding": padding}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn rejects_oversized_stdin_and_heartbeat_for_stopped_session() {
    let (base, _temp, _manager) = spawn_test_server(1, 120).await;
    let client = http_client();
    let created: serde_json::Value = client
        .post(format!("{base}/v1/sessions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"project":"demo","args":[]}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let session_id = created["session_id"].as_str().unwrap();

    let oversized = client
        .post(format!("{base}/v1/sessions/{session_id}/stdin"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"text": "x".repeat(1024 * 1024 + 1)}))
        .send()
        .await
        .unwrap();
    assert_eq!(oversized.status(), StatusCode::BAD_REQUEST);

    client
        .delete(format!("{base}/v1/sessions/{session_id}"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let heartbeat = client
        .post(format!("{base}/v1/sessions/{session_id}/heartbeat"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap();
    assert_eq!(heartbeat.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn forced_termination_releases_the_session_slot() {
    let (base, _temp, _manager) = spawn_test_server(1, 120).await;
    let client = http_client();
    let created: serde_json::Value = client
        .post(format!("{base}/v1/sessions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"project":"stubborn","args":["--ignore-exit"]}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let session_id = created["session_id"].as_str().unwrap();
    client
        .delete(format!("{base}/v1/sessions/{session_id}"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    for attempt in 0..20 {
        let response = client
            .post(format!("{base}/v1/sessions"))
            .bearer_auth("secret")
            .json(&serde_json::json!({"project":"replacement","args":[]}))
            .send()
            .await
            .unwrap();
        if response.status() == StatusCode::OK {
            let replacement: serde_json::Value = response.json().await.unwrap();
            client
                .delete(format!(
                    "{base}/v1/sessions/{}",
                    replacement["session_id"].as_str().unwrap()
                ))
                .bearer_auth("secret")
                .send()
                .await
                .unwrap();
            return;
        }
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(
            attempt < 19,
            "session slot was not released after forced kill"
        );
        time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn shutdown_finishes_a_cancelled_termination() {
    let (base, _temp, manager) = spawn_test_server(1, 120).await;
    let client = http_client();
    let created: serde_json::Value = client
        .post(format!("{base}/v1/sessions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"project":"stubborn","args":["--ignore-exit"]}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let session_id = uuid::Uuid::parse_str(created["session_id"].as_str().unwrap()).unwrap();

    let terminating_manager = manager.clone();
    let termination = tokio::spawn(async move {
        terminating_manager
            .terminate_session(session_id)
            .await
            .unwrap();
    });
    let mut marked_terminated = false;
    for _ in 0..20 {
        let info: serde_json::Value = client
            .get(format!("{base}/v1/sessions/{session_id}"))
            .bearer_auth("secret")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        if info["status"] == "terminated" {
            marked_terminated = true;
            break;
        }
        time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        marked_terminated,
        "termination did not enter its grace period"
    );
    termination.abort();
    let _ = termination.await;

    manager.shutdown().await;
    let replacement = client
        .post(format!("{base}/v1/sessions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"project":"replacement","args":[]}))
        .send()
        .await
        .unwrap();
    assert_eq!(replacement.status(), StatusCode::OK);
    let replacement: serde_json::Value = replacement.json().await.unwrap();
    client
        .delete(format!(
            "{base}/v1/sessions/{}",
            replacement["session_id"].as_str().unwrap()
        ))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap();
}
