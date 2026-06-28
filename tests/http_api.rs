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
) -> (String, TempDir) {
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
        sync_max_file_bytes: 1024 * 1024,
        sync_max_manifest_entries: 10_000,
        sync_session_ttl_secs: 60,
        tls: None,
    };
    let manager = SessionManager::new(config.clone());
    manager.spawn_heartbeat_reaper();
    let app = build_router(config, manager);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), temp)
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

#[tokio::test]
async fn rejects_missing_auth() {
    let (base, _temp) = spawn_test_server(1, 120).await;
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
    let (base, _temp) = spawn_test_server(1, 120).await;
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
    let (base, _temp) = spawn_test_server(1, 120).await;
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
    let (base, _temp) = spawn_test_server(1, 120).await;
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
    let (base, _temp) = spawn_test_server(1, 1).await;
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
