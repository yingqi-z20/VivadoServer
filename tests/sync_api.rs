use reqwest::StatusCode;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{fs, net::SocketAddr, path::PathBuf, time::Duration};
use tempfile::TempDir;
use vivado_server::{AppConfig, SessionManager, build_router};

async fn spawn_test_server() -> (String, TempDir) {
    let temp = tempfile::tempdir().unwrap();
    let config = AppConfig {
        listen_addr: "127.0.0.1:0".to_string(),
        vivado_path: PathBuf::from("vivado"),
        workspace_root: temp.path().to_path_buf(),
        auth_tokens: vec!["secret".to_string()],
        max_active_sessions: 1,
        heartbeat_timeout_secs: 120,
        output_buffer_bytes: 16 * 1024,
        sync_max_file_bytes: 4 * 1024 * 1024,
        sync_max_manifest_entries: 10_000,
        sync_session_ttl_secs: 60,
        tls: None,
    };
    let manager = SessionManager::new(config.clone());
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

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn file_entry(path: &str, bytes: &[u8]) -> Value {
    serde_json::json!({
        "path": path,
        "kind": "file",
        "size_bytes": bytes.len(),
        "mtime_unix_ms": 1_700_000_000_000_i64,
        "sha256": sha256_hex(bytes)
    })
}

fn dir_entry(path: &str) -> Value {
    serde_json::json!({
        "path": path,
        "kind": "dir",
        "mtime_unix_ms": 1_700_000_000_000_i64
    })
}

#[tokio::test]
async fn sync_routes_require_auth() {
    let (base, _temp) = spawn_test_server().await;
    let client = http_client();

    let response = client
        .post(format!("{base}/v1/projects/demo/sync/manifest"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn rejects_invalid_project_and_sync_path() {
    let (base, _temp) = spawn_test_server().await;
    let client = http_client();

    let invalid_project = client
        .post(format!("{base}/v1/projects/bad:project/sync/manifest"))
        .bearer_auth("secret")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid_project.status(), StatusCode::BAD_REQUEST);

    let invalid_path = client
        .get(format!("{base}/v1/projects/demo/sync/files/a:b"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap();
    assert_eq!(invalid_path.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn push_uploads_and_commits_changed_files() {
    let (base, temp) = spawn_test_server().await;
    let client = http_client();
    let bytes = b"puts hello\n";

    let plan: Value = client
        .post(format!("{base}/v1/projects/demo/sync/push/plan"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "entries": [dir_entry("src"), file_entry("src/top.tcl", bytes)],
            "delete_extra": false
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let sync_id = plan["sync_id"].as_str().unwrap();
    assert_eq!(plan["upload_files"].as_array().unwrap().len(), 1);
    assert_eq!(plan["create_dirs"].as_array().unwrap().len(), 1);

    client
        .put(format!(
            "{base}/v1/projects/demo/sync/{sync_id}/files/src/top.tcl"
        ))
        .bearer_auth("secret")
        .body(bytes.to_vec())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    client
        .post(format!("{base}/v1/projects/demo/sync/{sync_id}/commit"))
        .bearer_auth("secret")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let written = fs::read(temp.path().join("demo/src/top.tcl")).unwrap();
    assert_eq!(written, bytes);
}

#[tokio::test]
async fn pull_plan_and_download_return_file_with_checksum_headers() {
    let (base, temp) = spawn_test_server().await;
    let client = http_client();
    let project = temp.path().join("demo");
    fs::create_dir_all(project.join("out")).unwrap();
    fs::write(project.join("out/result.txt"), b"bitstream data").unwrap();

    let plan: Value = client
        .post(format!("{base}/v1/projects/demo/sync/pull/plan"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "entries": [],
            "delete_extra": false
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(plan["download_files"].as_array().unwrap().len(), 1);

    let response = client
        .get(format!("{base}/v1/projects/demo/sync/files/out/result.txt"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(
        response.headers()["x-sync-sha256"],
        sha256_hex(b"bitstream data")
    );
    let body = response.bytes().await.unwrap();
    assert_eq!(&body[..], b"bitstream data");
}

#[tokio::test]
async fn delete_extra_is_explicit() {
    let (base, temp) = spawn_test_server().await;
    let client = http_client();
    let project = temp.path().join("demo");
    fs::create_dir_all(&project).unwrap();
    fs::write(project.join("old.txt"), b"old").unwrap();

    let keep_plan: Value = client
        .post(format!("{base}/v1/projects/demo/sync/push/plan"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "entries": [],
            "delete_extra": false
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let keep_sync_id = keep_plan["sync_id"].as_str().unwrap();
    assert!(keep_plan["delete_files"].as_array().unwrap().is_empty());
    client
        .post(format!(
            "{base}/v1/projects/demo/sync/{keep_sync_id}/commit"
        ))
        .bearer_auth("secret")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert!(project.join("old.txt").exists());

    let delete_plan: Value = client
        .post(format!("{base}/v1/projects/demo/sync/push/plan"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "entries": [],
            "delete_extra": true
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let delete_sync_id = delete_plan["sync_id"].as_str().unwrap();
    assert_eq!(delete_plan["delete_files"][0], "old.txt");
    client
        .post(format!(
            "{base}/v1/projects/demo/sync/{delete_sync_id}/commit"
        ))
        .bearer_auth("secret")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert!(!project.join("old.txt").exists());
}

#[tokio::test]
async fn commit_detects_conflict_and_force_overwrites() {
    let (base, temp) = spawn_test_server().await;
    let client = http_client();
    let project = temp.path().join("demo");
    fs::create_dir_all(&project).unwrap();
    fs::write(project.join("top.tcl"), b"old").unwrap();

    let bytes = b"new";
    let plan: Value = client
        .post(format!("{base}/v1/projects/demo/sync/push/plan"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "entries": [file_entry("top.tcl", bytes)],
            "delete_extra": false
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let sync_id = plan["sync_id"].as_str().unwrap();

    fs::write(project.join("top.tcl"), b"changed after plan").unwrap();
    client
        .put(format!(
            "{base}/v1/projects/demo/sync/{sync_id}/files/top.tcl"
        ))
        .bearer_auth("secret")
        .body(bytes.to_vec())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let conflict = client
        .post(format!("{base}/v1/projects/demo/sync/{sync_id}/commit"))
        .bearer_auth("secret")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::CONFLICT);

    client
        .post(format!("{base}/v1/projects/demo/sync/{sync_id}/commit"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"force": true}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(fs::read(project.join("top.tcl")).unwrap(), bytes);
}

#[tokio::test]
async fn abort_removes_staging_without_touching_project_files() {
    let (base, temp) = spawn_test_server().await;
    let client = http_client();
    let bytes = vec![7_u8; 256 * 1024];

    let plan: Value = client
        .post(format!("{base}/v1/projects/demo/sync/push/plan"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "entries": [file_entry("large.bin", &bytes)],
            "delete_extra": false
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let sync_id = plan["sync_id"].as_str().unwrap();

    client
        .put(format!(
            "{base}/v1/projects/demo/sync/{sync_id}/files/large.bin"
        ))
        .bearer_auth("secret")
        .body(bytes)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    client
        .delete(format!("{base}/v1/projects/demo/sync/{sync_id}"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    assert!(!temp.path().join("demo/large.bin").exists());
    assert!(
        !temp
            .path()
            .join(format!("demo/.vivado-server-sync/{sync_id}"))
            .exists()
    );
}
