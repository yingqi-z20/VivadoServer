#![allow(dead_code)]

use reqwest::{Client, Method, RequestBuilder, Response};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tempfile::TempDir;
use tokio::{task::JoinHandle, time};
use uuid::Uuid;
use vivado_server::{AppConfig, AppRuntime};

pub const TOKEN: &str = "integration-token-0123456789-abcdef";

pub struct TestServer {
    pub base: String,
    pub temp: TempDir,
    pub runtime: Arc<AppRuntime>,
    pub client: Client,
    task: JoinHandle<()>,
}

pub fn fake_vivado() -> PathBuf {
    static FIXTURE: OnceLock<(TempDir, PathBuf)> = OnceLock::new();
    FIXTURE
        .get_or_init(|| {
            let temp = tempfile::tempdir().unwrap();
            let executable = temp.path().join("fake-vivado");
            let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/fake_vivado.rs");
            let output = Command::new(std::env::var("RUSTC").unwrap_or_else(|_| "rustc".into()))
                .arg("--edition=2024")
                .arg(source)
                .arg("-o")
                .arg(&executable)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "fixture compilation failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            (temp, executable)
        })
        .1
        .clone()
}

impl TestServer {
    pub async fn new() -> Self {
        Self::configured(|_| {}).await
    }

    pub async fn configured(adjust: impl FnOnce(&mut AppConfig)) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let mut config = AppConfig {
            listen_addr: "127.0.0.1:0".into(),
            vivado_path: fake_vivado(),
            workspace_root: temp.path().to_owned(),
            auth_tokens: vec![TOKEN.into()],
            allow_run_as_root: true,
            output_buffer_bytes: 64 * 1024,
            sync_max_file_bytes: 64 * 1024 * 1024,
            sync_max_manifest_entries: 10_000,
            shutdown_grace_secs: 5,
            ..AppConfig::default()
        };
        adjust(&mut config);
        let runtime = Arc::new(AppRuntime::initialize(config).await.unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = runtime.router();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            base: format!("http://{address}"),
            temp,
            runtime,
            task,
            client: Client::builder()
                .timeout(Duration::from_secs(20))
                .build()
                .unwrap(),
        }
    }

    pub fn request(&self, method: Method, path: &str) -> RequestBuilder {
        self.client
            .request(method, format!("{}{path}", self.base))
            .bearer_auth(TOKEN)
    }

    pub fn workflow(&self, method: Method, id: Uuid, suffix: &str) -> RequestBuilder {
        self.request(method, &format!("/v1/workflows/{id}{suffix}"))
    }

    pub async fn create(&self, project: &str, reset: bool) -> Uuid {
        let id = Uuid::new_v4();
        let created = ok_json(
            self.workflow(Method::PUT, id, "")
                .json(&json!({"project": project, "reset_project": reset}))
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(created["workflow_id"], id.to_string());
        assert_eq!(created["status"], "preparing");
        id
    }

    pub async fn plan(&self, id: Uuid, entries: Vec<Value>) -> Value {
        ok_json(
            self.workflow(Method::POST, id, "/sync/push/plan")
                .json(&json!({"entries": entries}))
                .send()
                .await
                .unwrap(),
        )
        .await
    }

    pub async fn upload(&self, id: Uuid, sync: &str, path: &str, bytes: &[u8]) {
        ok_json(
            self.workflow(Method::PUT, id, &format!("/sync/{sync}/files/{path}"))
                .body(bytes.to_vec())
                .send()
                .await
                .unwrap(),
        )
        .await;
    }

    pub async fn commit(&self, id: Uuid, sync: &str) -> Value {
        ok_json(
            self.workflow(Method::POST, id, &format!("/sync/{sync}/commit"))
                .json(&json!({}))
                .send()
                .await
                .unwrap(),
        )
        .await
    }

    pub async fn push(&self, id: Uuid, files: &[(&str, &[u8])]) -> Value {
        let plan = self
            .plan(
                id,
                files
                    .iter()
                    .map(|(path, bytes)| file_entry(path, bytes))
                    .collect(),
            )
            .await;
        let sync = plan["sync_id"].as_str().unwrap();
        for transfer in plan["upload_files"].as_array().unwrap() {
            let path = transfer["path"].as_str().unwrap();
            let (_, bytes) = files
                .iter()
                .find(|(candidate, _)| *candidate == path)
                .unwrap();
            self.upload(id, sync, path, bytes).await;
        }
        self.commit(id, sync).await
    }

    pub async fn start(&self, id: Uuid, args: &[&str]) -> Value {
        ok_json(
            self.workflow(Method::POST, id, "/session")
                .json(&json!({"args": args}))
                .send()
                .await
                .unwrap(),
        )
        .await
    }

    pub async fn input(&self, id: Uuid, text: &str) {
        let response = self
            .workflow(Method::POST, id, "/session/stdin")
            .json(&json!({"text":text}))
            .send()
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "stdin failed: {}",
            response.text().await.unwrap()
        );
    }

    pub async fn info(&self, id: Uuid) -> Value {
        ok_json(self.workflow(Method::GET, id, "").send().await.unwrap()).await
    }

    pub async fn wait_status(&self, id: Uuid, expected: &str) -> Value {
        let deadline = time::Instant::now() + Duration::from_secs(12);
        loop {
            let info = self.info(id).await;
            if info["status"] == expected {
                return info;
            }
            assert!(
                time::Instant::now() < deadline,
                "expected {expected}, received {info}"
            );
            time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub async fn run_to_pull(&self, id: Uuid) {
        self.start(id, &[]).await;
        self.input(id, "exit\n").await;
        self.wait_status(id, "pulling").await;
    }

    pub async fn finish(&self, id: Uuid) {
        let response = self
            .workflow(Method::POST, id, "/finish")
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "finish failed: {}",
            response.text().await.unwrap()
        );
        self.wait_status(id, "completed").await;
    }

    pub async fn cancel(&self, id: Uuid) {
        let response = self.workflow(Method::DELETE, id, "").send().await.unwrap();
        assert!(
            response.status().is_success(),
            "cancel failed: {}",
            response.text().await.unwrap()
        );
        self.wait_status(id, "cancelled").await;
    }

    pub async fn shutdown(&self) {
        self.runtime.shutdown().await;
        self.task.abort();
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub async fn ok_json(response: Response) -> Value {
    let status = response.status();
    let text = response.text().await.unwrap();
    assert!(status.is_success(), "HTTP {status}: {text}");
    serde_json::from_str(&text).unwrap_or_else(|error| panic!("invalid JSON: {error}: {text}"))
}

pub async fn error_json(response: Response, status: reqwest::StatusCode) -> Value {
    assert_eq!(response.status(), status);
    let body: Value = response.json().await.unwrap();
    assert!(body["error"]["code"].is_string(), "{body}");
    assert!(body["error"]["message"].is_string(), "{body}");
    assert!(body["error"]["details"].is_object(), "{body}");
    assert!(Uuid::parse_str(body["error"]["request_id"].as_str().unwrap()).is_ok());
    body
}

pub fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub fn file_entry(path: &str, bytes: &[u8]) -> Value {
    json!({"path":path,"kind":"file","size_bytes":bytes.len(),
        "mtime_unix_ms":1_700_000_000_000_i64,"sha256":sha256(bytes)})
}

pub fn dir_entry(path: &str) -> Value {
    json!({"path":path,"kind":"dir","mtime_unix_ms":1_700_000_000_000_i64})
}
