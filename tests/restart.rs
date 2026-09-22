#![cfg(target_os = "linux")]

mod support;

use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use reqwest::{Client, Method, RequestBuilder, StatusCode};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    net::TcpListener,
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};
use support::{TOKEN, error_json, fake_vivado, file_entry, ok_json};
use tokio::time;
use uuid::Uuid;

/// These tests cross an OS-process boundary: an in-process runtime restart would
/// not exercise release of the kernel workspace lock after SIGKILL.
struct ProcessServer {
    child: Child,
    base: String,
    client: Client,
    log_path: PathBuf,
}

impl ProcessServer {
    fn spawn(directory: &Path, workspace: &Path) -> Self {
        fs::create_dir_all(workspace).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let name = Uuid::new_v4();
        let config_path = directory.join(format!("server-{name}.toml"));
        let log_path = directory.join(format!("server-{name}.log"));
        let mut config = toml::Table::new();
        for (key, value) in [
            ("listen_addr", address.to_string()),
            ("vivado_path", fake_vivado().to_str().unwrap().to_owned()),
            ("workspace_root", workspace.to_str().unwrap().to_owned()),
        ] {
            config.insert(key.into(), toml::Value::String(value));
        }
        config.insert(
            "auth_tokens".into(),
            toml::Value::Array(vec![toml::Value::String(TOKEN.into())]),
        );
        config.insert("allow_run_as_root".into(), toml::Value::Boolean(true));
        config.insert("shutdown_grace_secs".into(), toml::Value::Integer(5));
        fs::write(&config_path, toml::to_string(&config).unwrap()).unwrap();
        let log = File::create(&log_path).unwrap();
        let stderr = log.try_clone().unwrap();
        // The CLI binds its own socket, so release the temporary port reservation
        // immediately before spawn. Every instance receives an independent port.
        drop(listener);
        let child = Command::new(env!("CARGO_BIN_EXE_vivado-server"))
            .arg("--config")
            .arg(config_path)
            .env("RUST_LOG", "vivado_server=info")
            .env("TOKIO_WORKER_THREADS", "2")
            .stdin(Stdio::null())
            .stdout(log)
            .stderr(stderr)
            .spawn()
            .unwrap();
        Self {
            child,
            base: format!("http://{address}"),
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            log_path,
        }
    }

    fn logs(&self) -> String {
        fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    async fn ready(&mut self) {
        let deadline = time::Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!("server exited before readiness: {status}\n{}", self.logs());
            }
            if let Ok(response) = self.workflow(Method::GET, Uuid::nil(), "").send().await
                && response.status() == StatusCode::NOT_FOUND
            {
                return;
            }
            assert!(
                time::Instant::now() < deadline,
                "server did not become ready\n{}",
                self.logs()
            );
            time::sleep(Duration::from_millis(25)).await;
        }
    }

    fn workflow(&self, method: Method, id: Uuid, suffix: &str) -> RequestBuilder {
        self.client
            .request(method, format!("{}/v1/workflows/{id}{suffix}", self.base))
            .bearer_auth(TOKEN)
    }

    async fn create(&self, reset: bool) -> Uuid {
        let id = Uuid::new_v4();
        let info = ok_json(
            self.workflow(Method::PUT, id, "")
                .json(&json!({"project":"demo", "reset_project":reset}))
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(info["workflow_id"], id.to_string());
        assert_eq!(info["status"], "preparing");
        assert_eq!(info["requires_full_upload"], reset);
        id
    }

    async fn push(&self, id: Uuid, files: &[(&str, &[u8])]) {
        let entries: Vec<_> = files
            .iter()
            .map(|(path, bytes)| file_entry(path, bytes))
            .collect();
        let plan = ok_json(
            self.workflow(Method::POST, id, "/sync/push/plan")
                .json(&json!({"entries":entries}))
                .send()
                .await
                .unwrap(),
        )
        .await;
        let sync = plan["sync_id"].as_str().unwrap();
        for transfer in plan["upload_files"].as_array().unwrap() {
            let path = transfer["path"].as_str().unwrap();
            let (_, contents) = files.iter().find(|(name, _)| *name == path).unwrap();
            ok_json(
                self.workflow(Method::PUT, id, &format!("/sync/{sync}/files/{path}"))
                    .body(contents.to_vec())
                    .send()
                    .await
                    .unwrap(),
            )
            .await;
        }
        ok_json(
            self.workflow(Method::POST, id, &format!("/sync/{sync}/commit"))
                .json(&json!({}))
                .send()
                .await
                .unwrap(),
        )
        .await;
        let info = self.info(id).await;
        assert_eq!(info["status"], "preparing");
        assert_eq!(info["requires_full_upload"], false);
    }

    async fn info(&self, id: Uuid) -> Value {
        ok_json(self.workflow(Method::GET, id, "").send().await.unwrap()).await
    }

    async fn run_to_pull(&self, id: Uuid) {
        ok_json(
            self.workflow(Method::POST, id, "/session")
                .json(&json!({"args":[]}))
                .send()
                .await
                .unwrap(),
        )
        .await;
        let input = self
            .workflow(Method::POST, id, "/session/stdin")
            .json(&json!({"text":"write result.txt generated\nexit\n"}))
            .send()
            .await
            .unwrap();
        assert!(
            input.status().is_success(),
            "{}",
            input.text().await.unwrap()
        );
        let deadline = time::Instant::now() + Duration::from_secs(15);
        loop {
            let info = self.info(id).await;
            if info["status"] == "pulling" {
                return;
            }
            assert!(
                time::Instant::now() < deadline,
                "normal exit did not reach pulling: {info}\n{}",
                self.logs()
            );
            time::sleep(Duration::from_millis(25)).await;
        }
    }

    fn crash(&mut self) {
        assert!(self.child.try_wait().unwrap().is_none());
        // Call only before Vivado starts or after pulling confirms it was reaped.
        // This deliberately bypasses the service's graceful-shutdown code.
        self.child.kill().unwrap();
        let status = self.child.wait().unwrap();
        assert_eq!(status.signal(), Some(nix::libc::SIGKILL));
    }

    async fn wait_exit(&mut self) -> ExitStatus {
        let deadline = time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(
                time::Instant::now() < deadline,
                "server did not exit\n{}",
                self.logs()
            );
            time::sleep(Duration::from_millis(25)).await;
        }
    }
}

impl Drop for ProcessServer {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(Some(_))) {
            return;
        }
        // A failed assertion could interrupt run_to_pull. Give the supervisor a
        // chance to stop its child before the bounded kill-and-wait fallback.
        let _ = kill(Pid::from_raw(self.child.id() as i32), Signal::SIGTERM);
        let deadline = Instant::now() + Duration::from_secs(7);
        while Instant::now() < deadline {
            if matches!(self.child.try_wait(), Ok(Some(_))) {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn dirty_snapshot_requires_full_reupload(files: &[(&str, &[u8])]) {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    let mut server = ProcessServer::spawn(temp.path(), &workspace);
    server.ready().await;
    let id = server.create(true).await;
    server.push(id, files).await;
    server.crash();

    let mut restarted = ProcessServer::spawn(temp.path(), &workspace);
    restarted.ready().await;
    let response = restarted
        .workflow(Method::PUT, Uuid::new_v4(), "")
        .json(&json!({"project":"demo", "reset_project":false}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        error_json(response, StatusCode::CONFLICT).await["error"]["code"],
        "project_reupload_required"
    );
    let replacement = restarted.create(true).await;
    restarted
        .push(
            replacement,
            &[("replacement.v", b"module replacement; endmodule")],
        )
        .await;
    for (path, _) in files {
        assert!(
            !workspace.join("demo").join(path).exists(),
            "full rebuild retained an old file outside its authoritative manifest"
        );
    }
    assert_eq!(
        fs::read(workspace.join("demo/replacement.v")).unwrap(),
        b"module replacement; endmodule"
    );
    restarted.run_to_pull(replacement).await;
}

#[tokio::test]
async fn sigkill_after_empty_push_commit_requires_full_reupload() {
    dirty_snapshot_requires_full_reupload(&[]).await;
}

#[tokio::test]
async fn sigkill_after_full_push_commit_requires_full_reupload() {
    dirty_snapshot_requires_full_reupload(&[("obsolete.v", b"module obsolete; endmodule")]).await;
}

#[tokio::test]
async fn sigkill_after_normal_exit_preserves_a_reusable_project() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    let mut server = ProcessServer::spawn(temp.path(), &workspace);
    server.ready().await;
    let id = server.create(true).await;
    server
        .push(id, &[("source.v", b"module source; endmodule")])
        .await;
    server.run_to_pull(id).await;
    server.crash();

    let mut restarted = ProcessServer::spawn(temp.path(), &workspace);
    restarted.ready().await;
    let reused = restarted.create(false).await;
    assert_eq!(
        fs::read(workspace.join("demo/source.v")).unwrap(),
        b"module source; endmodule"
    );
    assert_eq!(
        fs::read(workspace.join("demo/result.txt")).unwrap(),
        b"generated"
    );
    // Reuse must also permit starting Vivado without a new synchronization plan.
    restarted.run_to_pull(reused).await;
}

#[tokio::test]
async fn second_server_cannot_acquire_a_live_instances_workspace() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    let mut first = ProcessServer::spawn(temp.path(), &workspace);
    first.ready().await;
    let mut second = ProcessServer::spawn(temp.path(), &workspace);
    assert!(!second.wait_exit().await.success());
    assert!(
        second.logs().contains("workspace is already in use"),
        "second instance failed for an unexpected reason: {}",
        second.logs()
    );
    // The failed second startup must not release or corrupt the first instance.
    let id = first.create(true).await;
    first.push(id, &[]).await;
    assert_eq!(first.info(id).await["status"], "preparing");
}
