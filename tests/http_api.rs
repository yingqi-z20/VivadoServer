#![cfg(target_os = "linux")]
mod support;

use reqwest::{Method, StatusCode};
use serde_json::{Value, json};
use std::{fs, time::Duration};
use support::{TOKEN, TestServer, error_json, ok_json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time,
};
use uuid::Uuid;

#[tokio::test]
async fn authentication_is_required_and_bearer_scheme_is_case_insensitive() {
    let server = TestServer::new().await;
    for path in [
        "/v1/does-not-exist",
        "/v1/workflows/not-a-uuid/sync/manifest",
    ] {
        let response = server
            .client
            .get(format!("{}{path}", server.base))
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.headers()[reqwest::header::WWW_AUTHENTICATE],
            "Bearer"
        );
        error_json(response, StatusCode::UNAUTHORIZED).await;
    }
    let id = Uuid::new_v4();
    let url = format!("{}/v1/workflows/{id}", server.base);
    let response = server
        .client
        .put(&url)
        .header(reqwest::header::AUTHORIZATION, format!("bEaReR {TOKEN}"))
        .json(&json!({"project":"demo","reset_project":true}))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let wrong_token = server
        .client
        .get(&url)
        .bearer_auth(TOKEN.to_uppercase())
        .send()
        .await
        .unwrap();
    error_json(wrong_token, StatusCode::UNAUTHORIZED).await;
    server.shutdown().await;
}

#[tokio::test]
async fn workflow_creation_is_idempotent_and_globally_exclusive() {
    let server = TestServer::new().await;
    let id = Uuid::new_v4();
    let create = || {
        server
            .workflow(Method::PUT, id, "")
            .json(&json!({"project":"demo","reset_project":true}))
            .send()
    };
    let (left, right) = tokio::join!(create(), create());
    for response in [left.unwrap(), right.unwrap()] {
        assert_eq!(ok_json(response).await["workflow_id"], id.to_string());
    }
    let conflicting_retry = server
        .workflow(Method::PUT, id, "")
        .json(&json!({"project":"other","reset_project":true}))
        .send()
        .await
        .unwrap();
    error_json(conflicting_retry, StatusCode::CONFLICT).await;
    let second = server
        .workflow(Method::PUT, Uuid::new_v4(), "")
        .json(&json!({"project":"other","reset_project":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        error_json(second, StatusCode::CONFLICT).await["error"]["code"],
        "workflow_busy"
    );
    server.cancel(id).await;
    let replacement = server.create("other", true).await;
    server.cancel(replacement).await;
    server.shutdown().await;
}

#[tokio::test]
async fn concurrent_distinct_workflows_have_exactly_one_winner() {
    let server = TestServer::new().await;
    let ids = [Uuid::new_v4(), Uuid::new_v4()];
    let create = |id| {
        server
            .workflow(Method::PUT, id, "")
            .json(&json!({"project":"demo","reset_project":true}))
            .send()
    };
    let (left, right) = tokio::join!(create(ids[0]), create(ids[1]));
    let statuses = [left.unwrap().status(), right.unwrap().status()];
    assert_eq!(
        statuses.iter().filter(|status| status.is_success()).count(),
        1
    );
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == StatusCode::CONFLICT)
            .count(),
        1
    );
    server.shutdown().await;
}

#[tokio::test]
async fn malformed_unknown_and_unsupported_requests_use_error_envelopes() {
    let server = TestServer::new().await;
    let id = Uuid::new_v4();
    for body in [
        "{".to_string(),
        json!({"project":"demo","reset_project":true,"typo":1}).to_string(),
    ] {
        let response = server
            .workflow(Method::PUT, id, "")
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        assert!(response.headers().get("x-request-id").is_some());
        assert_eq!(
            error_json(response, StatusCode::BAD_REQUEST).await["error"]["code"],
            "invalid_request"
        );
    }
    for name in ["../escape", "bad:name", "", ".vivado-server"] {
        let response = server
            .workflow(Method::PUT, id, "")
            .json(&json!({"project":name,"reset_project":true}))
            .send()
            .await
            .unwrap();
        error_json(response, StatusCode::BAD_REQUEST).await;
    }
    let wrong_method = server.workflow(Method::PATCH, id, "").send().await.unwrap();
    error_json(wrong_method, StatusCode::METHOD_NOT_ALLOWED).await;
    for old_path in ["/v1/sessions", "/v1/projects/demo/sync/manifest"] {
        error_json(
            server
                .request(Method::POST, old_path)
                .json(&json!({}))
                .send()
                .await
                .unwrap(),
            StatusCode::NOT_FOUND,
        )
        .await;
    }
    server.shutdown().await;
}

#[tokio::test]
async fn new_project_requires_explicit_reset_and_full_manifest_before_running() {
    let server = TestServer::new().await;
    let response = server
        .workflow(Method::PUT, Uuid::new_v4(), "")
        .json(&json!({"project":"new-project"}))
        .send()
        .await
        .unwrap();
    error_json(response, StatusCode::CONFLICT).await;
    let id = server.create("new-project", true).await;
    assert_eq!(server.info(id).await["requires_full_upload"], true);
    let premature = server
        .workflow(Method::POST, id, "/session")
        .json(&json!({"args":[]}))
        .send()
        .await
        .unwrap();
    error_json(premature, StatusCode::CONFLICT).await;
    server.push(id, &[]).await;
    assert_eq!(server.info(id).await["requires_full_upload"], false);
    server.run_to_pull(id).await;
    server.finish(id).await;
    let reused = server.create("new-project", false).await;
    assert_eq!(server.info(reused).await["requires_full_upload"], false);
    server.run_to_pull(reused).await;
    server.finish(reused).await;
    server.shutdown().await;
}

#[tokio::test]
async fn lifecycle_seals_final_output_and_holds_slot_until_finish() {
    let server = TestServer::new().await;
    let id = server.create("demo", true).await;
    server.push(id, &[("source.tcl", b"source")]).await;
    let early_finish = server
        .workflow(Method::POST, id, "/finish")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    error_json(early_finish, StatusCode::CONFLICT).await;
    server.start(id, &[]).await;
    for suffix in ["/sync/push/plan", "/sync/pull/plan"] {
        let response = server
            .workflow(Method::POST, id, suffix)
            .json(&json!({"entries":[]}))
            .send()
            .await
            .unwrap();
        error_json(response, StatusCode::CONFLICT).await;
    }
    server
        .input(id, "puts hello\nwrite result.txt generated-result\nexit\n")
        .await;
    server.wait_status(id, "pulling").await;
    assert_eq!(
        fs::read(server.temp.path().join("demo/result.txt")).unwrap(),
        b"generated-result"
    );
    let info = ok_json(
        server
            .workflow(Method::GET, id, "/session")
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(info["status"], "exited");
    assert_eq!(info["exit_code"], 0);
    let output = ok_json(
        server
            .workflow(Method::GET, id, "/session/output?cursor=0&timeout_ms=0")
            .send()
            .await
            .unwrap(),
    )
    .await;
    let text = output["chunks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|chunk| chunk["text"].as_str().unwrap())
        .collect::<String>();
    assert!(text.contains("fake Vivado ready"), "{text}");
    assert!(text.contains("echo: puts hello"), "{text}");
    assert!(text.contains("final output before exit"), "{text}");
    let cursor = output["cursor"].as_u64().unwrap();
    time::sleep(Duration::from_millis(50)).await;
    let final_output = ok_json(
        server
            .workflow(
                Method::GET,
                id,
                &format!("/session/output?cursor={cursor}&timeout_ms=0"),
            )
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(final_output["cursor"], cursor);
    assert!(final_output["chunks"].as_array().unwrap().is_empty());
    let second = server
        .workflow(Method::PUT, Uuid::new_v4(), "")
        .json(&json!({"project":"another","reset_project":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        error_json(second, StatusCode::CONFLICT).await["error"]["code"],
        "workflow_busy"
    );
    error_json(
        server
            .workflow(Method::POST, id, "/session")
            .json(&json!({"args":[]}))
            .send()
            .await
            .unwrap(),
        StatusCode::CONFLICT,
    )
    .await;
    server.finish(id).await;
    server.shutdown().await;
}

#[tokio::test]
async fn nonzero_exit_requires_rebuilding_project() {
    let server = TestServer::new().await;
    let id = server.create("demo", true).await;
    server.push(id, &[]).await;
    server.start(id, &["--exit-code=23"]).await;
    server.input(id, "exit\n").await;
    server.wait_status(id, "failed").await;
    let session = ok_json(
        server
            .workflow(Method::GET, id, "/session")
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(session["exit_code"], 23);
    let reused = server
        .workflow(Method::PUT, Uuid::new_v4(), "")
        .json(&json!({"project":"demo"}))
        .send()
        .await
        .unwrap();
    error_json(reused, StatusCode::CONFLICT).await;
    let reset = server.create("demo", true).await;
    assert_eq!(server.info(reset).await["requires_full_upload"], true);
    server.shutdown().await;
}

#[tokio::test]
async fn heartbeats_extend_lease_and_expiry_cleans_up_preparing_workflows() {
    let server = TestServer::configured(|config| config.heartbeat_timeout_secs = 1).await;
    let id = server.create("demo", true).await;
    for _ in 0..4 {
        time::sleep(Duration::from_millis(350)).await;
        let response = server
            .workflow(Method::POST, id, "/heartbeat")
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
    }
    assert_eq!(server.info(id).await["status"], "preparing");
    server.wait_status(id, "cancelled").await;
    let replacement = server.create("replacement", true).await;
    server.cancel(replacement).await;
    server.shutdown().await;
}

#[tokio::test]
async fn rejects_mode_override_unknown_session_fields_and_oversized_stdin() {
    let server = TestServer::configured(|config| config.stdin_max_bytes = 1024).await;
    let id = server.create("demo", true).await;
    server.push(id, &[]).await;
    for body in [
        json!({"args":["-mode","gui"]}),
        json!({"args":[],"project":"other"}),
    ] {
        error_json(
            server
                .workflow(Method::POST, id, "/session")
                .json(&body)
                .send()
                .await
                .unwrap(),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    server.start(id, &[]).await;
    for body in [
        json!({"text":"x".repeat(1025)}),
        json!({"text":"puts hello\n","retry":true}),
    ] {
        error_json(
            server
                .workflow(Method::POST, id, "/session/stdin")
                .json(&body)
                .send()
                .await
                .unwrap(),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    server.cancel(id).await;
    server.shutdown().await;
}

#[tokio::test]
async fn forced_stop_bypasses_stdin_backpressure_and_kills_same_group_descendants() {
    let server = TestServer::new().await;
    let id = server.create("stubborn", true).await;
    server.push(id, &[]).await;
    server
        .start(id, &["--ignore-term", "--no-stdin", "--spawn-child"])
        .await;
    let pid_path = server.temp.path().join("stubborn/.fake-child-pid");
    let deadline = time::Instant::now() + Duration::from_secs(5);
    while !pid_path.exists() {
        assert!(time::Instant::now() < deadline);
        time::sleep(Duration::from_millis(20)).await;
    }
    let child_pid: i32 = fs::read_to_string(pid_path).unwrap().parse().unwrap();
    let input = server
        .workflow(Method::POST, id, "/session/stdin")
        .json(&json!({"text":"x".repeat(1024 * 1024)}));
    let pending = tokio::spawn(async move { input.send().await });
    time::sleep(Duration::from_millis(100)).await;
    time::timeout(Duration::from_secs(10), server.cancel(id))
        .await
        .expect("cancellation waited on stdin");
    pending.abort();
    // Reaped or zombie children cannot write; a live descendant must never outlast terminal status.
    if let Ok(stat) = fs::read_to_string(format!("/proc/{child_pid}/stat")) {
        let state = stat.rsplit_once(") ").unwrap().1.chars().next().unwrap();
        assert_eq!(state, 'Z', "descendant survived cancellation: {stat}");
    }
    let replacement = server.create("replacement", true).await;
    server.cancel(replacement).await;
    server.shutdown().await;
}

#[tokio::test]
async fn cancellation_continues_when_http_waiter_disconnects_and_shutdown_rejects_new_work() {
    let server = TestServer::new().await;
    let id = server.create("stubborn", true).await;
    server.push(id, &[]).await;
    server.start(id, &["--ignore-term", "--no-stdin"]).await;
    let request = server.workflow(Method::DELETE, id, "");
    let waiting = tokio::spawn(async move { request.send().await });
    let deadline = time::Instant::now() + Duration::from_secs(5);
    loop {
        let info = server.info(id).await;
        if info["status"] == "stopping" || info["status"] == "cancelled" {
            break;
        }
        assert!(
            time::Instant::now() < deadline,
            "cancellation was never accepted: {info}"
        );
        time::sleep(Duration::from_millis(20)).await;
    }
    waiting.abort();
    server.runtime.shutdown().await;
    let response = server
        .workflow(Method::PUT, Uuid::new_v4(), "")
        .json(&json!({"project":"new","reset_project":true}))
        .send()
        .await
        .unwrap();
    error_json(response, StatusCode::SERVICE_UNAVAILABLE).await;
    server.shutdown().await;
}

#[tokio::test]
async fn body_limit_and_public_readiness_have_precise_contracts() {
    let server = TestServer::configured(|config| {
        config.api_json_body_limit_bytes = 1024;
        config.stdin_max_bytes = 512;
    })
    .await;
    let oversized = server
        .workflow(Method::PUT, Uuid::new_v4(), "")
        .json(&json!({"project":"x".repeat(2048),"reset_project":true}))
        .send()
        .await
        .unwrap();
    error_json(oversized, StatusCode::PAYLOAD_TOO_LARGE).await;
    let ready: Value = server
        .client
        .get(format!("{}/readyz", server.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(ready, json!({"status":"ready"}));
    let openapi: Value = server
        .client
        .get(format!("{}/openapi.json", server.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(openapi["paths"]["/v1/workflows/{workflow_id}"]["put"].is_object());
    assert!(openapi["paths"]["/v1/sessions"].is_null());
    assert!(!openapi.to_string().contains("workspace_root"));
    server.shutdown().await;
}

#[tokio::test]
async fn incomplete_json_times_out_without_reserving_a_workflow() {
    let server = TestServer::configured(|config| {
        config.json_idle_timeout_secs = 1;
        config.json_deadline_secs = 3;
    })
    .await;
    let address = server.base.strip_prefix("http://").unwrap();
    let id = Uuid::new_v4();
    let mut connection = TcpStream::connect(address).await.unwrap();
    let request = format!(
        "PUT /v1/workflows/{id} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{{"
    );
    connection.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    time::timeout(
        Duration::from_secs(5),
        connection.read_to_end(&mut response),
    )
    .await
    .unwrap()
    .unwrap();
    let response = String::from_utf8(response).unwrap();
    assert!(response.starts_with("HTTP/1.1 408"), "{response}");
    assert!(response.contains("request_timeout"), "{response}");
    let replacement = server.create("demo", true).await;
    server.cancel(replacement).await;
    server.shutdown().await;
}

#[tokio::test]
async fn raw_pty_delivers_a_line_larger_than_the_terminal_canonical_limit() {
    let server = TestServer::new().await;
    let id = server.create("long-line", true).await;
    server.push(id, &[]).await;
    server.start(id, &[]).await;
    let line = "abcdefgh".repeat(2048);
    server.input(id, &format!("{line}\n")).await;
    server.input(id, "exit\n").await;
    server.wait_status(id, "pulling").await;
    let output = ok_json(
        server
            .workflow(Method::GET, id, "/session/output?cursor=0")
            .send()
            .await
            .unwrap(),
    )
    .await;
    let text: String = output["chunks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|chunk| chunk["text"].as_str().unwrap())
        .collect();
    assert!(
        text.contains(&format!("echo: {line}\n")),
        "large Tcl line was truncated"
    );
    assert!(text.contains("final output before exit"));
    server.finish(id).await;
    server.shutdown().await;
}
