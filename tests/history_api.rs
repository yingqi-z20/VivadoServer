mod support;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};
use support::{TestServer, error_json, ok_json};
use uuid::Uuid;

#[tokio::test]
async fn journal_captures_large_output_without_client_polling_and_preserves_native_contracts() {
    let server = TestServer::configured(|config| config.history.enabled = true).await;
    let id = server.create("history-native", true).await;
    server
        .push(id, &[("top.v", b"module top; endmodule")])
        .await;
    server.start(id, &[]).await;
    let parent = Uuid::new_v4();
    let response = server
        .workflow(Method::POST, id, "/session/stdin")
        .header("x-platform-request-id", parent.to_string())
        .json(&json!({"text":"burst 2097152\nexit\n"}))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let request_id = response.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .to_owned();
    server.wait_status(id, "pulling").await;
    server.finish(id).await;
    // Stop flushes durable history; no /session/output request was ever made.
    server.runtime.shutdown().await;
    let mut after = 0;
    let mut events = vec![];
    loop {
        let page = ok_json(
            server
                .request(
                    Method::GET,
                    &format!("/internal/history/v1/events?after={after}&limit=37"),
                )
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert!(page["gaps"].as_array().unwrap().is_empty(), "{page}");
        assert_eq!(page["recording_state"], "recording");
        let next = page["next_cursor"].as_u64().unwrap();
        events.extend(page["events"].as_array().unwrap().clone());
        if !page["has_more"].as_bool().unwrap() {
            break;
        }
        assert!(next > after);
        after = next;
    }
    let output: String = events
        .iter()
        .filter(|e| e["event"] == "session.output")
        .map(|e| e["data"]["text"].as_str().unwrap())
        .collect();
    assert_eq!(output.matches('x').count(), 2097152 + 1); // The final status line contains one additional x in "exit".
    assert!(output.contains("final output before exit"));
    for kind in [
        "workflow.created",
        "workflow.completed",
        "session.started",
        "session.process_started",
        "session.finished",
        "session.stdin_intent",
        "session.stdin_complete",
        "sync.operation",
        "sync.upload_staged",
    ] {
        assert!(events.iter().any(|e| e["event"] == kind), "missing {kind}");
    }
    let input = events
        .iter()
        .find(|e| e["event"] == "session.stdin_intent")
        .unwrap();
    assert_eq!(input["request_id"], request_id);
    let completed = events
        .iter()
        .find(|e| e["event"] == "session.stdin_complete")
        .unwrap();
    assert_eq!(completed["request_id"], request_id);
    assert_eq!(completed["data"]["parent_request_id"], parent.to_string());
    for event in events.iter().filter(|e| {
        matches!(
            e["event"].as_str(),
            Some("sync.operation" | "sync.upload_staged" | "request.completed")
        )
    }) {
        if event["data"]["operation"] == "sync_reaper" {
            continue;
        }
        assert_eq!(
            event["workflow_id"],
            id.to_string(),
            "missing workflow context: {event}"
        );
    }
    assert!(
        events
            .iter()
            .filter(|e| e["event"] == "session.output")
            .all(|e| e.get("request_id").is_none())
    );

    assert_eq!(input["data"]["parent_request_id"], parent.to_string());
    assert!(
        events
            .iter()
            .all(|e| Uuid::parse_str(e["instance_id"].as_str().unwrap()).is_ok())
    );
    // History content is separate from service diagnostics and never contains tokens.
    let disk = std::fs::read_to_string(
        server
            .temp
            .path()
            .join(".vivado-server/history-v1/events.jsonl"),
    )
    .unwrap();
    assert!(!disk.contains(support::TOKEN));
}

#[tokio::test]
async fn journal_requires_auth_and_valid_pagination() {
    let server = TestServer::configured(|config| config.history.enabled = true).await;
    error_json(
        server
            .client
            .get(format!("{}/internal/history/v1/events", server.base))
            .send()
            .await
            .unwrap(),
        StatusCode::UNAUTHORIZED,
    )
    .await;
    for query in ["limit=0", "limit=257", "after=-1", "bogus=1"] {
        error_json(
            server
                .request(Method::GET, &format!("/internal/history/v1/events?{query}"))
                .send()
                .await
                .unwrap(),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    let response = server
        .request(Method::GET, "/internal/history/v1/events")
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["cache-control"], "no-store");
    let _: Value = ok_json(response).await;
    server.shutdown().await;
    let disabled = TestServer::new().await;
    error_json(
        disabled
            .request(Method::GET, "/internal/history/v1/events")
            .send()
            .await
            .unwrap(),
        StatusCode::NOT_FOUND,
    )
    .await;
    disabled.shutdown().await;
}
