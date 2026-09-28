#![cfg(target_os = "linux")]
mod support;

use reqwest::{Method, StatusCode};
use std::{
    io::Write,
    sync::{Arc, Mutex},
    time::Duration,
};
use support::{TOKEN, TestServer, error_json};
use uuid::Uuid;

async fn scrape(server: &TestServer) -> String {
    let response = server
        .request(Method::GET, "/metrics")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[reqwest::header::CONTENT_TYPE],
        "text/plain; version=0.0.4; charset=utf-8"
    );
    assert_eq!(
        response.headers()[reqwest::header::CACHE_CONTROL],
        "no-store"
    );
    response.text().await.unwrap()
}

#[tokio::test]
async fn metrics_require_authentication_and_can_be_disabled() {
    let server = TestServer::new().await;
    let response = server
        .client
        .get(format!("{}/metrics", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.headers()[reqwest::header::WWW_AUTHENTICATE],
        "Bearer"
    );
    error_json(response, StatusCode::UNAUTHORIZED).await;
    let text = scrape(&server).await;
    assert!(text.contains("vivado_server_ready 1"));
    assert!(text.contains("vivado_server_build_info{version=\""));
    assert!(text.contains("vivado_server_log_dropped_messages 0"));
    server.shutdown().await;

    let server =
        TestServer::configured(|config| config.observability.metrics_enabled = false).await;
    error_json(
        server
            .request(Method::GET, "/metrics")
            .send()
            .await
            .unwrap(),
        StatusCode::NOT_FOUND,
    )
    .await;
    error_json(
        server
            .client
            .get(format!("{}/metrics", server.base))
            .send()
            .await
            .unwrap(),
        StatusCode::NOT_FOUND,
    )
    .await;
    server.shutdown().await;
}

#[tokio::test]
async fn metrics_have_bounded_labels_and_are_isolated_per_runtime() {
    let server = TestServer::new().await;
    let id = Uuid::new_v4();
    for suffix in [
        "",
        "/sync/files/private-project-secret.txt?private-query=secret",
    ] {
        error_json(
            server
                .workflow(Method::GET, id, suffix)
                .send()
                .await
                .unwrap(),
            StatusCode::NOT_FOUND,
        )
        .await;
    }
    error_json(
        server
            .request(Method::GET, "/random-private-path?private-query=secret")
            .send()
            .await
            .unwrap(),
        StatusCode::NOT_FOUND,
    )
    .await;
    // An arbitrary extension method must not become an unbounded label value.
    let custom = Method::from_bytes(b"PRIVATE-METHOD").unwrap();
    error_json(
        server.workflow(custom, id, "").send().await.unwrap(),
        StatusCode::METHOD_NOT_ALLOWED,
    )
    .await;
    let text = scrape(&server).await;
    assert!(text.contains("vivado_server_http_requests_total{method=\"GET\",route=\"/v1/workflows/{workflow_id}\",status=\"404\"} 1"), "{text}");
    assert!(text.contains("route=\"/v1/workflows/{workflow_id}/sync/files/{path}\""));
    assert!(text.contains("method=\"OTHER\""));
    assert!(text.contains("route=\"unmatched\""));
    assert!(text.contains("vivado_server_http_request_duration_seconds_count"));
    assert!(text.contains("vivado_server_http_response_duration_seconds_count"));
    assert!(text.contains("vivado_server_http_response_bytes_total"));
    for secret in [
        id.to_string().as_str(),
        "private-project",
        "private-query",
        "random-private",
        "PRIVATE-METHOD",
        TOKEN,
    ] {
        assert!(!text.contains(secret), "metric label leaked {secret}");
    }
    let second = TestServer::new().await;
    let isolated = scrape(&second).await;
    assert!(!isolated.contains("/v1/workflows/{workflow_id}"));
    second.shutdown().await;
    server.shutdown().await;
}

#[tokio::test]
async fn complete_file_downloads_head_and_empty_files_do_not_count_as_aborts() {
    let server = TestServer::new().await;
    let id = server.create("download-observability", true).await;
    let payload = vec![b'x'; 512 * 1024];
    server
        .push(id, &[("large.bin", &payload), ("empty.bin", b"")])
        .await;
    server.run_to_pull(id).await;
    let response = server
        .workflow(Method::GET, id, "/sync/files/large.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[reqwest::header::CONTENT_LENGTH],
        payload.len().to_string()
    );
    assert_eq!(response.bytes().await.unwrap().as_ref(), payload);
    let response = server
        .workflow(Method::HEAD, id, "/sync/files/large.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[reqwest::header::CONTENT_LENGTH],
        payload.len().to_string()
    );
    assert!(response.bytes().await.unwrap().is_empty());
    let response = server
        .workflow(Method::GET, id, "/sync/files/empty.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.bytes().await.unwrap().is_empty());
    let text = scrape(&server).await;
    let route = "/v1/workflows/{workflow_id}/sync/files/{path}";
    assert!(text.contains(&format!("vivado_server_http_responses_total{{method=\"GET\",outcome=\"complete\",route=\"{route}\",status=\"200\"}} 2")), "{text}");
    assert!(text.contains(&format!("vivado_server_http_responses_total{{method=\"HEAD\",outcome=\"complete\",route=\"{route}\",status=\"200\"}} 1")), "{text}");
    assert!(
        !text.lines().any(|line| line.contains(route)
            && (line.contains("outcome=\"aborted\"") || line.contains("outcome=\"body_error\""))),
        "{text}"
    );
    assert!(text.contains(&format!("vivado_server_http_response_bytes_total{{method=\"GET\",route=\"{route}\",status=\"200\"}} {}", payload.len())), "{text}");
    assert!(text.contains(&format!("vivado_server_http_response_bytes_total{{method=\"HEAD\",route=\"{route}\",status=\"200\"}} 0")), "{text}");
    assert!(
        !text.contains("kind=\"sync_download\",outcome=\"cancelled\""),
        "{text}"
    );
    assert!(
        text.contains(
            "vivado_server_operations_total{kind=\"sync_download_metadata\",outcome=\"success\"} 1"
        ),
        "{text}"
    );
    server.finish(id).await;
    server.shutdown().await;
}

struct CaptureWriter(Arc<Mutex<Vec<u8>>>);
impl Write for CaptureWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn info_logging_correlates_server_ids_and_excludes_request_secrets() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let writer = captured.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_env_filter("info")
        .with_writer(move || CaptureWriter(writer.clone()))
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
    let server = TestServer::new().await;
    let workflow_id = Uuid::new_v4();
    let response = server
        .workflow(Method::PUT, workflow_id, "?private-query=LOG_SECRET_QUERY")
        .header("x-request-id", "LOG_FORGED_REQUEST_ID")
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body("{LOG_SECRET_BODY")
        .send()
        .await
        .unwrap();
    let request_id = response.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_ne!(request_id, "LOG_FORGED_REQUEST_ID");
    assert!(Uuid::parse_str(&request_id).is_ok());
    let envelope = error_json(response, StatusCode::BAD_REQUEST).await;
    assert_eq!(envelope["error"]["request_id"], request_id);
    let success = server
        .request(Method::GET, "/openapi.json")
        .send()
        .await
        .unwrap();
    let success_id = success.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .to_owned();
    success.bytes().await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let text = loop {
        let text = String::from_utf8(captured.lock().unwrap().clone()).unwrap();
        if text.contains(&success_id) && text.contains(&request_id) {
            break text;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "completion logs missing: {text}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let records: Vec<serde_json::Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    for (id, level) in [(&request_id, "WARN"), (&success_id, "INFO")] {
        let record = records
            .iter()
            .find(|record| {
                record["fields"]["request_id"] == *id
                    && record["fields"]["event"] == "http_response_finished"
            })
            .unwrap();
        assert_eq!(record["level"], level);
        assert_eq!(record["span"]["request_id"], *id);
        assert_eq!(record["fields"]["outcome"], "complete");
        if id == &request_id {
            assert_eq!(record["span"]["workflow_id"], workflow_id.to_string());
        }
    }
    for secret in [
        "LOG_FORGED_REQUEST_ID",
        "LOG_SECRET_QUERY",
        "LOG_SECRET_BODY",
        TOKEN,
    ] {
        assert!(!text.contains(secret), "log leaked {secret}");
    }
    server.shutdown().await;
}
