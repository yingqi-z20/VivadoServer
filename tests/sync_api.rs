#![cfg(target_os = "linux")]
mod support;

use reqwest::{Method, StatusCode};
use serde_json::{Value, json};
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    time::Duration,
};
use support::{TOKEN, TestServer, dir_entry, error_json, file_entry, ok_json, sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time,
};
use uuid::Uuid;

#[tokio::test]
async fn required_entries_and_nested_unknown_fields_are_rejected() {
    let server = TestServer::new().await;
    let id = server.create("demo", true).await;
    for body in [
        json!({}),
        json!({"entires":[],"delete_extra":true}),
        json!({"entries":[{"path":"x","kind":"dir","typo":true}]}),
    ] {
        error_json(
            server
                .workflow(Method::POST, id, "/sync/push/plan")
                .json(&body)
                .send()
                .await
                .unwrap(),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    for body in [
        json!({"entries":[],"include_globs":["**"]}),
        json!({"entries":[],"exclude_globs":["cache"]}),
    ] {
        let response = server
            .workflow(Method::POST, id, "/sync/push/plan")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert!(
            response.status().is_client_error(),
            "reset accepted a filtered manifest"
        );
    }
    for path in ["../escape", "/absolute", "nested/../../escape", "bad:name"] {
        error_json(
            server
                .workflow(Method::POST, id, "/sync/push/plan")
                .json(&json!({"entries":[file_entry(path, b"x")]}))
                .send()
                .await
                .unwrap(),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    server.push(id, &[]).await;
    server.run_to_pull(id).await;
    error_json(
        server
            .workflow(Method::POST, id, "/sync/pull/plan")
            .json(&json!({"delete_extra":true}))
            .send()
            .await
            .unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;
    server.shutdown().await;
}

#[tokio::test]
async fn push_preserves_verified_upload_after_failed_retry_and_exposes_cleanup_state() {
    let server = TestServer::new().await;
    let id = server.create("demo", true).await;
    let expected = b"good";
    let plan = server
        .plan(id, vec![file_entry("rtl/generated/top.v", expected)])
        .await;
    assert_eq!(plan["create_dirs"], json!(["rtl", "rtl/generated"]));
    let sync = plan["sync_id"].as_str().unwrap();
    server
        .upload(id, sync, "rtl/generated/top.v", expected)
        .await;
    let retry = server
        .workflow(
            Method::PUT,
            id,
            &format!("/sync/{sync}/files/rtl/generated/top.v"),
        )
        .body(b"baad".to_vec())
        .send()
        .await
        .unwrap();
    error_json(retry, StatusCode::BAD_REQUEST).await;
    let result = server.commit(id, sync).await;
    assert_eq!(result["status"], "committed");
    assert_eq!(
        fs::read(server.temp.path().join("demo/rtl/generated/top.v")).unwrap(),
        expected
    );
    let status = ok_json(
        server
            .workflow(Method::GET, id, &format!("/sync/{sync}"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status["status"], "committed");
    assert!(status["cleanup_pending"].is_boolean());
    assert_eq!(status["result"]["status"], "committed");
    let force = server
        .workflow(Method::POST, id, &format!("/sync/{sync}/commit"))
        .json(&json!({"force":true}))
        .send()
        .await
        .unwrap();
    error_json(force, StatusCode::BAD_REQUEST).await;
    server.shutdown().await;
}

#[tokio::test]
async fn reset_uploads_every_file_and_removes_all_unlisted_old_files() {
    let server = TestServer::new().await;
    let first = server.create("demo", true).await;
    server
        .push(first, &[("same.txt", b"same"), ("cache/old.bin", b"old")])
        .await;
    server.run_to_pull(first).await;
    server.finish(first).await;
    let second = server.create("demo", true).await;
    let plan = server
        .plan(second, vec![file_entry("same.txt", b"same")])
        .await;
    assert_eq!(
        plan["upload_files"].as_array().unwrap().len(),
        1,
        "full upload reused an old digest"
    );
    let sync = plan["sync_id"].as_str().unwrap();
    let incomplete = server
        .workflow(Method::POST, second, &format!("/sync/{sync}/commit"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    error_json(incomplete, StatusCode::CONFLICT).await;
    server.upload(second, sync, "same.txt", b"same").await;
    server.commit(second, sync).await;
    assert_eq!(
        fs::read(server.temp.path().join("demo/same.txt")).unwrap(),
        b"same"
    );
    assert!(!server.temp.path().join("demo/cache").exists());
    server.run_to_pull(second).await;
    server.finish(second).await;
    server.shutdown().await;
}

#[tokio::test]
async fn incremental_delete_extra_is_explicit_and_both_type_replacements_are_required() {
    let server = TestServer::new().await;
    let id = server.create("demo", true).await;
    server
        .push(id, &[("node", b"old-file"), ("keep.txt", b"keep")])
        .await;
    let plan = server
        .plan(
            id,
            vec![dir_entry("node"), file_entry("node/child", b"child")],
        )
        .await;
    let sync = plan["sync_id"].as_str().unwrap();
    assert_eq!(plan["delete_files"], json!(["node"]));
    server.upload(id, sync, "node/child", b"child").await;
    server.commit(id, sync).await;
    assert_eq!(
        fs::read(server.temp.path().join("demo/node/child")).unwrap(),
        b"child"
    );
    assert!(server.temp.path().join("demo/keep.txt").exists());
    server.push(id, &[("node", b"replacement")]).await;
    assert_eq!(
        fs::read(server.temp.path().join("demo/node")).unwrap(),
        b"replacement"
    );
    assert!(server.temp.path().join("demo/keep.txt").exists());
    let delete = ok_json(
        server
            .workflow(Method::POST, id, "/sync/push/plan")
            .json(&json!({"entries":[file_entry("node", b"replacement")],"delete_extra":true}))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(delete["delete_files"], json!(["keep.txt"]));
    server.commit(id, delete["sync_id"].as_str().unwrap()).await;
    assert!(!server.temp.path().join("demo/keep.txt").exists());
    server.shutdown().await;
}

#[tokio::test]
async fn only_one_open_push_plan_and_abort_does_not_modify_project() {
    let server = TestServer::new().await;
    let id = server.create("demo", true).await;
    server.push(id, &[("old.txt", b"old")]).await;
    let plan = server.plan(id, vec![file_entry("new.txt", b"new")]).await;
    let sync = plan["sync_id"].as_str().unwrap();
    server.upload(id, sync, "new.txt", b"new").await;
    error_json(
        server
            .workflow(Method::POST, id, "/sync/push/plan")
            .json(&json!({"entries":[]}))
            .send()
            .await
            .unwrap(),
        StatusCode::CONFLICT,
    )
    .await;
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
    let aborted = server
        .workflow(Method::DELETE, id, &format!("/sync/{sync}"))
        .send()
        .await
        .unwrap();
    assert!(aborted.status().is_success());
    assert_eq!(
        fs::read(server.temp.path().join("demo/old.txt")).unwrap(),
        b"old"
    );
    assert!(!server.temp.path().join("demo/new.txt").exists());
    let status = ok_json(
        server
            .workflow(Method::GET, id, &format!("/sync/{sync}"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status["status"], "aborted");
    assert!(status["cleanup_pending"].is_boolean());
    server.push(id, &[("new.txt", b"new")]).await;
    server.shutdown().await;
}

#[tokio::test]
async fn parent_exclusion_is_consistent_between_manifest_and_push_plan() {
    let server = TestServer::new().await;
    let id = server.create("demo", true).await;
    server
        .push(
            id,
            &[("cache/nested/old.bin", b"old"), ("rtl/top.v", b"rtl")],
        )
        .await;
    let manifest = ok_json(
        server
            .workflow(Method::POST, id, "/sync/manifest")
            .json(&json!({"exclude_globs":["cache"]}))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert!(
        manifest["entries"]
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| !entry["path"].as_str().unwrap().starts_with("cache"))
    );
    let plan = ok_json(server.workflow(Method::POST, id, "/sync/push/plan")
        .json(&json!({"entries":[file_entry("cache/nested/new.bin", b"new"),file_entry("rtl/top.v", b"rtl")],
            "exclude_globs":["cache"],"delete_extra":true})).send().await.unwrap()).await;
    assert!(plan["upload_files"].as_array().unwrap().is_empty());
    assert!(plan["delete_files"].as_array().unwrap().is_empty());
    server.commit(id, plan["sync_id"].as_str().unwrap()).await;
    assert_eq!(
        fs::read(server.temp.path().join("demo/cache/nested/old.bin")).unwrap(),
        b"old"
    );
    assert!(
        !server
            .temp
            .path()
            .join("demo/cache/nested/new.bin")
            .exists()
    );
    server.shutdown().await;
}

#[tokio::test]
async fn oversized_or_short_uploads_cannot_satisfy_a_plan() {
    let server = TestServer::new().await;
    let id = server.create("demo", true).await;
    let plan = server.plan(id, vec![file_entry("file.bin", b"good")]).await;
    let sync = plan["sync_id"].as_str().unwrap();
    for (bytes, expected) in [
        (b"large".as_slice(), StatusCode::PAYLOAD_TOO_LARGE),
        (b"bad".as_slice(), StatusCode::BAD_REQUEST),
    ] {
        error_json(
            server
                .workflow(Method::PUT, id, &format!("/sync/{sync}/files/file.bin"))
                .body(bytes.to_vec())
                .send()
                .await
                .unwrap(),
            expected,
        )
        .await;
    }
    error_json(
        server
            .workflow(Method::POST, id, &format!("/sync/{sync}/commit"))
            .json(&json!({}))
            .send()
            .await
            .unwrap(),
        StatusCode::CONFLICT,
    )
    .await;
    assert!(!server.temp.path().join("demo/file.bin").exists());
    server.upload(id, sync, "file.bin", b"good").await;
    server.commit(id, sync).await;
    server.shutdown().await;
}

#[tokio::test]
async fn interrupted_upload_times_out_without_blocking_control_or_later_retries() {
    let server = TestServer::configured(|config| {
        config.upload_idle_timeout_secs = 1;
        config.upload_deadline_secs = 3;
        config.max_in_flight_requests = 1;
    })
    .await;
    let id = server.create("demo", true).await;
    let plan = server.plan(id, vec![file_entry("file.bin", b"good")]).await;
    let sync = plan["sync_id"].as_str().unwrap();
    let address = server.base.strip_prefix("http://").unwrap();
    let mut stream = TcpStream::connect(address).await.unwrap();
    let headers = format!(
        "PUT /v1/workflows/{id}/sync/{sync}/files/file.bin HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {TOKEN}\r\nContent-Length: 4\r\nConnection: close\r\n\r\ng"
    );
    stream.write_all(headers.as_bytes()).await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    let heartbeat = time::timeout(
        Duration::from_millis(750),
        server
            .workflow(Method::POST, id, "/heartbeat")
            .json(&json!({}))
            .send(),
    )
    .await
    .expect("upload blocked the independent control budget")
    .unwrap();
    assert!(heartbeat.status().is_success());
    let mut response = Vec::new();
    time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response = String::from_utf8(response).unwrap();
    assert!(response.starts_with("HTTP/1.1 408"), "{response}");
    drop(stream);
    server.upload(id, sync, "file.bin", b"good").await;
    server.commit(id, sync).await;
    assert_eq!(
        fs::read(server.temp.path().join("demo/file.bin")).unwrap(),
        b"good"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn pull_download_has_content_digest_etag_and_executable_metadata() {
    let server = TestServer::new().await;
    let id = server.create("demo", true).await;
    let bytes = b"#!/bin/sh\nexit 0\n";
    let mut entry = file_entry("run.sh", bytes);
    entry["executable"] = Value::Bool(true);
    entry["sha256"] = Value::String(sha256(bytes).to_uppercase());
    let plan = server.plan(id, vec![entry]).await;
    assert_eq!(plan["upload_files"][0]["sha256"], sha256(bytes));
    let sync = plan["sync_id"].as_str().unwrap();
    server.upload(id, sync, "run.sh", bytes).await;
    server.commit(id, sync).await;
    assert_ne!(
        fs::metadata(server.temp.path().join("demo/run.sh"))
            .unwrap()
            .permissions()
            .mode()
            & 0o111,
        0
    );
    server.run_to_pull(id).await;
    let pull = ok_json(
        server
            .workflow(Method::POST, id, "/sync/pull/plan")
            .json(&json!({"entries":[]}))
            .send()
            .await
            .unwrap(),
    )
    .await;
    let file = pull["download_files"]
        .as_array()
        .unwrap()
        .iter()
        .find(|file| file["path"] == "run.sh")
        .unwrap();
    assert_eq!(file["executable"], true);
    let etag = format!("\"sha256:{}\"", sha256(bytes));
    let response = server
        .workflow(Method::GET, id, "/sync/files/run.sh")
        .header(reqwest::header::IF_MATCH, &etag)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[reqwest::header::ETAG], etag);
    assert_eq!(response.headers()["x-sync-sha256"], sha256(bytes));
    assert_eq!(sha256(&response.bytes().await.unwrap()), sha256(bytes));
    server.finish(id).await;
    server.shutdown().await;
}

#[tokio::test]
async fn pull_requires_type_replacement_even_without_delete_extra() {
    let server = TestServer::new().await;
    let id = server.create("demo", true).await;
    server
        .push(
            id,
            &[
                ("file", b"server-file"),
                ("directory/child", b"server-child"),
            ],
        )
        .await;
    server.run_to_pull(id).await;
    let plan = ok_json(server.workflow(Method::POST, id, "/sync/pull/plan")
        .json(&json!({"entries":[dir_entry("file"),file_entry("file/old",b"local-child"),
            file_entry("directory",b"local-file"),file_entry("unrelated",b"keep")],"delete_extra":false}))
        .send().await.unwrap()).await;
    assert!(
        plan["delete_files"]
            .as_array()
            .unwrap()
            .contains(&json!("directory"))
    );
    assert!(
        plan["delete_files"]
            .as_array()
            .unwrap()
            .contains(&json!("file/old"))
    );
    assert!(
        plan["delete_dirs"]
            .as_array()
            .unwrap()
            .contains(&json!("file"))
    );
    assert!(
        !plan["delete_files"]
            .as_array()
            .unwrap()
            .contains(&json!("unrelated"))
    );
    server.shutdown().await;
}

#[tokio::test]
async fn stale_download_etags_and_symbolic_links_are_rejected() {
    let server = TestServer::new().await;
    let id = server.create("demo", true).await;
    server.push(id, &[("result.bin", b"first")]).await;
    server.run_to_pull(id).await;
    let etag = format!("\"sha256:{}\"", sha256(b"first"));
    fs::write(server.temp.path().join("demo/result.bin"), b"second").unwrap();
    error_json(
        server
            .workflow(Method::GET, id, "/sync/files/result.bin")
            .header(reqwest::header::IF_MATCH, etag)
            .send()
            .await
            .unwrap(),
        StatusCode::PRECONDITION_FAILED,
    )
    .await;
    let outside = server.temp.path().join("outside-secret");
    fs::write(&outside, b"secret").unwrap();
    symlink(&outside, server.temp.path().join("demo/link")).unwrap();
    error_json(
        server
            .workflow(Method::GET, id, "/sync/files/link")
            .send()
            .await
            .unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;
    server.shutdown().await;
}

#[tokio::test]
async fn active_download_holds_workflow_until_body_is_cancelled() {
    let server = TestServer::configured(|config| config.max_in_flight_requests = 1).await;
    let id = server.create("demo", true).await;
    server.push(id, &[]).await;
    server.start(id, &[]).await;
    server
        .input(id, "artifact large.bin 33554432\nexit\n")
        .await;
    server.wait_status(id, "pulling").await;
    let download = server
        .workflow(Method::GET, id, "/sync/files/large.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), StatusCode::OK);
    // A response object without body consumption leaves the server's bounded socket blocked.
    error_json(
        server
            .workflow(Method::POST, id, "/finish")
            .json(&json!({}))
            .send()
            .await
            .unwrap(),
        StatusCode::CONFLICT,
    )
    .await;
    let heartbeat = server
        .workflow(Method::POST, id, "/heartbeat")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert!(heartbeat.status().is_success());
    drop(download);
    let deadline = time::Instant::now() + Duration::from_secs(5);
    loop {
        let response = server
            .workflow(Method::POST, id, "/finish")
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        if response.status().is_success() {
            break;
        }
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(
            time::Instant::now() < deadline,
            "cancelled download kept its transfer permit"
        );
        time::sleep(Duration::from_millis(25)).await;
    }
    server.wait_status(id, "completed").await;
    let reused = server.create("demo", false).await;
    assert_eq!(server.info(reused).await["requires_full_upload"], false);
    server.shutdown().await;
}

#[tokio::test]
async fn another_workflow_cannot_access_an_old_sync_plan() {
    let server = TestServer::new().await;
    let id = server.create("demo", true).await;
    let plan = server.plan(id, vec![file_entry("file", b"data")]).await;
    let sync = plan["sync_id"].as_str().unwrap();
    error_json(
        server
            .workflow(Method::GET, Uuid::new_v4(), &format!("/sync/{sync}"))
            .send()
            .await
            .unwrap(),
        StatusCode::NOT_FOUND,
    )
    .await;
    server.cancel(id).await;
    let next = server.create("other", true).await;
    let response = server
        .workflow(Method::PUT, next, &format!("/sync/{sync}/files/file"))
        .body(b"data".to_vec())
        .send()
        .await
        .unwrap();
    assert!(matches!(
        response.status(),
        StatusCode::NOT_FOUND | StatusCode::CONFLICT
    ));
    assert!(!server.temp.path().join("other/file").exists());
    server.shutdown().await;
}
