//! The examples are part of the client contract, not merely valid JSON snippets.

use chrono::{DateTime, Utc};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;
use vivado_server::{
    session::{OutputChunk, OutputResponse, SendInputRequest, SessionStatus},
    sync::{
        CommitSyncRequest, CommitSyncResponse, ManifestRequest, PullPlanRequest, PushPlanRequest,
        SyncSessionStatus, SyncStatusResponse,
    },
    workflow::{CreateWorkflowRequest, StartSessionRequest, WorkflowInfo, WorkflowStatus},
};

const ENGLISH: &str = include_str!("../docs/client-development.md");
const CHINESE: &str = include_str!("../docs/client-development.zh-CN.md");
const EXAMPLES: &[&str] = &[
    "CreateWorkflowRequest",
    "WorkflowInfo",
    "ManifestRequest",
    "PushPlanRequest",
    "CommitSyncRequest",
    "SyncStatusResponse",
    "StartSessionRequest",
    "SendInputRequest",
    "OutputResponse",
    "PullPlanRequest",
];

fn contract_block<'a>(document: &'a str, name: &str) -> &'a str {
    let marker = format!("<!-- contract:{name} -->");
    assert_eq!(
        document.matches(&marker).count(),
        1,
        "expected one {marker}"
    );
    let after_marker = document.split_once(&marker).unwrap().1.trim_start();
    // Requiring the fence immediately after the marker prevents a removed
    // example from silently selecting another request's later JSON block.
    let after_fence = after_marker
        .strip_prefix("```json")
        .unwrap_or_else(|| panic!("missing JSON fence immediately after {marker}"));
    assert!(
        after_fence.starts_with(['\r', '\n']),
        "invalid fence for {marker}"
    );
    after_fence
        .split_once("```")
        .unwrap_or_else(|| panic!("unterminated JSON fence after {marker}"))
        .0
        .trim()
}

fn example(document: &str, name: &str) -> Value {
    serde_json::from_str(contract_block(document, name)).unwrap()
}

fn assert_request<T: DeserializeOwned>(document: &str, name: &str) {
    let mut value = example(document, name);
    serde_json::from_value::<T>(value.clone())
        .unwrap_or_else(|error| panic!("{name} example does not match the request DTO: {error}"));
    value
        .as_object_mut()
        .unwrap()
        .insert("unexpected_field".into(), Value::Bool(true));
    assert!(
        serde_json::from_value::<T>(value).is_err(),
        "{name} must reject unknown fields as documented"
    );
}

fn assert_response(document: &str, name: &str, response: impl Serialize) {
    assert_eq!(
        example(document, name),
        serde_json::to_value(response).unwrap(),
        "{name}"
    );
}

#[test]
fn both_guides_use_the_same_strict_request_examples() {
    for document in [ENGLISH, CHINESE] {
        for (index, tail) in document.split("```json").skip(1).enumerate() {
            let body = tail
                .split_once("```")
                .expect("unterminated JSON fence")
                .0
                .trim();
            serde_json::from_str::<Value>(body)
                .unwrap_or_else(|error| panic!("invalid JSON fence {index}: {error}"));
        }
        assert_request::<CreateWorkflowRequest>(document, "CreateWorkflowRequest");
        assert_request::<StartSessionRequest>(document, "StartSessionRequest");
        assert_request::<SendInputRequest>(document, "SendInputRequest");
        assert_request::<ManifestRequest>(document, "ManifestRequest");
        assert_request::<PushPlanRequest>(document, "PushPlanRequest");
        assert_request::<CommitSyncRequest>(document, "CommitSyncRequest");
        assert_request::<PullPlanRequest>(document, "PullPlanRequest");
    }
    for name in EXAMPLES {
        assert_eq!(
            example(ENGLISH, name),
            example(CHINESE, name),
            "translations differ for {name}"
        );
    }
}

#[test]
fn documented_destructive_boundaries_are_explicit() {
    for document in [ENGLISH, CHINESE] {
        let mut push = example(document, "PushPlanRequest");
        let entry = &push["entries"][0];
        assert_eq!(entry["size_bytes"], 6);
        assert_eq!(entry["sha256"], format!("{:x}", Sha256::digest(b"hello\n")));
        push.as_object_mut().unwrap().remove("entries");
        assert!(serde_json::from_value::<PushPlanRequest>(push).is_err());

        let mut pull = example(document, "PullPlanRequest");
        pull.as_object_mut().unwrap().remove("entries");
        assert!(serde_json::from_value::<PullPlanRequest>(pull).is_err());

        let mut commit = example(document, "CommitSyncRequest");
        commit["force"] = Value::Bool(true);
        assert!(serde_json::from_value::<CommitSyncRequest>(commit).is_err());

        let mut nested = example(document, "PushPlanRequest");
        nested["entries"][0]["unexpected_field"] = Value::Bool(true);
        assert!(serde_json::from_value::<PushPlanRequest>(nested).is_err());
    }
}

#[test]
fn response_examples_match_serialized_public_types() {
    let timestamp: DateTime<Utc> = "2026-09-12T00:00:00Z".parse().unwrap();
    let workflow_id = Uuid::parse_str("8e47a0ac-c6f5-4af0-a491-f9e7ce98efaf").unwrap();
    let sync_id = Uuid::parse_str("c0af7449-eaac-4c61-912f-0d9b4c79d393").unwrap();
    for document in [ENGLISH, CHINESE] {
        assert_response(
            document,
            "WorkflowInfo",
            WorkflowInfo {
                workflow_id,
                project: "demo".into(),
                status: WorkflowStatus::Preparing,
                session_id: None,
                requires_full_upload: true,
                cleanup_pending: false,
                started_at: timestamp,
                last_heartbeat_at: timestamp,
                ended_at: None,
                error_code: None,
                error_message: None,
            },
        );
        assert_response(
            document,
            "OutputResponse",
            OutputResponse {
                cursor: 1,
                chunks: vec![OutputChunk {
                    seq: 0,
                    timestamp,
                    text: "Vivado% ".into(),
                }],
                status: SessionStatus::Running,
                overrun: false,
                output_truncated: false,
            },
        );
        assert_response(
            document,
            "SyncStatusResponse",
            SyncStatusResponse {
                sync_id,
                project: "demo".into(),
                status: SyncSessionStatus::Committed,
                cleanup_pending: false,
                result: Some(CommitSyncResponse {
                    sync_id,
                    status: "committed",
                    uploaded_files: vec!["input.txt".into()],
                    created_dirs: vec![],
                    deleted_files: vec![],
                    deleted_dirs: vec![],
                }),
                error_code: None,
                error_message: None,
            },
        );
    }
}
