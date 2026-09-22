//! Public workflow protocol; component schemas come directly from the wire DTOs.

use crate::{
    api::{HealthResponse, ReadyResponse},
    error::{ErrorBody, ErrorEnvelope},
    session::{
        OutputChunk, OutputQuery, OutputResponse, SendInputRequest, SessionInfo, SessionStatus,
        TerminationReason,
    },
    sync::{
        AbortSyncResponse, CommitSyncRequest, CommitSyncResponse, FileTransfer, ManifestEntry,
        ManifestEntryKind, ManifestRequest, ManifestResponse, PullPlanRequest, PullPlanResponse,
        PushPlanRequest, PushPlanResponse, SyncSessionStatus, SyncStatusResponse, UploadResponse,
    },
    workflow::{CreateWorkflowRequest, StartSessionRequest, WorkflowInfo, WorkflowStatus},
};
use serde_json::{Value, json};
use std::sync::OnceLock;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    info(title = "VivadoServer API", version = "1.0.0"),
    components(schemas(
        HealthResponse,
        ReadyResponse,
        ErrorBody,
        ErrorEnvelope,
        CreateWorkflowRequest,
        StartSessionRequest,
        WorkflowInfo,
        WorkflowStatus,
        SendInputRequest,
        OutputQuery,
        SessionStatus,
        TerminationReason,
        SessionInfo,
        OutputChunk,
        OutputResponse,
        ManifestRequest,
        ManifestEntry,
        ManifestEntryKind,
        ManifestResponse,
        PushPlanRequest,
        PushPlanResponse,
        PullPlanRequest,
        PullPlanResponse,
        FileTransfer,
        CommitSyncRequest,
        CommitSyncResponse,
        UploadResponse,
        AbortSyncResponse,
        SyncSessionStatus,
        SyncStatusResponse
    ))
)]
struct ApiSchema;

pub(crate) fn document() -> Value {
    static DOCUMENT: OnceLock<Value> = OnceLock::new();
    DOCUMENT.get_or_init(|| {
        let mut document = serde_json::to_value(ApiSchema::openapi()).expect("OpenAPI schemas serialize");
        document["info"]["description"] = json!("Linux service for trusted clients. Exactly one workflow owns a project through preparing (push), running (Vivado), and pulling (download), then releases it through finish or cancellation. Create a workflow with a client-generated UUID; retry the same UUID and parameters to recover a lost response. An incomplete project requires reset_project=true and a complete upload before Vivado can start. Bearer-token holders can execute commands as the service account through Tcl.");
        document["components"]["securitySchemes"] = json!({"bearerAuth": {"type": "http", "scheme": "bearer"}});
        document["paths"] = paths();
        document
    }).clone()
}

fn paths() -> Value {
    let mut paths = json!({
        "/healthz": {"get": public_operation("Process health", "HealthResponse")},
        "/readyz": {"get": public_operation("Readiness; 503 while stopping or degraded", "ReadyResponse")},
        "/openapi.json": {"get": {
            "summary": "Read the public OpenAPI document", "security": [],
            "responses": {
                "200": {"description": "OpenAPI document", "headers": request_id_headers(), "content": {"application/json": {"schema": {"type": "object"}}}},
                "405": error_response("Method not allowed", true)
            }
        }},
        "/v1/workflows/{workflow_id}": {
            "put": operation("Reserve the single workflow", "Create or retry the same workflow UUID and parameters. reset_project=true requires a complete push before starting Vivado; it is not an immediate standalone delete operation. A different active workflow returns 409 workflow_busy.", Some("CreateWorkflowRequest"), "WorkflowInfo"),
            "get": operation("Read workflow phase and cleanup status", "Observe preparing, running, pulling, stopping, or a terminal result. Terminal records are retained for a configured period.", None, "WorkflowInfo"),
            "delete": operation("Cancel the workflow and clean up its active work", "Stop Vivado and transfers, clean staging, and release the workflow only after cleanup. An interrupted project may require a complete upload on its next workflow.", None, "WorkflowInfo")
        },
        "/v1/workflows/{workflow_id}/heartbeat": {
            "post": operation("Refresh the workflow lease", "Send heartbeats throughout push, Vivado execution, and pull. Stopping or terminal workflows reject heartbeats.", None, "WorkflowInfo")
        },
        "/v1/workflows/{workflow_id}/finish": {
            "post": operation("Finish pulling and release the workflow", "Call after downloading and verifying all required files. Requires the pulling phase with no active transfers; retrying an already completed workflow is safe.", None, "WorkflowInfo")
        },
        "/v1/workflows/{workflow_id}/session": {
            "post": operation("Start Vivado and enter the running phase", "Requires preparing, no open push transaction, and a complete uploaded project when requires_full_upload is true. Vivado runs in Tcl mode.", Some("StartSessionRequest"), "SessionInfo"),
            "get": operation("Read the workflow's Vivado session", "Session lifetime belongs to the workflow. Process termination and output completion are reflected in the session status.", None, "SessionInfo"),
            "delete": operation("Stop Vivado before pulling results", "Request Tcl exit, then terminate remaining descendants as needed. Poll workflow status until the pulling phase before requesting files.", None, "SessionInfo")
        },
        "/v1/workflows/{workflow_id}/session/stdin": {
            "post": operation("Write Tcl input during the running phase", "Send exactly the text to write, including any required newline. The server enforces its configured UTF-8 byte limit.", Some("SendInputRequest"), "SessionInfo")
        },
        "/v1/workflows/{workflow_id}/session/output": {
            "get": operation("Long-poll the workflow's Vivado output", "Begin at cursor 0 and resume from each returned cursor. overrun reports discarded history; output_truncated reports incomplete capture. Shutdown cancels pending long polls.", None, "OutputResponse")
        },
        "/v1/workflows/{workflow_id}/sync/manifest": {
            "post": operation("Read a filtered manifest while preparing or pulling", "Paths are relative to the workflow project. Input fields are strict; misspelled filters are rejected.", Some("ManifestRequest"), "ManifestResponse")
        },
        "/v1/workflows/{workflow_id}/sync/push/plan": {
            "post": operation("Plan a push during the preparing phase", "entries is required, including for an intentionally empty manifest. Only one push transaction may be open. Reset recovery requires a complete replacement upload.", Some("PushPlanRequest"), "PushPlanResponse")
        },
        "/v1/workflows/{workflow_id}/sync/pull/plan": {
            "post": operation("Plan downloads during the pulling phase", "entries is the required current client manifest. Verify every downloaded file's size and SHA-256 before installing it locally.", Some("PullPlanRequest"), "PullPlanResponse")
        },
        "/v1/workflows/{workflow_id}/sync/{sync_id}/files/{path}": {
            "put": operation("Upload one planned file while preparing", "Send the exact planned bytes. Known Content-Length is checked before reading; streamed bodies have size, idle, and total deadline limits. Retrying identical bytes is safe.", None, "UploadResponse")
        },
        "/v1/workflows/{workflow_id}/sync/{sync_id}/commit": {
            "post": operation("Commit a verified push during preparation", "The JSON body is an empty object. Commit is owned by the workflow and continues if the HTTP caller disconnects; query sync status to recover its result. There is no force option.", Some("CommitSyncRequest"), "CommitSyncResponse")
        },
        "/v1/workflows/{workflow_id}/sync/{sync_id}": {
            "get": operation("Read this workflow's push transaction status", "Terminal results are retained temporarily. cleanup_pending means transaction cleanup still needs to finish.", None, "SyncStatusResponse"),
            "delete": operation("Abort an uncommitted push during preparation", "Remove the staged transaction. The enclosing workflow remains reserved.", None, "AbortSyncResponse")
        },
        "/v1/workflows/{workflow_id}/sync/files/{path}": {
            "get": operation("Download a file during the pulling phase", "Use the pull-plan SHA-256 as a quoted If-Match entity tag. The client must verify the complete body before installation; workflow finish requires all transfers to end.", None, "FileTransfer")
        }
    });
    paths["/readyz"]["get"]["responses"]["503"] = success_response(
        "ReadyResponse",
        "Stopping or degraded; the response remains a readiness status object",
    );
    paths["/v1/workflows/{workflow_id}/session/output"]["get"]["parameters"] = json!([
        {"name": "cursor", "in": "query", "required": false, "schema": {"type": "integer", "format": "uint64", "minimum": 0, "default": 0}},
        {"name": "timeout_ms", "in": "query", "required": false, "description": "Default 30000 milliseconds; values above 60000 are clamped to 60000.", "schema": {"type": "integer", "format": "uint64", "minimum": 0, "default": 30000}}
    ]);
    let upload = &mut paths["/v1/workflows/{workflow_id}/sync/{sync_id}/files/{path}"]["put"];
    upload["requestBody"] = json!({"required": true, "content": binary_content()});
    upload["parameters"] = json!([
        {"name": "Content-Length", "in": "header", "required": false, "description": "When supplied, must equal the exact planned file size.", "schema": {"type": "integer", "format": "uint64", "minimum": 0}}
    ]);
    let download = &mut paths["/v1/workflows/{workflow_id}/sync/files/{path}"]["get"];
    download["parameters"] = json!([
        {"name": "If-Match", "in": "header", "required": false, "description": "Strong entity tag from the pull plan; a list of tags or * is also accepted. A stale tag returns 412.", "schema": {"type": "string", "example": "\"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\""}}
    ]);
    let mut headers = request_id_headers();
    headers["Content-Length"] =
        json!({"schema": {"type": "integer", "format": "uint64", "minimum": 0}});
    headers["ETag"] = json!({"schema": {"type": "string"}, "description": "Quoted sha256:<hex digest> strong entity tag"});
    headers["x-sync-size-bytes"] =
        json!({"schema": {"type": "integer", "format": "uint64", "minimum": 0}});
    headers["x-sync-mtime-unix-ms"] = json!({"schema": {"type": "integer", "format": "int64"}});
    headers["x-sync-sha256"] = json!({"schema": {"type": "string", "pattern": "^[a-f0-9]{64}$"}});
    headers["x-sync-executable"] = json!({"schema": {"type": "boolean"}});
    download["responses"]["200"] =
        json!({"description": "File bytes", "headers": headers, "content": binary_content()});

    for (path, item) in paths.as_object_mut().expect("paths is an object") {
        let mut parameters = Vec::new();
        for name in ["workflow_id", "sync_id", "path"] {
            if path.contains(&format!("{{{name}}}")) {
                let mut schema = json!({"type": "string"});
                if name != "path" {
                    schema["format"] = json!("uuid");
                }
                let mut parameter =
                    json!({"name": name, "in": "path", "required": true, "schema": schema});
                if name == "path" {
                    parameter["description"] = json!(
                        "Nonempty slash-separated project-relative file path; encode individual path segments."
                    );
                }
                parameters.push(parameter);
            }
        }
        if !parameters.is_empty() {
            item["parameters"] = json!(parameters);
        }
        // Axum GET routes also accept HEAD. Document the same metadata without a body.
        if let Some(get) = item.get("get") {
            let mut head = get.clone();
            head["summary"] = json!(format!(
                "HEAD: {}",
                get["summary"].as_str().unwrap_or("resource")
            ));
            for response in head["responses"]
                .as_object_mut()
                .expect("responses is an object")
                .values_mut()
            {
                response
                    .as_object_mut()
                    .expect("response is an object")
                    .remove("content");
            }
            item["head"] = head;
        }
    }
    paths
}

fn operation(summary: &str, description: &str, request: Option<&str>, response: &str) -> Value {
    let mut operation = json!({"summary": summary, "description": description, "security": [{"bearerAuth": []}], "responses": response_set(response)});
    if let Some(request) = request {
        operation["requestBody"] = json!({"required": true, "content": {"application/json": {"schema": schema_ref(request)}}});
    }
    operation
}

fn public_operation(summary: &str, response: &str) -> Value {
    json!({"summary": summary, "security": [], "responses": {
        "200": success_response(response, "Success"), "405": error_response("Method not allowed", true)
    }})
}

fn response_set(schema: &str) -> Value {
    let mut responses = json!({"200": success_response(schema, "Success")});
    for (status, description) in [
        (
            "400",
            "Invalid request, unknown field, or invalid path/query",
        ),
        ("401", "Bearer authentication required"),
        ("404", "Workflow or resource not found"),
        ("405", "Method not allowed"),
        ("408", "Request body timeout or long-poll cancellation"),
        (
            "409",
            "Workflow busy, wrong phase, incomplete project, or sync conflict",
        ),
        ("412", "If-Match does not match the current file"),
        ("413", "Body or file exceeds its limit"),
        ("429", "HTTP request capacity exhausted"),
        ("500", "Internal failure"),
        ("503", "Service is shutting down"),
    ] {
        responses[status] = error_response(description, status == "405");
    }
    responses["401"]["headers"]["WWW-Authenticate"] =
        json!({"schema": {"type": "string", "enum": ["Bearer"]}});
    responses
}

fn success_response(schema: &str, description: &str) -> Value {
    json!({"description": description, "headers": request_id_headers(), "content": {"application/json": {"schema": schema_ref(schema)}}})
}

fn error_response(description: &str, method_not_allowed: bool) -> Value {
    let mut headers = request_id_headers();
    if method_not_allowed {
        headers["Allow"] = json!({"schema": {"type": "string"}, "description": "Supported methods for this resource"});
    }
    json!({"description": description, "headers": headers, "content": {"application/json": {"schema": schema_ref("ErrorEnvelope")}}})
}

fn request_id_headers() -> Value {
    json!({"x-request-id": {"description": "Server-generated request correlation UUID", "schema": {"type": "string", "format": "uuid"}}})
}
fn binary_content() -> Value {
    json!({"application/octet-stream": {"schema": {"type": "string", "format": "binary"}}})
}
fn schema_ref(name: &str) -> Value {
    json!({"$ref": format!("#/components/schemas/{name}")})
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    const METHODS: [&str; 8] = [
        "get", "head", "put", "post", "delete", "patch", "options", "trace",
    ];

    fn operations(document: &Value) -> BTreeSet<(String, String)> {
        document["paths"]
            .as_object()
            .unwrap()
            .iter()
            .flat_map(|(path, item)| {
                METHODS
                    .into_iter()
                    .filter(|method| item.get(*method).is_some())
                    .map(|method| (path.clone(), method.to_string()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn document_covers_the_router_methods_without_legacy_routes() {
        // Read the route declarations, not a second manually maintained expected list.
        // This assertion makes a new router endpoint fail until its contract is documented.
        let mut expected = BTreeSet::new();
        for declaration in include_str!("api.rs").split(".route(").skip(1) {
            let declaration = declaration.trim_start().strip_prefix('"').unwrap();
            let (path, rest) = declaration.split_once('"').unwrap();
            let path = if ["/healthz", "/readyz", "/openapi.json"].contains(&path) {
                path.to_string()
            } else {
                format!("/v1{path}")
            }
            .replace("{*path}", "{path}");
            let mut depth = 1;
            let end = rest
                .char_indices()
                .find_map(|(index, ch)| {
                    if ch == '(' {
                        depth += 1;
                    }
                    if ch == ')' {
                        depth -= 1;
                    }
                    (depth == 0).then_some(index)
                })
                .unwrap();
            let expression = &rest[..end];
            for method in METHODS {
                if expression.contains(&format!("{method}(")) {
                    expected.insert((path.clone(), method.to_string()));
                    if method == "get" {
                        expected.insert((path.clone(), "head".to_string()));
                    }
                }
            }
        }
        let document = document();
        assert_eq!(operations(&document), expected);
        assert!(!document.to_string().contains("/v1/sessions"));
        assert!(!document.to_string().contains("/v1/projects"));
        assert!(
            document["components"]["schemas"]
                .get("RuntimeConfig")
                .is_none()
        );
        assert!(
            document["components"]["schemas"]
                .get("CreateSessionRequest")
                .is_none()
        );
    }

    #[test]
    fn authorization_status_and_response_headers_match_the_http_contract() {
        let document = document();
        for (path, method) in operations(&document) {
            let operation = &document["paths"][&path][&method];
            if path.starts_with("/v1/") {
                assert_eq!(operation["security"], json!([{"bearerAuth": []}]));
                for status in [
                    "400", "401", "404", "405", "408", "409", "412", "413", "429", "500", "503",
                ] {
                    let response = &operation["responses"][status];
                    assert!(response.is_object(), "missing {status}: {method} {path}");
                    if method != "head" {
                        assert_eq!(
                            response["content"]["application/json"]["schema"],
                            schema_ref("ErrorEnvelope")
                        );
                    }
                }
                assert_eq!(
                    operation["responses"]["401"]["headers"]["WWW-Authenticate"]["schema"]["enum"],
                    json!(["Bearer"])
                );
            } else {
                assert_eq!(operation["security"], json!([]));
                assert!(operation["responses"].get("401").is_none());
            }
            assert!(operation["responses"]["405"]["headers"]["Allow"].is_object());
            for response in operation["responses"].as_object().unwrap().values() {
                assert_eq!(
                    response["headers"]["x-request-id"]["schema"]["format"],
                    "uuid"
                );
                if method == "head" {
                    assert!(response.get("content").is_none());
                }
            }
        }
        assert_eq!(
            document["paths"]["/readyz"]["get"]["responses"]["503"]["content"]["application/json"]
                ["schema"],
            schema_ref("ReadyResponse")
        );
    }

    #[test]
    fn rust_request_schemas_keep_strict_fields_and_required_manifests() {
        let document = document();
        let schemas = &document["components"]["schemas"];
        for name in [
            "CreateWorkflowRequest",
            "StartSessionRequest",
            "SendInputRequest",
            "OutputQuery",
            "ManifestRequest",
            "ManifestEntry",
            "PushPlanRequest",
            "PullPlanRequest",
            "CommitSyncRequest",
        ] {
            assert_eq!(
                schemas[name]["additionalProperties"], false,
                "{name} must reject unknown fields"
            );
        }
        for name in ["PushPlanRequest", "PullPlanRequest"] {
            assert!(
                schemas[name]["required"]
                    .as_array()
                    .unwrap()
                    .contains(&json!("entries"))
            );
        }
        assert!(
            schemas["CreateWorkflowRequest"]["required"]
                .as_array()
                .unwrap()
                .contains(&json!("project"))
        );
        assert!(
            schemas["CommitSyncRequest"]["properties"]
                .as_object()
                .is_none_or(|properties| properties.is_empty())
        );
        assert!(
            schemas["CommitSyncRequest"]
                .to_string()
                .find("force")
                .is_none()
        );
        assert_eq!(
            schemas["WorkflowStatus"]["enum"],
            json!([
                "preparing",
                "running",
                "pulling",
                "stopping",
                "completed",
                "cancelled",
                "failed"
            ])
        );
    }

    #[test]
    fn request_response_references_and_parameters_are_complete() {
        let document = document();
        fn check_refs(value: &Value, document: &Value) {
            match value {
                Value::Object(object) => {
                    if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
                        assert!(reference.starts_with("#/components/schemas/"));
                        assert!(
                            document.pointer(&reference[1..]).is_some(),
                            "unresolved {reference}"
                        );
                    }
                    for child in object.values() {
                        check_refs(child, document);
                    }
                }
                Value::Array(array) => {
                    for child in array {
                        check_refs(child, document);
                    }
                }
                _ => {}
            }
        }
        check_refs(&document, &document);
        for (path, item) in document["paths"].as_object().unwrap() {
            for name in ["workflow_id", "sync_id", "path"] {
                if path.contains(&format!("{{{name}}}")) {
                    let parameter = item["parameters"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|parameter| parameter["name"] == name)
                        .unwrap();
                    assert_eq!(parameter["in"], "path");
                    assert_eq!(parameter["required"], true);
                    if name != "path" {
                        assert_eq!(parameter["schema"]["format"], "uuid");
                    }
                }
            }
        }
        for (path, method, request, response) in [
            (
                "/v1/workflows/{workflow_id}",
                "put",
                "CreateWorkflowRequest",
                "WorkflowInfo",
            ),
            (
                "/v1/workflows/{workflow_id}/session",
                "post",
                "StartSessionRequest",
                "SessionInfo",
            ),
            (
                "/v1/workflows/{workflow_id}/session/stdin",
                "post",
                "SendInputRequest",
                "SessionInfo",
            ),
            (
                "/v1/workflows/{workflow_id}/sync/manifest",
                "post",
                "ManifestRequest",
                "ManifestResponse",
            ),
            (
                "/v1/workflows/{workflow_id}/sync/push/plan",
                "post",
                "PushPlanRequest",
                "PushPlanResponse",
            ),
            (
                "/v1/workflows/{workflow_id}/sync/pull/plan",
                "post",
                "PullPlanRequest",
                "PullPlanResponse",
            ),
            (
                "/v1/workflows/{workflow_id}/sync/{sync_id}/commit",
                "post",
                "CommitSyncRequest",
                "CommitSyncResponse",
            ),
        ] {
            let operation = &document["paths"][path][method];
            assert_eq!(operation["requestBody"]["required"], true);
            assert_eq!(
                operation["requestBody"]["content"]["application/json"]["schema"],
                schema_ref(request)
            );
            assert_eq!(
                operation["responses"]["200"]["content"]["application/json"]["schema"],
                schema_ref(response)
            );
        }
        let query =
            document["paths"]["/v1/workflows/{workflow_id}/session/output"]["get"]["parameters"]
                .as_array()
                .unwrap();
        for name in ["cursor", "timeout_ms"] {
            let parameter = query
                .iter()
                .find(|parameter| parameter["name"] == name)
                .unwrap();
            assert_eq!(parameter["in"], "query");
            assert_eq!(parameter["required"], false);
            assert_eq!(parameter["schema"]["minimum"], 0);
        }
    }

    #[test]
    fn file_transfer_contract_preserves_binary_bodies_and_version_headers() {
        let document = document();
        let upload =
            &document["paths"]["/v1/workflows/{workflow_id}/sync/{sync_id}/files/{path}"]["put"];
        assert_eq!(upload["requestBody"]["content"], binary_content());
        assert_eq!(
            upload["responses"]["200"]["content"]["application/json"]["schema"],
            schema_ref("UploadResponse")
        );
        let download = &document["paths"]["/v1/workflows/{workflow_id}/sync/files/{path}"]["get"];
        assert!(
            download["parameters"]
                .as_array()
                .unwrap()
                .iter()
                .any(|parameter| parameter["name"] == "If-Match" && parameter["in"] == "header")
        );
        assert_eq!(download["responses"]["200"]["content"], binary_content());
        for name in [
            "ETag",
            "Content-Length",
            "x-sync-size-bytes",
            "x-sync-mtime-unix-ms",
            "x-sync-sha256",
            "x-sync-executable",
        ] {
            assert!(download["responses"]["200"]["headers"][name].is_object());
        }
    }
}
