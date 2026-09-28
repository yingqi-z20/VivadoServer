use crate::{
    AppServices,
    auth::require_auth,
    error::AppError,
    session::{OutputQuery, SendInputRequest},
    sync::{CommitSyncRequest, ManifestRequest, PullPlanRequest, PushPlanRequest},
    workflow::{CreateWorkflowRequest, StartSessionRequest},
    workspace_read::{self, WorkspaceListing, WorkspacePreview, WorkspaceQuery},
};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{FromRequest, MatchedPath, OriginalUri, Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tracing::Instrument;
use utoipa::ToSchema;
use uuid::Uuid;

const REQUEST_ID_HEADER: &str = "x-request-id";

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct HealthResponse {
    pub status: &'static str,
}
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ReadyResponse {
    pub status: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub reasons: Vec<&'static str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub failed_tasks: Vec<&'static str>,
}
#[derive(Clone)]
struct ReadinessResponse;

#[derive(Clone, Copy)]
struct JsonLimits {
    bytes: usize,
    idle: Duration,
    deadline: Duration,
}
#[derive(Clone)]
struct HttpBudget {
    data: Arc<Semaphore>,
    control: Arc<Semaphore>,
    json: JsonLimits,
}

struct ApiJson<T>(T);
impl<T, S> FromRequest<S> for ApiJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = AppError;
    async fn from_request(request: Request, state: &S) -> Result<Self, AppError> {
        let limits = *request
            .extensions()
            .get::<JsonLimits>()
            .ok_or_else(|| AppError::Internal("JSON limits missing".into()))?;
        if content_length(request.headers())?.is_some_and(|length| length > limits.bytes as u64) {
            return Err(AppError::PayloadTooLarge);
        }
        let (parts, mut body) = request.into_parts();
        let collect = async {
            let mut bytes = Vec::new();
            while let Some(frame) = tokio::time::timeout(limits.idle, body.frame())
                .await
                .map_err(|_| AppError::RequestTimeout)?
            {
                let frame =
                    frame.map_err(|_| AppError::BadRequest("incomplete request body".into()))?;
                if let Some(data) = frame.data_ref() {
                    if data.len() > limits.bytes.saturating_sub(bytes.len()) {
                        return Err(AppError::PayloadTooLarge);
                    }
                    bytes.extend_from_slice(data);
                }
            }
            Ok::<_, AppError>(Bytes::from(bytes))
        };
        let bytes = tokio::time::timeout(limits.deadline, collect)
            .await
            .map_err(|_| AppError::RequestTimeout)??;
        let request = Request::from_parts(parts, Body::from(bytes));
        Json::<T>::from_request(request, state)
            .await
            .map(|Json(value)| Self(value))
            .map_err(|rejection| {
                if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
                    AppError::PayloadTooLarge
                } else {
                    AppError::BadRequest(rejection.body_text())
                }
            })
    }
}

pub(crate) fn build_router(services: AppServices) -> Router {
    let config = &services.config;
    let budget = HttpBudget {
        data: Arc::new(Semaphore::new(config.max_in_flight_requests)),
        control: Arc::new(Semaphore::new(8)),
        json: JsonLimits {
            bytes: config.api_json_body_limit_bytes,
            idle: config.json_idle_timeout(),
            deadline: config.json_deadline(),
        },
    };
    let protected = Router::new()
        .route("/projects/{project}/workspace", get(workspace_list))
        .route("/projects/{project}/workspace-file", get(workspace_file))
        .route(
            "/projects/{project}/workspace-preview",
            get(workspace_preview),
        )
        .route(
            "/projects/{project}/workspace-archive",
            get(workspace_archive),
        )
        .route(
            "/workflows/{workflow_id}",
            put(create_workflow)
                .get(get_workflow)
                .delete(cancel_workflow),
        )
        .route("/workflows/{workflow_id}/heartbeat", post(heartbeat))
        .route("/workflows/{workflow_id}/finish", post(finish_workflow))
        .route(
            "/workflows/{workflow_id}/session",
            post(create_session).get(get_session).delete(delete_session),
        )
        .route("/workflows/{workflow_id}/session/stdin", post(send_stdin))
        .route("/workflows/{workflow_id}/session/output", get(read_output))
        .route(
            "/workflows/{workflow_id}/sync/manifest",
            post(sync_manifest),
        )
        .route(
            "/workflows/{workflow_id}/sync/push/plan",
            post(sync_push_plan),
        )
        .route(
            "/workflows/{workflow_id}/sync/pull/plan",
            post(sync_pull_plan),
        )
        .route(
            "/workflows/{workflow_id}/sync/{sync_id}/files/{*path}",
            put(sync_upload_file),
        )
        .route(
            "/workflows/{workflow_id}/sync/{sync_id}/commit",
            post(sync_commit),
        )
        .route(
            "/workflows/{workflow_id}/sync/{sync_id}",
            get(sync_status).delete(sync_abort),
        )
        .route(
            "/workflows/{workflow_id}/sync/files/{*path}",
            get(sync_download_file),
        )
        .fallback(api_not_found)
        .layer(axum::extract::DefaultBodyLimit::disable())
        .layer(middleware::from_fn_with_state(budget.clone(), admission))
        .layer(middleware::from_fn_with_state(
            services.config.history.clone(),
            journal_request,
        ))
        .layer(middleware::from_fn_with_state(
            services.auth.clone(),
            require_auth,
        ));
    let metrics =
        Router::new()
            .route("/metrics", get(metrics))
            .layer(middleware::from_fn_with_state(
                services.auth.clone(),
                require_auth,
            ));
    let history = Router::new()
        .route("/internal/history/v1/events", get(history_events))
        .layer(middleware::from_fn_with_state(budget, admission))
        .layer(middleware::from_fn_with_state(
            services.auth.clone(),
            require_auth,
        ));
    let telemetry = services.config.telemetry.clone();
    let mut router = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/openapi.json", get(openapi_json))
        .nest("/v1", protected)
        .merge(history)
        .fallback(api_not_found);
    if services.config.observability.metrics_enabled {
        router = router.merge(metrics);
    }
    router
        .with_state(services)
        .layer(middleware::from_fn_with_state(telemetry, request_context))
}

async fn admission(State(budget): State<HttpBudget>, mut request: Request, next: Next) -> Response {
    let path = request
        .extensions()
        .get::<OriginalUri>()
        .map(|uri| uri.0.path())
        .unwrap_or(request.uri().path());
    let method = request.method();
    let control = method == Method::DELETE
        || (method == Method::GET
            && !path.ends_with("/output")
            && !path.contains("/sync/files/")
            && !path.contains("/workspace"))
        || (method == Method::POST && (path.ends_with("/heartbeat") || path.ends_with("/finish")));
    let semaphore = if control { budget.control } else { budget.data };
    let permit = match semaphore.try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return AppError::Capacity("HTTP request budget exhausted".into()).into_response();
        }
    };
    request.extensions_mut().insert(budget.json);
    let response = next.run(request).await;
    let cancel = response
        .extensions()
        .get::<tokio_util::sync::CancellationToken>()
        .cloned();
    let (parts, body) = response.into_parts();
    Response::from_parts(parts, crate::body::hold(body, permit, cancel))
}

// Record authenticated operation metadata even when JSON/path validation rejects
// the request. Bodies, query strings and arbitrary paths are never service logs.
async fn journal_request(
    State(history): State<crate::history::History>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().as_str().to_owned();
    let route = metric_route(
        request
            .extensions()
            .get::<MatchedPath>()
            .map(MatchedPath::as_str),
    );
    let record = method != "GET" && method != "HEAD"
        || route.contains("/files/")
        || route.ends_with("workspace-file")
        || route.ends_with("workspace-archive");
    let response = next.run(request).await;
    if record {
        history.record(crate::history::Event::new(
            "request.completed",
            None,
            None,
            None,
            serde_json::json!({"method":method,"route":route,"status":response.status().as_u16(),"outcome":"headers_sent"}),
        ));
    }
    response
}

async fn request_context(
    State(telemetry): State<crate::observability::Observability>,
    request: Request,
    next: Next,
) -> Response {
    let request_id = Uuid::new_v4().to_string();
    let method = match *request.method() {
        Method::GET => "GET",
        Method::HEAD => "HEAD",
        Method::POST => "POST",
        Method::PUT => "PUT",
        Method::DELETE => "DELETE",
        Method::PATCH => "PATCH",
        Method::OPTIONS => "OPTIONS",
        Method::TRACE => "TRACE",
        Method::CONNECT => "CONNECT",
        _ => "OTHER",
    };
    let route = metric_route(
        request
            .extensions()
            .get::<MatchedPath>()
            .map(MatchedPath::as_str),
    );
    let span = tracing::info_span!("request", request_id = %request_id, method, route,
        workflow_id = tracing::field::Empty, sync_id = tracing::field::Empty);
    if route.starts_with("/v1/workflows/{workflow_id}") {
        let path = request
            .extensions()
            .get::<OriginalUri>()
            .map(|uri| uri.0.path())
            .unwrap_or(request.uri().path());
        if let Some(id) = path
            .split('/')
            .nth(3)
            .and_then(|value| Uuid::parse_str(value).ok())
        {
            span.record("workflow_id", tracing::field::display(id));
        }
        if route.contains("{sync_id}")
            && let Some(id) = path
                .split('/')
                .nth(5)
                .and_then(|value| Uuid::parse_str(value).ok())
        {
            span.record("sync_id", tracing::field::display(id));
        }
    }
    let mut observation =
        telemetry.request_started(method, route, request_id.clone(), span.clone());
    let context = crate::history::Context {
        request_id: Uuid::parse_str(&request_id).ok(),
        sync_id: request
            .extensions()
            .get::<OriginalUri>()
            .map(|uri| uri.0.path())
            .unwrap_or(request.uri().path())
            .split('/')
            .nth(5)
            .and_then(|v| Uuid::parse_str(v).ok()),
        workflow_id: request
            .extensions()
            .get::<OriginalUri>()
            .map(|uri| uri.0.path())
            .unwrap_or(request.uri().path())
            .split('/')
            .nth(3)
            .and_then(|v| Uuid::parse_str(v).ok()),
        parent_request_id: request
            .headers()
            .get("x-platform-request-id")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| Uuid::parse_str(v).ok()),
    };
    let response = crate::history::scoped(context, async {
        let response = next.run(request).await;
        normalize_response(response, &request_id).await
    })
    .instrument(span)
    .await;
    observation.headers(
        response.status(),
        response.extensions().get::<crate::error::ErrorDiagnostic>(),
    );
    let (mut parts, body) = response.into_parts();
    let expected_length = parts
        .headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok());
    let body = if method == "HEAD" {
        // Axum/Hyper suppress HEAD bodies; release body ownership without reporting
        // a client abort or counting bytes that can never be sent.
        if !parts.headers.contains_key(header::CONTENT_LENGTH)
            && let Some(length) = http_body::Body::size_hint(&body).exact()
        {
            parts.headers.insert(
                header::CONTENT_LENGTH,
                HeaderValue::from_str(&length.to_string()).expect("integer header"),
            );
        }
        observation.finish("complete");
        Body::empty()
    } else {
        crate::body::observe(body, observation, expected_length)
    };
    Response::from_parts(parts, body)
}

fn metric_route(matched: Option<&str>) -> &'static str {
    // An explicit allowlist keeps arbitrary paths and fallback paths out of logs
    // and time-series labels, even if router nesting changes in the future.
    match matched {
        Some("/healthz") => "/healthz",
        Some("/readyz") => "/readyz",
        Some("/metrics") => "/metrics",
        Some("/openapi.json") => "/openapi.json",
        Some("/internal/history/v1/events") => "/internal/history/v1/events",
        Some("/v1/projects/{project}/workspace") => "/v1/projects/{project}/workspace",
        Some("/v1/projects/{project}/workspace-file") => "/v1/projects/{project}/workspace-file",
        Some("/v1/projects/{project}/workspace-preview") => {
            "/v1/projects/{project}/workspace-preview"
        }
        Some("/v1/projects/{project}/workspace-archive") => {
            "/v1/projects/{project}/workspace-archive"
        }
        Some("/v1/workflows/{workflow_id}") => "/v1/workflows/{workflow_id}",
        Some("/v1/workflows/{workflow_id}/heartbeat") => "/v1/workflows/{workflow_id}/heartbeat",
        Some("/v1/workflows/{workflow_id}/finish") => "/v1/workflows/{workflow_id}/finish",
        Some("/v1/workflows/{workflow_id}/session") => "/v1/workflows/{workflow_id}/session",
        Some("/v1/workflows/{workflow_id}/session/stdin") => {
            "/v1/workflows/{workflow_id}/session/stdin"
        }
        Some("/v1/workflows/{workflow_id}/session/output") => {
            "/v1/workflows/{workflow_id}/session/output"
        }
        Some("/v1/workflows/{workflow_id}/sync/manifest") => {
            "/v1/workflows/{workflow_id}/sync/manifest"
        }
        Some("/v1/workflows/{workflow_id}/sync/push/plan") => {
            "/v1/workflows/{workflow_id}/sync/push/plan"
        }
        Some("/v1/workflows/{workflow_id}/sync/pull/plan") => {
            "/v1/workflows/{workflow_id}/sync/pull/plan"
        }
        Some("/v1/workflows/{workflow_id}/sync/{sync_id}/files/{*path}") => {
            "/v1/workflows/{workflow_id}/sync/{sync_id}/files/{path}"
        }
        Some("/v1/workflows/{workflow_id}/sync/{sync_id}/commit") => {
            "/v1/workflows/{workflow_id}/sync/{sync_id}/commit"
        }
        Some("/v1/workflows/{workflow_id}/sync/{sync_id}") => {
            "/v1/workflows/{workflow_id}/sync/{sync_id}"
        }
        Some("/v1/workflows/{workflow_id}/sync/files/{*path}") => {
            "/v1/workflows/{workflow_id}/sync/files/{path}"
        }
        _ => "unmatched",
    }
}

async fn normalize_response(response: Response, request_id: &str) -> Response {
    if response.extensions().get::<ReadinessResponse>().is_some()
        || (!response.status().is_client_error() && !response.status().is_server_error())
    {
        let mut response = response;
        insert_request_id(&mut response, request_id);
        return response;
    }
    let status = response.status();
    let (mut parts, body) = response.into_parts();
    if parts
        .extensions
        .get::<crate::error::ErrorDiagnostic>()
        .is_none()
    {
        parts.extensions.insert(crate::error::ErrorDiagnostic {
            code: default_error_code(status),
            diagnostic: None,
        });
    }
    let bytes = axum::body::to_bytes(body, 16 * 1024)
        .await
        .unwrap_or_default();
    let value = serde_json::from_slice::<serde_json::Value>(&bytes).ok();
    let mut error = value
        .as_ref()
        .and_then(|value| value.get("error"))
        .and_then(|value| value.as_object())
        .cloned()
        .unwrap_or_default();
    error
        .entry("code")
        .or_insert_with(|| default_error_code(status).into());
    error
        .entry("message")
        .or_insert_with(|| status.canonical_reason().unwrap_or("request failed").into());
    error.insert("request_id".into(), request_id.into());
    error
        .entry("details")
        .or_insert_with(|| serde_json::json!({}));
    let body =
        serde_json::to_vec(&serde_json::json!({"error":error})).expect("error envelope serializes");
    parts.headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    parts.headers.remove(header::CONTENT_LENGTH);
    let mut response = Response::from_parts(parts, Body::from(body));
    insert_request_id(&mut response, request_id);
    response
}

fn insert_request_id(response: &mut Response, request_id: &str) {
    response.headers_mut().insert(
        REQUEST_ID_HEADER,
        HeaderValue::from_str(request_id).expect("UUID is a valid header"),
    );
}

fn default_error_code(status: StatusCode) -> &'static str {
    match status {
        StatusCode::BAD_REQUEST
        | StatusCode::UNPROCESSABLE_ENTITY
        | StatusCode::UNSUPPORTED_MEDIA_TYPE => "invalid_request",
        StatusCode::UNAUTHORIZED => "unauthorized",
        StatusCode::NOT_FOUND => "not_found",
        StatusCode::METHOD_NOT_ALLOWED => "method_not_allowed",
        StatusCode::REQUEST_TIMEOUT => "request_timeout",
        StatusCode::CONFLICT => "sync_conflict",
        StatusCode::PRECONDITION_FAILED => "precondition_failed",
        StatusCode::PAYLOAD_TOO_LARGE => "payload_too_large",
        StatusCode::TOO_MANY_REQUESTS => "capacity_reached",
        StatusCode::SERVICE_UNAVAILABLE => "service_shutting_down",
        _ => "internal_error",
    }
}

fn content_length(headers: &HeaderMap) -> Result<Option<u64>, AppError> {
    headers
        .get(header::CONTENT_LENGTH)
        .map(|value| {
            value
                .to_str()
                .ok()
                .and_then(|value| value.parse().ok())
                .ok_or_else(|| AppError::BadRequest("invalid Content-Length".into()))
        })
        .transpose()
}

#[derive(Deserialize)]
struct WorkflowPath {
    workflow_id: Uuid,
}
#[derive(Deserialize)]
struct SyncPath {
    workflow_id: Uuid,
    sync_id: Uuid,
}
#[derive(Deserialize)]
struct UploadPath {
    workflow_id: Uuid,
    sync_id: Uuid,
    path: String,
}
#[derive(Deserialize)]
struct DownloadPath {
    workflow_id: Uuid,
    path: String,
}

async fn api_not_found() -> Result<(), AppError> {
    Err(AppError::NotFound("API route not found".into()))
}
async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}
async fn readyz(State(services): State<AppServices>) -> Response {
    let mut reasons = Vec::new();
    if services.shutdown.is_cancelled() {
        reasons.push("shutting_down");
    }
    if services.workflows.is_degraded() {
        reasons.push("workflow_cleanup_failed");
    }
    if !services.background_health.is_healthy() {
        reasons.push("background_task_failed");
    }
    let degraded = !reasons.is_empty();
    services.config.telemetry.set_ready(!degraded);
    let diagnostic = degraded.then(|| crate::error::ErrorDiagnostic {
        code: "service_not_ready",
        diagnostic: Some(format!("readiness checks failed: {}", reasons.join(","))),
    });
    let mut response = (
        if degraded {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::OK
        },
        Json(ReadyResponse {
            status: if degraded { "degraded" } else { "ready" },
            reasons,
            failed_tasks: services.background_health.failed_tasks(),
        }),
    )
        .into_response();
    response.extensions_mut().insert(ReadinessResponse);
    if let Some(diagnostic) = diagnostic {
        response.extensions_mut().insert(diagnostic);
    }
    response
}
async fn metrics(State(services): State<AppServices>) -> Result<Response, AppError> {
    services.config.telemetry.set_ready(
        !services.shutdown.is_cancelled()
            && !services.workflows.is_degraded()
            && services.background_health.is_healthy(),
    );
    let rendered = services
        .config
        .telemetry
        .render()
        .map_err(|error| AppError::Internal(format!("failed to encode metrics: {error}")))?;
    Ok((
        [
            (
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            ),
            (header::CACHE_CONTROL, "no-store"),
        ],
        rendered,
    )
        .into_response())
}
async fn openapi_json() -> Json<serde_json::Value> {
    Json(crate::openapi::document())
}

async fn create_workflow(
    State(services): State<AppServices>,
    Path(path): Path<WorkflowPath>,
    ApiJson(request): ApiJson<CreateWorkflowRequest>,
) -> Result<Json<crate::workflow::WorkflowInfo>, AppError> {
    Ok(Json(
        services.workflows.create(path.workflow_id, request).await?,
    ))
}
async fn get_workflow(
    State(services): State<AppServices>,
    Path(path): Path<WorkflowPath>,
) -> Result<Json<crate::workflow::WorkflowInfo>, AppError> {
    Ok(Json(services.workflows.get(path.workflow_id).await?))
}
async fn heartbeat(
    State(services): State<AppServices>,
    Path(path): Path<WorkflowPath>,
) -> Result<Json<crate::workflow::WorkflowInfo>, AppError> {
    Ok(Json(services.workflows.heartbeat(path.workflow_id).await?))
}
async fn finish_workflow(
    State(services): State<AppServices>,
    Path(path): Path<WorkflowPath>,
) -> Result<Json<crate::workflow::WorkflowInfo>, AppError> {
    Ok(Json(services.workflows.finish(path.workflow_id).await?))
}
async fn cancel_workflow(
    State(services): State<AppServices>,
    Path(path): Path<WorkflowPath>,
) -> Result<Json<crate::workflow::WorkflowInfo>, AppError> {
    Ok(Json(services.workflows.cancel(path.workflow_id).await?))
}
async fn create_session(
    State(services): State<AppServices>,
    Path(path): Path<WorkflowPath>,
    ApiJson(request): ApiJson<StartSessionRequest>,
) -> Result<Json<crate::session::SessionInfo>, AppError> {
    Ok(Json(
        services
            .workflows
            .start_session(path.workflow_id, request)
            .await?,
    ))
}
async fn get_session(
    State(services): State<AppServices>,
    Path(path): Path<WorkflowPath>,
) -> Result<Json<crate::session::SessionInfo>, AppError> {
    Ok(Json(services.workflows.session(path.workflow_id).await?))
}
async fn delete_session(
    State(services): State<AppServices>,
    Path(path): Path<WorkflowPath>,
) -> Result<Json<crate::session::SessionInfo>, AppError> {
    Ok(Json(
        services.workflows.stop_session(path.workflow_id).await?,
    ))
}
async fn send_stdin(
    State(services): State<AppServices>,
    Path(path): Path<WorkflowPath>,
    ApiJson(request): ApiJson<SendInputRequest>,
) -> Result<Json<crate::session::SessionInfo>, AppError> {
    Ok(Json(
        services.workflows.stdin(path.workflow_id, request).await?,
    ))
}
async fn read_output(
    State(services): State<AppServices>,
    Path(path): Path<WorkflowPath>,
    Query(query): Query<OutputQuery>,
) -> Result<Json<crate::session::OutputResponse>, AppError> {
    tokio::select! {
        result = services.workflows.output(path.workflow_id, query) => Ok(Json(result?)),
        _ = services.shutdown.cancelled() => Err(AppError::RequestTimeout),
    }
}
async fn sync_manifest(
    State(services): State<AppServices>,
    Path(path): Path<WorkflowPath>,
    ApiJson(request): ApiJson<ManifestRequest>,
) -> Result<Json<crate::sync::ManifestResponse>, AppError> {
    Ok(Json(
        services
            .workflows
            .manifest(path.workflow_id, request)
            .await?,
    ))
}
async fn sync_push_plan(
    State(services): State<AppServices>,
    Path(path): Path<WorkflowPath>,
    ApiJson(request): ApiJson<PushPlanRequest>,
) -> Result<Json<crate::sync::PushPlanResponse>, AppError> {
    Ok(Json(
        services
            .workflows
            .push_plan(path.workflow_id, request)
            .await?,
    ))
}
async fn sync_pull_plan(
    State(services): State<AppServices>,
    Path(path): Path<WorkflowPath>,
    ApiJson(request): ApiJson<PullPlanRequest>,
) -> Result<Json<crate::sync::PullPlanResponse>, AppError> {
    Ok(Json(
        services
            .workflows
            .pull_plan(path.workflow_id, request)
            .await?,
    ))
}
async fn sync_upload_file(
    State(services): State<AppServices>,
    Path(path): Path<UploadPath>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<crate::sync::UploadResponse>, AppError> {
    Ok(Json(
        services
            .workflows
            .upload(
                path.workflow_id,
                path.sync_id,
                path.path,
                content_length(&headers)?,
                body,
            )
            .await?,
    ))
}
async fn sync_commit(
    State(services): State<AppServices>,
    Path(path): Path<SyncPath>,
    ApiJson(request): ApiJson<CommitSyncRequest>,
) -> Result<Json<crate::sync::CommitSyncResponse>, AppError> {
    Ok(Json(
        services
            .workflows
            .commit(path.workflow_id, path.sync_id, request)
            .await?,
    ))
}
async fn sync_status(
    State(services): State<AppServices>,
    Path(path): Path<SyncPath>,
) -> Result<Json<crate::sync::SyncStatusResponse>, AppError> {
    Ok(Json(
        services
            .workflows
            .sync_status(path.workflow_id, path.sync_id)
            .await?,
    ))
}
async fn sync_abort(
    State(services): State<AppServices>,
    Path(path): Path<SyncPath>,
) -> Result<Json<crate::sync::AbortSyncResponse>, AppError> {
    Ok(Json(
        services
            .workflows
            .abort_sync(path.workflow_id, path.sync_id)
            .await?,
    ))
}
async fn sync_download_file(
    State(services): State<AppServices>,
    Path(path): Path<DownloadPath>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let if_match = headers
        .get(header::IF_MATCH)
        .map(|value| {
            value
                .to_str()
                .map(str::to_string)
                .map_err(|_| AppError::BadRequest("invalid If-Match".into()))
        })
        .transpose()?;
    if method == Method::HEAD {
        services
            .workflows
            .head_download(path.workflow_id, path.path, if_match)
            .await
    } else {
        services
            .workflows
            .download(path.workflow_id, path.path, if_match)
            .await
    }
}

async fn workspace_list(
    State(services): State<AppServices>,
    Path(project): Path<String>,
    query: Result<Query<WorkspaceQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Json<WorkspaceListing>, AppError> {
    let Query(query) = query.map_err(|error| AppError::BadRequest(error.to_string()))?;
    let root = services.config.workspace_root.clone();
    tokio::task::spawn_blocking(move || workspace_read::list(&root, &project, &query.path))
        .await
        .map_err(|error| AppError::Internal(format!("workspace list task failed: {error}")))?
        .map(Json)
}
async fn workspace_preview(
    State(services): State<AppServices>,
    Path(project): Path<String>,
    query: Result<Query<WorkspaceQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Json<WorkspacePreview>, AppError> {
    let Query(query) = query.map_err(|error| AppError::BadRequest(error.to_string()))?;
    let root = services.config.workspace_root.clone();
    tokio::task::spawn_blocking(move || workspace_read::preview(&root, &project, &query.path))
        .await
        .map_err(|error| AppError::Internal(format!("workspace preview task failed: {error}")))?
        .map(Json)
}
async fn workspace_file(
    State(services): State<AppServices>,
    Path(project): Path<String>,
    query: Result<Query<WorkspaceQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Response, AppError> {
    let Query(query) = query.map_err(|error| AppError::BadRequest(error.to_string()))?;
    let root = services.config.workspace_root.clone();
    let name = query
        .path
        .rsplit('/')
        .next()
        .unwrap_or("download")
        .to_string();
    let (file, size) =
        tokio::task::spawn_blocking(move || workspace_read::download(&root, &project, &query.path))
            .await
            .map_err(|error| {
                AppError::Internal(format!("workspace download task failed: {error}"))
            })??;
    Ok(workspace_read::file_response(file, size, &name, false))
}
async fn workspace_archive(
    State(services): State<AppServices>,
    Path(project): Path<String>,
    query: Result<Query<WorkspaceQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Response, AppError> {
    let Query(query) = query.map_err(|error| AppError::BadRequest(error.to_string()))?;
    let name = format!(
        "{}.zip",
        query
            .path
            .rsplit('/')
            .find(|part| !part.is_empty())
            .unwrap_or(&project)
    );
    let (file, size) =
        workspace_read::archive(services.config.workspace_root.clone(), project, query.path)
            .await?;
    Ok(workspace_read::file_response(file, size, &name, true))
}

async fn history_events(
    State(services): State<AppServices>,
    query: Result<Query<crate::history::Query>, axum::extract::rejection::QueryRejection>,
) -> Result<Response, AppError> {
    let Query(query) = query.map_err(|e| AppError::BadRequest(e.to_string()))?;
    let page = services
        .config
        .history
        .page(query.after, query.limit.unwrap_or(256))?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(page)).into_response())
}
