use crate::{
    AppConfig, SessionManager,
    auth::{AuthState, require_auth},
    error::AppError,
    session::{CreateSessionRequest, OutputQuery, SendInputRequest},
    sync::{
        AbortSyncResponse, CommitSyncRequest, CommitSyncResponse, DownloadPath, ManifestRequest,
        ManifestResponse, ProjectPath, PullPlanRequest, PullPlanResponse, PushPlanRequest,
        PushPlanResponse, SyncIdPath, SyncManager, SyncPath, UploadResponse,
    },
};
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, FromRequest, Path, Query, Request, State},
    http::{StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use http_body_util::BodyExt;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tower_http::trace::{
    DefaultMakeSpan, DefaultOnFailure, DefaultOnRequest, DefaultOnResponse, TraceLayer,
};
use tracing::Level;
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    manager: SessionManager,
    sync: SyncManager,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

struct ApiJson<T>(T);

impl<T, S> FromRequest<S> for ApiJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = AppError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(request, state).await {
            Ok(Json(value)) => Ok(Self(value)),
            Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
                Err(AppError::PayloadTooLarge)
            }
            Err(rejection) => Err(AppError::BadRequest(rejection.body_text())),
        }
    }
}

pub fn build_router(config: AppConfig, manager: SessionManager) -> Router {
    let json_limit = config.api_json_body_limit_bytes;
    let auth = AuthState::new(config.auth_tokens.clone());
    let sync = SyncManager::new(config.clone());
    sync.spawn_session_reaper();
    let protected = Router::new()
        .route(
            "/sessions",
            post(create_session).layer(DefaultBodyLimit::max(json_limit)),
        )
        .route(
            "/sessions/{session_id}",
            get(get_session).delete(delete_session),
        )
        .route(
            "/sessions/{session_id}/stdin",
            post(send_stdin).layer(DefaultBodyLimit::max(json_limit)),
        )
        .route("/sessions/{session_id}/output", get(read_output))
        .route("/sessions/{session_id}/heartbeat", post(heartbeat))
        .route(
            "/projects/{project}/sync/manifest",
            post(sync_manifest).layer(DefaultBodyLimit::max(json_limit)),
        )
        .route(
            "/projects/{project}/sync/push/plan",
            post(sync_push_plan).layer(DefaultBodyLimit::max(json_limit)),
        )
        .route(
            "/projects/{project}/sync/pull/plan",
            post(sync_pull_plan).layer(DefaultBodyLimit::max(json_limit)),
        )
        .route(
            "/projects/{project}/sync/{sync_id}/files/{*path}",
            axum::routing::put(sync_upload_file),
        )
        .route(
            "/projects/{project}/sync/{sync_id}/commit",
            post(sync_commit).layer(DefaultBodyLimit::max(json_limit)),
        )
        .route(
            "/projects/{project}/sync/{sync_id}",
            axum::routing::delete(sync_abort),
        )
        .route(
            "/projects/{project}/sync/files/{*path}",
            get(sync_download_file),
        )
        .fallback(api_not_found)
        .with_state(AppState { manager, sync })
        .layer(middleware::map_response(normalize_error_response))
        .layer(middleware::from_fn_with_state(auth, require_auth));

    Router::new()
        .route("/healthz", get(healthz))
        .nest("/v1", protected)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::DEBUG))
                .on_request(DefaultOnRequest::new().level(Level::DEBUG))
                .on_response(DefaultOnResponse::new().level(Level::DEBUG))
                .on_failure(DefaultOnFailure::new().level(Level::WARN)),
        )
}

async fn api_not_found() -> Result<(), AppError> {
    Err(AppError::NotFound("API route not found".to_string()))
}

async fn normalize_error_response(response: Response) -> Response {
    if !response.status().is_client_error() && !response.status().is_server_error() {
        return response;
    }
    if response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/json"))
    {
        return response;
    }

    let status = response.status();
    let body = response.into_body().collect().await;
    let detail = body
        .ok()
        .and_then(|body| String::from_utf8(body.to_bytes().to_vec()).ok())
        .filter(|text| !text.trim().is_empty())
        .unwrap_or_else(|| {
            status
                .canonical_reason()
                .unwrap_or("request failed")
                .to_string()
        });
    (status, Json(serde_json::json!({ "error": detail }))).into_response()
}

async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn create_session(
    State(state): State<AppState>,
    ApiJson(request): ApiJson<CreateSessionRequest>,
) -> Result<Json<crate::session::SessionInfo>, AppError> {
    Ok(Json(state.manager.create_session(request).await?))
}

async fn get_session(
    State(state): State<AppState>,
    Path(session_id): Path<Uuid>,
) -> Result<Json<crate::session::SessionInfo>, AppError> {
    Ok(Json(state.manager.get_session(session_id).await?))
}

async fn send_stdin(
    State(state): State<AppState>,
    Path(session_id): Path<Uuid>,
    ApiJson(request): ApiJson<SendInputRequest>,
) -> Result<Json<crate::session::SessionInfo>, AppError> {
    Ok(Json(state.manager.send_input(session_id, request).await?))
}

async fn read_output(
    State(state): State<AppState>,
    Path(session_id): Path<Uuid>,
    Query(query): Query<OutputQuery>,
) -> Result<Json<crate::session::OutputResponse>, AppError> {
    Ok(Json(state.manager.read_output(session_id, query).await?))
}

async fn heartbeat(
    State(state): State<AppState>,
    Path(session_id): Path<Uuid>,
) -> Result<Json<crate::session::SessionInfo>, AppError> {
    Ok(Json(state.manager.heartbeat(session_id).await?))
}

async fn delete_session(
    State(state): State<AppState>,
    Path(session_id): Path<Uuid>,
) -> Result<Json<crate::session::SessionInfo>, AppError> {
    Ok(Json(state.manager.terminate_session(session_id).await?))
}

async fn sync_manifest(
    State(state): State<AppState>,
    Path(path): Path<ProjectPath>,
    ApiJson(request): ApiJson<ManifestRequest>,
) -> Result<Json<ManifestResponse>, AppError> {
    Ok(Json(state.sync.manifest(path.project, request).await?))
}

async fn sync_push_plan(
    State(state): State<AppState>,
    Path(path): Path<ProjectPath>,
    ApiJson(request): ApiJson<PushPlanRequest>,
) -> Result<Json<PushPlanResponse>, AppError> {
    Ok(Json(state.sync.push_plan(path.project, request).await?))
}

async fn sync_pull_plan(
    State(state): State<AppState>,
    Path(path): Path<ProjectPath>,
    ApiJson(request): ApiJson<PullPlanRequest>,
) -> Result<Json<PullPlanResponse>, AppError> {
    Ok(Json(state.sync.pull_plan(path.project, request).await?))
}

async fn sync_upload_file(
    State(state): State<AppState>,
    Path(path): Path<SyncPath>,
    body: Body,
) -> Result<Json<UploadResponse>, AppError> {
    Ok(Json(
        state
            .sync
            .upload_file(path.project, path.sync_id, path.path, body)
            .await?,
    ))
}

async fn sync_commit(
    State(state): State<AppState>,
    Path(path): Path<SyncIdPath>,
    ApiJson(request): ApiJson<CommitSyncRequest>,
) -> Result<Json<CommitSyncResponse>, AppError> {
    Ok(Json(
        state
            .sync
            .commit(path.project, path.sync_id, request)
            .await?,
    ))
}

async fn sync_abort(
    State(state): State<AppState>,
    Path(path): Path<SyncIdPath>,
) -> Result<Json<AbortSyncResponse>, AppError> {
    Ok(Json(state.sync.abort(path.project, path.sync_id).await?))
}

async fn sync_download_file(
    State(state): State<AppState>,
    Path(path): Path<DownloadPath>,
) -> Result<Response, AppError> {
    state.sync.download_file(path.project, path.path).await
}
