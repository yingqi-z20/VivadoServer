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
    extract::{Path, Query, State},
    middleware,
    response::Response,
    routing::{get, post},
};
use serde::Serialize;
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

pub fn build_router(config: AppConfig, manager: SessionManager) -> Router {
    let auth = AuthState::new(config.auth_tokens.clone());
    let sync = SyncManager::new(config.clone());
    sync.spawn_session_reaper();
    let protected = Router::new()
        .route("/sessions", post(create_session))
        .route(
            "/sessions/{session_id}",
            get(get_session).delete(delete_session),
        )
        .route("/sessions/{session_id}/stdin", post(send_stdin))
        .route("/sessions/{session_id}/output", get(read_output))
        .route("/sessions/{session_id}/heartbeat", post(heartbeat))
        .route("/projects/{project}/sync/manifest", post(sync_manifest))
        .route("/projects/{project}/sync/push/plan", post(sync_push_plan))
        .route("/projects/{project}/sync/pull/plan", post(sync_pull_plan))
        .route(
            "/projects/{project}/sync/{sync_id}/files/{*path}",
            axum::routing::put(sync_upload_file),
        )
        .route(
            "/projects/{project}/sync/{sync_id}/commit",
            post(sync_commit),
        )
        .route(
            "/projects/{project}/sync/{sync_id}",
            axum::routing::delete(sync_abort),
        )
        .route(
            "/projects/{project}/sync/files/{*path}",
            get(sync_download_file),
        )
        .route_layer(middleware::from_fn_with_state(auth, require_auth))
        .with_state(AppState { manager, sync });

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

async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn create_session(
    State(state): State<AppState>,
    Json(request): Json<CreateSessionRequest>,
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
    Json(request): Json<SendInputRequest>,
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
    Json(request): Json<ManifestRequest>,
) -> Result<Json<ManifestResponse>, AppError> {
    Ok(Json(state.sync.manifest(path.project, request).await?))
}

async fn sync_push_plan(
    State(state): State<AppState>,
    Path(path): Path<ProjectPath>,
    Json(request): Json<PushPlanRequest>,
) -> Result<Json<PushPlanResponse>, AppError> {
    Ok(Json(state.sync.push_plan(path.project, request).await?))
}

async fn sync_pull_plan(
    State(state): State<AppState>,
    Path(path): Path<ProjectPath>,
    Json(request): Json<PullPlanRequest>,
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
    Json(request): Json<CommitSyncRequest>,
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
