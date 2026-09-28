//! Explicit ownership of every service and task, including shutdown admission.
use crate::{
    AppConfig, RuntimeConfig, SessionManager, api,
    auth::AuthState,
    diagnostics::Diagnostics,
    observability::{BackgroundHealth, Observability},
    project::ProjectStore,
    sync::SyncManager,
    workflow::WorkflowManager,
};
use anyhow::Context;
use axum::Router;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct AppServices {
    pub(crate) config: Arc<RuntimeConfig>,
    pub(crate) auth: AuthState,
    pub(crate) workflows: WorkflowManager,
    pub(crate) shutdown: CancellationToken,
    pub(crate) background_health: BackgroundHealth,
}

pub struct AppRuntime {
    services: AppServices,
    cancellation: CancellationToken,
    background: Mutex<Option<Vec<JoinHandle<()>>>>,
    shutdown_lock: tokio::sync::Mutex<()>,
    diagnostics: Diagnostics,
}

impl AppRuntime {
    pub async fn initialize(config: AppConfig) -> anyhow::Result<Self> {
        let (config, auth) = config.into_runtime()?;
        let projects = ProjectStore::initialize(config.workspace_root.clone()).await?;
        let sync = SyncManager::new(config.clone());
        // The instance lock is held before deleting any abandoned staging.
        sync.cleanup_orphans()
            .await
            .context("failed to clean abandoned sync staging")?;
        let diagnostics = Diagnostics::initialize(&config, projects.clone()).await?;
        let sessions = SessionManager::new_with_diagnostics(config.clone(), diagnostics.clone());
        let cancellation = CancellationToken::new();
        let sync_reaper = sync.spawn_session_reaper(cancellation.child_token());
        let workflows = WorkflowManager::new(config.clone(), sessions, sync, projects);
        let reaper = workflows.spawn_reaper(cancellation.child_token());
        let background_health = BackgroundHealth::default();
        let background = [("workflow_reaper", reaper), ("sync_reaper", sync_reaper)]
            .into_iter()
            .map(|(name, task)| {
                supervise_background(
                    name,
                    task,
                    cancellation.clone(),
                    background_health.clone(),
                    config.telemetry.clone(),
                )
            })
            .collect();
        config.telemetry.set_ready(true);
        tracing::info!(
            event = "service.initialized",
            metrics_enabled = config.observability.metrics_enabled,
            archive_configured = config.observability.archive_output,
            archive_enabled = diagnostics.is_enabled(),
            "service initialized"
        );
        Ok(Self {
            services: AppServices {
                config: Arc::new(config),
                auth,
                workflows,
                shutdown: cancellation.child_token(),
                background_health,
            },
            cancellation,
            background: Mutex::new(Some(background)),
            shutdown_lock: tokio::sync::Mutex::new(()),
            diagnostics,
        })
    }

    pub fn router(&self) -> Router {
        api::build_router(self.services.clone())
    }
    pub fn config(&self) -> &RuntimeConfig {
        &self.services.config
    }

    /// Export log writer overflow alongside this runtime's metrics.
    pub fn observe_logging(&self, logging: &crate::LoggingGuard) {
        self.services
            .config
            .telemetry
            .attach_log_error_counter(logging.counter.clone());
    }

    pub async fn shutdown(&self) {
        let _shutdown = self.shutdown_lock.lock().await;
        if self.cancellation.is_cancelled() {
            return;
        }
        let started = Instant::now();
        tracing::info!(
            event = "service.stopping",
            "draining workflows and background services"
        );
        self.services.config.telemetry.set_ready(false);
        self.cancellation.cancel();
        let background = self
            .background
            .lock()
            .expect("runtime task list poisoned")
            .take()
            .unwrap_or_default();
        let services = self.services.clone();
        let shutdown = async move {
            services.workflows.shutdown().await;
            for task in background {
                if let Err(error) = task.await {
                    tracing::warn!(%error, "background task failed during shutdown");
                }
            }
        };
        let drained = tokio::time::timeout(self.services.config.shutdown_grace(), shutdown)
            .await
            .is_ok();
        if !drained {
            self.services.config.telemetry.event("shutdown", "timeout");
            tracing::error!(
                "shutdown deadline elapsed; systemd must kill remaining descendants; incomplete projects require full upload"
            );
        }
        self.diagnostics.shutdown().await;
        let outcome = if drained { "success" } else { "timeout" };
        self.services
            .config
            .telemetry
            .operation_finished("shutdown", outcome, started.elapsed());
        tracing::info!(
            event = "service.stopped",
            duration_ms = started.elapsed().as_millis() as u64,
            outcome,
            "service shutdown procedure finished"
        );
    }
}

impl Drop for AppRuntime {
    fn drop(&mut self) {
        self.cancellation.cancel();
        // Explicit shutdown is required for a bounded drain. This fallback keeps
        // a dropped HTTP test/server from silently abandoning its child process.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let workflows = self.services.workflows.clone();
            let diagnostics = self.diagnostics.clone();
            handle.spawn(async move {
                workflows.shutdown().await;
                diagnostics.shutdown().await;
            });
        }
    }
}

fn supervise_background(
    name: &'static str,
    task: JoinHandle<()>,
    cancellation: CancellationToken,
    health: BackgroundHealth,
    telemetry: Observability,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let result = task.await;
        if result.is_err() || !cancellation.is_cancelled() {
            health.fail(name);
            telemetry.task_failed(name);
            telemetry.set_ready(false);
            match result {
                Err(error) => tracing::error!(event = "background.failed", task = name, %error,
                    "background service failed; readiness is degraded"),
                Ok(()) => tracing::error!(
                    event = "background.stopped",
                    task = name,
                    "background service exited unexpectedly; readiness is degraded"
                ),
            }
        } else {
            tracing::debug!(
                event = "background.stopped",
                task = name,
                "background service drained"
            );
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unexpected_background_exit_changes_health_before_shutdown() {
        for panics in [false, true] {
            let health = BackgroundHealth::default();
            let telemetry = Observability::new();
            let task = tokio::spawn(async move {
                assert!(!panics, "injected background failure");
            });
            supervise_background(
                "workflow_reaper",
                task,
                CancellationToken::new(),
                health.clone(),
                telemetry,
            )
            .await
            .unwrap();
            assert!(!health.is_healthy());
            assert_eq!(health.failed_tasks(), vec!["workflow_reaper"]);
        }
    }

    #[tokio::test]
    async fn expected_background_drain_does_not_report_failure() {
        let health = BackgroundHealth::default();
        let cancel = CancellationToken::new();
        cancel.cancel();
        supervise_background(
            "sync_reaper",
            tokio::spawn(async {}),
            cancel,
            health.clone(),
            Observability::new(),
        )
        .await
        .unwrap();
        assert!(health.is_healthy());
    }
}
