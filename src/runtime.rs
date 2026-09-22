//! Explicit ownership of every service and task, including shutdown admission.
use crate::{
    AppConfig, RuntimeConfig, SessionManager, api, auth::AuthState, project::ProjectStore,
    sync::SyncManager, workflow::WorkflowManager,
};
use anyhow::Context;
use axum::Router;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct AppServices {
    pub(crate) config: Arc<RuntimeConfig>,
    pub(crate) auth: AuthState,
    pub(crate) workflows: WorkflowManager,
    pub(crate) shutdown: CancellationToken,
}

pub struct AppRuntime {
    services: AppServices,
    cancellation: CancellationToken,
    background: Mutex<Option<Vec<JoinHandle<()>>>>,
    shutdown_lock: tokio::sync::Mutex<()>,
}

impl AppRuntime {
    pub async fn initialize(config: AppConfig) -> anyhow::Result<Self> {
        let (config, auth) = config.into_runtime()?;
        let projects = ProjectStore::initialize(config.workspace_root.clone()).await?;
        let sessions = SessionManager::new(config.clone());
        let sync = SyncManager::new(config.clone());
        // The instance lock is held before deleting any abandoned staging.
        sync.cleanup_orphans()
            .await
            .context("failed to clean abandoned sync staging")?;
        let cancellation = CancellationToken::new();
        let sync_reaper = sync.spawn_session_reaper(cancellation.child_token());
        let workflows = WorkflowManager::new(config.clone(), sessions, sync, projects);
        let reaper = workflows.spawn_reaper(cancellation.child_token());
        Ok(Self {
            services: AppServices {
                config: Arc::new(config),
                auth,
                workflows,
                shutdown: cancellation.child_token(),
            },
            cancellation,
            background: Mutex::new(Some(vec![reaper, sync_reaper])),
            shutdown_lock: tokio::sync::Mutex::new(()),
        })
    }

    pub fn router(&self) -> Router {
        api::build_router(self.services.clone())
    }
    pub fn config(&self) -> &RuntimeConfig {
        &self.services.config
    }

    pub async fn shutdown(&self) {
        let _shutdown = self.shutdown_lock.lock().await;
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
        if tokio::time::timeout(self.services.config.shutdown_grace(), shutdown)
            .await
            .is_err()
        {
            tracing::error!(
                "shutdown deadline elapsed; systemd must kill remaining descendants; incomplete projects require full upload"
            );
        }
    }
}

impl Drop for AppRuntime {
    fn drop(&mut self) {
        self.cancellation.cancel();
        // Explicit shutdown is required for a bounded drain. This fallback keeps
        // a dropped HTTP test/server from silently abandoning its child process.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let workflows = self.services.workflows.clone();
            handle.spawn(async move {
                workflows.shutdown().await;
            });
        }
    }
}
