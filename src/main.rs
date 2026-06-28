use anyhow::Context;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio::signal;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use vivado_server::{AppConfig, SessionManager, build_router};

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    #[arg(short, long, env = "VIVADO_SERVER_CONFIG")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "vivado_server=info,tower_http=info".into()),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_thread_ids(true),
        )
        .init();

    let args = Args::parse();
    tracing::debug!(config_path = %args.config.display(), "loading configuration");
    let config = AppConfig::from_file(&args.config)
        .with_context(|| format!("failed to read config {}", args.config.display()))?;
    tracing::debug!(
        listen_addr = %config.listen_addr,
        vivado_path = %config.vivado_path.display(),
        workspace_root = %config.workspace_root.display(),
        auth_token_count = config.auth_tokens.len(),
        max_active_sessions = config.max_active_sessions,
        heartbeat_timeout_secs = config.heartbeat_timeout_secs,
        output_buffer_bytes = config.output_buffer_bytes,
        sync_max_file_bytes = config.sync_max_file_bytes,
        sync_max_manifest_entries = config.sync_max_manifest_entries,
        sync_session_ttl_secs = config.sync_session_ttl_secs,
        tls_enabled = config.tls.is_some(),
        "configuration loaded"
    );

    let addr: SocketAddr = config
        .listen_addr
        .parse()
        .with_context(|| format!("invalid listen_addr {}", config.listen_addr))?;

    let manager = SessionManager::new(config.clone());
    manager.spawn_heartbeat_reaper();
    let app = build_router(config.clone(), manager);

    tracing::info!(%addr, "starting vivado server");
    if let Some(tls) = config.tls.as_ref() {
        tracing::debug!(
            cert_path = %tls.cert_path.display(),
            key_path = %tls.key_path.display(),
            "loading TLS configuration"
        );
        let tls_config =
            axum_server::tls_rustls::RustlsConfig::from_pem_file(&tls.cert_path, &tls.key_path)
                .await
                .context("failed to load TLS certificate/key")?;
        let handle = axum_server::Handle::new();
        let shutdown_handle = handle.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            shutdown_handle.graceful_shutdown(Some(Duration::from_secs(10)));
        });
        axum_server::bind_rustls(addr, tls_config)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .context("server error")?;
    } else {
        tracing::debug!("starting without TLS");
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("failed to bind {addr}"))?;
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .context("server error")?;
    }

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
