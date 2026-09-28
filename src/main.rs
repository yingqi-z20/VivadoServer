use anyhow::Context;
use clap::Parser;
use std::{future::Future, net::SocketAddr, path::PathBuf, time::Duration};
use tokio::signal;
use vivado_server::{AppConfig, AppRuntime, initialize_logging};

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    #[arg(short, long, env = "VIVADO_SERVER_CONFIG")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = AppConfig::from_file(&args.config).context("failed to load configuration")?;
    let logging = initialize_logging(&config.observability)?;
    tracing::info!(
        event = "service.starting",
        version = env!("CARGO_PKG_VERSION"),
        "initializing Vivado service"
    );
    let runtime = match AppRuntime::initialize(config).await {
        Ok(runtime) => runtime,
        Err(error) => {
            tracing::error!(event = "service.start_failed", error = %format!("{error:#}"), "service initialization failed");
            return Err(error);
        }
    };
    runtime.observe_logging(&logging);
    let result = run(&runtime).await;
    if let Err(error) = &result {
        tracing::error!(event = "service.serve_failed", error = %format!("{error:#}"), "HTTP service stopped with an error");
    }
    runtime.shutdown().await;
    result
}

async fn run(runtime: &AppRuntime) -> anyhow::Result<()> {
    let config = runtime.config();
    let addr: SocketAddr = config.listen_addr.parse()?;
    let app = runtime.router();
    let handle = axum_server::Handle::new();
    tracing::info!(event = "service.listening", %addr, tls = config.tls.is_some(), "starting Linux Vivado workflow server");
    if let Some(tls) = &config.tls {
        let tls =
            axum_server::tls_rustls::RustlsConfig::from_pem_file(&tls.cert_path, &tls.key_path)
                .await
                .context("failed to load TLS certificate/key")?;
        let server = axum_server::bind_rustls(addr, tls)
            .handle(handle.clone())
            .serve(app.into_make_service());
        serve_until_shutdown(server, handle, runtime, config.shutdown_grace()).await
    } else {
        let server = axum_server::bind(addr)
            .handle(handle.clone())
            .serve(app.into_make_service());
        serve_until_shutdown(server, handle, runtime, config.shutdown_grace()).await
    }
}

async fn serve_until_shutdown<F>(
    server: F,
    handle: axum_server::Handle<SocketAddr>,
    runtime: &AppRuntime,
    grace: Duration,
) -> anyhow::Result<()>
where
    F: Future<Output = std::io::Result<()>>,
{
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result.context("server error"),
        _ = shutdown_signal() => {
            handle.graceful_shutdown(Some(grace));
            runtime.shutdown().await;
            server.await.context("server error during shutdown")
        }
    }
}

async fn shutdown_signal() {
    let mut terminate = signal::unix::signal(signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");
    tokio::select! {
        result = signal::ctrl_c() => {
            if let Err(error) = result { tracing::error!(%error, "Ctrl+C handler failed"); }
            tracing::info!(event = "service.signal", signal = "SIGINT", "shutdown requested");
        },
        _ = terminate.recv() => {
            tracing::info!(event = "service.signal", signal = "SIGTERM", "shutdown requested");
        },
    }
}
