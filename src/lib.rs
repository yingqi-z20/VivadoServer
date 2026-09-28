#[cfg(not(target_os = "linux"))]
compile_error!("VivadoServer supports Linux only; build and test it in Linux or WSL.");

mod api;
mod auth;
mod body;
mod config;
mod diagnostics;
mod error;
mod logging;
mod observability;
mod openapi;
mod project;
mod runtime;
pub mod session;
pub mod sync;
pub mod workflow;
mod workspace;

pub use config::{AppConfig, LogFormat, ObservabilityConfig, RuntimeConfig, TlsConfig};
pub use logging::{LoggingGuard, initialize_logging};
pub use runtime::{AppRuntime, AppServices};
pub use session::SessionManager;
