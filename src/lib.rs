#[cfg(not(target_os = "linux"))]
compile_error!("VivadoServer supports Linux only; build and test it in Linux or WSL.");

mod api;
mod auth;
mod body;
mod config;
mod error;
mod openapi;
mod project;
mod runtime;
pub mod session;
pub mod sync;
pub mod workflow;
mod workspace;

pub use config::{AppConfig, RuntimeConfig, TlsConfig};
pub use runtime::{AppRuntime, AppServices};
pub use session::SessionManager;
