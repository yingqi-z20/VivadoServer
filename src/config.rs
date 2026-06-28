use anyhow::Context;
use serde::Deserialize;
use std::{fs, path::PathBuf, time::Duration};

fn default_listen_addr() -> String {
    "127.0.0.1:8080".to_string()
}

fn default_max_active_sessions() -> usize {
    1
}

fn default_heartbeat_timeout_secs() -> u64 {
    120
}

fn default_output_buffer_bytes() -> usize {
    1024 * 1024
}

fn default_sync_max_file_bytes() -> u64 {
    1024 * 1024 * 1024
}

fn default_sync_max_manifest_entries() -> usize {
    200_000
}

fn default_sync_session_ttl_secs() -> u64 {
    3_600
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    pub vivado_path: PathBuf,
    pub workspace_root: PathBuf,
    #[serde(default)]
    pub auth_tokens: Vec<String>,
    #[serde(default = "default_max_active_sessions")]
    pub max_active_sessions: usize,
    #[serde(default = "default_heartbeat_timeout_secs")]
    pub heartbeat_timeout_secs: u64,
    #[serde(default = "default_output_buffer_bytes")]
    pub output_buffer_bytes: usize,
    #[serde(default = "default_sync_max_file_bytes")]
    pub sync_max_file_bytes: u64,
    #[serde(default = "default_sync_max_manifest_entries")]
    pub sync_max_manifest_entries: usize,
    #[serde(default = "default_sync_session_ttl_secs")]
    pub sync_session_ttl_secs: u64,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

impl AppConfig {
    pub fn from_file(path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let path = path.into();
        let body = fs::read_to_string(&path)?;
        let mut config: AppConfig = toml::from_str(&body)?;
        if config.auth_tokens.iter().any(|token| token.is_empty()) {
            anyhow::bail!("auth_tokens must not contain empty tokens");
        }
        if config.max_active_sessions == 0 {
            anyhow::bail!("max_active_sessions must be at least 1");
        }
        if config.output_buffer_bytes == 0 {
            anyhow::bail!("output_buffer_bytes must be at least 1");
        }
        if config.sync_max_file_bytes == 0 {
            anyhow::bail!("sync_max_file_bytes must be at least 1");
        }
        if config.sync_max_manifest_entries == 0 {
            anyhow::bail!("sync_max_manifest_entries must be at least 1");
        }
        if config.sync_session_ttl_secs == 0 {
            anyhow::bail!("sync_session_ttl_secs must be at least 1");
        }
        config.workspace_root = config.workspace_root.canonicalize().with_context(|| {
            format!(
                "workspace_root does not exist: {}",
                config.workspace_root.display()
            )
        })?;
        Ok(config)
    }

    pub fn heartbeat_timeout(&self) -> Duration {
        Duration::from_secs(self.heartbeat_timeout_secs)
    }

    pub fn sync_session_ttl(&self) -> Duration {
        Duration::from_secs(self.sync_session_ttl_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_are_applied() {
        let parsed: AppConfig = toml::from_str(
            r#"
vivado_path = "vivado"
workspace_root = "."
auth_tokens = ["token"]
"#,
        )
        .unwrap();

        assert_eq!(parsed.listen_addr, "127.0.0.1:8080");
        assert_eq!(parsed.max_active_sessions, 1);
        assert_eq!(parsed.heartbeat_timeout_secs, 120);
        assert_eq!(parsed.output_buffer_bytes, 1024 * 1024);
        assert_eq!(parsed.sync_max_file_bytes, 1024 * 1024 * 1024);
        assert_eq!(parsed.sync_max_manifest_entries, 200_000);
        assert_eq!(parsed.sync_session_ttl_secs, 3_600);
    }
}
