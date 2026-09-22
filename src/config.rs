use crate::{
    auth::AuthState,
    workspace::{ensure_real_directory_chain, metadata_is_link},
};
use anyhow::Context;
use serde::Deserialize;
use std::{
    collections::HashSet,
    env, fs,
    io::Read,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

const MIN_TOKEN_BYTES: usize = 32;
const MAX_TOKEN_BYTES: usize = 4096;

/// Untrusted startup input. Consume this through `into_runtime` before starting services.
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AppConfig {
    pub listen_addr: String,
    pub vivado_path: PathBuf,
    pub workspace_root: PathBuf,
    pub auth_tokens: Vec<String>,
    pub auth_token_files: Vec<PathBuf>,
    pub allow_plaintext_non_loopback: bool,
    pub allow_run_as_root: bool,
    pub heartbeat_timeout_secs: u64,
    pub output_buffer_bytes: usize,
    pub api_json_body_limit_bytes: usize,
    pub workflow_retention_secs: u64,
    pub max_retained_workflows: usize,
    pub stdin_max_bytes: usize,
    pub sync_max_file_bytes: u64,
    pub sync_max_manifest_entries: usize,
    pub sync_session_ttl_secs: u64,
    pub sync_result_retention_secs: u64,
    pub upload_idle_timeout_secs: u64,
    pub upload_deadline_secs: u64,
    pub json_idle_timeout_secs: u64,
    pub json_deadline_secs: u64,
    pub shutdown_grace_secs: u64,
    pub max_in_flight_requests: usize,
    pub tls: Option<TlsConfig>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:8080".to_string(),
            vivado_path: PathBuf::new(),
            workspace_root: PathBuf::new(),
            auth_tokens: Vec::new(),
            auth_token_files: Vec::new(),
            allow_plaintext_non_loopback: false,
            allow_run_as_root: false,
            heartbeat_timeout_secs: 120,
            output_buffer_bytes: 1024 * 1024,
            api_json_body_limit_bytes: 64 * 1024 * 1024,
            workflow_retention_secs: 3600,
            max_retained_workflows: 128,
            stdin_max_bytes: 1024 * 1024,
            sync_max_file_bytes: 1024 * 1024 * 1024,
            sync_max_manifest_entries: 200_000,
            sync_session_ttl_secs: 3600,
            sync_result_retention_secs: 3600,
            upload_idle_timeout_secs: 60,
            upload_deadline_secs: 14_400,
            json_idle_timeout_secs: 30,
            json_deadline_secs: 120,
            shutdown_grace_secs: 15,
            max_in_flight_requests: 64,
            tls: None,
        }
    }
}

impl std::fmt::Debug for AppConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AppConfig")
            .field("listen_addr", &self.listen_addr)
            .field("vivado_path", &self.vivado_path)
            .field("workspace_root", &self.workspace_root)
            .field(
                "auth_tokens",
                &format_args!("<redacted:{}>", self.auth_tokens.len()),
            )
            .field("auth_token_files", &self.auth_token_files)
            .field("tls", &self.tls)
            .finish_non_exhaustive()
    }
}

/// Validated service settings. Authentication secrets never enter service configuration.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RuntimeConfig {
    pub listen_addr: String,
    pub vivado_path: PathBuf,
    pub workspace_root: PathBuf,
    pub heartbeat_timeout_secs: u64,
    pub output_buffer_bytes: usize,
    pub api_json_body_limit_bytes: usize,
    pub workflow_retention_secs: u64,
    pub max_retained_workflows: usize,
    pub stdin_max_bytes: usize,
    pub sync_max_file_bytes: u64,
    pub sync_max_manifest_entries: usize,
    pub sync_session_ttl_secs: u64,
    pub sync_result_retention_secs: u64,
    pub upload_idle_timeout_secs: u64,
    pub upload_deadline_secs: u64,
    pub json_idle_timeout_secs: u64,
    pub json_deadline_secs: u64,
    pub shutdown_grace_secs: u64,
    pub max_in_flight_requests: usize,
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

impl AppConfig {
    /// Parse a file and resolve its paths. Token files are read once, by `into_runtime`.
    pub fn from_file(path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let supplied_path = path.into();
        let config_path = supplied_path.canonicalize().with_context(|| {
            format!(
                "configuration file does not exist: {}",
                supplied_path.display()
            )
        })?;
        let base = config_path
            .parent()
            .context("configuration path has no parent directory")?;
        let body = fs::read_to_string(&config_path)?;
        let mut config = parse_config(&body)?;
        config.resolve_paths(base);
        Ok(config)
    }

    /// The common validation boundary for both TOML and programmatically built input.
    pub fn into_runtime(mut self) -> anyhow::Result<(RuntimeConfig, AuthState)> {
        self.resolve_paths(&env::current_dir().context("failed to determine current directory")?);
        self.validate_settings()?;
        if !self.auth_tokens.is_empty() {
            tracing::warn!(
                count = self.auth_tokens.len(),
                "inline auth_tokens are intended for development; use auth_token_files in production"
            );
        }
        for path in &self.auth_token_files {
            self.auth_tokens.push(read_token_file(path)?);
        }
        if self.auth_tokens.is_empty() {
            anyhow::bail!("configure at least one auth token or auth_token_file");
        }
        let mut unique = HashSet::new();
        for token in &self.auth_tokens {
            validate_token(token)?;
            if !unique.insert(token) {
                anyhow::bail!("auth tokens must be unique");
            }
        }
        drop(unique);
        // Consume the raw settings so services retain digests instead of token strings.
        let auth = AuthState::new(self.auth_tokens);
        let runtime = RuntimeConfig {
            listen_addr: self.listen_addr,
            vivado_path: self.vivado_path,
            workspace_root: self.workspace_root,
            heartbeat_timeout_secs: self.heartbeat_timeout_secs,
            output_buffer_bytes: self.output_buffer_bytes,
            api_json_body_limit_bytes: self.api_json_body_limit_bytes,
            workflow_retention_secs: self.workflow_retention_secs,
            max_retained_workflows: self.max_retained_workflows,
            stdin_max_bytes: self.stdin_max_bytes,
            sync_max_file_bytes: self.sync_max_file_bytes,
            sync_max_manifest_entries: self.sync_max_manifest_entries,
            sync_session_ttl_secs: self.sync_session_ttl_secs,
            sync_result_retention_secs: self.sync_result_retention_secs,
            upload_idle_timeout_secs: self.upload_idle_timeout_secs,
            upload_deadline_secs: self.upload_deadline_secs,
            json_idle_timeout_secs: self.json_idle_timeout_secs,
            json_deadline_secs: self.json_deadline_secs,
            shutdown_grace_secs: self.shutdown_grace_secs,
            max_in_flight_requests: self.max_in_flight_requests,
            tls: self.tls,
        };
        Ok((runtime, auth))
    }

    fn resolve_paths(&mut self, base: &Path) {
        self.workspace_root = resolve_relative(base, &self.workspace_root);
        self.vivado_path = resolve_executable(base, &self.vivado_path);
        for path in &mut self.auth_token_files {
            *path = resolve_relative(base, path);
        }
        if let Some(tls) = &mut self.tls {
            tls.cert_path = resolve_relative(base, &tls.cert_path);
            tls.key_path = resolve_relative(base, &tls.key_path);
        }
    }

    fn validate_settings(&mut self) -> anyhow::Result<()> {
        let addr: SocketAddr = self
            .listen_addr
            .parse()
            .with_context(|| format!("invalid listen_addr {}", self.listen_addr))?;
        if !addr.ip().is_loopback() && self.tls.is_none() && !self.allow_plaintext_non_loopback {
            anyhow::bail!(
                "non-loopback plaintext listening is disabled; configure TLS or explicitly set allow_plaintext_non_loopback=true for a trusted VPN"
            );
        }
        #[cfg(target_os = "linux")]
        if !self.allow_run_as_root && unsafe { nix::libc::geteuid() } == 0 {
            anyhow::bail!("refusing to run as root; use a dedicated service account");
        }
        #[cfg(not(target_os = "linux"))]
        anyhow::bail!("VivadoServer supports Linux only");
        if self.workspace_root.as_os_str().is_empty() {
            anyhow::bail!("workspace_root is required");
        }
        let metadata = fs::symlink_metadata(&self.workspace_root).with_context(|| {
            format!(
                "workspace_root does not exist: {}",
                self.workspace_root.display()
            )
        })?;
        if !metadata.is_dir() || metadata_is_link(&metadata) {
            anyhow::bail!("workspace_root must be a real directory, not a symbolic link");
        }
        ensure_real_directory_chain(&self.workspace_root)?;
        self.workspace_root = self.workspace_root.canonicalize()?;
        if self.vivado_path.as_os_str().is_empty() {
            anyhow::bail!("vivado_path is required");
        }
        if !self.vivado_path.is_file() {
            anyhow::bail!(
                "vivado_path does not exist or is not a file: {}",
                self.vivado_path.display()
            );
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::PermissionsExt;
            if fs::metadata(&self.vivado_path)?.permissions().mode() & 0o111 == 0 {
                anyhow::bail!(
                    "vivado_path is not executable: {}",
                    self.vivado_path.display()
                );
            }
        }
        self.vivado_path = self.vivado_path.canonicalize()?;
        if let Some(tls) = &mut self.tls {
            if !tls.cert_path.is_file() || !tls.key_path.is_file() {
                anyhow::bail!("TLS certificate and key paths must both be regular files");
            }
            tls.cert_path = tls.cert_path.canonicalize()?;
            tls.key_path = tls.key_path.canonicalize()?;
        }
        for (name, value) in [
            ("heartbeat_timeout_secs", self.heartbeat_timeout_secs),
            ("workflow_retention_secs", self.workflow_retention_secs),
            ("sync_session_ttl_secs", self.sync_session_ttl_secs),
            (
                "sync_result_retention_secs",
                self.sync_result_retention_secs,
            ),
            ("upload_idle_timeout_secs", self.upload_idle_timeout_secs),
            ("upload_deadline_secs", self.upload_deadline_secs),
            ("json_idle_timeout_secs", self.json_idle_timeout_secs),
            ("json_deadline_secs", self.json_deadline_secs),
            ("shutdown_grace_secs", self.shutdown_grace_secs),
        ] {
            validate_duration(name, value)?;
        }
        let ttl = chrono::Duration::from_std(Duration::from_secs(self.sync_session_ttl_secs))
            .context("sync_session_ttl_secs exceeds supported calendar duration")?;
        if chrono::Utc::now().checked_add_signed(ttl).is_none() {
            anyhow::bail!("sync_session_ttl_secs exceeds supported calendar deadline");
        }
        for (name, value) in [
            ("output_buffer_bytes", self.output_buffer_bytes),
            ("api_json_body_limit_bytes", self.api_json_body_limit_bytes),
            ("max_retained_workflows", self.max_retained_workflows),
            ("sync_max_manifest_entries", self.sync_max_manifest_entries),
        ] {
            validate_capacity(name, value, isize::MAX as usize)?;
        }
        validate_capacity(
            "max_in_flight_requests",
            self.max_in_flight_requests,
            tokio::sync::Semaphore::MAX_PERMITS,
        )?;
        // Stdin uses semaphore permits counted by a u32 acquire_many argument.
        validate_capacity(
            "stdin_max_bytes",
            self.stdin_max_bytes,
            tokio::sync::Semaphore::MAX_PERMITS.min(u32::MAX as usize),
        )?;
        if self.output_buffer_bytes < 4096 {
            anyhow::bail!("output_buffer_bytes must be at least 4096");
        }
        if self.sync_max_file_bytes == 0 || self.sync_max_file_bytes > i64::MAX as u64 {
            anyhow::bail!("sync_max_file_bytes must be between 1 and {}", i64::MAX);
        }
        if self.stdin_max_bytes > self.api_json_body_limit_bytes {
            anyhow::bail!("stdin_max_bytes must not exceed api_json_body_limit_bytes");
        }
        if self.upload_deadline_secs < self.upload_idle_timeout_secs {
            anyhow::bail!("upload_deadline_secs must be at least upload_idle_timeout_secs");
        }
        if self.json_deadline_secs < self.json_idle_timeout_secs {
            anyhow::bail!("json_deadline_secs must be at least json_idle_timeout_secs");
        }
        Ok(())
    }
}

impl RuntimeConfig {
    pub fn heartbeat_timeout(&self) -> Duration {
        Duration::from_secs(self.heartbeat_timeout_secs)
    }
    pub fn sync_session_ttl(&self) -> Duration {
        Duration::from_secs(self.sync_session_ttl_secs)
    }
    pub fn workflow_retention(&self) -> Duration {
        Duration::from_secs(self.workflow_retention_secs)
    }
    pub fn sync_result_retention(&self) -> Duration {
        Duration::from_secs(self.sync_result_retention_secs)
    }
    pub fn upload_idle_timeout(&self) -> Duration {
        Duration::from_secs(self.upload_idle_timeout_secs)
    }
    pub fn upload_deadline(&self) -> Duration {
        Duration::from_secs(self.upload_deadline_secs)
    }
    pub fn json_idle_timeout(&self) -> Duration {
        Duration::from_secs(self.json_idle_timeout_secs)
    }
    pub fn json_deadline(&self) -> Duration {
        Duration::from_secs(self.json_deadline_secs)
    }
    pub fn shutdown_grace(&self) -> Duration {
        Duration::from_secs(self.shutdown_grace_secs)
    }
}

fn parse_config(body: &str) -> anyhow::Result<AppConfig> {
    toml::from_str(body).map_err(|error: toml::de::Error| {
        // TOML errors can print the source line or an invalid value, including an
        // inline token. Report its location without retaining the source error.
        let offset = error.span().map_or(0, |span| span.start);
        let prefix = body.get(..offset.min(body.len())).unwrap_or(body);
        let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
        let column = prefix.rsplit('\n').next().unwrap_or("").chars().count() + 1;
        anyhow::anyhow!("invalid configuration syntax, field name, or value type at line {line}, column {column}")
    })
}

fn validate_duration(name: &str, seconds: u64) -> anyhow::Result<()> {
    if seconds == 0
        || Instant::now()
            .checked_add(Duration::from_secs(seconds))
            .is_none()
    {
        anyhow::bail!("{name} must be a positive, representable monotonic duration");
    }
    Ok(())
}

fn validate_capacity(name: &str, value: usize, maximum: usize) -> anyhow::Result<()> {
    if value == 0 || value > maximum {
        anyhow::bail!("{name} must be between 1 and {maximum}");
    }
    Ok(())
}

fn validate_token(token: &str) -> anyhow::Result<()> {
    if !(MIN_TOKEN_BYTES..=MAX_TOKEN_BYTES).contains(&token.len()) {
        anyhow::bail!(
            "auth tokens must contain between {MIN_TOKEN_BYTES} and {MAX_TOKEN_BYTES} bytes"
        );
    }
    // RFC 6750 bearer credentials allow token68 characters and trailing padding.
    let unpadded = token.trim_end_matches('=');
    if unpadded.is_empty()
        || !unpadded.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'+' | b'/')
        })
    {
        anyhow::bail!(
            "auth tokens must use ASCII bearer-token characters, with '=' only as trailing padding"
        );
    }
    Ok(())
}

fn resolve_relative(base: &Path, path: &Path) -> PathBuf {
    if path.as_os_str().is_empty() || path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn resolve_executable(base: &Path, configured: &Path) -> PathBuf {
    let local = resolve_relative(base, configured);
    if configured.as_os_str().is_empty()
        || configured.is_absolute()
        || configured.components().count() > 1
        || local.is_file()
    {
        return local;
    }
    for directory in env::var_os("PATH")
        .into_iter()
        .flat_map(|value| env::split_paths(&value).collect::<Vec<_>>())
    {
        let candidate = directory.join(configured);
        if candidate.is_file() {
            // PATH may itself contain relative entries; child processes change cwd.
            if let Ok(absolute) = candidate.canonicalize() {
                return absolute;
            }
        }
    }
    local
}

fn read_token_file(path: &Path) -> anyhow::Result<String> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK);
    }
    // Check the opened object, not a pathname that can change before the read.
    let file = options
        .open(path)
        .with_context(|| format!("failed to open token file {}", path.display()))?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        anyhow::bail!("token file must be a regular file: {}", path.display());
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            anyhow::bail!(
                "token file must not be accessible by group or others: {}",
                path.display()
            );
        }
    }
    let mut body = String::new();
    file.take((MAX_TOKEN_BYTES + 3) as u64)
        .read_to_string(&mut body)
        .with_context(|| format!("failed to read token file {}", path.display()))?;
    let token = body
        .strip_suffix("\r\n")
        .or_else(|| body.strip_suffix('\n'))
        .unwrap_or(&body);
    validate_token(token).with_context(|| format!("invalid token file {}", path.display()))?;
    Ok(token.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    const TOKEN: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn defaults_and_removed_fields_are_unambiguous() {
        let parsed: AppConfig = toml::from_str("").unwrap();
        assert_eq!(parsed.listen_addr, "127.0.0.1:8080");
        assert_eq!(parsed.max_retained_workflows, 128);
        assert_eq!(parsed.workflow_retention_secs, 3600);
        assert_eq!(parsed.json_idle_timeout_secs, 30);
        assert_eq!(parsed.json_deadline_secs, 120);
        for obsolete in [
            "max_active_sessions",
            "sync_max_active_sessions",
            "max_retained_sessions",
            "session_retention_secs",
            "typo_field",
        ] {
            assert!(toml::from_str::<AppConfig>(&format!("{obsolete} = 1")).is_err());
        }
    }

    #[test]
    fn bearer_tokens_reject_untransmittable_or_ambiguous_values() {
        for token in [
            "short".to_string(),
            "é".repeat(32),
            " ".repeat(32),
            format!("{TOKEN}\n"),
            format!("{TOKEN}=x"),
            "=".repeat(32),
            "x".repeat(MAX_TOKEN_BYTES + 1),
        ] {
            assert!(validate_token(&token).is_err());
        }
        for token in [TOKEN.to_string(), format!("{TOKEN}-._~+/==")] {
            validate_token(&token).unwrap();
        }
    }

    #[test]
    fn config_parse_errors_do_not_expose_inline_credentials() {
        let error = parse_config(&format!("auth_tokens = \"{TOKEN}\"\n")).unwrap_err();
        let diagnostic = format!("{error:#}");
        assert!(diagnostic.contains("line 1"));
        assert!(!diagnostic.contains(TOKEN));
    }

    #[test]
    fn dangerous_capacity_and_duration_values_are_rejected() {
        assert!(validate_capacity("capacity", 0, 100).is_err());
        assert!(validate_capacity("capacity", 101, 100).is_err());
        assert!(validate_duration("duration", 0).is_err());
        assert!(validate_duration("duration", u64::MAX).is_err());
    }

    #[cfg(target_os = "linux")]
    fn valid_config(temp: &tempfile::TempDir) -> AppConfig {
        use std::os::unix::fs::PermissionsExt;
        let executable = temp.path().join("vivado");
        fs::write(&executable, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
        AppConfig {
            vivado_path: executable,
            workspace_root: temp.path().to_path_buf(),
            auth_tokens: vec![TOKEN.to_string()],
            allow_run_as_root: true,
            ..AppConfig::default()
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn file_tokens_load_only_at_runtime_boundary_and_do_not_survive_in_settings() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let _ = valid_config(&temp);
        let file = temp.path().join("server.toml");
        fs::write(&file, "vivado_path = \"vivado\"\nworkspace_root = \".\"\nauth_token_files = [\"token\"]\nallow_run_as_root = true\n").unwrap();
        let raw = AppConfig::from_file(&file).unwrap();
        assert!(raw.auth_tokens.is_empty());
        assert!(raw.workspace_root.is_absolute());
        // This token does not exist until after parsing; only into_runtime reads it.
        let token_path = temp.path().join("token");
        fs::write(&token_path, format!("{TOKEN}\n")).unwrap();
        fs::set_permissions(&token_path, fs::Permissions::from_mode(0o600)).unwrap();
        let (runtime, auth) = raw.into_runtime().unwrap();
        assert!(runtime.vivado_path.is_absolute());
        assert!(!format!("{runtime:?} {auth:?}").contains(TOKEN));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn programmatic_settings_cannot_skip_validation() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = valid_config(&temp);
        config.auth_tokens = vec!["short".to_string()];
        assert!(
            config
                .into_runtime()
                .unwrap_err()
                .to_string()
                .contains("auth tokens")
        );
        let mut config = valid_config(&temp);
        config.max_in_flight_requests = tokio::sync::Semaphore::MAX_PERMITS + 1;
        assert!(
            config
                .into_runtime()
                .unwrap_err()
                .to_string()
                .contains("max_in_flight_requests")
        );
        let mut config = valid_config(&temp);
        config.json_deadline_secs = 1;
        assert!(
            config
                .into_runtime()
                .unwrap_err()
                .to_string()
                .contains("json_deadline_secs")
        );
        let mut config = valid_config(&temp);
        config.listen_addr = "0.0.0.0:8080".to_string();
        assert!(
            config
                .into_runtime()
                .unwrap_err()
                .to_string()
                .contains("non-loopback plaintext")
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn stdin_capacity_fits_both_the_semaphore_and_acquire_many_argument() {
        let temp = tempfile::tempdir().unwrap();
        let maximum = tokio::sync::Semaphore::MAX_PERMITS.min(u32::MAX as usize);
        for value in [0, maximum + 1] {
            let mut config = valid_config(&temp);
            config.stdin_max_bytes = value;
            config.api_json_body_limit_bytes = maximum + 1;
            let error = config.into_runtime().unwrap_err().to_string();
            assert!(error.contains("stdin_max_bytes"), "{error}");
        }
        let mut config = valid_config(&temp);
        config.stdin_max_bytes = maximum;
        config.api_json_body_limit_bytes = maximum;
        let (runtime, _) = config.into_runtime().unwrap();
        assert_eq!(runtime.stdin_max_bytes, maximum);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn token_files_reject_links_permissions_multiline_and_duplicates() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("token");
        fs::write(&path, TOKEN).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_token_file(&path).is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let link = temp.path().join("link");
        symlink(&path, &link).unwrap();
        assert!(read_token_file(&link).is_err());
        let mut config = valid_config(&temp);
        config.auth_token_files.push(path.clone());
        assert!(
            config
                .into_runtime()
                .unwrap_err()
                .to_string()
                .contains("unique")
        );
        fs::write(&path, format!("{TOKEN}\n\n")).unwrap();
        assert!(read_token_file(&path).is_err());
        fs::write(&path, "x".repeat(MAX_TOKEN_BYTES + 1)).unwrap();
        assert!(read_token_file(&path).is_err());
    }
}
