//! Process logging setup. Keep the guard alive until every service has drained.
use crate::{LogFormat, ObservabilityConfig};
use anyhow::Context;
use tracing_appender::non_blocking::{ErrorCounter, WorkerGuard};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Owns the bounded log writer and flushes queued events when dropped.
pub struct LoggingGuard {
    _worker: WorkerGuard,
    pub(crate) counter: ErrorCounter,
}

impl LoggingGuard {
    /// Events discarded when the log collector cannot keep up with the service.
    pub fn dropped_lines(&self) -> usize {
        self.counter.dropped_lines()
    }
}

/// Install JSON or text logging, using `RUST_LOG` as an explicit filter override.
///
/// The 4096-line queue protects PTY handling from a blocked stdout collector.
/// Queue overflow is observable through the authenticated metrics endpoint.
pub fn initialize_logging(config: &ObservabilityConfig) -> anyhow::Result<LoggingGuard> {
    let override_filter = std::env::var("RUST_LOG").ok();
    let filter = parse_filter(config, override_filter.as_deref())?;
    let (writer, worker) = tracing_appender::non_blocking::NonBlockingBuilder::default()
        .buffered_lines_limit(4096)
        .lossy(true)
        .thread_name("vivado-log-writer")
        .finish(std::io::stdout());
    let counter = writer.error_counter();
    let subscriber = tracing_subscriber::registry().with(filter);
    match config.log_format {
        LogFormat::Json => subscriber
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_target(true)
                    .with_current_span(true)
                    .with_span_list(true)
                    .with_writer(writer),
            )
            .try_init(),
        LogFormat::Text => subscriber
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_target(true)
                    .with_writer(writer),
            )
            .try_init(),
    }
    .map_err(|error| anyhow::anyhow!("failed to initialize logging: {error}"))?;
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        tracing::error!(event = "process.panic", error = %info, "task panicked");
        // Preserve Rust's stderr/backtrace behavior, including before queue flush.
        previous(info);
    }));
    Ok(LoggingGuard {
        _worker: worker,
        counter,
    })
}

fn parse_filter(config: &ObservabilityConfig, supplied: Option<&str>) -> anyhow::Result<EnvFilter> {
    let filter = supplied.unwrap_or(&config.log_filter);
    anyhow::ensure!(!filter.trim().is_empty(), "log filter cannot be empty");
    EnvFilter::try_new(filter)
        .context("invalid log filter (RUST_LOG overrides observability.log_filter)")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_invalid_filter_is_not_silently_replaced() {
        let config = ObservabilityConfig::default();
        assert!(parse_filter(&config, None).is_ok());
        assert!(parse_filter(&config, Some("vivado_server=debug")).is_ok());
        assert!(parse_filter(&config, Some("vivado_server=invalid_level")).is_err());
        assert!(parse_filter(&config, Some("")).is_err());
    }
}
