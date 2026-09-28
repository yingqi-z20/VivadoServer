//! Per-runtime telemetry. Labels describe server operations, never user input.
use axum::http::StatusCode;
use prometheus::{
    HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder,
};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Default)]
pub(crate) struct BackgroundHealth(Arc<Mutex<BTreeSet<&'static str>>>);

impl BackgroundHealth {
    pub(crate) fn is_healthy(&self) -> bool {
        self.0
            .lock()
            .expect("background health poisoned")
            .is_empty()
    }
    pub(crate) fn failed_tasks(&self) -> Vec<&'static str> {
        self.0
            .lock()
            .expect("background health poisoned")
            .iter()
            .copied()
            .collect()
    }
    pub(crate) fn fail(&self, task: &'static str) {
        self.0
            .lock()
            .expect("background health poisoned")
            .insert(task);
    }
}

#[derive(Clone)]
pub(crate) struct Observability {
    registry: Registry,
    requests: IntCounterVec,
    header_duration: HistogramVec,
    response_duration: HistogramVec,
    responses: IntCounterVec,
    response_bytes: IntCounterVec,
    aborted: IntCounterVec,
    in_flight: IntGaugeVec,
    operations: IntCounterVec,
    operation_duration: HistogramVec,
    active: IntGaugeVec,
    bytes: IntCounterVec,
    events: IntCounterVec,
    ready: IntGauge,
    ready_state: Arc<Mutex<bool>>,
    task_failures: IntCounterVec,
    uptime: IntGauge,
    started: Instant,
    log_dropped: IntGauge,
    log_counter: Arc<Mutex<Option<tracing_appender::non_blocking::ErrorCounter>>>,
    log_file_failures: IntGauge,
    log_file_counter: Arc<Mutex<Option<Arc<std::sync::atomic::AtomicU64>>>>,
}

impl std::fmt::Debug for Observability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Observability").finish_non_exhaustive()
    }
}

impl Observability {
    pub(crate) fn new() -> Self {
        let registry = Registry::new_custom(Some("vivado_server".into()), None)
            .expect("valid metric namespace");
        macro_rules! register {
            ($metric:expr) => {{
                let metric = $metric.expect("valid static metric definition");
                registry
                    .register(Box::new(metric.clone()))
                    .expect("unique metric name");
                metric
            }};
        }
        let requests = register!(IntCounterVec::new(
            Opts::new(
                "http_requests_total",
                "Responses whose headers have been generated; this does not imply body delivery."
            ),
            &["method", "route", "status"]
        ));
        let header_duration = register!(HistogramVec::new(
            duration_opts(
                "http_request_duration_seconds",
                "Time from admission to generated response headers."
            ),
            &["method", "route", "status"]
        ));
        let response_duration = register!(HistogramVec::new(
            duration_opts(
                "http_response_duration_seconds",
                "Time until response body is exhausted, fails, or is dropped; includes handler time."
            ),
            &["method", "route", "status", "outcome"]
        ));
        let responses = register!(IntCounterVec::new(
            Opts::new(
                "http_responses_total",
                "Response lifetimes by complete, body_error, or aborted outcome; complete means body yielded to HTTP transport, not acknowledged by client."
            ),
            &["method", "route", "status", "outcome"]
        ));
        let response_bytes = register!(IntCounterVec::new(
            Opts::new(
                "http_response_bytes_total",
                "Response data bytes yielded to HTTP transport; not confirmed client receipts."
            ),
            &["method", "route", "status"]
        ));
        let aborted = register!(IntCounterVec::new(
            Opts::new(
                "http_aborted_requests_total",
                "Requests dropped before response generation or before body completion."
            ),
            &["method", "route", "phase"]
        ));
        let in_flight = register!(IntGaugeVec::new(
            Opts::new(
                "http_in_flight_requests",
                "Requests executing or retaining an unfinished response body."
            ),
            &["method", "route"]
        ));
        let operations = register!(IntCounterVec::new(
            Opts::new(
                "operations_total",
                "Completed domain operations by kind and outcome."
            ),
            &["kind", "outcome"]
        ));
        let operation_duration = register!(HistogramVec::new(
            duration_opts(
                "operation_duration_seconds",
                "Completed domain operation duration."
            ),
            &["kind", "outcome"]
        ));
        let active = register!(IntGaugeVec::new(
            Opts::new("active", "Current domain resources by kind."),
            &["kind"]
        ));
        let bytes = register!(IntCounterVec::new(
            Opts::new("bytes_total", "Domain bytes processed by kind."),
            &["kind"]
        ));
        let events = register!(IntCounterVec::new(
            Opts::new("events_total", "Domain events by kind and outcome."),
            &["kind", "outcome"]
        ));
        let ready = register!(IntGauge::new(
            "ready",
            "Whether shutdown, workflow cleanup, and supervised background-task health allow readiness."
        ));
        let task_failures = register!(IntCounterVec::new(
            Opts::new(
                "background_task_failures_total",
                "Unexpected supervised background task exits."
            ),
            &["task"]
        ));
        let uptime = register!(IntGauge::new(
            "uptime_seconds",
            "Seconds since runtime initialization."
        ));
        let log_dropped = register!(IntGauge::new(
            "log_dropped_messages",
            "Log messages dropped by the bounded asynchronous log writer since startup."
        ));
        let log_file_failures = register!(IntGauge::new(
            "log_file_write_failures",
            "Service log file write, flush, or retention failures since logging initialization."
        ));
        let build_info = register!(IntGaugeVec::new(
            Opts::new("build_info", "Build identity; value is always one."),
            &["version"]
        ));
        build_info
            .with_label_values(&[env!("CARGO_PKG_VERSION")])
            .set(1);
        let start = register!(IntGauge::new(
            "start_time_seconds",
            "Runtime initialization time in Unix seconds."
        ));
        start.set(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
        );
        Self {
            registry,
            requests,
            header_duration,
            response_duration,
            responses,
            response_bytes,
            aborted,
            in_flight,
            operations,
            operation_duration,
            active,
            bytes,
            events,
            ready,
            ready_state: Arc::new(Mutex::new(false)),
            task_failures,
            uptime,
            started: Instant::now(),
            log_dropped,
            log_counter: Arc::new(Mutex::new(None)),
            log_file_failures,
            log_file_counter: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn operation_finished(
        &self,
        kind: &'static str,
        outcome: &'static str,
        duration: Duration,
    ) {
        self.operations.with_label_values(&[kind, outcome]).inc();
        self.operation_duration
            .with_label_values(&[kind, outcome])
            .observe(duration.as_secs_f64());
    }
    pub(crate) fn add_active(&self, kind: &'static str, delta: i64) {
        self.active.with_label_values(&[kind]).add(delta);
    }
    pub(crate) fn add_bytes(&self, kind: &'static str, bytes: u64) {
        self.bytes.with_label_values(&[kind]).inc_by(bytes);
    }
    pub(crate) fn event(&self, kind: &'static str, outcome: &'static str) {
        self.events.with_label_values(&[kind, outcome]).inc();
    }
    pub(crate) fn set_ready(&self, ready: bool) {
        let mut previous = self.ready_state.lock().expect("readiness state poisoned");
        if *previous == ready {
            return;
        }
        *previous = ready;
        self.ready.set(i64::from(ready));
        if ready {
            tracing::info!(
                event = "service.readiness_changed",
                ready,
                "service readiness changed"
            );
        } else {
            tracing::warn!(
                event = "service.readiness_changed",
                ready,
                "service readiness changed"
            );
        }
    }
    pub(crate) fn task_failed(&self, task: &'static str) {
        self.task_failures.with_label_values(&[task]).inc();
    }
    pub(crate) fn attach_log_error_counter(
        &self,
        counter: tracing_appender::non_blocking::ErrorCounter,
    ) {
        *self.log_counter.lock().expect("log counter lock poisoned") = Some(counter);
    }
    pub(crate) fn render(&self) -> Result<String, prometheus::Error> {
        self.uptime.set(self.started.elapsed().as_secs() as i64);
        if let Some(counter) = self
            .log_counter
            .lock()
            .expect("log counter lock poisoned")
            .as_ref()
        {
            self.log_dropped
                .set(counter.dropped_lines().min(i64::MAX as usize) as i64);
        }
        if let Some(counter) = self
            .log_file_counter
            .lock()
            .expect("log file counter lock poisoned")
            .as_ref()
        {
            self.log_file_failures.set(
                counter
                    .load(std::sync::atomic::Ordering::Relaxed)
                    .min(i64::MAX as u64) as i64,
            );
        }
        TextEncoder::new().encode_to_string(&self.registry.gather())
    }

    pub(crate) fn attach_log_file_error_counter(&self, counter: Arc<std::sync::atomic::AtomicU64>) {
        *self
            .log_file_counter
            .lock()
            .expect("log file counter lock poisoned") = Some(counter);
    }

    pub(crate) fn request_started(
        &self,
        method: &'static str,
        route: &'static str,
        request_id: String,
        span: tracing::Span,
    ) -> HttpObservation {
        self.in_flight.with_label_values(&[method, route]).inc();
        HttpObservation {
            telemetry: self.clone(),
            method,
            route,
            request_id,
            span,
            start: Instant::now(),
            status: None,
            error_code: None,
            diagnostic: None,
            bytes: 0,
            finished: false,
        }
    }
}

fn duration_opts(name: &str, help: &str) -> HistogramOpts {
    HistogramOpts::new(name, help).buckets(vec![
        0.005, 0.025, 0.1, 0.5, 1.0, 5.0, 15.0, 60.0, 300.0, 1800.0, 7200.0,
    ])
}

/// Moves from the middleware future into the response body. Drop also handles
/// cancelled handler futures and unpolled/disconnected streaming responses.
pub(crate) struct HttpObservation {
    telemetry: Observability,
    method: &'static str,
    route: &'static str,
    request_id: String,
    span: tracing::Span,
    start: Instant,
    status: Option<StatusCode>,
    error_code: Option<&'static str>,
    diagnostic: Option<String>,
    bytes: u64,
    finished: bool,
}

impl HttpObservation {
    pub(crate) fn headers(
        &mut self,
        status: StatusCode,
        error: Option<&crate::error::ErrorDiagnostic>,
    ) {
        self.status = Some(status);
        if let Some(error) = error {
            self.error_code = Some(error.code);
            self.diagnostic.clone_from(&error.diagnostic);
        }
        let labels = [self.method, self.route, status.as_str()];
        self.telemetry.requests.with_label_values(&labels).inc();
        self.telemetry
            .header_duration
            .with_label_values(&labels)
            .observe(self.start.elapsed().as_secs_f64());
        tracing::debug!(parent: &self.span, event = "http_response_headers", request_id = %self.request_id, method = self.method, route = self.route, status = status.as_u16(), elapsed_ms = self.start.elapsed().as_millis() as u64, "response headers generated");
    }
    pub(crate) fn add_bytes(&mut self, bytes: usize) {
        self.bytes += bytes as u64;
    }
    pub(crate) fn body_failed(&mut self, error: &str) {
        self.error_code = Some("response_body_error");
        self.diagnostic = Some(error.chars().take(4096).collect());
        self.finish("body_error");
    }
    pub(crate) fn finish(&mut self, outcome: &'static str) {
        if self.finished {
            return;
        }
        self.finished = true;
        let elapsed = self.start.elapsed();
        let status = self.status.as_ref().map_or("none", StatusCode::as_str);
        let labels = [self.method, self.route, status, outcome];
        self.telemetry.responses.with_label_values(&labels).inc();
        self.telemetry
            .response_duration
            .with_label_values(&labels)
            .observe(elapsed.as_secs_f64());
        self.telemetry
            .response_bytes
            .with_label_values(&[self.method, self.route, status])
            .inc_by(self.bytes);
        self.telemetry
            .in_flight
            .with_label_values(&[self.method, self.route])
            .dec();
        if outcome == "aborted" {
            self.telemetry
                .aborted
                .with_label_values(&[
                    self.method,
                    self.route,
                    if self.status.is_some() {
                        "body"
                    } else {
                        "handler"
                    },
                ])
                .inc();
        }
        macro_rules! record {
            ($level:ident) => { tracing::$level!(parent: &self.span, event = "http_response_finished", request_id = %self.request_id, method = self.method, route = self.route, status, outcome, duration_ms = elapsed.as_millis() as u64, response_bytes = self.bytes, error_code = self.error_code.unwrap_or(""), error = self.diagnostic.as_deref().unwrap_or(""), "HTTP request finished") };
        }
        if outcome == "body_error" || self.status.is_some_and(|status| status.is_server_error()) {
            record!(error);
        } else if outcome == "aborted" || self.status.is_some_and(|status| status.is_client_error())
        {
            record!(warn);
        } else if ["/healthz", "/readyz", "/metrics"].contains(&self.route) {
            record!(debug);
        } else {
            record!(info);
        }
    }
}

impl Drop for HttpObservation {
    fn drop(&mut self) {
        self.finish("aborted");
    }
}
