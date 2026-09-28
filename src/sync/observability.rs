//! Sync metrics deliberately use static operation/outcome labels. Identifiers
//! and paths stay in tracing spans and never become metric dimensions.
use crate::{error::AppError, observability::Observability};
use axum::body::{Body, Bytes};
use http_body::{Body as HttpBody, Frame, SizeHint};
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Instant,
};

pub(super) struct SyncOperation {
    telemetry: Observability,
    kind: &'static str,
    started: Instant,
    span: tracing::Span,
    supervised: bool,
    failure_marker: Option<Arc<AtomicBool>>,
    finished: bool,
}

impl SyncOperation {
    pub(super) fn new(telemetry: Observability, kind: &'static str, supervised: bool) -> Self {
        telemetry.add_active(kind, 1);
        Self {
            telemetry,
            kind,
            started: Instant::now(),
            span: tracing::Span::current(),
            supervised,
            failure_marker: None,
            finished: false,
        }
    }

    pub(super) fn with_failure_marker(mut self, marker: Arc<AtomicBool>) -> Self {
        self.failure_marker = Some(marker);
        self
    }

    pub(super) fn finish_result<T>(&mut self, result: &Result<T, AppError>) {
        match result {
            Ok(_) => self.finish("success"),
            Err(error) => {
                self.span.in_scope(|| {
                    if !matches!(
                        self.kind,
                        "sync_reaper" | "sync_abort_project" | "sync_abort"
                    ) && matches!(
                        error,
                        AppError::Internal(_) | AppError::ProjectReuploadRequired(_)
                    ) {
                        tracing::error!(operation = self.kind, error_code = error.code(), %error,
                            "sync operation failed");
                    } else {
                        tracing::debug!(operation = self.kind, error_code = error.code(), %error,
                            "sync operation rejected");
                    }
                });
                self.finish("error");
            }
        }
    }

    pub(super) fn finish(&mut self, outcome: &'static str) {
        if self.finished {
            return;
        }
        self.finished = true;
        let duration = self.started.elapsed();
        self.telemetry
            .operation_finished(self.kind, outcome, duration);
        self.telemetry.add_active(self.kind, -1);
        self.span.in_scope(|| {
            tracing::debug!(
                operation = self.kind,
                outcome,
                elapsed_ms = duration.as_millis() as u64,
                "sync operation completed"
            );
        });
    }
}

impl Drop for SyncOperation {
    fn drop(&mut self) {
        if !self.finished {
            if self.supervised {
                if let Some(marker) = &self.failure_marker {
                    marker.store(true, Ordering::Release);
                }
                // TaskTracker keeps accepted work alive after its observer
                // disconnects. Early destruction here means panic/termination.
                self.telemetry.task_failed("sync_operation");
                self.span.in_scope(|| {
                    tracing::error!(
                        operation = self.kind,
                        "accepted sync operation terminated before settling"
                    );
                });
            }
            self.finish("cancelled");
        }
    }
}

/// Measure bytes only as the HTTP consumer polls data frames. Dropping an
/// unconsumed/partially consumed response records cancellation, not success.
/// Completion describes handing bytes to HTTP, not remote receipt or fsync.
pub(super) struct ObservedDownload {
    inner: Body,
    observed: SyncOperation,
    expected_bytes: u64,
    bytes: u64,
}

impl ObservedDownload {
    pub(super) fn new(inner: Body, expected_bytes: u64, observed: SyncOperation) -> Self {
        Self {
            inner,
            observed,
            expected_bytes,
            bytes: 0,
        }
    }

    fn finish(&mut self, outcome: &'static str) {
        if !self.observed.finished {
            self.observed.span.in_scope(|| {
                tracing::debug!(
                    bytes = self.bytes,
                    expected_bytes = self.expected_bytes,
                    outcome,
                    "sync download body completed"
                );
            });
            self.observed.finish(outcome);
        }
    }
}

impl HttpBody for ObservedDownload {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let result = Pin::new(&mut self.inner).poll_frame(cx);
        match &result {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    self.bytes = self.bytes.saturating_add(data.len() as u64);
                    self.observed
                        .telemetry
                        .add_bytes("sync_download", data.len() as u64);
                    // HTTP may stop polling at Content-Length without polling
                    // EOF. Count a fully yielded body exactly once in that case.
                    if self.bytes == self.expected_bytes {
                        self.finish("success");
                    }
                }
            }
            Poll::Ready(Some(Err(error))) => {
                self.observed.span.in_scope(|| {
                    tracing::error!(%error, bytes = self.bytes, "sync download body read failed");
                });
                self.finish("error");
            }
            Poll::Ready(None) => {
                if self.bytes == self.expected_bytes {
                    self.finish("success");
                } else {
                    self.observed.span.in_scope(|| {
                        tracing::warn!(
                            bytes = self.bytes,
                            expected_bytes = self.expected_bytes,
                            "sync download ended with a different length"
                        );
                    });
                    self.finish("error");
                }
            }
            Poll::Pending => {}
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for ObservedDownload {
    fn drop(&mut self) {
        // A zero-byte Content-Length response can be completed without a body
        // poll; no file bytes remain to be delivered in that case.
        self.finish(if self.expected_bytes == self.bytes {
            "success"
        } else {
            "cancelled"
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use std::collections::VecDeque;

    struct Frames(VecDeque<Result<Frame<Bytes>, std::io::Error>>);

    impl HttpBody for Frames {
        type Data = Bytes;
        type Error = std::io::Error;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            Poll::Ready(self.0.pop_front())
        }
    }

    #[tokio::test]
    async fn partial_download_error_records_actual_bytes_and_settles_once() {
        let telemetry = Observability::new();
        let frames = Frames(VecDeque::from([
            Ok(Frame::data(Bytes::from_static(b"part"))),
            Err(std::io::Error::other("injected read failure")),
        ]));
        let observed = SyncOperation::new(telemetry.clone(), "sync_download", false);
        let body = Body::new(ObservedDownload::new(Body::new(frames), 10, observed));
        assert!(body.collect().await.is_err());
        let metrics = telemetry.render().unwrap();
        assert!(metrics.contains("vivado_server_bytes_total{kind=\"sync_download\"} 4\n"));
        assert!(metrics.contains(
            "vivado_server_operations_total{kind=\"sync_download\",outcome=\"error\"} 1\n"
        ));
        assert!(metrics.contains("vivado_server_active{kind=\"sync_download\"} 0\n"));
        assert!(!metrics.contains("outcome=\"cancelled\""));
    }

    #[tokio::test]
    async fn dropping_partial_download_records_only_yielded_bytes() {
        let telemetry = Observability::new();
        let frames = Frames(VecDeque::from([
            Ok(Frame::data(Bytes::from_static(b"part"))),
            Ok(Frame::data(Bytes::from_static(b"unread"))),
        ]));
        let observed = SyncOperation::new(telemetry.clone(), "sync_download", false);
        let mut body = Body::new(ObservedDownload::new(Body::new(frames), 10, observed));
        assert_eq!(
            body.frame().await.unwrap().unwrap().data_ref().unwrap(),
            "part"
        );
        drop(body);
        let metrics = telemetry.render().unwrap();
        assert!(metrics.contains("vivado_server_bytes_total{kind=\"sync_download\"} 4\n"));
        assert!(metrics.contains(
            "vivado_server_operations_total{kind=\"sync_download\",outcome=\"cancelled\"} 1\n"
        ));
        assert!(metrics.contains("vivado_server_active{kind=\"sync_download\"} 0\n"));
    }
}
