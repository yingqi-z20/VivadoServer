//! Response ownership extends through streaming, not merely through the handler.
use axum::body::{Body, Bytes};
use http_body::{Frame, SizeHint};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio_util::sync::CancellationToken;

type StreamState<T> = Arc<Mutex<Option<(Body, T)>>>;

/// Keep request telemetry alive while a streaming body is retained by Hyper.
/// Completion records bytes yielded to the transport, not acknowledgement by a client.
pub(crate) fn observe(
    body: Body,
    mut observation: crate::observability::HttpObservation,
    expected_length: Option<u64>,
) -> Body {
    let expected_length = expected_length.or_else(|| http_body::Body::size_hint(&body).exact());
    if expected_length == Some(0) {
        observation.finish("complete");
        return body;
    }
    if http_body::Body::is_end_stream(&body) {
        if expected_length.is_some_and(|length| length > 0) {
            observation.body_failed("response body ended before its declared length");
        } else {
            observation.finish("complete");
        }
        return body;
    }
    Body::new(ObservedBody {
        body,
        observation,
        remaining: expected_length,
    })
}

struct ObservedBody {
    body: Body,
    observation: crate::observability::HttpObservation,
    remaining: Option<u64>,
}

impl http_body::Body for ObservedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.body).poll_frame(cx);
        match &result {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    this.observation.add_bytes(data.len());
                    if let Some(remaining) = &mut this.remaining {
                        if data.len() as u64 > *remaining {
                            this.observation
                                .body_failed("response body exceeded its declared length");
                        }
                        *remaining = remaining.saturating_sub(data.len() as u64);
                    }
                }
                // HTTP/1 may stop polling as soon as Content-Length is consumed,
                // without asking an unknown-size ReaderStream for its final EOF.
                if this.remaining == Some(0) {
                    this.observation.finish("complete");
                } else if this.body.is_end_stream() {
                    this.finish_at_eof();
                }
            }
            Poll::Ready(Some(Err(error))) => this.observation.body_failed(&error.to_string()),
            Poll::Ready(None) => this.finish_at_eof(),
            Poll::Pending => {}
        }
        result
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

impl ObservedBody {
    fn finish_at_eof(&mut self) {
        if self.remaining.is_some_and(|remaining| remaining > 0) {
            self.observation
                .body_failed("response body ended before its declared length");
        } else {
            self.observation.finish("complete");
        }
    }
}

pub(crate) fn hold<T: Send + Unpin + 'static>(
    body: Body,
    owner: T,
    cancel: Option<CancellationToken>,
) -> Body {
    let state = Arc::new(Mutex::new(Some((body, owner))));
    let done = CancellationToken::new();
    if let Some(cancel) = &cancel {
        let state = state.clone();
        let stopped = done.clone();
        let cancel = cancel.clone();
        // A stalled socket may never poll the body again. Cancellation therefore
        // releases the file and workflow lease independently of downstream polls.
        tokio::spawn(async move {
            tokio::select! {
                _ = stopped.cancelled() => {},
                _ = cancel.cancelled() => { state.lock().expect("stream state poisoned").take(); },
            }
        });
    }
    Body::new(HeldBody {
        state,
        done,
        ended: false,
        cancelled: cancel.map(|token| {
            Box::pin(token.cancelled_owned()) as Pin<Box<dyn Future<Output = ()> + Send>>
        }),
    })
}

struct HeldBody<T> {
    state: StreamState<T>,
    done: CancellationToken,
    ended: bool,
    cancelled: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<T> Drop for HeldBody<T> {
    fn drop(&mut self) {
        self.done.cancel();
        self.state.lock().expect("stream state poisoned").take();
    }
}

impl<T: Send + Unpin> http_body::Body for HeldBody<T> {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        if let Some(cancelled) = &mut this.cancelled
            && cancelled.as_mut().poll(cx).is_ready()
        {
            this.state.lock().expect("stream state poisoned").take();
            this.ended = true;
            this.done.cancel();
            return Poll::Ready(Some(Err(axum::Error::new(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "workflow cancelled",
            )))));
        }
        let mut state = this.state.lock().expect("stream state poisoned");
        let Some((body, _owner)) = state.as_mut() else {
            return Poll::Ready(None);
        };
        let result = Pin::new(&mut *body).poll_frame(cx);
        if matches!(&result, Poll::Ready(None) | Poll::Ready(Some(Err(_))))
            || (result.is_ready() && body.is_end_stream())
        {
            state.take();
            this.ended = true;
            this.done.cancel();
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.ended
            || self
                .state
                .lock()
                .expect("stream state poisoned")
                .as_ref()
                .is_some_and(|(body, _)| body.is_end_stream())
    }
    fn size_hint(&self) -> SizeHint {
        self.state
            .lock()
            .expect("stream state poisoned")
            .as_ref()
            .map_or_else(SizeHint::default, |(body, _)| body.size_hint())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use tokio::sync::Semaphore;

    struct PendingBody;
    impl http_body::Body for PendingBody {
        type Data = Bytes;
        type Error = std::io::Error;
        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            Poll::Pending
        }
    }

    #[tokio::test]
    async fn cancellation_releases_an_unpolled_stream_lease() {
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let cancel = CancellationToken::new();
        let mut body = hold(Body::new(PendingBody), permit, Some(cancel.clone()));
        assert_eq!(semaphore.available_permits(), 0);
        cancel.cancel();
        let _next = tokio::time::timeout(std::time::Duration::from_secs(2), semaphore.acquire())
            .await
            .expect("cancellation must not require downstream polling")
            .unwrap();
        assert!(body.frame().await.unwrap().is_err());
    }

    #[tokio::test]
    async fn dropping_an_incomplete_response_releases_http_capacity() {
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let body = hold(Body::new(PendingBody), permit, None);
        assert!(semaphore.try_acquire().is_err());
        drop(body);
        assert_eq!(semaphore.available_permits(), 1);
    }

    struct ChunkThenPending(bool);
    impl http_body::Body for ChunkThenPending {
        type Data = Bytes;
        type Error = std::io::Error;
        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            if self.0 {
                return Poll::Pending;
            }
            self.0 = true;
            Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(b"abc")))))
        }
    }

    fn http_observation(
        telemetry: &crate::observability::Observability,
    ) -> crate::observability::HttpObservation {
        let mut observation = telemetry.request_started(
            "GET",
            "/stream",
            "test-request".into(),
            tracing::Span::none(),
        );
        observation.headers(axum::http::StatusCode::OK, None);
        observation
    }

    #[tokio::test]
    async fn stream_metrics_remain_in_flight_until_body_drop_and_count_only_yielded_bytes() {
        let telemetry = crate::observability::Observability::new();
        let mut body = observe(
            Body::new(ChunkThenPending(false)),
            http_observation(&telemetry),
            None,
        );
        assert_eq!(
            body.frame().await.unwrap().unwrap().into_data().unwrap(),
            "abc"
        );
        let running = telemetry.render().unwrap();
        assert!(
            running.contains(
                "vivado_server_http_in_flight_requests{method=\"GET\",route=\"/stream\"} 1"
            )
        );
        assert!(running.contains(
            "vivado_server_http_requests_total{method=\"GET\",route=\"/stream\",status=\"200\"} 1"
        ));
        assert!(!running.contains("vivado_server_http_responses_total{"));
        drop(body);
        let completed = telemetry.render().unwrap();
        assert!(
            completed.contains(
                "vivado_server_http_in_flight_requests{method=\"GET\",route=\"/stream\"} 0"
            )
        );
        assert!(completed.contains("vivado_server_http_responses_total{method=\"GET\",outcome=\"aborted\",route=\"/stream\",status=\"200\"} 1"));
        assert!(completed.contains("vivado_server_http_response_bytes_total{method=\"GET\",route=\"/stream\",status=\"200\"} 3"));
        assert!(completed.contains("vivado_server_http_aborted_requests_total{method=\"GET\",phase=\"body\",route=\"/stream\"} 1"));
    }

    #[tokio::test]
    async fn exhausted_streams_are_completed_once_and_cancelled_streams_are_body_errors() {
        let telemetry = crate::observability::Observability::new();
        let body = observe(Body::from("abc"), http_observation(&telemetry), None);
        assert_eq!(body.collect().await.unwrap().to_bytes(), "abc");
        let completed = telemetry.render().unwrap();
        assert!(completed.contains("vivado_server_http_responses_total{method=\"GET\",outcome=\"complete\",route=\"/stream\",status=\"200\"} 1"));
        assert!(!completed.contains("outcome=\"aborted\""));

        let cancel = CancellationToken::new();
        let body = hold(Body::new(PendingBody), (), Some(cancel.clone()));
        let mut body = observe(body, http_observation(&telemetry), None);
        cancel.cancel();
        assert!(body.frame().await.unwrap().is_err());
        drop(body);
        let completed = telemetry.render().unwrap();
        assert!(completed.contains("vivado_server_http_responses_total{method=\"GET\",outcome=\"body_error\",route=\"/stream\",status=\"200\"} 1"));
        assert!(!completed.contains("outcome=\"aborted\""));
    }

    #[tokio::test]
    async fn declared_length_completes_without_an_extra_eof_poll_and_detects_short_bodies() {
        let telemetry = crate::observability::Observability::new();
        let mut body = observe(
            Body::new(ChunkThenPending(false)),
            http_observation(&telemetry),
            Some(3),
        );
        body.frame().await.unwrap().unwrap();
        drop(body);
        let text = telemetry.render().unwrap();
        assert!(text.contains("vivado_server_http_responses_total{method=\"GET\",outcome=\"complete\",route=\"/stream\",status=\"200\"} 1"));
        assert!(!text.contains("outcome=\"aborted\""));

        let body = observe(Body::from("abc"), http_observation(&telemetry), Some(6));
        body.collect().await.unwrap();
        let text = telemetry.render().unwrap();
        assert!(text.contains("vivado_server_http_responses_total{method=\"GET\",outcome=\"body_error\",route=\"/stream\",status=\"200\"} 1"));
    }

    #[test]
    fn cancelled_handler_future_releases_metrics_without_claiming_a_generated_response() {
        let telemetry = crate::observability::Observability::new();
        let observation = telemetry.request_started(
            "POST",
            "/operation",
            "cancelled-request".into(),
            tracing::Span::none(),
        );
        drop(observation);
        let text = telemetry.render().unwrap();
        assert!(text.contains(
            "vivado_server_http_in_flight_requests{method=\"POST\",route=\"/operation\"} 0"
        ));
        assert!(text.contains("vivado_server_http_aborted_requests_total{method=\"POST\",phase=\"handler\",route=\"/operation\"} 1"));
        assert!(!text.contains("vivado_server_http_requests_total{"));
    }
}
