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
}
