//! Stream combination and resilience operators.
//!
//! Free functions that combine several streams ([`merge`]) or re-invoke an
//! underlying call on failure ([`retry`], [`retry_with`], [`retry_with_delay`]).
//! They are implemented as pull-based combinators (no channel, no task).

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_lite::future::FutureExt;
use ice_rpc::Event;

/// Boxed factory future and stream, used internally by [`Retry`].
type BoxedFuture<T, E> = Pin<Box<dyn Future<Output = ice_rpc::Observable<T, E>> + Send>>;
type BoxedStream<T, E> = Pin<Box<dyn futures_lite::Stream<Item = Event<T, E>> + Send>>;
type BoxedFactory<T, E> = Box<dyn FnMut() -> BoxedFuture<T, E> + Send>;

/// Merges multiple streams into one, forwarding events from all of them.
///
/// The returned stream closes once every source stream is consumed. Ordering
/// between sources is not deterministic.
pub fn merge<T, E, S>(streams: Vec<S>) -> Merge<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
    S: futures_lite::Stream<Item = Event<T, E>> + Send + 'static,
{
    Merge {
        streams: streams
            .into_iter()
            .map(|s| Box::pin(s) as BoxedStream<T, E>)
            .collect(),
        next: 0,
    }
}

/// See [`merge`].
pub struct Merge<T, E> {
    streams: Vec<BoxedStream<T, E>>,
    next: usize,
}

impl<T, E> futures_lite::Stream for Merge<T, E> {
    type Item = Event<T, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        while !this.streams.is_empty() {
            let idx = this.next % this.streams.len();
            this.next += 1;
            match futures_lite::Stream::poll_next(this.streams[idx].as_mut(), cx) {
                Poll::Ready(Some(event)) => return Poll::Ready(Some(event)),
                Poll::Ready(None) => {
                    let _ = this.streams.swap_remove(idx);
                }
                Poll::Pending => {}
            }
        }
        Poll::Ready(None)
    }
}

/// Retries the source factory on a business `Error`, up to `retries` times.
///
/// The factory is re-invoked on each retry, so it typically wraps a proxy call.
pub fn retry<T, E, Fut, F>(mut factory: F, retries: usize) -> Retry<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = ice_rpc::Observable<T, E>> + Send + 'static,
{
    let factory: BoxedFactory<T, E> = Box::new(move || Box::pin(factory()) as BoxedFuture<T, E>);
    Retry::new(factory, retries, |_| true, None)
}

/// Retries the source factory only when `should_retry` accepts the error.
pub fn retry_with<T, E, Fut, F, P>(mut factory: F, retries: usize, should_retry: P) -> Retry<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = ice_rpc::Observable<T, E>> + Send + 'static,
    P: Fn(&E) -> bool + Send + 'static,
{
    let factory: BoxedFactory<T, E> = Box::new(move || Box::pin(factory()) as BoxedFuture<T, E>);
    Retry::new(factory, retries, should_retry, None)
}

/// Retries the source factory, sleeping `delay` between attempts.
pub fn retry_with_delay<T, E, Fut, F>(
    mut factory: F,
    retries: usize,
    delay: std::time::Duration,
) -> Retry<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = ice_rpc::Observable<T, E>> + Send + 'static,
{
    let factory: BoxedFactory<T, E> = Box::new(move || Box::pin(factory()) as BoxedFuture<T, E>);
    Retry::new(factory, retries, |_| true, Some(delay))
}

/// See [`retry`].
pub struct Retry<T, E> {
    factory: BoxedFactory<T, E>,
    should_retry: Box<dyn Fn(&E) -> bool + Send>,
    retries: usize,
    delay: Option<std::time::Duration>,
    current: Option<BoxedStream<T, E>>,
    pending_factory: Option<BoxedFuture<T, E>>,
    delay_sleep: Option<futures_lite::future::Boxed<()>>,
    done: bool,
}

impl<T, E> Retry<T, E> {
    fn new<P>(
        factory: BoxedFactory<T, E>,
        retries: usize,
        should_retry: P,
        delay: Option<std::time::Duration>,
    ) -> Self
    where
        P: Fn(&E) -> bool + Send + 'static,
    {
        Self {
            factory,
            should_retry: Box::new(should_retry),
            retries,
            delay,
            current: None,
            pending_factory: None,
            delay_sleep: None,
            done: false,
        }
    }
}

impl<T, E> futures_lite::Stream for Retry<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
{
    type Item = Event<T, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        loop {
            if this.done {
                return Poll::Ready(None);
            }

            // Wait for the optional retry delay.
            if let Some(sleep) = this.delay_sleep.as_mut() {
                if sleep.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                this.delay_sleep = None;
            }

            // Drive the in-flight factory call.
            if let Some(mut fut) = this.pending_factory.take() {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(stream)) => {
                        let stream: BoxedStream<T, E> = Box::pin(stream);
                        this.current = Some(stream);
                    }
                    Poll::Ready(Err(e)) => {
                        this.done = true;
                        return Poll::Ready(Some(Event::RpcError(e)));
                    }
                    Poll::Pending => {
                        this.pending_factory = Some(fut);
                        return Poll::Pending;
                    }
                }
            }

            // Consume the current stream.
            if let Some(mut stream) = this.current.take() {
                match stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Event::Next(v))) => {
                        this.current = Some(stream);
                        return Poll::Ready(Some(Event::Next(v)));
                    }
                    Poll::Ready(Some(Event::Error(e))) => {
                        if this.retries > 0 && (this.should_retry)(&e) {
                            this.retries -= 1;
                            this.pending_factory = Some((this.factory)());
                            if let Some(d) = this.delay {
                                this.delay_sleep = Some(ice_rpc::rt::sleep(d).boxed());
                            }
                        } else {
                            this.done = true;
                            return Poll::Ready(Some(Event::Error(e)));
                        }
                    }
                    Poll::Ready(Some(other)) => {
                        this.done = true;
                        return Poll::Ready(Some(other));
                    }
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Pending => {
                        this.current = Some(stream);
                        return Poll::Pending;
                    }
                }
            }

            // Start a fresh call.
            this.pending_factory = Some((this.factory)());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::of;
    use ice_rpc::Event;

    async fn drain<S, T, E>(stream: S) -> Vec<Event<T, E>>
    where
        S: futures_lite::Stream<Item = Event<T, E>>,
    {
        let mut stream = Box::pin(stream);
        let mut out = Vec::new();
        while let Some(event) =
            futures_lite::future::poll_fn(|cx| futures_lite::Stream::poll_next(stream.as_mut(), cx))
                .await
        {
            out.push(event);
        }
        out
    }

    #[test]
    fn merge_combines_streams() {
        let stream = merge(vec![of(1), of(2)]);
        let events = pollster::block_on(drain(stream));

        let mut values = Vec::new();
        let mut completed = 0;
        for ev in events {
            match ev {
                Event::Next(v) => values.push(v),
                Event::Complete => completed += 1,
                other => panic!("unexpected event: {:?}", other),
            }
        }
        values.sort_unstable();
        assert_eq!(values, vec![1, 2]);
        assert_eq!(completed, 2);
    }

    #[test]
    fn retry_recovers_after_business_error() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();
        let factory = move || {
            let a = attempts_clone.clone();
            async move {
                let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                let (tx, rx) = ice_rpc::channel::<i32, String>(2);
                if n < 3 {
                    let _ = tx.try_send_error("boom".to_string());
                } else {
                    let _ = tx.try_send_next(42);
                    let _ = tx.try_send_complete();
                }
                Ok(rx)
            }
        };

        let stream = retry(factory, 2);
        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 42));
        assert!(matches!(&events[1], Event::Complete));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn retry_with_respects_predicate() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();
        let factory = move || {
            let a = attempts_clone.clone();
            async move {
                let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                let (tx, rx) = ice_rpc::channel::<i32, String>(1);
                if n == 1 {
                    let _ = tx.try_send_error("retryable".to_string());
                } else {
                    let _ = tx.try_send_error("fatal".to_string());
                }
                Ok(rx)
            }
        };

        let stream = retry_with(factory, 3, |e| e == "retryable");
        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Event::Error(e) if e == "fatal"));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
