//! Error Handling Operators.
//!
//! ReactiveX category: [`catch_error`](super::RxStreamExt::catch_error) (Catch)
//! and [`retry`] / [`retry_with`] / [`retry_with_delay`] (Retry).

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_lite::future::FutureExt;
use ice_rpc::Event;

/// Boxed factory future and stream, used internally by [`Retry`].
type BoxedFuture<T, E> = Pin<Box<dyn Future<Output = ice_rpc::Observable<T, E>> + Send>>;
type BoxedStream<T, E> = Pin<Box<dyn futures_lite::Stream<Item = Event<T, E>> + Send>>;
type BoxedFactory<T, E> = Box<dyn FnMut() -> BoxedFuture<T, E> + Send>;

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::catch_error`](super::RxStreamExt::catch_error).
    pub struct CatchError<S, F, T, E> {
        #[pin]
        stream: S,
        f: Option<F>,
        completed: bool,
        done: bool,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, F, T, E> CatchError<S, F, T, E> {
    pub(super) fn new(stream: S, f: F) -> Self {
        Self {
            stream,
            f: Some(f),
            completed: false,
            done: false,
            _marker: PhantomData,
        }
    }
}

impl<S, F, T, E> futures_lite::Stream for CatchError<S, F, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    F: FnOnce(E) -> T,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.done {
            return Poll::Ready(None);
        }
        if *this.completed {
            *this.done = true;
            return Poll::Ready(Some(Event::Complete));
        }
        match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
            Poll::Ready(Some(Event::Next(v))) => Poll::Ready(Some(Event::Next(v))),
            // Only a business error can be caught; a technical error is fatal.
            Poll::Ready(Some(Event::Error(ice_rpc::ObservableError::Business(e)))) => {
                match this.f.take() {
                    Some(f) => {
                        *this.completed = true;
                        Poll::Ready(Some(Event::Next(f(e))))
                    }
                    None => {
                        *this.done = true;
                        Poll::Ready(Some(Event::Complete))
                    }
                }
            }
            Poll::Ready(Some(other)) => {
                *this.done = true;
                Poll::Ready(Some(other))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
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
                    Poll::Ready(stream) => {
                        let stream: BoxedStream<T, E> = Box::pin(stream);
                        this.current = Some(stream);
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
                    // Only a business error is retryable; a technical error is fatal.
                    Poll::Ready(Some(Event::Error(ice_rpc::ObservableError::Business(e)))) => {
                        if this.retries > 0 && (this.should_retry)(&e) {
                            this.retries -= 1;
                            this.pending_factory = Some((this.factory)());
                            if let Some(d) = this.delay {
                                this.delay_sleep = Some(ice_rpc::rt::sleep(d).boxed());
                            }
                        } else {
                            this.done = true;
                            return Poll::Ready(Some(Event::Error(
                                ice_rpc::ObservableError::Business(e),
                            )));
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
