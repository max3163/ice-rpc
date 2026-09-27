//! Error Handling Operators.
//!
//! ReactiveX category: [`catch_error`](crate::Observable::catch_error) (Catch).
//!
//! The poll-based combinator and the `Observable` method that exposes it both
//! live in this file.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::Event;
use crate::Observable;
use crate::ObservableError;
use futures_lite::future::FutureExt;

pin_project_lite::pin_project! {
    /// See [`Observable::catch_error`](crate::Observable::catch_error).
    pub struct CatchError<S, F, T, E> {
        #[pin]
        stream: S,
        // The selector, consumed by the first business error (`FnOnce`).
        f: Option<F>,
        // The recovery stream: it replaces the failed source entirely, values
        // and terminal included.
        #[pin]
        recovery: Option<Observable<T, E>>,
        // Set once our own terminal has been emitted.
        finished: bool,
    }
}

impl<S, F, T, E> CatchError<S, F, T, E> {
    pub(super) fn new(stream: S, f: F) -> Self {
        Self {
            stream,
            f: Some(f),
            recovery: None,
            finished: false,
        }
    }
}

impl<S, F, T, E> futures_lite::Stream for CatchError<S, F, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    F: FnOnce(E) -> Observable<T, E>,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if *this.finished {
                return Poll::Ready(None);
            }

            // Recovering: the recovery stream is the only source left, and its
            // terminal is ours.
            if this.recovery.is_some() {
                let polled = {
                    let recovery = this
                        .recovery
                        .as_mut()
                        .as_pin_mut()
                        .expect("recovery stream");
                    futures_lite::Stream::poll_next(recovery, cx)
                };
                return match polled {
                    Poll::Ready(Some(event)) => {
                        if event.is_terminal() {
                            *this.finished = true;
                        }
                        Poll::Ready(Some(event))
                    }
                    Poll::Ready(None) => {
                        *this.finished = true;
                        Poll::Ready(None)
                    }
                    Poll::Pending => Poll::Pending,
                };
            }

            match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
                Poll::Ready(Some(Event::Next(v))) => return Poll::Ready(Some(Event::Next(v))),
                // Only a business error is recoverable; a technical error and
                // `Empty` are fatal, so they travel untouched.
                Poll::Ready(Some(Event::Error(crate::ObservableError::Business(e)))) => {
                    // The selector runs exactly once: an error raised by the
                    // recovery is not caught again, as in RxJS.
                    let f = this
                        .f
                        .take()
                        .expect("the selector runs when the first business error arrives");
                    this.recovery.set(Some(f(e)));
                }
                Poll::Ready(Some(Event::Error(other))) => {
                    *this.finished = true;
                    return Poll::Ready(Some(Event::Error(other)));
                }
                Poll::Ready(Some(Event::Complete)) => {
                    *this.finished = true;
                    return Poll::Ready(Some(Event::Complete));
                }
                // An abrupt close ends the stream for good: `finished` is set, so
                // a later poll never touches the dead source again.
                Poll::Ready(None) => {
                    *this.finished = true;
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<T, E> Observable<T, E> {
    /// Replaces a **business** error with a fallback **stream** (RxJS
    /// `catchError`).
    ///
    /// On `ObservableError::Business(e)`, `f(e)` is subscribed in place of the
    /// failed source: its values **and** its terminal become the output. That is
    /// what makes an asynchronous recovery possible — a call to a replica, a
    /// delayed retry — and a rethrow possible too, through
    /// [`throw_error`](crate::throw_error).
    ///
    /// A technical error is **not** caught: it is forwarded and terminates the
    /// stream, because a service implementation cannot recover from a transport,
    /// discovery or protocol failure. `f` runs **at most once**: an error raised
    /// by the recovery is not caught again, exactly as in RxJS.
    ///
    /// # Example
    /// ```rust
    /// use ice_rpc_rx::{of, rt::block_on, throw_error};
    ///
    /// let recovered = block_on(
    ///     throw_error::<i32, String>("missing".to_string())
    ///         .catch_error(|_e| of(-1))
    ///         .collect(),
    /// )
    /// .expect("the failure becomes a value, then Complete");
    /// assert_eq!(recovered, vec![-1]);
    /// ```
    ///
    /// A recovery is a stream: it may emit several values, and it may fail in
    /// turn — its error then travels downstream, uncaught.
    /// ```rust
    /// use ice_rpc_rx::{from, rt::block_on, throw_error};
    ///
    /// let values = block_on(
    ///     throw_error::<i32, String>("missing".to_string())
    ///         .catch_error(|_e| from([1, 2]))
    ///         .collect(),
    /// )
    /// .expect("the recovery completes cleanly");
    /// assert_eq!(values, vec![1, 2]);
    /// ```
    pub fn catch_error<F>(self, f: F) -> Observable<T, E>
    where
        F: FnOnce(E) -> Observable<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(CatchError::new(self, f))
    }
}

/// Policy of [`retry_with`].
///
/// A `Default` policy **retries nothing**: one attempt, no delay, every error
/// eligible — the eligibility only matters from the second attempt on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total number of attempts. `1` means "try once, never again"; `0` behaves
    /// as `1`.
    pub attempts: usize,
    /// Silence observed before every re-attempt (`Duration::ZERO` = at once).
    pub delay: Duration,
    /// Restrict the retry to the failures
    /// [`RpcError::is_retryable`](crate::RpcError::is_retryable) accepts. When
    /// `false` — the default — **every** error is retried, business and
    /// technical alike, as in RxJS.
    pub only_retryable: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            attempts: 1,
            delay: Duration::ZERO,
            only_retryable: false,
        }
    }
}

/// Runs `factory` again on every failure, up to the policy's attempts (RxJS
/// `retry`).
///
/// Each attempt builds a **fresh** stream, so the values an attempt emitted
/// before failing stay emitted and are followed by the next attempt's:
/// duplication is assumed, exactly as in RxJS, where `retry` resubscribes the
/// source. A `Complete` is final — only an error earns another attempt — and
/// once the policy gives up, the last error travels downstream unchanged.
///
/// By default every error is retried. [`RetryPolicy::only_retryable`] restricts
/// it to the failures [`RpcError::is_retryable`](crate::RpcError::is_retryable)
/// accepts, which is what a service wanting "retry the transport, not the
/// service" asks for.
///
/// The factory is the one [`defer`](crate::defer) takes, so a service call is
/// passed as it is; the retry lives **here** rather than on the stream, because
/// an `Observable` is single-subscription and cannot be replayed.
///
/// # Example
/// ```rust
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// use std::sync::Arc;
///
/// use ice_rpc_rx::{of, retry_with, rt::block_on, throw_error, Observable, RetryPolicy};
///
/// let calls = Arc::new(AtomicUsize::new(0));
/// let attempt = Arc::clone(&calls);
/// let stream: Observable<i32, String> = retry_with(
///     move || {
///         let n = attempt.fetch_add(1, Ordering::SeqCst);
///         async move {
///             if n == 0 {
///                 throw_error("boom".to_string())
///             } else {
///                 of(42)
///             }
///         }
///     },
///     RetryPolicy {
///         attempts: 2,
///         ..RetryPolicy::default()
///     },
/// );
///
/// assert_eq!(block_on(stream.collect()).expect("the retry recovered"), vec![42]);
/// assert_eq!(calls.load(Ordering::SeqCst), 2);
/// ```
pub fn retry_with<T, E, F, Fut>(factory: F, policy: RetryPolicy) -> Observable<T, E>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Observable<T, E>> + Send + 'static,
    T: Send + 'static,
    E: Send + 'static,
{
    Observable::from_stream(Retrying::new(factory, policy))
}

/// Whether another attempt is allowed for `error`.
fn should_retry<E>(attempts: usize, policy: &RetryPolicy, error: &ObservableError<E>) -> bool {
    if attempts >= policy.attempts.max(1) {
        return false;
    }
    if !policy.only_retryable {
        return true;
    }
    matches!(error, ObservableError::Technical(transport) if transport.is_retryable())
}

pin_project_lite::pin_project! {
    /// See [`retry_with`].
    struct Retrying<F, Fut, T, E> {
        factory: F,
        policy: RetryPolicy,
        // Attempts started so far.
        attempts: usize,
        // The factory's future, until it yields the attempt's stream.
        #[pin]
        pending: Option<Fut>,
        // The stream of the current attempt.
        #[pin]
        stream: Option<Observable<T, E>>,
        // Pause before the next attempt. Not `#[pin]`: a `Boxed` future is
        // `Unpin`, and polling it only needs `&mut` (see `Delay`).
        sleep: Option<futures_lite::future::Boxed<()>>,
        // Our terminal has been emitted: no further attempt.
        finished: bool,
    }
}

impl<F, Fut, T, E> Retrying<F, Fut, T, E> {
    fn new(factory: F, policy: RetryPolicy) -> Self {
        Self {
            factory,
            policy,
            attempts: 0,
            pending: None,
            stream: None,
            sleep: None,
            finished: false,
        }
    }
}

impl<F, Fut, T, E> futures_lite::Stream for Retrying<F, Fut, T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Observable<T, E>>,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if *this.finished {
                return Poll::Ready(None);
            }

            // Pause between two attempts, when the policy asks for one.
            if let Some(sleep) = this.sleep.as_mut() {
                match sleep.as_mut().poll(cx) {
                    Poll::Ready(()) => *this.sleep = None,
                    Poll::Pending => return Poll::Pending,
                }
            }

            // Start an attempt: the factory builds a fresh stream every time.
            if this.pending.is_none() && this.stream.is_none() {
                *this.attempts += 1;
                let future = (this.factory)();
                this.pending.set(Some(future));
            }

            // Await the factory's future, then forward the attempt's events.
            if let Some(pending) = this.pending.as_mut().as_pin_mut() {
                match pending.poll(cx) {
                    Poll::Ready(observable) => {
                        this.pending.set(None);
                        this.stream.set(Some(observable));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            if let Some(stream) = this.stream.as_mut().as_pin_mut() {
                match futures_lite::Stream::poll_next(stream, cx) {
                    Poll::Ready(Some(Event::Next(value))) => {
                        return Poll::Ready(Some(Event::Next(value)));
                    }
                    // Only an error earns another attempt; the values the failed
                    // attempt emitted stay emitted, as in RxJS.
                    Poll::Ready(Some(Event::Error(error))) => {
                        this.stream.set(None);
                        if should_retry(*this.attempts, this.policy, &error) {
                            if this.policy.delay > Duration::ZERO {
                                *this.sleep = Some(crate::rt::sleep(this.policy.delay).boxed());
                            }
                            continue;
                        }
                        *this.finished = true;
                        return Poll::Ready(Some(Event::Error(error)));
                    }
                    Poll::Ready(Some(Event::Complete)) => {
                        this.stream.set(None);
                        *this.finished = true;
                        return Poll::Ready(Some(Event::Complete));
                    }
                    Poll::Ready(None) => {
                        this.stream.set(None);
                        *this.finished = true;
                        return Poll::Ready(None);
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
