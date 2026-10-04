//! Conditional and Boolean Operators.
//!
//! ReactiveX category: [`take_until`](crate::Observable::take_until) (TakeUntil)
//! and its crate-specific sibling
//! [`take_until_token`](crate::Observable::take_until_token).
//!
//! The poll-based combinator and the `Observable` method that exposes it both
//! live in this file.

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::Event;
use crate::Observable;
use crate::ObservableInput;

pin_project_lite::pin_project! {
    /// See [`Observable::take_until`](crate::Observable::take_until).
    pub struct TakeUntil<S, U, T, E> {
        #[pin]
        stream: S,
        // The notifier, polled first and dropped as soon as it is over: one that
        // completes without a value never stops the source (RxJS).
        #[pin]
        notifier: Option<Observable<U, E>>,
        // The notifier fired: the source is over.
        stopped: bool,
        // Our terminal has been emitted.
        done: bool,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, U, T, E> TakeUntil<S, U, T, E> {
    pub(super) fn new(stream: S, notifier: Observable<U, E>) -> Self {
        Self {
            stream,
            notifier: Some(notifier),
            stopped: false,
            done: false,
            _marker: PhantomData,
        }
    }
}

impl<S, U, T, E> futures_lite::Stream for TakeUntil<S, U, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.done {
            return Poll::Ready(None);
        }

        // The notifier is polled first, so a notifier ready at the same instant
        // as a source value wins — the rule RxJS applies since v7.
        if this.notifier.is_some() {
            let polled = {
                let notifier = this.notifier.as_mut().as_pin_mut().expect("notifier");
                futures_lite::Stream::poll_next(notifier, cx)
            };
            match polled {
                Poll::Ready(Some(Event::Next(_))) => {
                    this.notifier.set(None);
                    *this.stopped = true;
                }
                // A notifier that ends without a value never stops the source.
                Poll::Ready(Some(Event::Complete)) | Poll::Ready(None) => {
                    this.notifier.set(None);
                }
                // An error travels downstream like any other terminal.
                Poll::Ready(Some(Event::Error(e))) => {
                    this.notifier.set(None);
                    *this.done = true;
                    return Poll::Ready(Some(Event::Error(e)));
                }
                Poll::Pending => {}
            }
        }

        if *this.stopped {
            *this.done = true;
            return Poll::Ready(Some(Event::Complete));
        }

        futures_lite::Stream::poll_next(this.stream.as_mut(), cx)
    }
}

pin_project_lite::pin_project! {
    /// See [`Observable::take_until_token`](crate::Observable::take_until_token).
    pub struct TakeUntilToken<S, T, E> {
        #[pin]
        stream: S,
        token: crate::CancellationToken,
        done: bool,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, T, E> TakeUntilToken<S, T, E> {
    pub(super) fn new(stream: S, token: crate::CancellationToken) -> Self {
        Self {
            stream,
            token,
            done: false,
            _marker: PhantomData,
        }
    }
}

impl<S, T, E> futures_lite::Stream for TakeUntilToken<S, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.done {
            return Poll::Ready(None);
        }
        if this.token.is_cancelled() {
            *this.done = true;
            return Poll::Ready(Some(Event::Error(crate::ObservableError::Technical(
                crate::RpcError::Cancelled,
            ))));
        }
        futures_lite::Stream::poll_next(this.stream.as_mut(), cx)
    }
}

impl<T, E> Observable<T, E> {
    /// Completes as soon as `notifier` emits its first value (RxJS `takeUntil`).
    ///
    /// The stop is a **completion**, not a failure: the values already emitted
    /// are kept, so `source.take_until(stop).collect()` returns the prefix
    /// gathered so far. The notifier is polled before the source, so a notifier
    /// already carrying a value wins even when the source has one ready.
    ///
    /// A notifier that completes without ever emitting is **ignored** — the
    /// source then runs to its own end — and an error from the notifier travels
    /// downstream like any other terminal. The notifier's own values are
    /// discarded.
    ///
    /// To abort a call on a [`CancellationToken`](crate::CancellationToken) —
    /// the crate's cancellation primitive, which reports a technical
    /// [`RpcError::Cancelled`](crate::RpcError::Cancelled) — use
    /// [`take_until_token`](Self::take_until_token).
    ///
    /// # Example
    /// ```rust
    /// use ice_rpc_rx::{from, rt::block_on};
    ///
    /// // A notifier that is already firing: the source is cut before it emits,
    /// // and the stream still completes cleanly.
    /// let (stop_tx, stop_rx) = ice_rpc_rx::channel::<(), String>(1);
    /// stop_tx.try_send_next(()).expect("the channel has room");
    /// drop(stop_tx);
    ///
    /// let values = block_on(from::<i32, String, _>([1, 2, 3]).take_until(stop_rx).collect())
    ///     .expect("a stop is a completion, not a failure");
    /// assert!(values.is_empty());
    /// ```
    ///
    /// The notifier accepts an [`ObservableInput`](crate::ObservableInput): an
    /// [`Observable`], or a **future** that produces one — the shape of an
    /// `async fn` call, so a stream can be stopped on an asynchronous signal:
    /// ```rust
    /// use ice_rpc_rx::{from, of, rt::block_on, Observable};
    ///
    /// async fn stop() -> Observable<(), String> {
    ///     of(())
    /// }
    ///
    /// let values = block_on(
    ///     from::<i32, String, _>([1, 2, 3]).take_until(stop()).collect(),
    /// )
    /// .expect("a stop is a completion, not a failure");
    /// assert!(values.is_empty());
    /// ```
    pub fn take_until<U, I>(self, notifier: I) -> Observable<T, E>
    where
        I: ObservableInput<U, E>,
        U: Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        // The notifier is normalized here: `TakeUntil` only ever sees an
        // `Observable`, whatever the caller handed in.
        Observable::from_stream(TakeUntil::new(self, notifier.into_observable()))
    }

    /// Emits a technical [`RpcError::Cancelled`](crate::RpcError::Cancelled) and
    /// stops as soon as `token` is cancelled.
    ///
    /// The crate-specific sibling of [`take_until`](Self::take_until): this is
    /// what **abandons an in-flight call**, so its terminal is an error — a
    /// cancelled call is not a completed one — and
    /// [`catch_error`](Self::catch_error) does not swallow it. The token is
    /// checked before the source is polled, so a token cancelled beforehand ends
    /// the stream immediately, without reading a single value. The token is
    /// cloned into the operator, so the caller keeps ownership.
    ///
    /// # Example
    /// ```rust
    /// use ice_rpc_rx::{from, rt::block_on, CancellationToken, ObservableError};
    ///
    /// let token = CancellationToken::new();
    /// token.cancel();
    /// let result = block_on(
    ///     from::<i32, String, _>([1, 2, 3])
    ///         .take_until_token(&token)
    ///         .collect(),
    /// );
    /// assert!(matches!(result, Err(ObservableError::Technical(_))));
    /// ```
    pub fn take_until_token(self, token: &crate::CancellationToken) -> Observable<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(TakeUntilToken::new(self, token.clone()))
    }
}

#[cfg(test)]
mod tests;
