//! Conditional and Boolean Operators.
//!
//! ReactiveX category: [`take_until`](crate::Observable::take_until) (TakeUntil).
//!
//! The poll-based combinator and the `Observable` method that exposes it both
//! live in this file.

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::Event;
use crate::Observable;

pin_project_lite::pin_project! {
    /// See [`Observable::take_until`](crate::Observable::take_until).
    pub struct TakeUntil<S, T, E> {
        #[pin]
        stream: S,
        token: crate::CancellationToken,
        done: bool,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, T, E> TakeUntil<S, T, E> {
    pub(super) fn new(stream: S, token: crate::CancellationToken) -> Self {
        Self {
            stream,
            token,
            done: false,
            _marker: PhantomData,
        }
    }
}

impl<S, T, E> futures_lite::Stream for TakeUntil<S, T, E>
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
    /// Emits a technical [`RpcError::Cancelled`](crate::RpcError::Cancelled) and
    /// stops as soon as `token` is cancelled (RxJS `takeUntil`).
    ///
    /// The token is checked before the source is polled, so a token cancelled
    /// beforehand ends the stream immediately, without reading a single value.
    /// The cancellation surfaces as a **technical** error, so a consumer sees an
    /// explicit end of stream rather than a silent truncation, and
    /// [`catch_error`](Self::catch_error) does not swallow it. The token is
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
    ///         .take_until(&token)
    ///         .collect(),
    /// );
    /// assert!(matches!(result, Err(ObservableError::Technical(_))));
    /// ```
    pub fn take_until(self, token: &crate::CancellationToken) -> Observable<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(TakeUntil::new(self, token.clone()))
    }
}
