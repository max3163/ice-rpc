//! Conditional and Boolean Operators.
//!
//! ReactiveX category: [`take_until`](crate::Observable::take_until) (TakeUntil).

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::Event;

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
