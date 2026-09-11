//! Conditional and Boolean Operators.
//!
//! ReactiveX category: [`take_until`](super::RxStreamExt::take_until) (TakeUntil).

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use ice_rpc::Event;

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::take_until`](super::RxStreamExt::take_until).
    pub struct TakeUntil<S, T, E> {
        #[pin]
        stream: S,
        token: ice_rpc::CancellationToken,
        done: bool,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, T, E> TakeUntil<S, T, E> {
    pub(super) fn new(stream: S, token: ice_rpc::CancellationToken) -> Self {
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
            return Poll::Ready(Some(Event::Error(ice_rpc::ObservableError::Technical(
                ice_rpc::RpcError::Cancelled,
            ))));
        }
        futures_lite::Stream::poll_next(this.stream.as_mut(), cx)
    }
}
