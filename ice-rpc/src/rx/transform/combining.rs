//! Combining Observables.
//!
//! ReactiveX category: [`start_with`](crate::Observable::start_with) (operator).

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::Event;

pin_project_lite::pin_project! {
    /// See [`Observable::start_with`](crate::Observable::start_with).
    pub struct StartWith<S, T, E> {
        #[pin]
        stream: S,
        first: Option<T>,
        _marker: PhantomData<E>,
    }
}

impl<S, T, E> StartWith<S, T, E> {
    pub(super) fn new(stream: S, value: T) -> Self {
        Self {
            stream,
            first: Some(value),
            _marker: PhantomData,
        }
    }
}

impl<S, T, E> futures_lite::Stream for StartWith<S, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if let Some(value) = this.first.take() {
            return Poll::Ready(Some(Event::Next(value)));
        }
        futures_lite::Stream::poll_next(this.stream.as_mut(), cx)
    }
}
