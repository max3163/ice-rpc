//! Combining Observables.
//!
//! ReactiveX category: [`start_with`](crate::Observable::start_with) (operator).
//!
//! The poll-based combinator and the `Observable` method that exposes it both
//! live in this file.

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::Event;
use crate::Observable;

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

impl<T, E> Observable<T, E> {
    /// Emits `value` before anything the source emits (RxJS `startWith`).
    ///
    /// The prefix is a single `Next(value)`, delivered before the source is
    /// polled even once. Useful to give a stream a known initial state — a
    /// header, a zero, the current cache — without inspecting the source.
    ///
    /// # Example
    /// ```rust
    /// use ice_rpc_rx::{from, rt::block_on};
    ///
    /// let with_header = block_on(
    ///     from::<i32, String, _>([1, 2]).start_with(0).collect(),
    /// )
    /// .expect("the stream completes cleanly");
    /// assert_eq!(with_header, vec![0, 1, 2]);
    /// ```
    pub fn start_with(self, value: T) -> Observable<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(StartWith::new(self, value))
    }
}
