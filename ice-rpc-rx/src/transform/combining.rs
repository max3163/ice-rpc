//! Combining Observables.
//!
//! ReactiveX category: [`start_with`](super::RxStreamExt::start_with) (operator)
//! and [`merge`] (free function).

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use ice_rpc::Event;

type BoxedStream<T, E> = Pin<Box<dyn futures_lite::Stream<Item = Event<T, E>> + Send>>;

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::start_with`](super::RxStreamExt::start_with).
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
