//! Stream creation helpers.
//!
//! [`from`] and [`of`] build local poll-based streams, mirroring the RxJS
//! constructors of the same name. They are lazy: values are emitted only once
//! the consumer starts polling.

use std::pin::Pin;
use std::task::{Context, Poll};

use crate::RxError;
use ice_rpc::Event;

/// Creates a stream from an iterator, emitting each value as `Next` then
/// `Complete`.
///
/// Equivalent to RxJS `from`.
///
/// # Example
/// ```rust,ignore
/// use ice_rpc_rx::from;
///
/// let stream = from([1, 2, 3]);
/// ```
pub fn from<T, I>(iter: I) -> From<T>
where
    I: IntoIterator<Item = T>,
{
    From {
        values: iter.into_iter().collect::<Vec<_>>().into_iter(),
        done: false,
    }
}

pin_project_lite::pin_project! {
    /// See [`from`].
    pub struct From<T> {
        values: std::vec::IntoIter<T>,
        done: bool,
    }
}

impl<T> futures_lite::Stream for From<T> {
    type Item = Event<T, RxError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if *this.done {
            return Poll::Ready(None);
        }
        match this.values.next() {
            Some(v) => Poll::Ready(Some(Event::Next(v))),
            None => {
                *this.done = true;
                Poll::Ready(Some(Event::Complete))
            }
        }
    }
}

/// Creates a single-value stream.
///
/// Consumers observe the value as `Next` followed by `Complete`. Equivalent to
/// RxJS `of`.
///
/// # Example
/// ```rust,ignore
/// use ice_rpc_rx::of;
///
/// let stream = of(42);
/// ```
pub fn of<T>(value: T) -> Of<T> {
    Of {
        value: Some(value),
        completed: false,
    }
}

pin_project_lite::pin_project! {
    /// See [`of`].
    pub struct Of<T> {
        value: Option<T>,
        completed: bool,
    }
}

impl<T> futures_lite::Stream for Of<T> {
    type Item = Event<T, RxError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if let Some(v) = this.value.take() {
            return Poll::Ready(Some(Event::Next(v)));
        }
        if *this.completed {
            return Poll::Ready(None);
        }
        *this.completed = true;
        Poll::Ready(Some(Event::Complete))
    }
}
