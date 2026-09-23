//! Combining Observables.
//!
//! ReactiveX category: [`merge`](crate::Observable::merge) and
//! [`start_with`](crate::Observable::start_with).
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

pin_project_lite::pin_project! {
    /// See [`Observable::merge`](crate::Observable::merge).
    pub struct Merge<A, B, T, E> {
        #[pin]
        left: A,
        #[pin]
        right: B,
        left_done: bool,
        right_done: bool,
        completed: bool,
        // Alternates which source is polled first.
        left_first: bool,
        _marker: PhantomData<(T, E)>,
    }
}

impl<A, B, T, E> Merge<A, B, T, E> {
    pub(super) fn new(left: A, right: B) -> Self {
        Self {
            left,
            right,
            left_done: false,
            right_done: false,
            completed: false,
            left_first: true,
            _marker: PhantomData,
        }
    }
}

impl<A, B, T, E> futures_lite::Stream for Merge<A, B, T, E>
where
    A: futures_lite::Stream<Item = Event<T, E>>,
    B: futures_lite::Stream<Item = Event<T, E>>,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            // One `Complete` is synthesized when the **last** source ends, and
            // the merge is over for good afterwards.
            if *this.completed {
                return Poll::Ready(None);
            }
            if *this.left_done && *this.right_done {
                *this.completed = true;
                return Poll::Ready(Some(Event::Complete));
            }

            // Alternate the poll order: without it, a source that is always
            // ready would starve the other one for as long as it produces.
            let left_first = *this.left_first;
            *this.left_first = !left_first;

            for left in [left_first, !left_first] {
                let ready = if left {
                    if *this.left_done {
                        continue;
                    }
                    futures_lite::Stream::poll_next(this.left.as_mut(), cx)
                } else {
                    if *this.right_done {
                        continue;
                    }
                    futures_lite::Stream::poll_next(this.right.as_mut(), cx)
                };

                match ready {
                    Poll::Ready(Some(Event::Next(value))) => {
                        return Poll::Ready(Some(Event::Next(value)));
                    }
                    // A terminal error is fatal for the whole merge: the other
                    // source is dropped with the pipeline, as in RxJS.
                    Poll::Ready(Some(Event::Error(error))) => {
                        *this.completed = true;
                        return Poll::Ready(Some(Event::Error(error)));
                    }
                    // One source is over; the other may still produce, so the
                    // merged stream does not end here.
                    Poll::Ready(Some(Event::Complete)) | Poll::Ready(None) => {
                        if left {
                            *this.left_done = true;
                        } else {
                            *this.right_done = true;
                        }
                    }
                    Poll::Pending => {}
                }
            }

            // Both sources ended during this pass: go back and emit the single
            // `Complete` rather than report an end of stream.
            if *this.left_done && *this.right_done {
                continue;
            }
            return Poll::Pending;
        }
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

    /// Emits the values of `other` as they come, interleaved with this stream
    /// (RxJS `merge`).
    ///
    /// Both sources are polled on every poll, in a **round-robin** order: a
    /// source that is always ready cannot starve the other one, so the
    /// interleaving stays fair and deterministic.
    ///
    /// # Terminals
    ///
    /// - a terminal `Error` from **either** source ends the merged stream
    ///   immediately, and the other source is dropped with the pipeline — the
    ///   rule RxJS applies;
    /// - a `Complete` is **swallowed**: the merged stream ends only when the
    ///   **last** of the two sources ends, and it emits exactly one `Complete`
    ///   of its own. A source that closes without a terminal is read as a clean
    ///   end, the convention [`Observable::first_value`](crate::Observable::first_value)
    ///   already follows.
    ///
    /// RxJS exposes `merge` as a creation function; here it is an inherent
    /// method like every other operator, so the left operand is the receiver.
    ///
    /// # Example
    /// ```rust
    /// use ice_rpc_rx::{from, rt::block_on};
    ///
    /// // Both sources are inline, hence always ready: the fair interleaving is
    /// // exactly 1, 10, 2, 20.
    /// let merged = block_on(
    ///     from::<i32, String, _>([1, 2])
    ///         .merge(from::<i32, String, _>([10, 20]))
    ///         .collect(),
    /// )
    /// .expect("the stream completes cleanly");
    /// assert_eq!(merged, vec![1, 10, 2, 20]);
    /// ```
    ///
    /// # See also
    /// [`switch_map`](Observable::switch_map) projects to a *new* inner stream
    /// per value instead of merging two fixed ones.
    pub fn merge(self, other: Observable<T, E>) -> Observable<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(Merge::new(self, other))
    }
}

#[cfg(test)]
mod tests;
