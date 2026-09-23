//! Filtering Observables.
//!
//! ReactiveX category: [`filter`](crate::Observable::filter),
//! [`take`](crate::Observable::take), [`skip`](crate::Observable::skip),
//! [`first`](crate::Observable::first) and [`first_with`](crate::Observable::first_with).
//!
//! The poll-based combinator and the `Observable` method that exposes it both
//! live in this file.

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::Event;
use crate::Observable;

pin_project_lite::pin_project! {
    /// See [`Observable::filter`](crate::Observable::filter).
    pub struct Filter<S, F, T, E> {
        #[pin]
        stream: S,
        f: F,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, F, T, E> Filter<S, F, T, E> {
    pub(super) fn new(stream: S, f: F) -> Self {
        Self {
            stream,
            f,
            _marker: PhantomData,
        }
    }
}

impl<S, F, T, E> futures_lite::Stream for Filter<S, F, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    F: FnMut(&T) -> bool,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
                Poll::Ready(Some(Event::Next(v))) => {
                    if (this.f)(&v) {
                        return Poll::Ready(Some(Event::Next(v)));
                    }
                }
                Poll::Ready(Some(other)) => return Poll::Ready(Some(other)),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`Observable::take`](crate::Observable::take).
    pub struct Take<S, T, E> {
        #[pin]
        stream: S,
        remaining: usize,
        done: bool,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, T, E> Take<S, T, E> {
    pub(super) fn new(stream: S, n: usize) -> Self {
        Self {
            stream,
            remaining: n,
            done: false,
            _marker: PhantomData,
        }
    }
}

impl<S, T, E> futures_lite::Stream for Take<S, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.done {
            return Poll::Ready(None);
        }
        match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
            Poll::Ready(Some(Event::Next(v))) => {
                if *this.remaining == 0 {
                    *this.done = true;
                    return Poll::Ready(Some(Event::Complete));
                }
                *this.remaining -= 1;
                Poll::Ready(Some(Event::Next(v)))
            }
            Poll::Ready(Some(other)) => Poll::Ready(Some(other)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`Observable::skip`](crate::Observable::skip).
    pub struct Skip<S, T, E> {
        #[pin]
        stream: S,
        remaining: usize,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, T, E> Skip<S, T, E> {
    pub(super) fn new(stream: S, n: usize) -> Self {
        Self {
            stream,
            remaining: n,
            _marker: PhantomData,
        }
    }
}

impl<S, T, E> futures_lite::Stream for Skip<S, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
                Poll::Ready(Some(Event::Next(v))) => {
                    if *this.remaining > 0 {
                        *this.remaining -= 1;
                    } else {
                        return Poll::Ready(Some(Event::Next(v)));
                    }
                }
                Poll::Ready(Some(other)) => return Poll::Ready(Some(other)),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`Observable::first_with`](crate::Observable::first_with).
    pub struct First<S, F, T, E> {
        #[pin]
        stream: S,
        f: F,
        completed: bool,
        done: bool,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, F, T, E> First<S, F, T, E> {
    pub(super) fn new(stream: S, f: F) -> Self {
        Self {
            stream,
            f,
            completed: false,
            done: false,
            _marker: PhantomData,
        }
    }
}

impl<S, F, T, E> futures_lite::Stream for First<S, F, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    F: FnMut(&T) -> bool,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.done {
            return Poll::Ready(None);
        }
        if *this.completed {
            *this.done = true;
            return Poll::Ready(Some(Event::Complete));
        }
        loop {
            match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
                Poll::Ready(Some(Event::Next(v))) => {
                    if (this.f)(&v) {
                        *this.completed = true;
                        return Poll::Ready(Some(Event::Next(v)));
                    }
                }
                Poll::Ready(Some(other)) => {
                    *this.done = true;
                    return Poll::Ready(Some(other));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<T, E> Observable<T, E> {
    /// Keeps the values for which `predicate` returns `true` (RxJS `filter`).
    ///
    /// `predicate` borrows the value (`FnMut(&T) -> bool`), so nothing is moved
    /// out before the decision. A rejected value is dropped without delaying the
    /// terminal events: a `Complete` or an `Error` that arrives between two
    /// candidates is delivered immediately.
    ///
    /// # Example
    /// ```rust
    /// use ice_rpc_rx::{from, rt::block_on};
    ///
    /// let evens = block_on(
    ///     from::<i32, String, _>(1..=6).filter(|v| v % 2 == 0).collect(),
    /// )
    /// .expect("the stream completes cleanly");
    /// assert_eq!(evens, vec![2, 4, 6]);
    /// ```
    pub fn filter<F>(self, predicate: F) -> Observable<T, E>
    where
        F: FnMut(&T) -> bool + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(Filter::new(self, predicate))
    }

    /// Forwards at most `n` values, then completes (RxJS `take`).
    ///
    /// The stream ends with `Complete` of `take`'s own making — not with the
    /// source's terminal. The completion is emitted when the source produces the
    /// value **after** the `n`-th (that value is discarded) or when the source
    /// ends. An `Error` raised before the bound is reached still wins: `take`
    /// never swallows an error. With `n == 0` the completion therefore waits for
    /// the source's first value (or for its end), rather than firing at once.
    ///
    /// # Example
    /// ```rust
    /// use ice_rpc_rx::{from, rt::block_on};
    ///
    /// let head = block_on(from::<i32, String, _>([10, 20, 30, 40]).take(2).collect())
    ///     .expect("take ends the stream with Complete");
    /// assert_eq!(head, vec![10, 20]);
    /// ```
    pub fn take(self, n: usize) -> Observable<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(Take::new(self, n))
    }

    /// Drops the first `n` values, then forwards the rest (RxJS `skip`).
    ///
    /// Only `Next` events are counted, so terminals are never skipped: a source
    /// that ends before `n` values completes as usual, and its error still wins.
    ///
    /// # Example
    /// ```rust
    /// use ice_rpc_rx::{from, rt::block_on};
    ///
    /// let tail = block_on(from::<i32, String, _>([10, 20, 30, 40]).skip(2).collect())
    ///     .expect("the stream completes cleanly");
    /// assert_eq!(tail, vec![30, 40]);
    /// ```
    pub fn skip(self, n: usize) -> Observable<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(Skip::new(self, n))
    }

    /// Emits only the first value, then completes (RxJS `first`).
    ///
    /// Shorthand for [`first_with`](Self::first_with) with a predicate that always
    /// matches. A `Complete` or an `Error` arriving before any value is forwarded
    /// unchanged, so an empty stream stays empty rather than yielding a value.
    ///
    /// # Example
    /// ```rust
    /// use ice_rpc_rx::{from, rt::block_on};
    ///
    /// let only = block_on(from::<i32, String, _>([7, 8, 9]).first().collect())
    ///     .expect("the stream completes cleanly");
    /// assert_eq!(only, vec![7]);
    /// ```
    pub fn first(self) -> Observable<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        self.first_with(|_: &T| true)
    }

    /// Emits the first value matching `predicate`, then completes.
    ///
    /// Values that do not match are dropped. If the source terminates before a
    /// match, that terminal (`Complete` or `Error`) is forwarded as-is: there is
    /// no "no match found" error, so a caller that needs to tell "found" from
    /// "never came" reads the terminal through [`collect`](Self::collect) or
    /// [`first_value`](Self::first_value).
    ///
    /// # Example
    /// ```rust
    /// use ice_rpc_rx::{from, rt::block_on};
    ///
    /// let first_odd = block_on(
    ///     from::<i32, String, _>([2, 4, 5, 6, 7])
    ///         .first_with(|v| v % 2 == 1)
    ///         .collect(),
    /// )
    /// .expect("the stream completes cleanly");
    /// assert_eq!(first_odd, vec![5]);
    /// ```
    pub fn first_with<F>(self, predicate: F) -> Observable<T, E>
    where
        F: FnMut(&T) -> bool + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(First::new(self, predicate))
    }
}
