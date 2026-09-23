//! Transforming Observables.
//!
//! ReactiveX category: [`map`](crate::Observable::map),
//! [`scan`](crate::Observable::scan), [`switch_map`](crate::Observable::switch_map)
//! and [`map_err`](crate::Observable::map_err).
//!
//! The poll-based combinator and the `Observable` method that exposes it both
//! live in this file.

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::Event;
use crate::Observable;

pin_project_lite::pin_project! {
    /// See [`Observable::map`](crate::Observable::map).
    pub struct Map<S, F, T, U, E> {
        #[pin]
        stream: S,
        f: F,
        _marker: PhantomData<(T, U, E)>,
    }
}

impl<S, F, T, U, E> Map<S, F, T, U, E> {
    pub(super) fn new(stream: S, f: F) -> Self {
        Self {
            stream,
            f,
            _marker: PhantomData,
        }
    }
}

impl<S, F, T, U, E> futures_lite::Stream for Map<S, F, T, U, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    F: FnMut(T) -> U,
{
    type Item = Event<U, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
            Poll::Ready(Some(Event::Next(v))) => Poll::Ready(Some(Event::Next((this.f)(v)))),
            Poll::Ready(Some(Event::Complete)) => Poll::Ready(Some(Event::Complete)),
            Poll::Ready(Some(Event::Error(e))) => Poll::Ready(Some(Event::Error(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`Observable::map_err`](crate::Observable::map_err).
    pub struct MapErr<S, F, T, E, E2> {
        #[pin]
        stream: S,
        f: F,
        _marker: PhantomData<(T, E, E2)>,
    }
}

impl<S, F, T, E, E2> MapErr<S, F, T, E, E2> {
    pub(super) fn new(stream: S, f: F) -> Self {
        Self {
            stream,
            f,
            _marker: PhantomData,
        }
    }
}

impl<S, F, T, E, E2> futures_lite::Stream for MapErr<S, F, T, E, E2>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    F: FnMut(E) -> E2,
{
    type Item = Event<T, E2>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
            Poll::Ready(Some(Event::Next(v))) => Poll::Ready(Some(Event::Next(v))),
            // Only the business error is remapped; technical errors pass through.
            Poll::Ready(Some(Event::Error(crate::ObservableError::Business(e)))) => {
                Poll::Ready(Some(Event::Error(crate::ObservableError::Business((this
                    .f)(
                    e
                )))))
            }
            Poll::Ready(Some(Event::Error(crate::ObservableError::Technical(e)))) => {
                Poll::Ready(Some(Event::Error(crate::ObservableError::Technical(e))))
            }
            // `Empty` carries no business payload, so there is nothing to remap.
            Poll::Ready(Some(Event::Error(crate::ObservableError::Empty))) => {
                Poll::Ready(Some(Event::Error(crate::ObservableError::Empty)))
            }
            Poll::Ready(Some(Event::Complete)) => Poll::Ready(Some(Event::Complete)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`Observable::scan`](crate::Observable::scan).
    pub struct Scan<S, F, T, U, E> {
        #[pin]
        stream: S,
        f: F,
        acc: Option<U>,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, F, T, U, E> Scan<S, F, T, U, E> {
    pub(super) fn new(stream: S, initial: U, f: F) -> Self {
        Self {
            stream,
            f,
            acc: Some(initial),
            _marker: PhantomData,
        }
    }
}

impl<S, F, T, U, E> futures_lite::Stream for Scan<S, F, T, U, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    U: Clone,
    F: FnMut(U, T) -> U,
{
    type Item = Event<U, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
            Poll::Ready(Some(Event::Next(v))) => {
                let acc = this.acc.take().expect("scan accumulator");
                let next = (this.f)(acc, v);
                let out = next.clone();
                *this.acc = Some(next);
                Poll::Ready(Some(Event::Next(out)))
            }
            Poll::Ready(Some(Event::Complete)) => Poll::Ready(Some(Event::Complete)),
            Poll::Ready(Some(Event::Error(e))) => Poll::Ready(Some(Event::Error(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`Observable::switch_map`](crate::Observable::switch_map).
    pub struct SwitchMap<S, F, T, U, E> {
        #[pin]
        stream: S,
        f: F,
        #[pin]
        inner: Option<crate::Observable<U, E>>,
        done: bool,
        _marker: PhantomData<T>,
    }
}

impl<S, F, T, U, E> SwitchMap<S, F, T, U, E> {
    pub(super) fn new(stream: S, f: F) -> Self {
        Self {
            stream,
            f,
            inner: None,
            done: false,
            _marker: PhantomData,
        }
    }
}

impl<S, F, T, U, E> futures_lite::Stream for SwitchMap<S, F, T, U, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    F: FnMut(T) -> crate::Observable<U, E>,
{
    type Item = Event<U, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if *this.done {
                return Poll::Ready(None);
            }
            if this.inner.is_some() {
                let result = {
                    let inner = this.inner.as_mut().as_pin_mut().expect("inner stream");
                    futures_lite::Stream::poll_next(inner, cx)
                };
                match result {
                    Poll::Ready(Some(Event::Next(u))) => return Poll::Ready(Some(Event::Next(u))),
                    Poll::Ready(Some(Event::Complete)) => {
                        this.inner.set(None);
                    }
                    Poll::Ready(Some(Event::Error(e))) => {
                        *this.done = true;
                        return Poll::Ready(Some(Event::Error(e)));
                    }
                    Poll::Ready(None) => {
                        this.inner.set(None);
                    }
                    Poll::Pending => {}
                }
            }
            match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
                Poll::Ready(Some(Event::Next(v))) => {
                    this.inner.set(Some((this.f)(v)));
                }
                Poll::Ready(Some(Event::Complete)) => {
                    *this.done = true;
                    return Poll::Ready(Some(Event::Complete));
                }
                Poll::Ready(Some(Event::Error(e))) => {
                    *this.done = true;
                    return Poll::Ready(Some(Event::Error(e)));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<T, E> Observable<T, E> {
    /// Applies `f` to every value, leaving the terminal events alone (RxJS
    /// `map`).
    ///
    /// Each `Next(v)` becomes `Next(f(v))`; `Complete` and `Error` pass through
    /// unchanged, so the error type is preserved.
    ///
    /// # Example
    /// ```rust
    /// use ice_rpc_rx::{of, rt::block_on, Observable};
    ///
    /// let stream: Observable<i32, String> = of(21);
    /// let doubled = block_on(stream.map(|v| v * 2).collect()).expect("no error");
    /// assert_eq!(doubled, vec![42]);
    /// ```
    ///
    /// # See also
    /// [`map_err`](Self::map_err) transforms the error channel instead;
    /// [`scan`](Self::scan) carries a state from one value to the next.
    pub fn map<U, F>(self, f: F) -> Observable<U, E>
    where
        F: FnMut(T) -> U + Send + 'static,
        T: Send + 'static,
        U: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(Map::new(self, f))
    }

    /// Applies `f` to the **business** error, leaving values and technical errors
    /// alone (RxJS `map`, error channel only).
    ///
    /// Only `ObservableError::Business(e)` becomes `Business(f(e))`. A technical
    /// error and `Empty` pass through untouched: rewriting a technical error as a
    /// service-level type would hide a framework failure behind a business one,
    /// and [`ObservableError::Empty`](crate::ObservableError::Empty) carries no
    /// payload to rewrite.
    ///
    /// # Example
    /// ```rust
    /// use ice_rpc_rx::{rt::block_on, throw_error, ObservableError};
    ///
    /// let result = block_on(
    ///     throw_error::<i32, String>("not found".to_string())
    ///         .map_err(|e| e.len())
    ///         .collect(),
    /// );
    /// assert!(matches!(result, Err(ObservableError::Business(9))));
    /// ```
    pub fn map_err<F, E2>(self, f: F) -> Observable<T, E2>
    where
        F: FnMut(E) -> E2 + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
        E2: Send + 'static,
    {
        Observable::from_stream(MapErr::new(self, f))
    }

    /// Emits the running accumulator, one value per source value (RxJS `scan`).
    ///
    /// `accumulator` receives the previous state and the new value and returns the
    /// next state, which is emitted: the first output is
    /// `accumulator(initial, first_value)`. This is a `fold` that keeps every
    /// intermediate result instead of only the last, which is what makes it
    /// usable for a running total or a rolling window.
    ///
    /// The state type `U` is `Clone` because the accumulator is both kept for the
    /// next step and emitted downstream.
    ///
    /// # Example
    /// ```rust
    /// use ice_rpc_rx::{from, rt::block_on};
    ///
    /// let running_total = block_on(
    ///     from::<i32, String, _>([1, 2, 3]).scan(0, |acc, v| acc + v).collect(),
    /// )
    /// .expect("the stream completes cleanly");
    /// assert_eq!(running_total, vec![1, 3, 6]);
    /// ```
    pub fn scan<U, F>(self, initial: U, accumulator: F) -> Observable<U, E>
    where
        U: Clone + Send + 'static,
        F: FnMut(U, T) -> U + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(Scan::new(self, initial, accumulator))
    }

    /// Projects every value into an inner stream and emits from the **latest**
    /// one, dropping the previous one (RxJS `switchMap`).
    ///
    /// Each outer value calls `f` and replaces the inner stream in flight; the
    /// abandoned stream is dropped, so its pending values are never emitted. A
    /// terminal error from either the outer stream or the active inner stream
    /// ends the pipeline.
    ///
    /// The completion rule is **stricter than RxJS**: when the *outer* stream
    /// completes, the pipeline completes immediately, even if an inner stream is
    /// still in flight (RxJS awaits that last inner stream first). A projection
    /// that must not lose its final result belongs in
    /// [`map`](Self::map) + [`collect`](Self::collect) instead.
    ///
    /// # Example
    /// ```rust
    /// use ice_rpc_rx::{from, of, rt::block_on};
    ///
    /// let values = block_on(
    ///     from::<i32, String, _>([1, 2])
    ///         .switch_map(|v| of::<i32, String>(v * 10))
    ///         .collect(),
    /// )
    /// .expect("the stream completes cleanly");
    /// assert_eq!(values, vec![10, 20]);
    /// ```
    pub fn switch_map<F, U>(self, f: F) -> Observable<U, E>
    where
        F: FnMut(T) -> Observable<U, E> + Send + 'static,
        T: Send + 'static,
        U: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(SwitchMap::new(self, f))
    }
}
