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
use crate::ObservableInput;

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

/// How the **source** stream of a [`SwitchMap`] ended.
#[derive(Clone, Copy)]
enum SourceEnd {
    /// The source completed normally.
    Completed,
    /// The source closed without a terminal event.
    Closed,
}

pin_project_lite::pin_project! {
    /// See [`Observable::switch_map`](crate::Observable::switch_map).
    pub struct SwitchMap<S, F, T, U, E> {
        #[pin]
        stream: S,
        f: F,
        // The **projected** stream, replaced on every new source value.
        #[pin]
        projected: Option<crate::Observable<U, E>>,
        // Set once the source ended: the projected stream in flight is then
        // drained before the pipeline terminates (RxJS semantics).
        source_end: Option<SourceEnd>,
        // Set once our own terminal has been emitted.
        finished: bool,
        _marker: PhantomData<T>,
    }
}

impl<S, F, T, U, E> SwitchMap<S, F, T, U, E> {
    pub(super) fn new(stream: S, f: F) -> Self {
        Self {
            stream,
            f,
            projected: None,
            source_end: None,
            finished: false,
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
            if *this.finished {
                return Poll::Ready(None);
            }

            // The active projected stream is always polled first: it may still
            // carry values, even after the source has ended.
            if this.projected.is_some() {
                let polled = {
                    let projected = this
                        .projected
                        .as_mut()
                        .as_pin_mut()
                        .expect("projected stream");
                    futures_lite::Stream::poll_next(projected, cx)
                };
                match polled {
                    Poll::Ready(Some(Event::Next(u))) => {
                        return Poll::Ready(Some(Event::Next(u)));
                    }
                    // RxJS: an error from either stream ends the pipeline at once.
                    Poll::Ready(Some(Event::Error(e))) => {
                        *this.finished = true;
                        return Poll::Ready(Some(Event::Error(e)));
                    }
                    // RxJS: a projected completion is *not* a downstream
                    // completion — the source may still project another value.
                    Poll::Ready(Some(Event::Complete)) | Poll::Ready(None) => {
                        this.projected.set(None);
                    }
                    Poll::Pending => {}
                }
            }

            // The source ended and the last projected stream has been drained: the
            // pipeline now ends the way the source did. This is the RxJS rule — the
            // source's completion waits for the projected stream in flight instead
            // of dropping it.
            if let Some(end) = *this.source_end {
                if this.projected.is_none() {
                    *this.finished = true;
                    return match end {
                        SourceEnd::Completed => Poll::Ready(Some(Event::Complete)),
                        // A source that closed without a terminal is passed
                        // through as such, the convention every other operator
                        // follows.
                        SourceEnd::Closed => Poll::Ready(None),
                    };
                }
                // The projected stream is still pending: the poll above registered
                // its waker, and the source must not be read again.
                return Poll::Pending;
            }

            match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
                Poll::Ready(Some(Event::Next(v))) => {
                    this.projected.set(Some((this.f)(v)));
                }
                Poll::Ready(Some(Event::Complete)) => {
                    *this.source_end = Some(SourceEnd::Completed);
                }
                Poll::Ready(Some(Event::Error(e))) => {
                    *this.finished = true;
                    return Poll::Ready(Some(Event::Error(e)));
                }
                // RxJS has no "closed without a terminal": the crate maps it to
                // the same wait, then forwards the end as `None`.
                Poll::Ready(None) => {
                    *this.source_end = Some(SourceEnd::Closed);
                }
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
    /// alone.
    ///
    /// Its RxJS spelling is `catchError(e => throwError(f(e)))`: the error channel
    /// is mapped, not recovered — the stream still ends on the failure.
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

    /// Projects every value into a **projected stream** and emits from the
    /// **latest** one, dropping the previous one (RxJS `switchMap`).
    ///
    /// Each source value calls `f` and replaces the projected stream in flight;
    /// the abandoned stream is dropped, so its pending values are never emitted. A
    /// terminal error from either the source stream or the active projected stream
    /// ends the pipeline.
    ///
    /// # Terminals (RxJS semantics)
    ///
    /// - a new source value **switches**: the previous projected stream is
    ///   dropped, so its pending values are never emitted;
    /// - the source's `Complete` **waits for the projected stream in flight**: the
    ///   pipeline completes only once that stream has ended, so its last values
    ///   are never lost;
    /// - an `Error` from either stream ends the pipeline at once and drops the
    ///   other one.
    ///
    /// Because the last projected stream is awaited, a projection that returns a
    /// long-lived stream — a notification or a state feed — keeps the pipeline
    /// alive for as long as that stream lives. Bound it with [`take`](Self::take),
    /// [`take_until_token`](Self::take_until_token) or
    /// [`timeout`](Self::timeout). To keep
    /// every projection instead of only the latest one, [`merge`](Self::merge)
    /// them.
    ///
    /// # Example
    ///
    /// A projection that returns an [`Observable`] directly:
    /// ```rust
    /// use ice_rpc_rx::{from, of, rt::block_on};
    ///
    /// let values = block_on(
    ///     from::<i32, String, _>([1, 2])
    ///         .switch_map(|v| of::<String, String>(format!("value: {}", v)))
    ///         .collect(),
    /// )
    /// .expect("the stream completes cleanly");
    /// assert_eq!(values, vec!["value: 1", "value: 2"]);
    /// ```
    ///
    /// A projection that returns a **future** — the shape of an `async fn` client
    /// call — is accepted too, through
    /// [`ObservableInput`](crate::ObservableInput):
    /// ```rust
    /// use ice_rpc_rx::{from, of, rt::block_on, Observable};
    ///
    /// async fn fetch(v: i32) -> Observable<String, String> {
    ///     of(format!("value: {v}"))
    /// }
    ///
    /// let values = block_on(
    ///     from::<i32, String, _>([1, 2])
    ///         .switch_map(|v| async move { fetch(v).await })
    ///         .collect(),
    /// )
    /// .expect("the stream completes cleanly");
    /// assert_eq!(values, vec!["value: 1", "value: 2"]);
    /// ```
    ///
    /// The future is created by the closure and polled **once**, on the first poll
    /// of the projected stream it produces: nothing runs before a consumer pulls
    /// the pipeline.
    pub fn switch_map<F, I, U>(self, mut f: F) -> Observable<U, E>
    where
        F: FnMut(T) -> I + Send + 'static,
        I: ObservableInput<U, E>,
        T: Send + 'static,
        U: Send + 'static,
        E: Send + 'static,
    {
        // The projected stream is always an `Observable`: the projection's output
        // is normalized here, so `SwitchMap` itself never sees a future.
        Observable::from_stream(SwitchMap::new(self, move |v| f(v).into_observable()))
    }
}

#[cfg(test)]
mod tests;
