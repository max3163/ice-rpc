//! Transforming Observables.
//!
//! ReactiveX category: [`map`](super::RxStreamExt::map),
//! [`scan`](super::RxStreamExt::scan), [`switch_map`](super::RxStreamExt::switch_map)
//! and [`map_err`](super::RxStreamExt::map_err).

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::Event;

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::map`](super::RxStreamExt::map).
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
    /// See [`RxStreamExt::map_err`](super::RxStreamExt::map_err).
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
            Poll::Ready(Some(Event::Complete)) => Poll::Ready(Some(Event::Complete)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::scan`](super::RxStreamExt::scan).
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
    /// See [`RxStreamExt::switch_map`](super::RxStreamExt::switch_map).
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
