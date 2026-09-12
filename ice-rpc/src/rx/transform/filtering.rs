//! Filtering Observables.
//!
//! ReactiveX category: [`filter`](super::RxStreamExt::filter),
//! [`take`](super::RxStreamExt::take), [`skip`](super::RxStreamExt::skip),
//! [`first`](super::RxStreamExt::first) and [`first_with`](super::RxStreamExt::first_with).

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::Event;

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::filter`](super::RxStreamExt::filter).
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
    /// See [`RxStreamExt::take`](super::RxStreamExt::take).
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
    /// See [`RxStreamExt::skip`](super::RxStreamExt::skip).
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
    /// See [`RxStreamExt::first_with`](super::RxStreamExt::first_with).
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
