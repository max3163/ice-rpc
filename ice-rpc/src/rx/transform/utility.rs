//! Observable Utility Operators.
//!
//! ReactiveX category: [`tap`](super::RxStreamExt::tap) (Do),
//! [`finalize`](super::RxStreamExt::finalize), [`delay`](super::RxStreamExt::delay)
//! and [`timeout`](super::RxStreamExt::timeout).

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::Event;
use futures_lite::future::FutureExt;

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::tap`](super::RxStreamExt::tap).
    pub struct Tap<S, F, T, E> {
        #[pin]
        stream: S,
        f: F,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, F, T, E> Tap<S, F, T, E> {
    pub(super) fn new(stream: S, f: F) -> Self {
        Self {
            stream,
            f,
            _marker: PhantomData,
        }
    }
}

impl<S, F, T, E> futures_lite::Stream for Tap<S, F, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    F: FnMut(&T),
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
            Poll::Ready(Some(Event::Next(v))) => {
                (this.f)(&v);
                Poll::Ready(Some(Event::Next(v)))
            }
            Poll::Ready(Some(other)) => Poll::Ready(Some(other)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::finalize`](super::RxStreamExt::finalize).
    pub struct Finalize<S, F, T, E> {
        #[pin]
        stream: S,
        f: Option<F>,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, F, T, E> Finalize<S, F, T, E> {
    pub(super) fn new(stream: S, f: F) -> Self {
        Self {
            stream,
            f: Some(f),
            _marker: PhantomData,
        }
    }
}

impl<S, F, T, E> futures_lite::Stream for Finalize<S, F, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    F: FnOnce(),
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
            Poll::Ready(Some(event)) => {
                if event.is_terminal() {
                    if let Some(f) = this.f.take() {
                        f();
                    }
                }
                Poll::Ready(Some(event))
            }
            Poll::Ready(None) => {
                if let Some(f) = this.f.take() {
                    f();
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::delay`](super::RxStreamExt::delay).
    pub struct Delay<S, T, E> {
        #[pin]
        stream: S,
        duration: std::time::Duration,
        pending: Option<Event<T, E>>,
        sleep: Option<futures_lite::future::Boxed<()>>,
    }
}

impl<S, T, E> Delay<S, T, E> {
    pub(super) fn new(stream: S, duration: std::time::Duration) -> Self {
        Self {
            stream,
            duration,
            pending: None,
            sleep: None,
        }
    }
}

impl<S, T, E> futures_lite::Stream for Delay<S, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if this.pending.is_some() {
                let ready = this
                    .sleep
                    .as_mut()
                    .expect("sleep future present when an event is pending")
                    .as_mut()
                    .poll(cx);
                match ready {
                    Poll::Ready(()) => {
                        *this.sleep = None;
                        let event = this.pending.take().expect("pending event");
                        return Poll::Ready(Some(event));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
            match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
                Poll::Ready(Some(event)) => {
                    *this.pending = Some(event);
                    *this.sleep = Some(crate::rt::sleep(*this.duration).boxed());
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::timeout`](super::RxStreamExt::timeout).
    pub struct Timeout<S, T, E> {
        #[pin]
        stream: S,
        duration: std::time::Duration,
        sleep: Option<futures_lite::future::Boxed<()>>,
        done: bool,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, T, E> Timeout<S, T, E> {
    pub(super) fn new(stream: S, duration: std::time::Duration) -> Self {
        Self {
            stream,
            duration,
            sleep: None,
            done: false,
            _marker: PhantomData,
        }
    }
}

impl<S, T, E> futures_lite::Stream for Timeout<S, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.done {
            return Poll::Ready(None);
        }
        if this.sleep.is_none() {
            *this.sleep = Some(crate::rt::sleep(*this.duration).boxed());
        }
        match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
            Poll::Ready(Some(event)) => {
                // Reset the silence deadline after every received event.
                *this.sleep = Some(crate::rt::sleep(*this.duration).boxed());
                if event.is_terminal() {
                    *this.done = true;
                }
                Poll::Ready(Some(event))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => match this.sleep.as_mut().expect("sleep future").as_mut().poll(cx) {
                Poll::Ready(()) => {
                    *this.done = true;
                    Poll::Ready(Some(Event::Error(crate::ObservableError::Technical(
                        crate::RpcError::Timeout,
                    ))))
                }
                Poll::Pending => Poll::Pending,
            },
        }
    }
}
