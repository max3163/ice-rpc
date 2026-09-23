//! Observable Utility Operators.
//!
//! ReactiveX category: [`tap`](crate::Observable::tap) (Do),
//! [`finalize`](crate::Observable::finalize), [`delay`](crate::Observable::delay)
//! and [`timeout`](crate::Observable::timeout).
//!
//! The poll-based combinator and the `Observable` method that exposes it both
//! live in this file.

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::Event;
use crate::Observable;
use futures_lite::future::FutureExt;
use std::time::Duration;

pin_project_lite::pin_project! {
    /// See [`Observable::tap`](crate::Observable::tap).
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
    /// See [`Observable::finalize`](crate::Observable::finalize).
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
    /// See [`Observable::delay`](crate::Observable::delay).
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
    /// See [`Observable::timeout`](crate::Observable::timeout).
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

impl<T, E> Observable<T, E> {
    /// Runs `f` on every value without altering it (RxJS `tap`).
    ///
    /// The side effect sees a reference, so the value continues down the pipeline
    /// untouched — this is for logging and metrics, not for transformation.
    /// Terminals are not passed to `f`; use
    /// [`finalize`](Self::finalize) for an end-of-stream hook.
    ///
    /// `f` is `Send + 'static`, so it must **own** what it observes: capture an
    /// `Arc` (as below) or send on a channel. It cannot borrow a local, because a
    /// pipeline outlives the call that built it.
    ///
    /// # Example
    /// ```rust
    /// use ice_rpc_rx::{from, rt::block_on};
    /// use std::sync::{Arc, Mutex};
    ///
    /// let seen = Arc::new(Mutex::new(Vec::new()));
    /// let log = Arc::clone(&seen);
    /// let values = block_on(
    ///     from::<i32, String, _>([1, 2])
    ///         .tap(move |v| log.lock().expect("not poisoned").push(*v))
    ///         .collect(),
    /// )
    /// .expect("the stream completes cleanly");
    /// assert_eq!(values, vec![1, 2]);
    /// assert_eq!(*seen.lock().expect("not poisoned"), vec![1, 2]);
    /// ```
    pub fn tap<F>(self, f: F) -> Observable<T, E>
    where
        F: FnMut(&T) + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(Tap::new(self, f))
    }

    /// Runs `f` once when the stream ends, whatever the outcome (RxJS
    /// `finalize`).
    ///
    /// The hook fires on a terminal event (`Complete` or `Error`) or when the
    /// source is exhausted. It does **not** fire when the pipeline is dropped
    /// before it ends: a dropped [`Subscription`](crate::Subscription), or an
    /// `Observable` nobody consumes, skips it. When the end must be observable in
    /// that case too, make it explicit with
    /// [`take_until`](Self::take_until) or [`timeout`](Self::timeout), whose
    /// cancellation is a terminal event.
    ///
    /// # Example
    /// ```rust
    /// use ice_rpc_rx::{from, rt::block_on};
    /// use std::sync::{
    ///     atomic::{AtomicUsize, Ordering},
    ///     Arc,
    /// };
    ///
    /// let runs = Arc::new(AtomicUsize::new(0));
    /// let counter = Arc::clone(&runs);
    /// let values = block_on(
    ///     from::<i32, String, _>([1, 2])
    ///         .finalize(move || {
    ///             counter.fetch_add(1, Ordering::SeqCst);
    ///         })
    ///         .collect(),
    /// )
    /// .expect("the stream completes cleanly");
    /// assert_eq!(values, vec![1, 2]);
    /// assert_eq!(runs.load(Ordering::SeqCst), 1);
    /// ```
    pub fn finalize<F>(self, f: F) -> Observable<T, E>
    where
        F: FnOnce() + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(Finalize::new(self, f))
    }

    /// Delays every event — values **and** terminals — by `duration` (RxJS
    /// `delay`).
    ///
    /// Events are held one at a time and released after the delay, so a burst is
    /// spread out instead of being replayed at once: the source is read again
    /// only once the previous event has been released. The first event starts the
    /// timer.
    ///
    /// Needs the execution facade: it sleeps through [`rt::sleep`](crate::rt::sleep),
    /// so one of the three execution modes must be enabled.
    ///
    /// # Example
    /// ```rust,no_run
    /// use ice_rpc_rx::{from, rt::block_on, Observable};
    /// use std::time::Duration;
    ///
    /// let paced: Observable<i32, String> =
    ///     from([1, 2, 3]).delay(Duration::from_millis(100));
    /// let values = block_on(paced.collect()).expect("the stream completes cleanly");
    /// assert_eq!(values, vec![1, 2, 3]);
    /// ```
    pub fn delay(self, duration: Duration) -> Observable<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(Delay::new(self, duration))
    }

    /// Emits a technical [`RpcError::Timeout`](crate::RpcError::Timeout) if no
    /// event arrives within `duration` (RxJS `timeout`).
    ///
    /// This is a **silence watchdog**, not a total-duration bound: the deadline is
    /// reset after every event, including the first one, so a stream that keeps
    /// producing is never cut short no matter how long it lives. That is the
    /// failure a fixed overall deadline would miss.
    ///
    /// The error is technical on purpose — a timeout means the peer or the
    /// transport stopped answering, not that the service said "no" — so
    /// [`catch_error`](Self::catch_error) does not swallow it. Needs the
    /// execution facade: it sleeps through [`rt::sleep`](crate::rt::sleep).
    ///
    /// # Example
    /// ```rust,no_run
    /// use ice_rpc_rx::{from, rt::block_on, Observable};
    /// use std::time::Duration;
    ///
    /// let guarded: Observable<i32, String> = from([1]).timeout(Duration::from_secs(5));
    /// let values = block_on(guarded.collect()).expect("the value arrives at once");
    /// assert_eq!(values, vec![1]);
    /// ```
    pub fn timeout(self, duration: Duration) -> Observable<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(Timeout::new(self, duration))
    }
}
