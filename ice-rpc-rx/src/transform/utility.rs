//! Observable Utility Operators.
//!
//! ReactiveX category: [`tap`](crate::Observable::tap) (Do),
//! [`finalize`](crate::Observable::finalize), [`delay`](crate::Observable::delay)
//! and [`timeout`](crate::Observable::timeout).
//!
//! The poll-based combinator and the `Observable` method that exposes it both
//! live in this file.

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crate::Event;
use crate::Observable;
use futures_lite::future::FutureExt;

/// How many events one `poll_next` of [`Delay`] reads ahead at most.
///
/// Without a bound, an always-ready source would spin in the drain loop without
/// ever yielding to the timer, and the queue would grow while the stream never
/// returns `Pending`.
const DELAY_READ_AHEAD: usize = 64;

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
        duration: Duration,
        // Events read ahead, each with the instant it must be released. An
        // event's deadline is *its own* arrival time plus the delay, so the
        // source's spacing survives (RxJS `delay`).
        queue: VecDeque<(Instant, Event<T, E>)>,
        // Timer armed for the head of `queue`, when one is waiting.
        sleep: Option<futures_lite::future::Boxed<()>>,
        // Set once the source has no more events: the end is forwarded after the
        // queue drains.
        source_finished: bool,
    }
}

impl<S, T, E> Delay<S, T, E> {
    pub(super) fn new(stream: S, duration: Duration) -> Self {
        Self {
            stream,
            duration,
            queue: VecDeque::new(),
            sleep: None,
            source_finished: false,
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
        let mut read = 0usize;
        loop {
            // Release the head as soon as its own deadline is reached.
            if let Some((deadline, _)) = this.queue.front() {
                let now = Instant::now();
                if *deadline <= now {
                    let (_, event) = this.queue.pop_front().expect("queue head");
                    // The next head needs a timer of its own.
                    *this.sleep = None;
                    return Poll::Ready(Some(event));
                }
                if this.sleep.is_none() {
                    *this.sleep = Some(crate::rt::sleep(*deadline - now).boxed());
                }
            } else {
                *this.sleep = None;
                if *this.source_finished {
                    return Poll::Ready(None);
                }
            }

            // Drain a bounded batch from the source: a burst stays a burst, it is
            // merely shifted, so the source is never throttled. The queue is
            // ordered, so a newly pushed event never precedes the head and the
            // armed timer stays valid.
            if !*this.source_finished && read < DELAY_READ_AHEAD {
                match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
                    Poll::Ready(Some(event)) => {
                        this.queue
                            .push_back((Instant::now() + *this.duration, event));
                        read += 1;
                        continue;
                    }
                    Poll::Ready(None) => {
                        *this.source_finished = true;
                        continue;
                    }
                    Poll::Pending => {}
                }
            }

            // Nothing is due: wait for the head's deadline. A pending source has
            // already registered its waker.
            if let Some(sleep) = this.sleep.as_mut() {
                match sleep.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        *this.sleep = None;
                        continue;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
            return Poll::Pending;
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

    /// Shifts **every** notification — values **and** terminals — by `duration`
    /// (RxJS `delay`).
    ///
    /// Each event is scheduled from **its own arrival time**, so the source's
    /// spacing survives: a burst comes out as a burst, `duration` later, and a
    /// source that paces itself keeps its pace. The source is drained while
    /// events wait, so it is never throttled — this is a **shift**, not a
    /// pacer. To slow a burst down, `delay` is the wrong tool.
    ///
    /// The price is memory: events are held for `duration`, so a source faster
    /// than that window grows the queue. That is the queue RxJS gets from its
    /// scheduler — bounded read-ahead per poll keeps the executor responsive,
    /// but not the backlog.
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

#[cfg(test)]
mod tests;
