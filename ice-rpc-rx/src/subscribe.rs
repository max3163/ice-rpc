//! Terminal subscription: adapts a pull-based stream to Rx-style callbacks.
//!
//! [`RxStreamExt::subscribe`](crate::RxStreamExt::subscribe) spawns a **single**
//! task that pulls the pipeline and pushes the events into an [`Observer`].
//! This is the only operator that spawns; `for_each`, `first_value` and
//! `collect` stay purely pull-based.
//!
//! Because the unified model carries the error inside the stream
//! ([`ice_rpc::Event::Error`]), the mapping event → callback is 1:1: the
//! observer receives the [`ObservableError`] unchanged, with no projection and
//! no `Empty` case.

use std::pin::Pin;

use ice_rpc::{Event, ObservableError};

/// Push-based consumer of an observable (Rx `Observer`).
///
/// [`Observer::next`] is called for every value, then exactly one of
/// [`Observer::error`] / [`Observer::complete`] terminates the subscription.
pub trait Observer<T, E>: Send + 'static {
    /// Receives a business value.
    fn next(&mut self, value: T);

    /// Receives a terminal error (business or technical).
    fn error(&mut self, error: ObservableError<E>);

    /// Receives the normal end of the stream.
    fn complete(&mut self);
}

/// Observer built from three closures (see
/// [`RxStreamExt::subscribe_with`](crate::RxStreamExt::subscribe_with)).
pub struct ObserverFns<N, Er, C> {
    on_next: N,
    on_error: Er,
    on_complete: C,
}

impl<N, Er, C> ObserverFns<N, Er, C> {
    /// Creates an observer from its three callbacks.
    pub fn new(on_next: N, on_error: Er, on_complete: C) -> Self {
        Self {
            on_next,
            on_error,
            on_complete,
        }
    }
}

impl<N, Er, C, T, E> Observer<T, E> for ObserverFns<N, Er, C>
where
    N: FnMut(T) + Send + 'static,
    Er: FnMut(ObservableError<E>) + Send + 'static,
    C: FnMut() + Send + 'static,
{
    fn next(&mut self, value: T) {
        (self.on_next)(value)
    }

    fn error(&mut self, error: ObservableError<E>) {
        (self.on_error)(error)
    }

    fn complete(&mut self) {
        (self.on_complete)()
    }
}

/// A plain `FnMut(T)` can be used directly as a value-only observer: it ignores
/// errors and completion.
impl<T, E, F> Observer<T, E> for F
where
    F: FnMut(T) + Send + 'static,
{
    fn next(&mut self, value: T) {
        self(value)
    }

    fn error(&mut self, _error: ObservableError<E>) {}

    fn complete(&mut self) {}
}

/// Handle of a running subscription.
///
/// Dropping it cancels the underlying task (Rx `unsubscribe`, silent: no
/// callback is invoked).
pub struct Subscription {
    cancel: ice_rpc::CancellationToken,
}

impl Subscription {
    pub(crate) fn new(cancel: ice_rpc::CancellationToken) -> Self {
        Self { cancel }
    }

    /// Cancels the subscription explicitly.
    pub fn unsubscribe(&self) {
        self.cancel.cancel();
    }

    /// Resolves once the subscription has ended.
    ///
    /// This happens on a terminal event (`Complete` or `Error`), on an explicit
    /// [`Subscription::unsubscribe`], or when the `Subscription` is dropped.
    pub async fn closed(&self) {
        self.cancel.cancelled().await;
    }

    /// Returns `true` once the subscription has ended (cancelled, completed or
    /// errored).
    pub fn is_closed(&self) -> bool {
        self.cancel.is_cancelled()
    }
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("closed", &self.is_closed())
            .finish()
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Awaits the next event of a boxed, pinned stream.
pub(crate) async fn next_event<S, T, E>(stream: &mut Pin<Box<S>>) -> Option<Event<T, E>>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
{
    futures_lite::future::poll_fn(|cx| futures_lite::Stream::poll_next(stream.as_mut(), cx)).await
}

/// Spawns the pushing task backing
/// [`subscribe`](crate::RxStreamExt::subscribe).
pub(crate) fn spawn_push<S, O, T, E>(stream: S, mut observer: O, cancel: ice_rpc::CancellationToken)
where
    S: futures_lite::Stream<Item = Event<T, E>> + Send + 'static,
    O: Observer<T, E>,
    T: Send + 'static,
    E: Send + 'static,
{
    let token = cancel.clone();
    ice_rpc::rt::spawn(async move {
        run_push(stream, &mut observer, &token).await;
        // Signal completion (either terminal event or cancellation) so that
        // `Subscription::is_closed` becomes observable.
        token.cancel();
    });
}

/// Pulls the stream and pushes each event into the observer.
async fn run_push<S, O, T, E>(stream: S, observer: &mut O, cancel: &ice_rpc::CancellationToken)
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    O: Observer<T, E>,
    T: Send + 'static,
    E: Send + 'static,
{
    enum Push<T, E> {
        Event(Option<Event<T, E>>),
        Cancelled,
    }

    let mut stream = Box::pin(stream);
    loop {
        let outcome = futures_lite::future::race(
            async { Push::Event(next_event(&mut stream).await) },
            async {
                cancel.cancelled().await;
                Push::Cancelled
            },
        )
        .await;

        match outcome {
            Push::Cancelled => return,
            Push::Event(Some(Event::Next(v))) => observer.next(v),
            Push::Event(Some(Event::Complete)) | Push::Event(None) => {
                observer.complete();
                return;
            }
            Push::Event(Some(Event::Error(e))) => {
                observer.error(e);
                return;
            }
        }
    }
}

#[cfg(all(test, not(feature = "tokio")))]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::{Event, ObservableError};
    use crate::RxStreamExt;

    /// Waits (bounded) for a condition set by the subscription task.
    fn wait_for(cond: impl Fn() -> bool) -> bool {
        for _ in 0..400 {
            if cond() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        cond()
    }

    #[test]
    fn subscribe_pushes_values_then_complete() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let done = Arc::new(AtomicBool::new(false));

        let seen_c = seen.clone();
        let done_c = done.clone();
        let stream: ice_rpc::Observable<i32, String> = crate::from([1, 2, 3]);
        let _sub = stream.subscribe_with(
            move |v| seen_c.lock().unwrap().push(v),
            |_e: ObservableError<String>| {},
            move || done_c.store(true, Ordering::SeqCst),
        );

        assert!(wait_for(|| done.load(Ordering::SeqCst)));
        assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn subscribe_reports_technical_error() {
        let got: Arc<Mutex<Option<ObservableError<String>>>> = Arc::new(Mutex::new(None));
        let got_c = got.clone();

        let stream =
            ice_rpc::Observable::<i32, String>::from_technical_error(ice_rpc::RpcError::Timeout);
        let _sub = stream.subscribe_with(
            |_v| {},
            move |e: ObservableError<String>| *got_c.lock().unwrap() = Some(e),
            || {},
        );

        assert!(wait_for(|| got.lock().unwrap().is_some()));
        assert!(matches!(
            got.lock().unwrap().as_ref(),
            Some(ObservableError::Technical(ice_rpc::RpcError::Timeout))
        ));
    }

    #[test]
    fn subscribe_reports_business_error() {
        let got: Arc<Mutex<Option<ObservableError<String>>>> = Arc::new(Mutex::new(None));
        let got_c = got.clone();

        let stream: ice_rpc::Observable<i32, String> =
            ice_rpc::Observable::from_events([Event::Error(ObservableError::Business(
                "boom".into(),
            ))]);
        let _sub = stream.subscribe_with(
            |_v| {},
            move |e: ObservableError<String>| *got_c.lock().unwrap() = Some(e),
            || {},
        );

        assert!(wait_for(|| got.lock().unwrap().is_some()));
        assert!(matches!(
            got.lock().unwrap().as_ref(),
            Some(ObservableError::Business(e)) if e == "boom"
        ));
    }

    #[test]
    fn subscription_drop_cancels_before_terminal() {
        let next_called = Arc::new(AtomicBool::new(false));
        let next_c = next_called.clone();

        // A channel-backed stream that never emits: the task parks on the pull.
        let (tx, rx) = ice_rpc::gen::channel::<i32, String>(1);
        let sub = rx.subscribe(move |_v| next_c.store(true, Ordering::SeqCst));

        // Dropping cancels silently: no callback, no panic.
        drop(sub);
        std::thread::sleep(Duration::from_millis(50));
        assert!(!next_called.load(Ordering::SeqCst));

        drop(tx);
    }

    #[test]
    fn subscription_closed_resolves_on_complete() {
        let stream: ice_rpc::Observable<i32, String> = crate::of(1);
        let sub = stream.subscribe(|_v| {});

        // The push task cancels its token when it returns, so `closed` resolves
        // on a terminal event as well.
        pollster::block_on(sub.closed());
        assert!(sub.is_closed());
    }

    #[test]
    fn for_each_runs_to_completion() {
        let sum = Arc::new(AtomicI32::new(0));
        let sum_c = sum.clone();
        let stream: ice_rpc::Observable<i32, String> = crate::from([1, 2, 3]);

        let result = pollster::block_on(stream.for_each(move |v| {
            sum_c.fetch_add(v, Ordering::SeqCst);
        }));

        assert!(result.is_ok());
        assert_eq!(sum.load(Ordering::SeqCst), 6);
    }

    #[test]
    fn for_each_returns_business_error() {
        let stream: ice_rpc::Observable<i32, String> = ice_rpc::Observable::from_events([
            Event::Next(1),
            Event::Error(ObservableError::Business("boom".into())),
        ]);

        let err = pollster::block_on(stream.for_each(|_v| {})).unwrap_err();
        assert!(matches!(err, ObservableError::Business(e) if e == "boom"));
    }
}
