//! Stream creation helpers.
//!
//! [`from`] and [`of`] build local observables, mirroring the RxJS constructors
//! of the same name. They are backed by [`crate::Observable::from_events`], so
//! they allocate no channel and spawn no task: the events are produced lazily
//! by the consumer's own polling.

use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{Event, Observable, ObservableError};

/// Creates an observable from an iterator, emitting each value as `Next` then
/// `Complete`.
///
/// Equivalent to RxJS `from`.
///
/// # Example
/// ```rust,ignore
/// use crate::from;
///
/// let stream: crate::Observable<i32, String> = from([1, 2, 3]);
/// ```
pub fn from<T, E, I>(iter: I) -> Observable<T, E>
where
    I: IntoIterator<Item = T>,
{
    let mut events: Vec<Event<T, E>> = iter.into_iter().map(Event::Next).collect();
    events.push(Event::Complete);
    crate::Observable::from_events(events)
}

/// Creates a single-value observable.
///
/// Consumers observe the value as `Next` followed by `Complete`. Equivalent to
/// RxJS `of`.
///
/// # Example
/// ```rust,ignore
/// use crate::of;
///
/// let stream: crate::Observable<i32, String> = of(42);
/// ```
pub fn of<T, E>(value: T) -> Observable<T, E> {
    crate::Observable::from_events([Event::Next(value), Event::Complete])
}

/// Creates an observable that only emits a business error.
///
/// Equivalent to RxJS `throwError`: the stream terminates on
/// [`Event::Error`] with the business variant of [`ObservableError`]. It is the
/// counterpart of [`of`] for single-response services that must fail on the
/// business channel.
///
/// # Example
/// ```rust,ignore
/// use crate::throw_error;
///
/// let stream: crate::Observable<i32, MyError> = throw_error(MyError::NotFound);
/// ```
pub fn throw_error<T, E>(error: E) -> Observable<T, E> {
    crate::Observable::from_events([Event::Error(ObservableError::Business(error))])
}

/// Creates an observable from a **deferred** factory (RxJS `defer`).
///
/// The factory runs at the **first poll** — never before — and exactly once: the
/// work it starts (a call, a channel, a read) is paid only by a consumer. It
/// returns a future, so a service call is passed **as it is**:
///
/// ```rust
/// use ice_rpc_rx::{defer, rt::block_on, Observable};
///
/// async fn fetch() -> Observable<i32, String> {
///     ice_rpc_rx::of(1)
/// }
///
/// let stream = defer(move || fetch());
/// assert_eq!(block_on(stream.collect()).expect("clean completion"), vec![1]);
/// ```
///
/// A factory with nothing to await has the same shape: an `async` block is a
/// closure returning a future.
///
/// ```rust
/// use ice_rpc_rx::{defer, of, rt::block_on};
///
/// let stream = defer(move || async { of::<i32, String>(1) });
/// assert_eq!(block_on(stream.collect()).expect("clean completion"), vec![1]);
/// ```
pub fn defer<T, E, F, Fut>(factory: F) -> Observable<T, E>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Observable<T, E>> + Send + 'static,
    T: Send + 'static,
    E: Send + 'static,
{
    Observable::from_stream(Deferred::new(factory))
}

/// Anything a combinator can accept as a source: an [`Observable`] itself, or a
/// [`Future`](std::future::Future) that produces one.
///
/// This is the Rust counterpart of RxJS `ObservableInput`. It lets a projection
/// hand back **either** form, so an `async fn` client method composes directly —
/// `stream.switch_map(move |x| async move { proxy.get_other(x).await })` — without
/// an explicit [`defer`].
///
/// A future is turned into a **cold** observable: nothing runs until a consumer
/// polls it, exactly as if it had been passed to [`defer`]. The future is created
/// by the caller and polled **once**.
///
/// # Example
/// ```rust
/// use ice_rpc_rx::{rt::block_on, Observable, ObservableInput};
///
/// // Every `Observable` is its own input.
/// let direct: Observable<i32, String> = ice_rpc_rx::of(1).into_observable();
/// assert_eq!(block_on(direct.collect()).expect("clean"), vec![1]);
///
/// // A future resolving to an `Observable` is accepted too — the shape an
/// // `async fn` client method returns.
/// let deferred = async { ice_rpc_rx::of::<i32, String>(2) }.into_observable();
/// assert_eq!(block_on(deferred.collect()).expect("clean"), vec![2]);
/// ```
pub trait ObservableInput<T, E> {
    /// Views `self` as an [`Observable`].
    fn into_observable(self) -> Observable<T, E>;
}

impl<T, E> ObservableInput<T, E> for Observable<T, E> {
    #[inline]
    fn into_observable(self) -> Observable<T, E> {
        self
    }
}

impl<T, E, F> ObservableInput<T, E> for F
where
    F: std::future::Future<Output = Observable<T, E>> + Send + 'static,
    T: Send + 'static,
    E: Send + 'static,
{
    fn into_observable(self) -> Observable<T, E> {
        // The future is created by the caller and polled once, on the first poll
        // of the returned observable: the same laziness as `defer`.
        let mut future = Some(self);
        defer(move || future.take().expect("the future is polled once"))
    }
}

/// Creates a one-value observable from a future (RxJS `from(promise)`).
///
/// The future is polled at the **first poll**, never before. `Ok(value)` emits
/// that value, then `Complete`; `Err(error)` emits a **business** error, since
/// the operation is the service's own — a technical failure belongs to the
/// transport and reaches the caller as an [`RpcError`](crate::RpcError)
/// instead.
///
/// Sugar over [`defer`]: same laziness, with a value where `defer` produces a
/// stream.
///
/// ```rust
/// use ice_rpc_rx::{from_future, rt::block_on, Observable};
///
/// let stream: Observable<i32, String> = from_future(async { Ok(7) });
/// assert_eq!(block_on(stream.collect()).expect("clean completion"), vec![7]);
/// ```
pub fn from_future<T, E, F>(future: F) -> Observable<T, E>
where
    F: std::future::Future<Output = Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: Send + 'static,
{
    let mut future = Some(future);
    defer(move || {
        let future = future.take().expect("the future is polled once");
        async move {
            match future.await {
                Ok(value) => of(value),
                Err(error) => throw_error(error),
            }
        }
    })
}

pin_project_lite::pin_project! {
    /// See [`defer`].
    struct Deferred<F, Fut, T, E> {
        // Taken by the first poll: the factory runs once per consumption.
        factory: Option<F>,
        // The factory's future, until it yields the stream.
        #[pin]
        pending: Option<Fut>,
        // The stream the future resolved to.
        #[pin]
        stream: Option<Observable<T, E>>,
    }
}

impl<F, Fut, T, E> Deferred<F, Fut, T, E> {
    fn new(factory: F) -> Self {
        Self {
            factory: Some(factory),
            pending: None,
            stream: None,
        }
    }
}

impl<F, Fut, T, E> futures_lite::Stream for Deferred<F, Fut, T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Observable<T, E>>,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            // The first poll is where the factory runs — and the only one.
            if this.pending.is_none() && this.stream.is_none() {
                let mut factory = this.factory.take().expect("the factory runs once");
                this.pending.set(Some(factory()));
            }

            // Await the factory's future, then hand over to what it produced.
            if let Some(pending) = this.pending.as_mut().as_pin_mut() {
                match pending.poll(cx) {
                    Poll::Ready(observable) => {
                        this.pending.set(None);
                        this.stream.set(Some(observable));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            if let Some(stream) = this.stream.as_mut().as_pin_mut() {
                return futures_lite::Stream::poll_next(stream, cx);
            }
        }
    }
}
