//! Composable operators for [`ice_rpc::Observable`].
//!
//! The [`RxStreamExt`] trait extends any poll-based stream of [`ice_rpc::Event`]
//! with the classic reactive operators. Operators are implemented as pull-based
//! combinators: they wrap the source and implement `futures_lite::Stream`, so a
//! pipeline `take(filter(map(source)))` pulls events through a call stack with
//! no intermediate channel allocation and no spawned task.
//!
//! Available operators:
//! - [`map`](RxStreamExt::map) — transforms every `Next` value;
//! - [`filter`](RxStreamExt::filter) — keeps only the `Next` values matching a
//!   predicate;
//! - [`take`](RxStreamExt::take) — emits at most `n` `Next` values then
//!   completes;
//! - [`skip`](RxStreamExt::skip) — ignores the first `n` values;
//! - [`first`](RxStreamExt::first) — emits only the first `Next` value;
//! - [`first_with`](RxStreamExt::first_with) — emits the first `Next` value
//!   matching a predicate;
//! - [`start_with`](RxStreamExt::start_with) — prefixes the stream with a value;
//! - [`map_err`](RxStreamExt::map_err) — maps the error type to another one;
//! - [`scan`](RxStreamExt::scan) — emits a running accumulator state;
//! - [`tap`](RxStreamExt::tap) — runs a side effect per value;
//! - [`finalize`](RxStreamExt::finalize) — runs a callback once at termination;
//! - [`catch_error`](RxStreamExt::catch_error) — replaces a business `Error`
//!   with a fallback value and completes;
//! - [`delay`](RxStreamExt::delay) — delays every event;
//! - [`timeout`](RxStreamExt::timeout) — emits a technical timeout error on
//!   silence;
//! - [`switch_map`](RxStreamExt::switch_map) — projects each value to an inner
//!   stream and emits from the latest one.

use ice_rpc::Event;

/// Extension trait adding reactive operators to any poll-based stream of
/// [`ice_rpc::Event`].
pub trait RxStreamExt<T, E>: futures_lite::Stream<Item = Event<T, E>> + Sized {
    /// Transforms every `Next` value with `f`. Terminal events are forwarded
    /// unchanged.
    fn map<U, F>(self, f: F) -> Map<Self, F, T, U, E>
    where
        F: FnMut(T) -> U,
    {
        Map::new(self, f)
    }

    /// Keeps only the `Next` values for which `f` returns `true`.
    fn filter<F>(self, f: F) -> Filter<Self, F, T, E>
    where
        F: FnMut(&T) -> bool,
    {
        Filter::new(self, f)
    }

    /// Emits at most `n` `Next` values, then forces a `Complete`.
    fn take(self, n: usize) -> Take<Self, T, E> {
        Take::new(self, n)
    }

    /// Ignores the first `n` `Next` values, then forwards the rest.
    fn skip(self, n: usize) -> Skip<Self, T, E> {
        Skip::new(self, n)
    }

    /// Emits only the first `Next` value, then forces a `Complete`.
    fn first(self) -> First<Self, fn(&T) -> bool, T, E> {
        self.first_with((|_| true) as fn(&T) -> bool)
    }

    /// Emits the first `Next` value matching `predicate`, then completes.
    fn first_with<F>(self, predicate: F) -> First<Self, F, T, E>
    where
        F: FnMut(&T) -> bool,
    {
        First::new(self, predicate)
    }

    /// Prefixes the stream with an initial `Next(value)`.
    fn start_with(self, value: T) -> StartWith<Self, T, E> {
        StartWith::new(self, value)
    }

    /// Transforms the error type `E` into `E2` with `f`.
    fn map_err<F, E2>(self, f: F) -> MapErr<Self, F, T, E, E2>
    where
        F: FnMut(E) -> E2,
    {
        MapErr::new(self, f)
    }

    /// Accumulates every `Next` value into a running state.
    fn scan<U, F>(self, initial: U, f: F) -> Scan<Self, F, T, U, E>
    where
        U: Clone,
        F: FnMut(U, T) -> U,
    {
        Scan::new(self, initial, f)
    }

    /// Runs a side effect on each `Next` value without altering it.
    fn tap<F>(self, f: F) -> Tap<Self, F, T, E>
    where
        F: FnMut(&T),
    {
        Tap::new(self, f)
    }

    /// Runs `f` exactly once when the stream terminates.
    fn finalize<F>(self, f: F) -> Finalize<Self, F, T, E>
    where
        F: FnOnce(),
    {
        Finalize::new(self, f)
    }

    /// Replaces an `Error` with a fallback value, then completes.
    fn catch_error<F>(self, f: F) -> CatchError<Self, F, T, E>
    where
        F: FnOnce(E) -> T,
    {
        CatchError::new(self, f)
    }

    /// Delays every event by `duration`.
    fn delay(self, duration: std::time::Duration) -> Delay<Self, T, E> {
        Delay::new(self, duration)
    }

    /// Emits a technical timeout error if no event arrives within `duration`.
    fn timeout(self, duration: std::time::Duration) -> Timeout<Self, T, E> {
        Timeout::new(self, duration)
    }

    /// Projects each value to an inner stream and emits from the latest one,
    /// cancelling previous subscriptions (RxJS `switchMap`).
    fn switch_map<F, U>(self, f: F) -> SwitchMap<Self, F, T, U, E>
    where
        F: FnMut(T) -> ice_rpc::Observable<U, E>,
    {
        SwitchMap::new(self, f)
    }

    /// Emits a technical `Cancelled` error and stops once `token` is cancelled
    /// (RxJS `takeUntil`).
    fn take_until(self, token: &ice_rpc::CancellationToken) -> TakeUntil<Self, T, E> {
        TakeUntil::new(self, token.clone())
    }

    /// Freezes the pipeline into the concrete [`ice_rpc::Observable`], so it can
    /// be returned by a **service method**.
    ///
    /// A service must return `Observable<T, E>`: the generated proxy needs a
    /// single return type shared by its `Provider` (in-process implementation)
    /// and `Consumer` (IPC client) modes, so an operator type (`Map<…>`,
    /// `Delay<…>`, …) cannot be returned directly. This wraps the pipeline into
    /// the boxed variant of `Observable`.
    ///
    /// The `CompleteWith` single-sample optimization is preserved.
    ///
    /// # Example
    /// ```rust,ignore
    /// async fn watch(&self, count: u32) -> Observable<u32, String> {
    ///     from(1..=count)
    ///         .delay(Duration::from_millis(100))
    ///         .into_observable()
    ///     }
    /// ```
    fn into_observable(self) -> ice_rpc::Observable<T, E>
    where
        Self: Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        ice_rpc::Observable::from_stream(self)
    }

    /// Awaits the first emitted value of the stream.
    ///
    /// `Next(v)` → `Ok(v)`, `Error(e)` → `Err(e.into())`,
    /// `Complete`/closed → `Err(StreamError::Empty)`.
    ///
    /// Same implementation as [`ice_rpc::Observable::first_value`]: both
    /// surfaces delegate to the canonical `ice_rpc::gen::first_event`, so they
    /// cannot diverge. Use this one on an operator pipeline, and the inherent
    /// method on a raw [`ice_rpc::Observable`].
    #[allow(async_fn_in_trait)]
    async fn first_value(self) -> Result<T, ice_rpc::StreamError<E>>
    where
        Self: Sized,
    {
        ice_rpc::gen::first_event(self).await
    }

    /// Collects every emitted value into a `Vec`.
    ///
    /// The stream is consumed until `Complete` (or until it is closed). On a
    /// terminal `Error` the collected values are discarded and the error is
    /// returned.
    ///
    /// Same implementation as [`ice_rpc::Observable::collect`]: both surfaces
    /// delegate to the canonical `ice_rpc::gen::collect_values`.
    #[allow(async_fn_in_trait)]
    async fn collect(self) -> Result<Vec<T>, ice_rpc::ObservableError<E>>
    where
        Self: Sized,
    {
        ice_rpc::gen::collect_values(self).await
    }

    /// Consumes the stream with a callback per value (RxJS `forEach`).
    ///
    /// Fully pull-based: no task is spawned. `Complete` (or a closed source)
    /// yields `Ok(())`; a terminal error yields `Err(ObservableError)`.
    #[allow(async_fn_in_trait)]
    async fn for_each<F>(self, mut f: F) -> Result<(), ice_rpc::ObservableError<E>>
    where
        F: FnMut(T),
        Self: Sized,
    {
        let mut stream = Box::pin(self);
        loop {
            match crate::subscribe::next_event(&mut stream).await {
                Some(Event::Next(v)) => f(v),
                Some(Event::Complete) | None => return Ok(()),
                Some(Event::Error(e)) => return Err(e),
            }
        }
    }

    /// Subscribes to the stream with an [`Observer`](crate::Observer).
    ///
    /// Spawns **one** task that pulls the pipeline and pushes events. Dropping
    /// the returned [`Subscription`](crate::Subscription) cancels it silently.
    ///
    /// # Example
    /// ```rust,ignore
    /// let sub = stream.subscribe_with(
    ///     |v| println!("next: {v:?}"),
    ///     |e| eprintln!("error: {e:?}"),
    ///     || println!("complete"),
    /// );
    /// // ... later
    /// drop(sub);
    /// ```
    fn subscribe<O>(self, observer: O) -> crate::Subscription
    where
        O: crate::Observer<T, E>,
        T: Send + 'static,
        E: Send + 'static,
        Self: Send + 'static,
    {
        let cancel = ice_rpc::CancellationToken::new();
        crate::subscribe::spawn_push(self, observer, cancel.clone());
        crate::Subscription::new(cancel)
    }

    /// Subscribes with three closures (see [`RxStreamExt::subscribe`]).
    fn subscribe_with<N, Er, C>(
        self,
        on_next: N,
        on_error: Er,
        on_complete: C,
    ) -> crate::Subscription
    where
        N: FnMut(T) + Send + 'static,
        Er: FnMut(ice_rpc::ObservableError<E>) + Send + 'static,
        C: FnMut() + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
        Self: Send + 'static,
    {
        self.subscribe(crate::ObserverFns::new(on_next, on_error, on_complete))
    }
}

impl<S, T, E> RxStreamExt<T, E> for S where S: futures_lite::Stream<Item = Event<T, E>> + Sized {}

// Operator implementations, grouped by ReactiveX category.
mod combining;
mod conditional;
mod error_handling;
mod filtering;
mod transforming;
mod utility;

#[cfg(test)]
mod tests;

pub use combining::{merge, StartWith};
pub use conditional::TakeUntil;
pub use error_handling::{retry, retry_with, retry_with_delay, CatchError};
pub use filtering::{Filter, First, Skip, Take};
pub use transforming::{Map, MapErr, Scan, SwitchMap};
pub use utility::{Delay, Finalize, Tap, Timeout};
