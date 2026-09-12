//! Reactive operators, as **inherent methods** on [`crate::Observable`].
//!
//! There is no extension trait to import and no second stream type to name:
//! every operator is called directly on the `Observable` returned by a service
//! and returns another `Observable`, so a pipeline reads like RxJS:
//!
//! ```rust,ignore
//! // `stream` is the native type returned by an ice-rpc service.
//! let stream: crate::Observable<i32, String> = proxy.foo().await;
//!
//! let odds = stream
//!     .filter(|v| *v % 2 == 1)
//!     .map(|v| v * 10)
//!     .take(5);
//! ```
//!
//! Each step wraps the previous one through
//! [`Observable::from_stream`](crate::Observable::from_stream), i.e. a boxed
//! poll-based combinator: no intermediate channel, no spawned task, no
//! `Arc`/lock. The cost is one box per operator, measured by
//! `benches/pipeline.rs`.
//!
//! Operators:
//! - [`Observable::map`] / [`Observable::map_err`] — transform the value / the
//!   business error;
//! - [`Observable::filter`] — keeps the values matching a predicate;
//! - [`Observable::take`] / [`Observable::skip`] — limits / skips the first
//!   `n` values;
//! - [`Observable::first`] / [`Observable::first_with`] — emits one value then
//!   completes;
//! - [`Observable::start_with`] — prefixes an initial value;
//! - [`Observable::scan`] — emits a running accumulator;
//! - [`Observable::tap`] / [`Observable::finalize`] — side effects per value /
//!   at termination;
//! - [`Observable::catch_error`] — replaces a business error with a fallback;
//! - [`Observable::delay`] / [`Observable::timeout`] — time-based operators;
//! - [`Observable::switch_map`] — projects each value to the latest inner
//!   stream;
//! - [`Observable::take_until`] — stops when a [`crate::CancellationToken`]
//!   fires.
//!
//! Terminals (`first_value`, `collect`, [`Observable::for_each`],
//! [`Observable::subscribe`], [`Observable::subscribe_with`]) consume the
//! stream and are the only ones that end the chain.

use std::time::Duration;

use crate::{Observable, ObservableError};

use super::{Observer, ObserverFns, Subscription};

impl<T, E> Observable<T, E> {
    /// Transforms every `Next` value with `f`; terminal events pass through
    /// unchanged (RxJS `map`).
    pub fn map<U, F>(self, f: F) -> Observable<U, E>
    where
        F: FnMut(T) -> U + Send + 'static,
        T: Send + 'static,
        U: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(Map::new(self, f))
    }

    /// Transforms the **business** error type with `f`; technical errors pass
    /// through unchanged (RxJS `map`, error channel only).
    pub fn map_err<F, E2>(self, f: F) -> Observable<T, E2>
    where
        F: FnMut(E) -> E2 + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
        E2: Send + 'static,
    {
        Observable::from_stream(MapErr::new(self, f))
    }

    /// Keeps only the `Next` values for which `predicate` returns `true`
    /// (RxJS `filter`).
    pub fn filter<F>(self, predicate: F) -> Observable<T, E>
    where
        F: FnMut(&T) -> bool + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(Filter::new(self, predicate))
    }

    /// Emits at most `n` values, then completes (RxJS `take`).
    pub fn take(self, n: usize) -> Observable<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(Take::new(self, n))
    }

    /// Ignores the first `n` values, then forwards the rest (RxJS `skip`).
    pub fn skip(self, n: usize) -> Observable<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(Skip::new(self, n))
    }

    /// Emits only the first value, then completes (RxJS `first`).
    pub fn first(self) -> Observable<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        self.first_with(|_: &T| true)
    }

    /// Emits the first value matching `predicate`, then completes.
    pub fn first_with<F>(self, predicate: F) -> Observable<T, E>
    where
        F: FnMut(&T) -> bool + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(First::new(self, predicate))
    }

    /// Prefixes the stream with an initial value (RxJS `startWith`).
    pub fn start_with(self, value: T) -> Observable<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(StartWith::new(self, value))
    }

    /// Emits a running accumulator, one value per source value (RxJS `scan`).
    pub fn scan<U, F>(self, initial: U, accumulator: F) -> Observable<U, E>
    where
        U: Clone + Send + 'static,
        F: FnMut(U, T) -> U + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(Scan::new(self, initial, accumulator))
    }

    /// Runs `f` on every value without altering it (RxJS `tap`).
    pub fn tap<F>(self, f: F) -> Observable<T, E>
    where
        F: FnMut(&T) + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(Tap::new(self, f))
    }

    /// Runs `f` exactly once when the stream terminates, whatever the outcome
    /// (RxJS `finalize`).
    pub fn finalize<F>(self, f: F) -> Observable<T, E>
    where
        F: FnOnce() + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(Finalize::new(self, f))
    }

    /// Replaces a **business** error with a fallback value, then completes
    /// (RxJS `catchError`). Technical errors are forwarded unchanged.
    pub fn catch_error<F>(self, f: F) -> Observable<T, E>
    where
        F: FnOnce(E) -> T + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(CatchError::new(self, f))
    }

    /// Delays every event by `duration` (RxJS `delay`).
    pub fn delay(self, duration: Duration) -> Observable<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(Delay::new(self, duration))
    }

    /// Emits a technical [`crate::RpcError::Timeout`] if no event arrives
    /// within `duration`; the timer resets after every event (RxJS `timeout`).
    pub fn timeout(self, duration: Duration) -> Observable<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(Timeout::new(self, duration))
    }

    /// Projects each value to an inner stream and emits from the latest one,
    /// cancelling the previous one (RxJS `switchMap`).
    pub fn switch_map<F, U>(self, f: F) -> Observable<U, E>
    where
        F: FnMut(T) -> Observable<U, E> + Send + 'static,
        T: Send + 'static,
        U: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(SwitchMap::new(self, f))
    }

    /// Emits a technical [`crate::RpcError::Cancelled`] and stops once `token`
    /// is cancelled (RxJS `takeUntil`).
    pub fn take_until(self, token: &crate::CancellationToken) -> Observable<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(TakeUntil::new(self, token.clone()))
    }

    /// Consumes the stream value by value (RxJS `forEach`).
    ///
    /// Fully pull-based: no task is spawned. Returns `Ok(())` on a normal end
    /// and the terminal error (business or technical) otherwise.
    pub async fn for_each<F>(mut self, mut f: F) -> Result<(), ObservableError<E>>
    where
        F: FnMut(T),
    {
        while let Some(event) = self.next().await {
            match event {
                Ok(value) => f(value),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Subscribes with an [`Observer`]: **one** task pulls the stream and
    /// pushes `next` / `error` / `complete`.
    ///
    /// Dropping the returned [`Subscription`] cancels it silently.
    pub fn subscribe<O>(self, observer: O) -> Subscription
    where
        O: Observer<T, E>,
        T: Send + 'static,
        E: Send + 'static,
    {
        let cancel = crate::CancellationToken::new();
        super::subscribe::spawn_push(self, observer, cancel.clone());
        Subscription::new(cancel)
    }

    /// Subscribes with the three RxJS callbacks: `on_next`, `on_error`
    /// (business **or** technical) and `on_complete`.
    ///
    /// Exactly one of `on_error` / `on_complete` runs, and neither runs when the
    /// [`Subscription`] is dropped (RxJS `unsubscribe`).
    pub fn subscribe_with<N, Er, C>(self, on_next: N, on_error: Er, on_complete: C) -> Subscription
    where
        N: FnMut(T) + Send + 'static,
        Er: FnMut(ObservableError<E>) + Send + 'static,
        C: FnMut() + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.subscribe(ObserverFns::new(on_next, on_error, on_complete))
    }
}

// Operator implementations, grouped by ReactiveX category. They are private to
// the crate: the operator *types* never appear in a public signature anymore
// (every method above returns `Observable`).
mod combining;
mod conditional;
mod error_handling;
mod filtering;
mod transforming;
mod utility;

#[cfg(test)]
mod tests;

pub(crate) use combining::StartWith;
pub(crate) use conditional::TakeUntil;
pub(crate) use error_handling::CatchError;
pub(crate) use filtering::{Filter, First, Skip, Take};
pub(crate) use transforming::{Map, MapErr, Scan, SwitchMap};
pub(crate) use utility::{Delay, Finalize, Tap, Timeout};
