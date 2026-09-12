//! Reactive operators for ice-rpc event streams, inspired by RxJS.
//!
//! This module extends the poll-based [`Observable`](crate::Observable) with
//! composable operators and provides multicast primitives, constructors and
//! terminal consumption helpers. Operators are pull-based combinators: a
//! pipeline composes without allocating an intermediate channel or spawning a
//! task per operator.
//!
//! - [`RxStreamExt`] — `map`, `filter`, `map_err`, `scan`, `switch_map`, `take`,
//!   `skip`, `first`, `first_with`, `start_with`, `tap`, `delay`, `finalize`,
//!   `timeout`, `catch_error` operators, plus the terminals `first_value`,
//!   `collect`, `for_each` and `subscribe`, applied directly on
//!   [`crate::Observable`]. Every operator is a pull-based combinator: it
//!   allocates no intermediate channel and spawns no task. The terminals
//!   `first_value` and `collect` delegate to the same canonical implementation
//!   as the inherent [`crate::Observable::first_value`] /
//!   [`crate::Observable::collect`], so the two surfaces cannot diverge.
//! - [`Observer`] / [`Subscription`] — push-based consumption: `subscribe`
//!   spawns a single task that pushes `next` / `error` / `complete`.
//! - [`merge`] — merges several streams into one.
//! - [`retry`] — retries the underlying call on a business `Error`.
//! - [`from`] — builds a stream from an iterator.
//! - [`of`] — builds a single-value stream (channel-free).
//! - [`throw_error`] — builds a stream that only emits a business error.
//! - [`crate::Observable::first_value`] — awaits the first value of a stream,
//!   and the same terminal is available through [`RxStreamExt::first_value`].
//! - [`crate::Observable::collect`] — gathers every value into a `Vec`, and
//!   the same terminal is available through [`RxStreamExt::collect`].
//! - [`Subject`] — a multi-producer / multi-consumer multicast source.
//! - [`ShareReplay`] — a multicast source that replays the last value to late
//!   subscribers (equivalent to RxJS `shareReplay(1)`).
//!
//! ## Quick example
//!
//! ```rust,ignore
//! use crate::RxStreamExt;
//!
//! // `stream` is the native type returned by an ice-rpc service.
//! let stream: crate::Observable<i32, String> = proxy.foo().await;
//!
//! // Operators chain on the native type and return poll-based combinator streams.
//! let odds = stream
//!     .filter(|v| *v % 2 == 1)
//!     .map(|v| v * 10)
//!     .take(5);
//! ```
//!
//! ## Consuming the first value
//!
//! Terminal consumption is provided natively by [`crate::Observable`]:
//!
//! ```rust,ignore
//! let value = proxy.get("my.key".into()).await.first_value().await?;
//! let all = proxy.list().await.collect().await?; // Vec<T>
//! ```
//!
//! ## Normalization
//!
//! ice-rpc can transport a single response as an internal `CompleteWith`
//! sample. [`crate::Observable::recv`] normalizes it into `Next` followed by
//! `Complete`, so consuming code always observes a uniform stream of `Next`
//! values followed by a terminal event (`Complete` or `Error`).
//!
//! ## Timeouts
//!
//! Two independent timeouts exist, and they cover disjoint phases:
//!
//! - `discovery_timeout` — a **service-level** attribute
//!   (`#[service("Name", discovery_timeout = "5s")]`) bounding the node
//!   discovery performed before the call is sent. This is the only place where a
//!   discovery deadline applies.
//! - [`RxStreamExt::timeout`] — a per-event **silence watchdog** on an active
//!   stream. The timer resets after every received event; once it fires, the
//!   stream terminates with a technical `RpcError::Timeout`.
//!

mod creation;
mod share_replay;
mod subject;
mod subscribe;
mod transform;

pub use creation::{from, of, throw_error};
pub use share_replay::ShareReplay;
pub use subject::Subject;
pub use subscribe::{Observer, ObserverFns, Subscription};
pub use transform::{merge, retry, retry_with, retry_with_delay, RxStreamExt};

/// Default capacity of the channels created by the multicast primitives.
///
/// The operators themselves are pull-based combinators and create no channel;
/// only [`Subject`] and [`ShareReplay`] fan out to per-subscriber channels. A
/// bounded channel provides backpressure: a producer waits when the queue is
/// full, which keeps memory usage bounded.
pub(crate) const MULTICAST_CHANNEL_CAPACITY: usize = 8;

#[cfg(test)]
mod tests {
    use super::{from, of, throw_error, RxStreamExt};
    use crate::{Event, ObservableError};
    use std::convert::Infallible;

    async fn drain<S, T, E>(stream: S) -> Vec<Event<T, E>>
    where
        S: futures_lite::Stream<Item = Event<T, E>>,
    {
        let mut stream = Box::pin(stream);
        let mut out = Vec::new();
        while let Some(event) =
            futures_lite::future::poll_fn(|cx| futures_lite::Stream::poll_next(stream.as_mut(), cx))
                .await
        {
            out.push(event);
        }
        out
    }

    #[test]
    fn from_emits_values_then_complete() {
        let events: Vec<Event<i32, Infallible>> = pollster::block_on(drain(from([1, 2, 3])));
        assert_eq!(events.len(), 4);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        assert!(matches!(&events[1], Event::Next(v) if *v == 2));
        assert!(matches!(&events[2], Event::Next(v) if *v == 3));
        assert!(matches!(&events[3], Event::Complete));
    }

    #[test]
    fn of_emits_next_then_complete() {
        let events: Vec<Event<i32, Infallible>> = pollster::block_on(drain(of(42)));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 42));
        assert!(matches!(&events[1], Event::Complete));
    }

    #[test]
    fn collect_gathers_all_values() {
        let stream: crate::Observable<i32, Infallible> = from([1, 2, 3]);
        let values = pollster::block_on(stream.collect()).unwrap();
        assert_eq!(values, vec![1, 2, 3]);
    }

    #[test]
    fn of_returns_a_channel_free_observable() {
        let stream: crate::Observable<i32, Infallible> = of(7);
        let values = pollster::block_on(stream.collect()).unwrap();
        assert_eq!(values, vec![7]);
    }

    #[test]
    fn pipeline_can_be_frozen_into_an_observable() {
        // A pipeline is usable as the return value of a service method once it
        // is frozen into the concrete `Observable`.
        let stream: crate::Observable<i32, Infallible> =
            from([1, 2, 3]).map(|v| v * 2).into_observable();

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 4);
        assert!(matches!(&events[0], Event::Next(v) if *v == 2));
        assert!(matches!(&events[1], Event::Next(v) if *v == 4));
        assert!(matches!(&events[2], Event::Next(v) if *v == 6));
        assert!(matches!(&events[3], Event::Complete));
    }

    #[test]
    fn frozen_single_value_pipeline_keeps_one_wire_sample() {
        let mut stream: crate::Observable<i32, Infallible> = from([42]).into_observable();

        assert!(matches!(
            pollster::block_on(stream.recv_wire()),
            Ok(crate::gen::WireEvent::CompleteWith(42))
        ));
        assert!(pollster::block_on(stream.recv_wire()).is_err());
    }

    /// Terminal consumption through the inherent [`crate::Observable`] methods.
    fn native_first(events: Vec<Event<i32, String>>) -> Result<i32, crate::StreamError<String>> {
        pollster::block_on(crate::Observable::<i32, String>::from_events(events).first_value())
    }

    /// Same input, consumed through the `RxStreamExt` default method (the
    /// pipeline type is `Map<…>`, so the trait method is selected).
    fn pipeline_first(events: Vec<Event<i32, String>>) -> Result<i32, crate::StreamError<String>> {
        pollster::block_on(
            crate::Observable::<i32, String>::from_events(events)
                .map(|v| v)
                .first_value(),
        )
    }

    /// Same pair, for `collect`.
    fn native_collect(
        events: Vec<Event<i32, String>>,
    ) -> Result<Vec<i32>, crate::ObservableError<String>> {
        pollster::block_on(crate::Observable::<i32, String>::from_events(events).collect())
    }

    fn pipeline_collect(
        events: Vec<Event<i32, String>>,
    ) -> Result<Vec<i32>, crate::ObservableError<String>> {
        pollster::block_on(
            crate::Observable::<i32, String>::from_events(events)
                .map(|v| v)
                .collect(),
        )
    }

    /// Every outcome, fed to both terminal surfaces, must be identical.
    fn terminal_cases() -> Vec<Vec<Event<i32, String>>> {
        vec![
            // Value then `Complete`.
            vec![Event::Next(5), Event::Complete],
            // Values then `Complete`.
            vec![Event::Next(1), Event::Next(2), Event::Complete],
            // Business error, before and after a value.
            vec![Event::Error(ObservableError::Business("boom".into()))],
            vec![
                Event::Next(1),
                Event::Error(ObservableError::Business("boom".into())),
            ],
            // Technical error.
            vec![Event::Error(ObservableError::Technical(
                crate::RpcError::Timeout,
            ))],
            // Empty.
            vec![Event::Complete],
            vec![],
        ]
    }

    #[test]
    fn terminal_first_value_surfaces_agree_on_every_outcome() {
        for case in terminal_cases() {
            let native = native_first(case.clone());
            let pipeline = pipeline_first(case.clone());
            assert_eq!(
                format!("{native:?}"),
                format!("{pipeline:?}"),
                "first_value diverged on {case:?}"
            );
        }

        assert_eq!(
            native_first(vec![Event::Next(5), Event::Complete]).unwrap(),
            5
        );
        assert!(matches!(
            pipeline_first(vec![Event::Complete]),
            Err(crate::StreamError::Empty)
        ));
    }

    #[test]
    fn terminal_collect_surfaces_agree_on_every_outcome() {
        for case in terminal_cases() {
            let native = native_collect(case.clone());
            let pipeline = pipeline_collect(case.clone());
            assert_eq!(
                format!("{native:?}"),
                format!("{pipeline:?}"),
                "collect diverged on {case:?}"
            );
        }

        assert_eq!(
            native_collect(vec![Event::Next(1), Event::Next(2), Event::Complete]).unwrap(),
            vec![1, 2]
        );
        assert!(matches!(
            pipeline_collect(vec![Event::Next(1)]),
            Ok(values) if values == vec![1]
        ));
    }

    #[test]
    fn throw_error_emits_business_error() {
        let events: Vec<Event<i32, String>> =
            pollster::block_on(drain(throw_error::<i32, String>("boom".into())));
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            Event::Error(ObservableError::Business(e)) if e == "boom"
        ));
    }
}
