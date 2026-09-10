//! # ice-rpc-rx
//!
//! Reactive extensions for [`ice_rpc`] event streams, inspired by RxJS.
//!
//! This crate extends any poll-based stream of [`ice_rpc::Event`] with
//! composable operators and provides multicast primitives, constructors and
//! terminal consumption helpers. Operators are pull-based combinators: a
//! pipeline composes without allocating an intermediate channel or spawning a
//! task per operator.
//!
//! - [`RxStreamExt`] — `map`, `filter`, `map_err`, `scan`, `switch_map`, `take`,
//!   `skip`, `first`, `first_with`, `start_with`, `tap`, `delay`, `finalize`,
//!   `timeout`, `catch_error` operators, plus the terminals `first_value`,
//!   `collect`, `for_each` and `subscribe`, applied directly on
//!   [`ice_rpc::Stream`].
//! - [`Observer`] / [`Subscription`] — push-based consumption: `subscribe`
//!   spawns a single task that pushes `next` / `error` / `complete`.
//! - [`merge`] — merges several streams into one.
//! - [`retry`] — retries the underlying call on a business `Error`.
//! - [`from`] — builds a stream from an iterator.
//! - [`of`] — builds a single-value stream (channel-free).
//! - [`throw_error`] — builds a stream that only emits a business error.
//! - [`ice_rpc::Stream::first_value`] — awaits the first value of a stream.
//! - [`ice_rpc::Stream::collect`] — gathers every value into a `Vec`.
//! - [`Subject`] — a multi-producer / multi-consumer multicast source.
//! - [`ShareReplay`] — a multicast source that replays the last value to late
//!   subscribers (equivalent to RxJS `shareReplay(1)`).
//!
//! ## Quick example
//!
//! ```rust,ignore
//! use ice_rpc_rx::RxStreamExt;
//!
//! // `stream` is the native type returned by an ice-rpc service.
//! let stream: ice_rpc::Stream<i32, String> = proxy.foo().await;
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
//! Terminal consumption is provided natively by [`ice_rpc::Stream`]:
//!
//! ```rust,ignore
//! let value = proxy.get("my.key".into()).await.first_value().await?;
//! let all = proxy.list().await.collect().await?; // Vec<T>
//! ```
//!
//! ## Normalization
//!
//! ice-rpc can transport a single response as an internal `CompleteWith`
//! sample. [`ice_rpc::Stream::recv`] normalizes it into `Next` followed by
//! `Complete`, so consuming code always observes a uniform stream of `Next`
//! values followed by a terminal event (`Complete` or `Error`).
//!
//! [`ice_rpc`]: ../ice_rpc
//! [`ice_rpc::Stream`]: ../ice_rpc/type.Stream.html

mod creation;
mod join;
mod share_replay;
mod subject;
mod subscribe;
mod transform;

pub use creation::{from, of, throw_error};
pub use join::{merge, retry, retry_with, retry_with_delay};
pub use share_replay::ShareReplay;
pub use subject::Subject;
pub use subscribe::{Observer, ObserverFns, Subscription};
pub use transform::RxStreamExt;

/// Placeholder business-error type for local reactive sources.
///
/// Currently uninhabited: it is meant to be used as the `E` parameter of
/// [`from`] and [`of`] when no business error can occur. Technical failures are
/// reported through [`ice_rpc::ObservableError::Technical`].
#[derive(Debug)]
pub enum RxError {}

/// Default capacity of the intermediate channels created by the operators.
///
/// A bounded channel provides backpressure: a producer waits when the queue is
/// full, which keeps memory usage bounded in reactive pipelines.
pub(crate) const OPERATOR_CHANNEL_CAPACITY: usize = 8;

#[cfg(test)]
mod tests {
    use super::{from, of, throw_error, RxError, RxStreamExt};
    use ice_rpc::{Event, ObservableError};

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
        let events: Vec<Event<i32, RxError>> = pollster::block_on(drain(from([1, 2, 3])));
        assert_eq!(events.len(), 4);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        assert!(matches!(&events[1], Event::Next(v) if *v == 2));
        assert!(matches!(&events[2], Event::Next(v) if *v == 3));
        assert!(matches!(&events[3], Event::Complete));
    }

    #[test]
    fn of_emits_next_then_complete() {
        let events: Vec<Event<i32, RxError>> = pollster::block_on(drain(of(42)));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 42));
        assert!(matches!(&events[1], Event::Complete));
    }

    #[test]
    fn collect_gathers_all_values() {
        let stream: ice_rpc::Stream<i32, RxError> = from([1, 2, 3]);
        let values = pollster::block_on(stream.collect()).unwrap();
        assert_eq!(values, vec![1, 2, 3]);
    }

    #[test]
    fn of_returns_a_channel_free_observable() {
        let stream: ice_rpc::Stream<i32, RxError> = of(7);
        let values = pollster::block_on(stream.collect()).unwrap();
        assert_eq!(values, vec![7]);
    }

    #[test]
    fn pipeline_can_be_frozen_into_an_observable() {
        // A pipeline is usable as the return value of a service method once it
        // is frozen into the concrete `Stream`.
        let stream: ice_rpc::Stream<i32, RxError> =
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
        let mut stream: ice_rpc::Stream<i32, RxError> = from([42]).into_observable();

        assert!(matches!(
            pollster::block_on(stream.recv_wire()),
            Ok(ice_rpc::WireEvent::CompleteWith(42))
        ));
        assert!(pollster::block_on(stream.recv_wire()).is_err());
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
