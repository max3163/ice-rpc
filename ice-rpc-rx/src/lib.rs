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
//!   `timeout` and `catch_error` operators applied directly on
//!   [`ice_rpc::Stream`].
//! - [`merge`] — merges several streams into one.
//! - [`retry`] — retries the underlying call on a business `Error`.
//! - [`from`] — builds a stream from an iterator.
//! - [`of`] — builds a single-value stream.
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
//! let stream: ice_rpc::Stream<i32, String> = proxy.foo().await?;
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
//! let value = proxy.get("my.key".into()).await?.first_value().await?;
//! let all = proxy.list().await?.collect().await?; // Vec<T>
//! ```
//!
//! ## Normalization
//!
//! ice-rpc can transport a single response as an internal `CompleteWith`
//! sample. [`ice_rpc::Stream::recv`] normalizes it into `Next` followed by
//! `Complete`, so consuming code always observes a uniform stream of `Next`
//! values followed by a terminal event (`Complete`, `Error` or `RpcError`).
//!
//! [`ice_rpc`]: ../ice_rpc
//! [`ice_rpc::Stream`]: ../ice_rpc/type.Stream.html

mod creation;
mod join;
mod share_replay;
mod subject;
mod transform;

pub use creation::{from, of};
pub use join::{merge, retry, retry_with, retry_with_delay};
pub use share_replay::ShareReplay;
pub use subject::Subject;
pub use transform::RxStreamExt;

/// Error type for local reactive sources and operators.
///
/// Currently uninhabited: local constructors such as [`from`] and [`of`] never
/// fail. Future fallible operators (e.g. `timeout`, `retry`) will add variants
/// to this enum.
#[derive(Debug)]
pub enum RxError {}

/// Default capacity of the intermediate channels created by the operators.
///
/// A bounded channel provides backpressure: a producer waits when the queue is
/// full, which keeps memory usage bounded in reactive pipelines.
pub(crate) const OPERATOR_CHANNEL_CAPACITY: usize = 8;

#[cfg(test)]
mod tests {
    use super::{from, of, RxStreamExt};
    use ice_rpc::Event;

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
        let events = pollster::block_on(drain(from([1, 2, 3])));
        assert_eq!(events.len(), 4);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        assert!(matches!(&events[1], Event::Next(v) if *v == 2));
        assert!(matches!(&events[2], Event::Next(v) if *v == 3));
        assert!(matches!(&events[3], Event::Complete));
    }

    #[test]
    fn of_emits_next_then_complete() {
        let events = pollster::block_on(drain(of(42)));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 42));
        assert!(matches!(&events[1], Event::Complete));
    }

    #[test]
    fn collect_gathers_all_values() {
        let values = pollster::block_on(from([1, 2, 3]).collect()).unwrap();
        assert_eq!(values, vec![1, 2, 3]);
    }
}
