//! # ice-rpc-rx
//!
//! Reactive extensions for [`ice_rpc`] event streams, inspired by RxJS.
//!
//! This crate extends the native [`ice_rpc::Stream`] type with composable
//! operators and provides multicast primitives, constructors and terminal
//! consumption helpers:
//!
//! - [`RxStreamExt`] — `map`, `filter`, `map_err`, `scan`, `take`, `skip`,
//!   `first`, `first_with`, `start_with`, `tap`, `delay`, `finalize`, `timeout`
//!   and `catch_error` operators applied directly on [`ice_rpc::Stream`].
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
//! // Operators chain on the native type and return the native type.
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
    use super::{from, of};
    use ice_rpc::Event;

    #[test]
    fn from_emits_values_then_complete() {
        let stream = from([1, 2, 3]);

        let events = pollster::block_on(async {
            let mut out = Vec::new();
            while let Ok(ev) = stream.recv().await {
                match ev {
                    Event::Next(v) => out.push(v),
                    Event::Complete => break,
                    other => panic!("unexpected event: {:?}", other),
                }
            }
            out
        });
        assert_eq!(events, vec![1, 2, 3]);
    }

    #[test]
    fn of_emits_next_then_complete() {
        let stream = of(42);

        let events = pollster::block_on(async {
            let mut out = Vec::new();
            while let Ok(ev) = stream.recv().await {
                match ev {
                    Event::Next(v) => out.push(v),
                    Event::Complete => break,
                    other => panic!("unexpected event: {:?}", other),
                }
            }
            out
        });
        assert_eq!(events, vec![42]);
    }

    #[test]
    fn collect_gathers_all_values() {
        let values = pollster::block_on(from([1, 2, 3]).collect()).unwrap();
        assert_eq!(values, vec![1, 2, 3]);
    }
}
