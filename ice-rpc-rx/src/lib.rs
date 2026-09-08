//! # ice-rpc-rx
//!
//! Reactive extensions for [`ice_rpc`] event streams, inspired by RxJS.
//!
//! This crate extends the native [`ice_rpc::Stream`] type with composable
//! operators and provides two multicast primitives:
//!
//! - [`RxStreamExt`] — `map`, `filter`, `take`, `finalize`, `tap`, `delay` and
//!   `catch_error` operators applied directly on [`ice_rpc::Stream`].
//! - [`Subject`] — a multi-producer / multi-consumer multicast source.
//! - [`ShareReplay`] — a multicast source that replays the last value to late
//!   subscribers (equivalent to RxJS `shareReplay(1)`).
//! - [`from`] — builds a stream from an iterator;
//! - [`of`] — builds a single-value stream via `CompleteWith`.
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
//! ## Normalization
//!
//! ice-rpc can transport a single response as [`ice_rpc::Event::CompleteWith`].
//! All operators in this crate treat `CompleteWith` exactly like `Next`, so
//! consuming code always observes a uniform stream of `Next` values followed by
//! a terminal event (`Complete`, `Error` or `RpcError`).
//!
//! [`ice_rpc`]: ../ice_rpc
//! [`ice_rpc::Stream`]: ../ice_rpc/type.Stream.html
//! [`ice_rpc::Event::CompleteWith`]: ../ice_rpc/enum.Event.html

mod operators;
mod share_replay;
mod subject;

pub use operators::RxStreamExt;
pub use share_replay::ShareReplay;
pub use subject::Subject;

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

/// Creates a [`Stream`] from an iterator, emitting each value as `Next` then
/// `Complete`.
///
/// Equivalent to RxJS `from`.
///
/// # Example
/// ```rust,ignore
/// use ice_rpc_rx::from;
///
/// let stream = from([1, 2, 3]);
/// ```
pub fn from<T, I>(iter: I) -> ice_rpc::Stream<T, RxError>
where
    I: IntoIterator<Item = T>,
    T: Send + 'static,
{
    // Collect upfront so the iterator itself does not need to be `Send`: only
    // the resulting `Vec<T>` is moved into the spawned task.
    let values: Vec<T> = iter.into_iter().collect();
    let (tx, rx) = ice_rpc::channel::<T, RxError>(OPERATOR_CHANNEL_CAPACITY);
    ice_rpc::rt::spawn(async move {
        for value in values {
            if tx.send(ice_rpc::Event::Next(value)).await.is_err() {
                return;
            }
        }
        let _ = tx.send(ice_rpc::Event::Complete).await;
    });
    rx
}

/// Creates a single-value [`Stream`].
///
/// Consumers observe the value as `Next` followed by `Complete`: the transport
/// optimization `CompleteWith` is used internally and normalized away.
/// Equivalent to RxJS `of`.
///
/// # Example
/// ```rust,ignore
/// use ice_rpc_rx::of;
///
/// let stream = of(42);
/// ```
pub fn of<T>(value: T) -> ice_rpc::Stream<T, RxError>
where
    T: Send + 'static,
{
    // `of` emits the value as a transport-level `CompleteWith`, then the
    // returned stream is normalized so consumers observe `Next` + `Complete`.
    let (tx, rx) = ice_rpc::channel::<T, RxError>(1);
    ice_rpc::rt::spawn(async move {
        let _ = tx.send(ice_rpc::Event::CompleteWith(value)).await;
    });
    ice_rpc::normalize_stream(rx)
}

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
}
