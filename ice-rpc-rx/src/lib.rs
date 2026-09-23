//! Reactive layer of ice-rpc: the stream vocabulary, the operators, the
//! multicast primitive and the execution facade they need.
//!
//! This crate is the foundation of `ice-rpc`. It owns [`Observable`], [`Event`],
//! [`ObservableError`] and [`RpcError`], and it knows **nothing about the wire**:
//! no framing, no serialization format, no transport. `ice-rpc` depends on it and
//! re-exports every item under its historical path, so a consumer of `ice-rpc`
//! sees one single stream type and imports nothing new.
//!
//! | Module | Contents |
//! |--------|----------|
//! | `event` | [`Event`], [`ObservableError`] and the producer-side [`Sender`] |
//! | `stream` | [`Observable`] and the local [`channel`] constructors |
//! | `error` | [`RpcError`], the technical error of the whole stack |
//! | `creation` | [`from`], [`of`], [`throw_error`] |
//! | `subject` | [`Subject`], the multicast source |
//! | `subscribe` | [`Subscription`], the cancellation handle |
//! | `transform` | the operators carried by [`Observable`] |
//! | [`rt`] | execution facade and [`CancellationToken`] |
//!
//! The operators are **inherent methods** on [`Observable`]: no extension trait to
//! import, one single stream type from the first operator to the last.
//!
//! ```rust,ignore
//! // `stream` is the native type returned by an ice-rpc service.
//! let stream: ice_rpc_rx::Observable<i32, String> = proxy.foo().await;
//!
//! let odds = stream.filter(|v| *v % 2 == 1).map(|v| v * 10).take(5);
//! let first = odds.first_value().await?;
//! ```
//!
//! - [`Observable`] — the operators (`map`, `filter`, `map_err`, `scan`,
//!   `switch_map`, `take`, `skip`, `first`, `first_with`, `start_with`, `tap`,
//!   `finalize`, `delay`, `timeout`, `catch_error`, `take_until`) and the
//!   terminals (`first_value`, `collect`, `for_each`, `subscribe`,
//!   `subscribe_all`, `next`, `recv`). Every operator is a pull-based
//!   combinator: no intermediate channel, no spawned task. Each has its own
//!   reference, with a runnable example, on [`Observable`].
//! - [`Subject`] — the push side: a multi-producer / multi-consumer multicast
//!   source. [`Subject::new`] multicasts to the current subscribers,
//!   [`Subject::replay`] also replays the last `n` values (and the terminal
//!   state) to late subscribers.
//! - `Observable::subscribe` / `Observable::subscribe_all` — push-based
//!   consumption through RxJS-style callbacks. [`Subscription`] is the
//!   cancellation handle.
//! - [`from`], [`of`], [`throw_error`] — channel-free local constructors.
//!
//! ## Operators
//!
//! | ReactiveX category | Operators |
//! |---|---|
//! | Transforming | [`map`](Observable::map), [`map_err`](Observable::map_err), [`scan`](Observable::scan), [`switch_map`](Observable::switch_map) |
//! | Filtering | [`filter`](Observable::filter), [`take`](Observable::take), [`skip`](Observable::skip), [`first`](Observable::first), [`first_with`](Observable::first_with) |
//! | Combining | [`start_with`](Observable::start_with) |
//! | Conditional / Boolean | [`take_until`](Observable::take_until) |
//! | Error handling | [`catch_error`](Observable::catch_error) |
//! | Utility | [`tap`](Observable::tap), [`finalize`](Observable::finalize), [`delay`](Observable::delay), [`timeout`](Observable::timeout) |
//! | Terminals (they end the chain) | [`collect`](Observable::collect), [`first_value`](Observable::first_value), [`for_each`](Observable::for_each), [`subscribe`](Observable::subscribe), [`subscribe_all`](Observable::subscribe_all), [`next`](Observable::next), [`recv`](Observable::recv) |
//!
//! Two rules hold for every operator: it is **pull-based and lazy** (it only
//! wraps its source in a boxed stream — no intermediate channel, no spawned
//! task), and it never drops or reorders a terminal event unless its own
//! documentation says otherwise. Only [`map_err`](Observable::map_err) and
//! [`catch_error`](Observable::catch_error) act on the **business** error; a
//! technical [`RpcError`] is fatal and travels untouched.
//!
//! ## Normalization
//!
//! A source may carry a single response as an internal single-sample
//! optimization. [`Observable::recv`] normalizes it into `Next` followed by
//! `Complete`, so consuming code always observes a uniform stream of `Next`
//! values followed by a terminal event (`Complete` or `Error`).
//!
//! ## Timeouts
//!
//! [`Observable::timeout`] is a per-event **silence watchdog** on an
//! active stream, reset after every event.

#![cfg_attr(test, allow(clippy::unwrap_used))] // test code may panic

mod creation;
mod error;
mod event;
pub mod rt;
mod stream;
mod subject;
mod subscribe;
mod transform;

#[cfg(test)]
mod tests;

pub use creation::{from, of, throw_error};
pub use error::RpcError;
pub use event::{Event, ObservableError, Sender};
pub use rt::CancellationToken;
pub use stream::{channel, collect_values, first_event, unbounded_channel, Observable};
pub use subject::Subject;
pub use subscribe::Subscription;

/// Default capacity of the channels created by the multicast primitive.
///
/// A bounded channel provides backpressure: a producer waits when the queue is
/// full, which keeps memory usage bounded.
pub(crate) const MULTICAST_CHANNEL_CAPACITY: usize = 8;
