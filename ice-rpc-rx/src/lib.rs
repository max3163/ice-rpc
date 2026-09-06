//! # ice-rpc-rx
//!
//! Reactive extensions for [`ice_rpc`] event streams, inspired by RxJS.
//!
//! This crate extends the native [`ice_rpc::Stream`] type with composable
//! operators and provides two multicast primitives:
//!
//! - [`RxStreamExt`] — `map`, `filter` and `take` operators applied directly
//!   on [`ice_rpc::Stream`].
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
