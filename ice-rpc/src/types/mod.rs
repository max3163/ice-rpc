//! Fundamental types of the ice-rpc RPC protocol.
//!
//! The module is split by concern; every item is re-exported here, so
//! `crate::types::X` (and the public `ice_rpc::X`) keep working unchanged.
//!
//! | Sub-module   | Contents                                                        |
//! |--------------|-----------------------------------------------------------------|
//! | [`node`]     | [`NodeId`] and the PID conversion helper                        |
//! | [`header`]   | [`RpcHeader`] (zero-copy `user_header`), [`EventKind`]          |
//! | [`wire`]     | [`ObservableError`], [`Event`], [`WireEvent`], [`Sender`]       |
//! | [`stream`]   | [`Observable`] (the concrete stream), [`StreamError`], [`channel`] |
//! | [`error`]    | [`RpcError`]                                                    |
//! | [`consts`]   | Name-length limits shared with `ice-rpc-macros`                 |

mod consts;
mod error;
mod header;
mod node;
mod stream;
mod wire;

#[cfg(test)]
mod tests;

pub use consts::*;
pub use error::RpcError;
pub use header::{
    fmt_correlation_id, next_correlation_id, EventKind, RpcHeader, CORRELATION_ID_LEN,
};
pub use node::*;
pub use stream::{
    channel, collect_values, first_event, unbounded_channel, Observable, StreamError,
};
pub use wire::{Event, ObservableError, Sender, WireEvent};

// Shared by the transport to normalize the `WireEvent` it receives over the
// wire into the user-facing `Event`.
pub(crate) use wire::normalize_wire_event;
