//! Fundamental types of the ice-rpc RPC protocol.
//!
//! The module is split by concern; every item is re-exported here, so
//! `crate::types::X` (and the public `ice_rpc::X`) keep working unchanged.
//!
//! | Sub-module   | Contents                                                        |
//! |--------------|-----------------------------------------------------------------|
//! | [`node`]     | [`NodeId`] and the PID conversion helper                        |
//! | [`header`]   | [`RpcHeader`] (zero-copy `user_header`), [`EventKind`]          |
//! | [`wire`]     | [`ObservableError`] (the single error type), [`Event`], [`WireEvent`], [`Sender`] |
//! | [`stream`]   | [`Observable`] (the concrete stream), [`channel`]               |
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
    fmt_correlation_id, next_correlation_id, service_id_of, EventKind, RpcHeader,
    CORRELATION_ID_LEN,
};
pub use node::*;
pub use stream::{channel, collect_values, first_event, unbounded_channel, Observable};
pub use wire::{Event, ObservableError, Sender, WireEvent};

// Used by the transport to normalize a received `WireEvent` into an `Event`.
pub(crate) use wire::normalize_wire_event;
