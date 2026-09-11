//! Fundamental types of the ice-rpc RPC protocol.
//!
//! The module is split by concern; every item is re-exported here, so
//! `crate::types::X` (and the public `ice_rpc::X`) keep working unchanged.
//!
//! | Sub-module   | Contents                                                        |
//! |--------------|-----------------------------------------------------------------|
//! | [`node`]     | [`NodeId`] and the PID conversion helper                        |
//! | [`wire`]     | [`ObservableError`], [`Event`], [`WireEvent`], [`Sender`], [`EventKind`] |
//! | [`stream`]   | [`Observable`] (the concrete stream), [`StreamError`], [`channel`] |
//! | [`header`]   | [`RpcHeader`] and the correlation-id helpers                    |
//! | [`error`]    | [`RpcError`]                                                    |
//! | [`consts`]   | Tuning constants (timeouts, buffer sizes, …)                    |

pub use iceoryx2_bb_container::string::StaticString;

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
pub use header::{caller_pid_from_cid, fmt_correlation_id, fmt_correlation_id_short, RpcHeader};
pub use node::*;
pub use stream::{
    channel, collect_values, first_event, unbounded_channel, Observable, StreamError,
};
pub use wire::{Event, EventKind, ObservableError, Sender, WireEvent};

// Shared by the native request/response transport to normalize the `WireEvent`
// it receives over the wire into the user-facing `Event`.
pub(crate) use wire::normalize_wire_event;
