//! Fundamental types of the ice-rpc protocol.
//!
//! The module gathers what the protocol and the transport exchange, and
//! **re-exports** the reactive vocabulary from `ice-rpc-rx`, so the transport
//! keeps naming `crate::types::Event` even though the item lives in the stream
//! crate.
//!
//! | Sub-module   | Contents                                                        |
//! |--------------|-----------------------------------------------------------------|
//! | [`node`]     | [`NodeId`] and the PID conversion helper                        |
//! | [`context`]  | [`CallContext`] — the call being served, read-only               |
//! | [`header`]   | [`RpcHeader`] (zero-copy `user_header`), [`EventKind`]          |
//! | [`wire`]     | [`WireEvent`] — the serializable event the transport publishes   |
//! | [`consts`]   | Name-length limits shared with `ice-rpc-macros`                 |
//!
//! [`Event`], [`ObservableError`], [`Observable`], [`RpcError`], [`Sender`] and
//! the channel constructors are defined by [`ice_rpc_rx`]; they are re-exported
//! here and on the crate root so a consumer sees the same paths as before.

mod consts;
mod context;
mod header;
mod node;
mod wire;

pub use consts::*;
pub use context::{call_scoped, local_call_scoped, BoxResponseFuture, CallContext, TraceContext};

// The transport installs the token of a call around every poll of its task; the
// accessor itself is public, the installer stays inside the crate.
pub(crate) use context::install_call_cancellation;
pub use header::{
    fmt_correlation_id, next_correlation_id, now_ns, service_id_of, EventKind, RpcHeader,
    ServiceRef, CORRELATION_ID_LEN,
};
pub use node::*;
pub use wire::WireEvent;

// Used by the transport to normalize a received `WireEvent` into an `Event`.
pub(crate) use wire::normalize_wire_event;

// The reactive vocabulary, re-exported so this module stays the single place
// from which the transport and the generated code name their types.
pub use ice_rpc_rx::{
    channel, collect_values, first_event, unbounded_channel, Event, Observable, ObservableError,
    RpcError, Sender,
};
