//! Node identity and iceoryx2 topic naming.
//!
//! The topic names derive from the [`NodeId`]; their format is part of the wire
//! contract shared with the Node.js gateway.

use iceoryx2::prelude::*;

/// Compact identifier of an ice-rpc node on the iceoryx2 bus.
///
/// Corresponds to the PID of the host process, guaranteeing uniqueness
/// across processes on the same machine.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ZeroCopySend, Default)]
pub struct NodeId(pub u32);

impl NodeId {
    /// Creates a [`NodeId`] from the current process PID.
    #[inline]
    pub fn current() -> Self {
        Self(std::process::id())
    }
}

/// Converts an iceoryx2 raw process identifier to the unsigned value stored in
/// [`NodeId`].
///
/// iceoryx2 exposes a node's process id as a signed `pid_t` (see
/// `UniqueNodeId::pid()`), while [`NodeId`] mirrors the `u32` returned by
/// [`std::process::id`]. Any live process has a positive PID, so the conversion
/// is infallible in practice; an anomalous value is mapped to `0` (a PID no real
/// process owns) rather than silently wrapping via `as` into a plausible-looking
/// but wrong identity.
///
/// The helper is generic over the input so it compiles whatever signedness the
/// target exposes for `pid_t` (it is `i32` on unix) without a platform `cfg`.
#[inline]
pub fn raw_pid_to_u32<P: TryInto<u32>>(pid: P) -> u32 {
    pid.try_into().unwrap_or(0)
}

impl std::fmt::Display for NodeId {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node-{}", self.0)
    }
}
/// Returns the `"default"` topic name for a given node.
pub fn node_default_topic(node_id: NodeId) -> String {
    format!("node_{}_default", node_id.0)
}

/// Returns the `"large"` topic name for a given node.
pub fn node_large_topic(node_id: NodeId) -> String {
    format!("node_{}_large", node_id.0)
}

/// Returns the `"notify"` topic name for a given node.
pub fn node_notify_topic(node_id: NodeId) -> String {
    format!("node_{}_notify", node_id.0)
}
