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
