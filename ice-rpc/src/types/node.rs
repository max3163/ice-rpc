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
/// iceoryx2 exposes a node's process id as a signed `pid_t`; an anomalous value
/// is mapped to `0` rather than wrapping into a plausible-looking but wrong id.
/// Generic over the input so it compiles whatever signedness `pid_t` has.
#[inline]
pub fn raw_pid_to_u32<P: TryInto<u32>>(pid: P) -> u32 {
    pid.try_into().unwrap_or(0)
}

/// Converts the result of an iceoryx2 node id's `pid()` call into the unsigned
/// value stored in [`NodeId`].
///
/// Since iceoryx2 0.10 a service's `UniqueIdGenerator` may legitimately not
/// provide a process id (a custom generator returns `NotImplemented`), so
/// `UniqueNodeId::pid()` yields a `Result`; a missing or anomalous value is
/// mapped to `0`, the same sentinel [`raw_pid_to_u32`] uses.
#[inline]
pub(crate) fn node_pid_to_u32<E>(pid: Result<ProcessId, E>) -> u32 {
    pid.map(|p| raw_pid_to_u32(p.value())).unwrap_or(0)
}

impl std::fmt::Display for NodeId {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node-{}", self.0)
    }
}
