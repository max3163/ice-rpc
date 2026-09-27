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
        Self(cached_pid())
    }
}

/// The host process id, read **once**.
///
/// `std::process::id()` is a syscall, and two per-call paths need it: the
/// correlation id a consumer mints ([`next_correlation_id`]) and the span id a
/// provider mints ([`next_span_id`]). `perf` showed `__getpid` at ~0.5 % of the
/// provider's self time before this cache. A process id cannot change after the
/// process starts, so a `OnceLock` holds it for the life of the process.
///
/// [`next_correlation_id`]: super::header::next_correlation_id
/// [`next_span_id`]: super::context::next_span_id
#[inline]
pub(crate) fn cached_pid() -> u32 {
    use std::sync::OnceLock;

    static PID: OnceLock<u32> = OnceLock::new();
    *PID.get_or_init(std::process::id)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_current_node_is_this_process() {
        assert_eq!(NodeId::current().0, std::process::id());
        // The pid is read once and cached for the life of the process.
        assert_eq!(cached_pid(), cached_pid());
    }

    #[test]
    fn a_node_id_renders_with_its_prefix() {
        assert_eq!(NodeId(7).to_string(), "node-7");
        assert_eq!(NodeId(0).to_string(), "node-0");
    }

    #[test]
    fn only_representable_pids_survive_the_conversion() {
        assert_eq!(raw_pid_to_u32(42u32), 42);
        assert_eq!(raw_pid_to_u32(0u32), 0);
        // An anomalous raw `pid_t` must not wrap into a plausible-looking node id.
        assert_eq!(raw_pid_to_u32(-1i32), 0);
        assert_eq!(raw_pid_to_u32(i64::from(i32::MIN)), 0);
        assert_eq!(raw_pid_to_u32(u64::MAX), 0);
    }

    #[test]
    fn a_missing_process_id_is_reported_as_zero() {
        // A custom iceoryx2 id generator may not provide a pid at all.
        assert_eq!(node_pid_to_u32::<()>(Err(())), 0);
    }
}
