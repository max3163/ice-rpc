//! Node liveness through iceoryx2's native node monitoring.
//!
//! `Node::list` exposes each node as `Alive` / `Dead`, and a node's process id
//! maps it back to the emitter the collector tracks. The scan is expensive, so it
//! is pulled on demand rather than polled.

use std::collections::HashSet;

use ice_rpc::codegen::iceoryx2::prelude::*;
use ice_rpc::codegen::iceoryx2::service::ipc_threadsafe;
use ice_rpc::codegen::raw_pid_to_u32;

/// Returns the set of PIDs owning at least one **alive** iceoryx2 node.
///
/// Returns `None` when `Node::list` itself fails, so callers can distinguish
/// "no node is alive" from "could not tell" (and never report a spurious death).
pub fn alive_pids() -> Option<HashSet<u32>> {
    let mut pids = HashSet::new();

    let result = Node::<ipc_threadsafe::Service>::list(Config::global_config(), |state| {
        if matches!(state, NodeState::Alive(_)) {
            let pid = state.node_id().pid::<ipc_threadsafe::Service>();
            pids.insert(pid.ok().map(|p| raw_pid_to_u32(p.value())).unwrap_or(0));
        }
        CallbackProgression::Continue
    });

    match result {
        Ok(()) => Some(pids),
        Err(e) => {
            log::warn!("[monitor] Node::list failed: {:?}", e);
            None
        }
    }
}

/// Returns `true` when the given PID owns an alive iceoryx2 node.
///
/// Conservative on error: an inconclusive scan reports the node as alive, so
/// that a monitoring failure never declares a live emitter dead.
pub fn is_pid_alive(pid: u32) -> bool {
    alive_pids().map(|pids| pids.contains(&pid)).unwrap_or(true)
}
