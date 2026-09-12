//! Node liveness through iceoryx2's native node monitoring.
//!
//! `Node::list` exposes each node as [`NodeState::Alive`] / [`NodeState::Dead`],
//! and `UniqueNodeId::pid()` maps it back to the process ([`NodeId`]). The list
//! is expensive, so a single background poller scans every watched node once per
//! tick. Only an explicit `Dead` state counts as a crash; a clean shutdown makes
//! the node disappear instead.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use iceoryx2::prelude::*;

use crate::types::{raw_pid_to_u32, NodeId};

/// Polling interval of the liveness poller (ms).
///
/// A too frequent `Node::list` perturbs the shared-memory notifier path.
/// Detection latency is bounded by this interval and amortised across every
/// watched node.
pub const LIVENESS_POLL_MS: u64 = 500;

/// Granularity of the interruptible sleep (ms).
///
/// The poll interval is slept in slices of this length so that a cancellation
/// is honoured within this bound instead of waiting for the whole interval.
const SLEEP_SLICE_MS: u64 = 100;

/// Effective poll interval, overridable with `ICE_RPC_LIVENESS_POLL_MS`.
///
/// Read **once** per process: the polling loop calls this on every tick, and
/// `std::env::var` takes the process-wide environment lock.
fn poll_interval_ms() -> u64 {
    static INTERVAL: OnceLock<u64> = OnceLock::new();
    *INTERVAL.get_or_init(|| {
        std::env::var("ICE_RPC_LIVENESS_POLL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|v| *v > 0)
            .unwrap_or(LIVENESS_POLL_MS)
    })
}

/// Number of [`SLEEP_SLICE_MS`] slices making up one poll interval.
///
/// Rounds **up**, so the configured interval is a lower bound.
fn sleep_slices() -> u64 {
    poll_interval_ms().div_ceil(SLEEP_SLICE_MS)
}

// ---------------------------------------------------------------------------
// Provider marker
// ---------------------------------------------------------------------------

static IS_PROVIDER: AtomicBool = AtomicBool::new(false);

/// Marks this process as a discovery provider.
///
/// Called when a process starts providing at least one service. The iceoryx2
/// [`Node`] it owns already carries the native monitoring token, so an external
/// watcher can tell a clean shutdown from a crash.
pub fn mark_provider() {
    IS_PROVIDER.store(true, Ordering::Relaxed);
}

/// Returns `true` when this process provides at least one service.
pub fn is_provider() -> bool {
    IS_PROVIDER.load(Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Native liveness
// ---------------------------------------------------------------------------

/// Returns the set of PIDs owning at least one **alive** iceoryx2 node.
///
/// Returns `None` when `Node::list` itself fails, so callers can distinguish
/// "no node is alive" from "could not tell" (and never fire a spurious death).
pub fn alive_pids() -> Option<HashSet<u32>> {
    let config = crate::config::build_iceoryx2_config();
    let mut pids = HashSet::new();

    let result = Node::<ipc_threadsafe::Service>::list(&config, |state| {
        if matches!(state, NodeState::Alive(_)) {
            pids.insert(raw_pid_to_u32(state.node_id().pid().value()));
        }
        CallbackProgression::Continue
    });

    match result {
        Ok(()) => Some(pids),
        Err(e) => {
            log::warn!("[node_liveness] Node::list failed: {:?}", e);
            None
        }
    }
}

/// Returns `true` when the given PID owns an alive iceoryx2 node.
///
/// Conservative on error: an inconclusive scan reports the node as alive, so
/// that a monitoring failure never triggers a false crash.
pub fn is_pid_alive(pid: u32) -> bool {
    alive_pids().map(|pids| pids.contains(&pid)).unwrap_or(true)
}

// ---------------------------------------------------------------------------
// Watcher registry and unique poller
// ---------------------------------------------------------------------------

fn watched_registry() -> &'static Mutex<HashMap<u32, ()>> {
    static REGISTRY: OnceLock<Mutex<HashMap<u32, ()>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn poller_started() -> &'static OnceLock<()> {
    static STARTED: OnceLock<()> = OnceLock::new();
    &STARTED
}

/// Registers a remote node for crash detection.
///
/// Idempotent. Starts the unique poller on first use. A node equal to the
/// current process is ignored (a process cannot watch itself).
pub fn register_node_liveness_watcher(node_id: NodeId) {
    if node_id == NodeId::current() {
        return;
    }
    crate::sync::lock(watched_registry()).insert(node_id.0, ());
    ensure_poller();
}

/// Removes a remote node from crash detection.
pub fn unregister_node_liveness_watcher(node_id: NodeId) {
    crate::sync::lock(watched_registry()).remove(&node_id.0);
}

/// Returns `true` when `node_id` is currently registered (tests).
///
/// Per-node rather than a global count: the tests run in parallel.
#[cfg(test)]
pub fn is_watched(node_id: NodeId) -> bool {
    crate::sync::lock(watched_registry()).contains_key(&node_id.0)
}

fn ensure_poller() {
    poller_started().get_or_init(|| {
        let handle = crate::rt::spawn_blocking(poller_loop);
        crate::locator::ServiceLocator::global().register_shutdown_handle(handle);
    });
}

/// Single polling loop: one `Node::list` per tick for every watched node.
fn poller_loop() {
    let cancel = crate::global_cancel_token().clone();

    loop {
        if cancel.is_cancelled() {
            break;
        }

        let watched: Vec<u32> = crate::sync::lock(watched_registry())
            .keys()
            .copied()
            .collect();

        if !watched.is_empty() {
            if let Some(alive) = alive_pids() {
                for pid in watched {
                    if alive.contains(&pid) {
                        continue;
                    }
                    // Confirm with a targeted query: absence alone is ambiguous
                    // between a crash and a clean shutdown.
                    if !is_confirmed_dead(pid) {
                        continue;
                    }

                    log::warn!("[node_liveness] CRASH DETECTED for Node {}", pid);
                    crate::sync::lock(watched_registry()).remove(&pid);
                }
            }
        }

        // Interruptible sleep so shutdown is honoured promptly.
        for _ in 0..sleep_slices() {
            if cancel.is_cancelled() {
                return;
            }
            std::thread::sleep(Duration::from_millis(SLEEP_SLICE_MS));
        }
    }
}

/// Confirms that a watched node is really gone.
///
/// `alive_pids()` only reports `Alive` nodes: a missing PID means the node
/// either crashed (listed as `Dead`) or shut down cleanly (not listed at all).
/// `Inaccessible` / `Undefined` states are treated as inconclusive and retried.
fn is_confirmed_dead(pid: u32) -> bool {
    let config = crate::config::build_iceoryx2_config();
    let mut found = false;
    let mut crashed = false;

    let result = Node::<ipc_threadsafe::Service>::list(&config, |state| {
        if raw_pid_to_u32(state.node_id().pid().value()) != pid {
            return CallbackProgression::Continue;
        }
        found = true;
        crashed = matches!(state, NodeState::Dead(_));
        CallbackProgression::Stop
    });

    if result.is_err() {
        // Inconclusive: never declare a death on a failed scan.
        return false;
    }
    // `Dead` = crash; not found = clean shutdown (resources removed).
    crashed || !found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_marker_roundtrip() {
        IS_PROVIDER.store(false, Ordering::Relaxed);
        assert!(!is_provider());
        mark_provider();
        assert!(is_provider());
        IS_PROVIDER.store(false, Ordering::Relaxed);
        assert!(!is_provider());
    }

    #[test]
    fn register_and_unregister_are_tracked() {
        // Assert on the *specific* node rather than on the global count.
        let fake = NodeId(0x0D1E_0001);
        assert_ne!(fake, NodeId::current());

        unregister_node_liveness_watcher(fake);
        assert!(!is_watched(fake));
        register_node_liveness_watcher(fake);
        assert!(is_watched(fake));
        unregister_node_liveness_watcher(fake);
        assert!(!is_watched(fake));
    }

    #[test]
    fn a_process_never_watches_itself() {
        register_node_liveness_watcher(NodeId::current());
        assert!(!is_watched(NodeId::current()));
    }

    /// The interval is memoized and usable as a sleep duration.
    #[test]
    fn poll_interval_is_stable_and_positive() {
        let first = poll_interval_ms();
        assert!(first > 0, "a zero interval would busy-loop");
        assert_eq!(first, poll_interval_ms());
    }

    /// The slept duration must be at least the configured interval.
    #[test]
    fn sleep_slices_covers_at_least_the_interval() {
        let slices = sleep_slices();
        assert!(slices >= 1, "the loop must always sleep at least one slice");
        assert!(
            slices * SLEEP_SLICE_MS >= poll_interval_ms(),
            "{slices} slice(s) of {SLEEP_SLICE_MS} ms do not cover {} ms",
            poll_interval_ms()
        );
    }
}
