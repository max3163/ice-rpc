//! Reconnection callbacks fired when a remote node is detected as dead.
//!
//! These callbacks are registered by clients (consumers) and fired
//! by the crash watcher ([`crate::node_lock`]) or the hub
//! ([`crate::hub::NodeHub`]) when a send error is detected.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

/// Type of the reconnection callback fired when a node is detected as dead.
pub type ReconnectCallback = Arc<dyn Fn(u32) + Send + Sync>;

/// Global storage of reconnection callbacks indexed by NodeId.
fn callbacks() -> &'static Mutex<HashMap<u32, Vec<ReconnectCallback>>> {
    static CALLBACKS: OnceLock<Mutex<HashMap<u32, Vec<ReconnectCallback>>>> = OnceLock::new();
    CALLBACKS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Registers a reconnection callback.
pub fn register(node_id: u32, cb: ReconnectCallback) {
    callbacks()
        .lock()
        .expect("reconnect callbacks lock poisoning")
        .entry(node_id)
        .or_default()
        .push(cb);
}

/// Registers a reconnection callback only once per NodeId.
///
/// The deduplication relies on the callback registry itself: if the node
/// already has at least one callback, the new one is discarded.
///
/// # Returns
/// `true` if the callback was registered (first call for this NodeId).
pub fn register_once(node_id: u32, cb: ReconnectCallback) -> bool {
    let mut map = callbacks()
        .lock()
        .expect("reconnect callbacks lock poisoning");
    if let Some(existing) = map.get(&node_id) {
        if !existing.is_empty() {
            return false;
        }
    }
    map.entry(node_id).or_default().push(cb);
    true
}

/// Removes all reconnection callbacks for a given node.
pub fn unregister(node_id: u32) {
    callbacks()
        .lock()
        .expect("reconnect callbacks lock poisoning")
        .remove(&node_id);
}

/// Fires all reconnection callbacks registered for a NodeId.
pub fn fire(node_id: u32) {
    let cbs = {
        callbacks()
            .lock()
            .expect("reconnect callbacks lock poisoning")
            .get(&node_id)
            .cloned()
            .unwrap_or_default()
    };
    for cb in &cbs {
        cb(node_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    const NODE_A: u32 = 0x0FEE_0001;
    const NODE_B: u32 = 0x0FEE_0002;

    #[test]
    fn register_fire_then_unregister_stops_firing() {
        let fired = Arc::new(AtomicU32::new(0));
        let cb: ReconnectCallback = {
            let fired = fired.clone();
            Arc::new(move |id: u32| {
                assert_eq!(id, NODE_A);
                fired.fetch_add(1, Ordering::Relaxed);
            })
        };
        register(NODE_A, cb);
        fire(NODE_A);
        assert_eq!(fired.load(Ordering::Relaxed), 1);
        unregister(NODE_A);
        fire(NODE_A);
        assert_eq!(fired.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn register_once_accepts_only_first_registration() {
        let cb1: ReconnectCallback = Arc::new(|_| {});
        let cb2: ReconnectCallback = Arc::new(|_| {});
        assert!(register_once(NODE_B, cb1));
        assert!(!register_once(NODE_B, cb2.clone()));
        unregister(NODE_B);
        assert!(register_once(NODE_B, cb2));
        unregister(NODE_B);
    }
}
