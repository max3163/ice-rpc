//! Reconnection callbacks fired when a remote node is detected as dead.
//!
//! These callbacks are registered by clients (consumers) and fired
//! by the crash watcher ([`crate::node_lock`]) or the hub
//! ([`crate::hub::NodeHub`]) when a send error is detected.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

/// Type of the reconnection callback fired when a node is detected as dead.
pub type ReconnectCallback = Arc<dyn Fn(u32) + Send + Sync>;

/// Global storage of reconnection callbacks indexed by NodeId.
fn callbacks() -> &'static Mutex<HashMap<u32, Vec<ReconnectCallback>>> {
    static CALLBACKS: OnceLock<Mutex<HashMap<u32, Vec<ReconnectCallback>>>> = OnceLock::new();
    CALLBACKS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Set of NodeIds for which a reconnection callback has already been registered.
fn registered() -> &'static Mutex<HashSet<u32>> {
    static REGISTERED: OnceLock<Mutex<HashSet<u32>>> = OnceLock::new();
    REGISTERED.get_or_init(|| Mutex::new(HashSet::new()))
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
/// # Returns
/// `true` if the callback was registered (first call for this NodeId).
pub fn register_once(node_id: u32, cb: ReconnectCallback) -> bool {
    let mut reg = registered()
        .lock()
        .expect("reconnect registered lock poisoning");
    if reg.contains(&node_id) {
        return false;
    }
    reg.insert(node_id);
    drop(reg);
    register(node_id, cb);
    true
}

/// Removes all reconnection callbacks for a given node.
pub fn unregister(node_id: u32) {
    callbacks()
        .lock()
        .expect("reconnect callbacks lock poisoning")
        .remove(&node_id);
    registered()
        .lock()
        .expect("reconnect registered lock poisoning")
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
