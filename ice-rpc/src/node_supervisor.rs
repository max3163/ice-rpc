//! Node supervisor: one logical watcher per remote node, broadcasting node
//! death events to all subscribed clients.
//!
//! This replaces the previous global callback registry
//! (`crate::reconnect`), which deduplicated callbacks by `NodeId` only and
//! therefore dropped every service except the first one targeting a given
//! remote node. The supervisor keeps one subscription entry per client
//! (`ClientCore`) and fires all of them when the node dies.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// Type of the callback fired when a remote node is detected as dead.
pub type ReconnectCallback = Arc<dyn Fn(u32) + Send + Sync>;

/// Unique identifier of a subscriber (one per `ClientCore` instance).
pub type SubscriberId = u64;

/// Per-node subscriber list.
#[derive(Default)]
struct NodeState {
    subscribers: HashMap<SubscriberId, ReconnectCallback>,
}

/// Supervises remote nodes and broadcasts their death to subscribers.
pub struct NodeSupervisor {
    nodes: Mutex<HashMap<u32, NodeState>>,
    next_id: AtomicU64,
}

impl NodeSupervisor {
    /// Returns the process-wide supervisor instance.
    pub fn global() -> &'static NodeSupervisor {
        static SUPERVISOR: OnceLock<NodeSupervisor> = OnceLock::new();
        SUPERVISOR.get_or_init(|| NodeSupervisor {
            nodes: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        })
    }

    /// Subscribes a callback to a node and returns a RAII handle.
    ///
    /// The subscription is automatically removed when the returned
    /// [`Subscription`] is dropped.
    pub fn subscribe(&self, node_id: u32, cb: ReconnectCallback) -> Subscription {
        let subscriber_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.nodes
            .lock()
            .expect("node supervisor lock poisoning")
            .entry(node_id)
            .or_default()
            .subscribers
            .insert(subscriber_id, cb);
        Subscription {
            node_id,
            subscriber_id,
        }
    }

    /// Removes a subscription and the node entry once it becomes empty.
    fn unsubscribe(&self, node_id: u32, subscriber_id: SubscriberId) {
        let mut nodes = self.nodes.lock().expect("node supervisor lock poisoning");
        let mut remove_node = false;
        if let Some(state) = nodes.get_mut(&node_id) {
            state.subscribers.remove(&subscriber_id);
            remove_node = state.subscribers.is_empty();
        }
        if remove_node {
            nodes.remove(&node_id);
        }
    }

    /// Broadcasts a node death to every subscriber of that node.
    ///
    /// The callbacks are cloned out of the lock before being invoked so that
    /// re-entrant calls (subscribe/unsubscribe from inside a callback) cannot
    /// deadlock the supervisor.
    pub fn notify_node_dead(&self, node_id: u32) {
        let cbs: Vec<ReconnectCallback> = {
            let nodes = self.nodes.lock().expect("node supervisor lock poisoning");
            nodes
                .get(&node_id)
                .map(|state| state.subscribers.values().cloned().collect())
                .unwrap_or_default()
        };
        for cb in cbs {
            cb(node_id);
        }
    }
}

/// RAII handle returned by [`NodeSupervisor::subscribe`].
pub struct Subscription {
    node_id: u32,
    subscriber_id: SubscriberId,
}

impl Subscription {
    /// Returns the node this subscription is attached to.
    pub fn node_id(&self) -> u32 {
        self.node_id
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        NodeSupervisor::global().unsubscribe(self.node_id, self.subscriber_id);
    }
}

/// Fires every callback subscribed to a node (compatibility helper).
pub fn fire(node_id: u32) {
    NodeSupervisor::global().notify_node_dead(node_id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    const NODE_MULTI_SVC: u32 = 0x0FEE_1001;
    const NODE_MULTI_INST: u32 = 0x0FEE_1002;
    const NODE_OTHER: u32 = 0x0FEE_1003;
    const NODE_DROP: u32 = 0x0FEE_1004;

    fn counting_cb(counter: Arc<AtomicU32>) -> ReconnectCallback {
        Arc::new(move |_| {
            counter.fetch_add(1, Ordering::Relaxed);
        })
    }

    #[test]
    fn fire_notifies_all_services_of_the_node() {
        let fired_a = Arc::new(AtomicU32::new(0));
        let fired_b = Arc::new(AtomicU32::new(0));
        let _sub_a = NodeSupervisor::global().subscribe(NODE_MULTI_SVC, counting_cb(fired_a.clone()));
        let _sub_b = NodeSupervisor::global().subscribe(NODE_MULTI_SVC, counting_cb(fired_b.clone()));

        fire(NODE_MULTI_SVC);

        assert_eq!(fired_a.load(Ordering::Relaxed), 1);
        assert_eq!(fired_b.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn fire_notifies_multiple_instances_of_the_node() {
        let fired_a = Arc::new(AtomicU32::new(0));
        let fired_b = Arc::new(AtomicU32::new(0));
        let _sub_a = NodeSupervisor::global().subscribe(NODE_MULTI_INST, counting_cb(fired_a.clone()));
        let _sub_b = NodeSupervisor::global().subscribe(NODE_MULTI_INST, counting_cb(fired_b.clone()));

        fire(NODE_MULTI_INST);

        assert_eq!(fired_a.load(Ordering::Relaxed), 1);
        assert_eq!(fired_b.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn fire_does_not_cross_nodes() {
        let fired = Arc::new(AtomicU32::new(0));
        let _sub = NodeSupervisor::global().subscribe(NODE_MULTI_SVC, counting_cb(fired.clone()));

        fire(NODE_OTHER);

        assert_eq!(fired.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn drop_unsubscribes() {
        let fired = Arc::new(AtomicU32::new(0));
        let sub = NodeSupervisor::global().subscribe(NODE_DROP, counting_cb(fired.clone()));
        drop(sub);

        fire(NODE_DROP);

        assert_eq!(fired.load(Ordering::Relaxed), 0);
    }
}
