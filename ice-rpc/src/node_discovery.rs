//! Node discovery and service→NodeId resolution.
//!
//! # Architecture
//!
//! This module manages the **local cache** of the topology. The source of truth
//! is the [`crate::blackboard`] registry (1 Blackboard per node,
//! 1 key per service).
//!
//! ## Flow
//!
//! 1. [`locate_service`] : cache → registry → update cache.
//! 2. [`discover_live_nodes`] : `list_nodes()` + `list_services()`.
//! 3. The [`crate::registry_listener::spawn`] listener keeps the cache up to date.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use crate::types::NodeId;

// ---------------------------------------------------------------------------
// NodeRecord
// ---------------------------------------------------------------------------

/// One entry of the local topology cache: a node, its last known status, and
/// when it was last seen.
///
/// The cache is *advisory*: the blackboard registry and iceoryx2's native node
/// monitoring stay the source of truth (see the module documentation).
#[derive(Debug, Clone)]
pub struct NodeRecord {
    /// The node this record describes.
    pub node_id: NodeId,
    /// Last known status, one of [`NodeRecord::STATUS_OK`] or
    /// [`NodeRecord::STATUS_DEAD`].
    pub status: u8,
    /// Instant of the last [`NodeDiscovery::upsert`] for this node.
    pub last_seen: std::time::Instant,
}

impl NodeRecord {
    /// Status meaning the node is gone: its PID no longer owns a live iceoryx2
    /// node.
    pub const STATUS_DEAD: u8 = 0;
    /// Status meaning the node was seen alive by the registry.
    pub const STATUS_OK: u8 = 1;
}

// ---------------------------------------------------------------------------
// NodeDiscovery
// ---------------------------------------------------------------------------

/// Local cache of the node topology and of the `service name → NodeId` mapping.
///
/// Shared behind an `Arc` (see [`crate::ServiceLocator`]). Every method takes
/// `&self` and applies its own fine-grained lock so that a registry round-trip
/// never blocks readers for its whole duration.
pub struct NodeDiscovery {
    /// One record per raw node id, guarded independently from `service_map`.
    records: std::sync::Mutex<HashMap<u32, NodeRecord>>,
    /// Service name → hosting node id; written by [`Self::upsert`], read on the
    /// hot path by [`Self::locate_service`].
    service_map: std::sync::RwLock<HashMap<String, NodeId>>,
    /// Set once the registry listener has been spawned for this instance, so the
    /// listener is started at most once.
    pub(crate) registry_listener_started: AtomicBool,
    /// Events produced since the last [`Self::drain_events`] call.
    pending_events: std::sync::Mutex<Vec<DiscoveryEvent>>,
}

impl NodeDiscovery {
    /// Creates an empty cache. No IPC resource is opened here: the first
    /// registry access happens in [`Self::locate_service`] or
    /// [`Self::discover_live_nodes`].
    pub fn new() -> Self {
        Self {
            records: std::sync::Mutex::new(HashMap::new()),
            service_map: std::sync::RwLock::new(HashMap::new()),
            registry_listener_started: AtomicBool::new(false),
            pending_events: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Records `node_id` as hosting `service_name` and emits the resulting
    /// [`DiscoveryEvent`]s (node up/down, service appeared/disappeared).
    ///
    /// `status` is [`NodeRecord::STATUS_OK`] or [`NodeRecord::STATUS_DEAD`]. An
    /// empty `service_name` means the update concerns the node itself.
    pub fn upsert(&self, node_id: NodeId, status: u8, service_name: &str) {
        let is_new_node = {
            let mut map = crate::sync::lock(&self.records);
            let is_new = !map.contains_key(&node_id.0);
            map.insert(
                node_id.0,
                NodeRecord {
                    node_id,
                    status,
                    last_seen: std::time::Instant::now(),
                },
            );
            is_new
        };

        let mut events: Vec<DiscoveryEvent> = Vec::new();

        if status == NodeRecord::STATUS_DEAD && service_name.is_empty() {
            let services_lost: Vec<String> = {
                let smap = crate::sync::read(&self.service_map);
                smap.iter()
                    .filter(|(_, v)| v.0 == node_id.0)
                    .map(|(k, _)| k.clone())
                    .collect()
            };
            events.push(DiscoveryEvent::NodeDown {
                node_id,
                services_lost,
            });
        } else if status == NodeRecord::STATUS_OK && is_new_node {
            events.push(DiscoveryEvent::NodeUp {
                node_id,
                services: Vec::new(),
            });
        }

        if !service_name.is_empty() {
            let mut smap = crate::sync::write(&self.service_map);
            if status == NodeRecord::STATUS_OK {
                let is_new = smap.insert(service_name.to_string(), node_id).is_none();
                if is_new {
                    events.push(DiscoveryEvent::ServiceAppeared {
                        service_name: service_name.to_string(),
                        node_id: Some(node_id),
                    });
                }
            } else {
                let was_present = smap.remove(service_name).is_some();
                if was_present {
                    events.push(DiscoveryEvent::ServiceDisappeared {
                        service_name: service_name.to_string(),
                        node_id: Some(node_id),
                    });
                }
            }
        }

        if !events.is_empty() {
            crate::sync::lock(&self.pending_events).extend(events);
        }
    }

    /// Takes the events accumulated since the previous call, leaving the queue
    /// empty. Consumed by the reconnection layer.
    pub fn drain_events(&self) -> Vec<DiscoveryEvent> {
        std::mem::take(&mut *crate::sync::lock(&self.pending_events))
    }

    /// Every service name currently cached, whether or not its node is still
    /// alive. Used to detect disappearances during a reconciliation.
    pub fn all_known_services(&self) -> Vec<String> {
        let smap = crate::sync::read(&self.service_map);
        smap.keys().cloned().collect()
    }

    /// Discovers the **live** nodes from the registry.
    ///
    /// 1. `list_nodes()` → all candidate NodeIds (including dead ones).
    /// 2. `node_liveness::alive_pids()` → one native `Node::list`, keeps the
    ///    candidates whose PID owns an alive iceoryx2 node.
    /// 3. `list_services()` → reads the services of the live nodes.
    pub fn discover_live_nodes(&self) -> HashMap<NodeId, Vec<String>> {
        let mut result: HashMap<NodeId, Vec<String>> = HashMap::new();
        // One native `Node::list` pass for every candidate instead of one
        // `flock` check per node.
        let alive = crate::node_liveness::alive_pids();
        for nid_raw in crate::blackboard::list_nodes() {
            let is_alive = alive
                .as_ref()
                .map(|set| set.contains(&nid_raw))
                .unwrap_or(false);
            if is_alive {
                result.insert(NodeId(nid_raw), crate::blackboard::list_services(nid_raw));
            } else {
                log::debug!(
                    "[discovery] Node {} DEAD, cleaning IPC resources...",
                    nid_raw
                );
                // Cleans the IPC artifacts of the dead node (Blackboard, events…).
                // The IPC resource cleanup is handled by iceoryx2
                // (cleanup_dead_nodes_on_creation = true in the config).
            }
        }
        result
    }

    /// Flattened list of the services exposed by the live nodes.
    ///
    /// Convenience wrapper over [`Self::discover_live_nodes`]: it performs the
    /// same registry scan.
    pub fn discover_live_services(&self) -> Vec<String> {
        self.discover_live_nodes().into_values().flatten().collect()
    }

    /// Re-synchronizes the cache with a snapshot of the live nodes and returns
    /// the `NodeId`s that disappeared.
    ///
    /// This is what the registry listener calls on a topology notification: the
    /// event is only a wake-up token (it no longer carries a node id), so the
    /// cache is reconciled against the blackboard + native liveness, which stay
    /// the source of truth. The current process is ignored.
    pub fn reconcile(&self, live: &HashMap<NodeId, Vec<String>>) -> Vec<NodeId> {
        let me = NodeId::current();

        for (node_id, services) in live {
            if *node_id == me {
                continue;
            }
            for service in services {
                self.upsert(*node_id, NodeRecord::STATUS_OK, service);
            }
            // Watch newly discovered nodes for crashes.
            crate::node_liveness::register_node_liveness_watcher(*node_id);
        }

        let cached: Vec<NodeId> = self
            .snapshot()
            .into_iter()
            .filter(|r| r.status == NodeRecord::STATUS_OK && r.node_id != me)
            .map(|r| r.node_id)
            .collect();

        let mut dead = Vec::new();
        for node_id in cached {
            if !live.contains_key(&node_id) {
                self.invalidate_node_services(node_id);
                dead.push(node_id);
            }
        }
        dead
    }

    /// Cached node ids whose last known status is [`NodeRecord::STATUS_OK`].
    pub fn active_nodes(&self) -> Vec<NodeId> {
        let map = crate::sync::lock(&self.records);
        map.values()
            .filter(|r| r.status == NodeRecord::STATUS_OK)
            .map(|r| r.node_id)
            .collect()
    }

    /// Returns whether the cache currently considers `node_id` alive.
    pub fn is_node_ok(&self, node_id: NodeId) -> bool {
        let map = crate::sync::lock(&self.records);
        map.get(&node_id.0)
            .map(|r| r.status == NodeRecord::STATUS_OK)
            .unwrap_or(false)
    }

    /// Looks for the NodeId hosting a service: cache → registry → None.
    pub fn locate_service(&self, service_name: &str) -> Option<NodeId> {
        {
            let smap = crate::sync::read(&self.service_map);
            if let Some(nid) = smap.get(service_name).copied() {
                return Some(nid);
            }
        }
        // Cache miss: rebuild from the registry.
        let live = self.discover_live_nodes();
        for (node_id, services) in &live {
            for svc in services {
                self.upsert(*node_id, NodeRecord::STATUS_OK, svc);
            }
            // Watches the node through iceoryx2's native monitoring.
            crate::node_liveness::register_node_liveness_watcher(*node_id);
        }
        let smap = crate::sync::read(&self.service_map);
        smap.get(service_name).copied()
    }

    /// Clones the node records for inspection (diagnostics, tests).
    pub fn snapshot(&self) -> Vec<NodeRecord> {
        let map = crate::sync::lock(&self.records);
        map.values().cloned().collect()
    }

    /// Drops the `service_name` entry so the next call re-resolves it from the
    /// registry. Invoked when a call fails against a stale target.
    pub fn invalidate_service(&self, service_name: &str) {
        let mut smap = crate::sync::write(&self.service_map);
        smap.remove(service_name);
        log::info!(
            "Cache invalidated for service '{}' (reconnecting)",
            service_name
        );
    }

    /// Removes `node_id` from the cache along with every service it hosted.
    /// Invoked once the node is confirmed dead.
    pub fn invalidate_node_services(&self, node_id: NodeId) {
        {
            let mut rmap = crate::sync::lock(&self.records);
            rmap.remove(&node_id.0);
        }
        {
            let mut smap = crate::sync::write(&self.service_map);
            smap.retain(|_, v| v.0 != node_id.0);
        }
        log::warn!("Node {} marked dead, cache cleared", node_id);
    }
}

impl Default for NodeDiscovery {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// DiscoveryEvent
// ---------------------------------------------------------------------------

/// Topology change produced by [`NodeDiscovery::upsert`] and consumed through
/// [`NodeDiscovery::drain_events`].
#[derive(Debug, Clone)]
pub enum DiscoveryEvent {
    /// A node was seen for the first time since the cache was created.
    NodeUp {
        /// The node that appeared.
        node_id: NodeId,
        /// Services already known for that node.
        services: Vec<String>,
    },
    /// A node was confirmed dead.
    NodeDown {
        /// The node that disappeared.
        node_id: NodeId,
        /// Services that were cached for it and are now unreachable.
        services_lost: Vec<String>,
    },
    /// A service became resolvable.
    ServiceAppeared {
        /// Service name, as declared by the `#[service("…")]` attribute.
        service_name: String,
        /// Node hosting the service, when known.
        node_id: Option<NodeId>,
    },
    /// A service stopped being resolvable.
    ServiceDisappeared {
        /// Service name that went away.
        service_name: String,
        /// Node that hosted it, when known.
        node_id: Option<NodeId>,
    },
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconcile_returns_vanished_nodes() {
        let nd = NodeDiscovery::new();
        let gone = NodeId(0x0D1E_1001);
        let alive = NodeId(0x0D1E_1002);
        nd.upsert(gone, NodeRecord::STATUS_OK, "GoneService");
        nd.upsert(alive, NodeRecord::STATUS_OK, "AliveService");

        let mut live = HashMap::new();
        live.insert(alive, vec!["AliveService".to_string()]);

        let dead = nd.reconcile(&live);

        assert_eq!(dead, vec![gone]);
        assert!(
            !nd.is_node_ok(gone),
            "the vanished node must leave the cache"
        );
    }

    #[test]
    fn node_discovery_upsert_and_query() {
        let nd = NodeDiscovery::new();
        nd.upsert(NodeId(100), NodeRecord::STATUS_OK, "ConfigService");
        nd.upsert(NodeId(100), NodeRecord::STATUS_OK, "DatabaseService");
        nd.upsert(NodeId(200), NodeRecord::STATUS_OK, "HttpService");
        assert!(nd.is_node_ok(NodeId(100)));
        assert!(nd.is_node_ok(NodeId(200)));
        assert_eq!(nd.active_nodes().len(), 2);
        assert_eq!(nd.locate_service("ConfigService"), Some(NodeId(100)));
        assert_eq!(nd.locate_service("DatabaseService"), Some(NodeId(100)));
        assert_eq!(nd.locate_service("HttpService"), Some(NodeId(200)));
        assert_eq!(nd.locate_service("UnknownService"), None);
    }

    #[test]
    fn node_discovery_dead_service() {
        let nd = NodeDiscovery::new();
        nd.upsert(NodeId(100), NodeRecord::STATUS_OK, "ConfigService");
        nd.upsert(NodeId(100), NodeRecord::STATUS_DEAD, "ConfigService");
        assert_eq!(nd.locate_service("ConfigService"), None);
        assert!(!nd.is_node_ok(NodeId(100)));
    }

    #[test]
    fn node_discovery_snapshot() {
        let nd = NodeDiscovery::new();
        nd.upsert(NodeId(42), NodeRecord::STATUS_OK, "TestService");
        let snap = nd.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].node_id.0, 42);
    }

    #[test]
    fn invalidate_service_clears_cache() {
        let nd = NodeDiscovery::new();
        nd.upsert(NodeId(100), NodeRecord::STATUS_OK, "MyService");
        assert_eq!(nd.locate_service("MyService"), Some(NodeId(100)));
        nd.invalidate_service("MyService");
        assert_eq!(nd.locate_service("MyService"), None);
    }

    #[test]
    fn invalidate_node_services_clears_all_services_of_node() {
        let nd = NodeDiscovery::new();
        nd.upsert(NodeId(200), NodeRecord::STATUS_OK, "ServiceA");
        nd.upsert(NodeId(200), NodeRecord::STATUS_OK, "ServiceB");
        nd.upsert(NodeId(300), NodeRecord::STATUS_OK, "ServiceC");
        assert_eq!(nd.locate_service("ServiceA"), Some(NodeId(200)));
        assert_eq!(nd.locate_service("ServiceB"), Some(NodeId(200)));
        assert_eq!(nd.locate_service("ServiceC"), Some(NodeId(300)));
        nd.invalidate_node_services(NodeId(200));
        assert_eq!(nd.locate_service("ServiceA"), None);
        assert_eq!(nd.locate_service("ServiceB"), None);
        assert_eq!(nd.locate_service("ServiceC"), Some(NodeId(300)));
    }

    #[test]
    fn invalidate_node_services_removes_node_record() {
        let nd = NodeDiscovery::new();
        nd.upsert(NodeId(400), NodeRecord::STATUS_OK, "SomeService");
        assert!(nd.is_node_ok(NodeId(400)));
        nd.invalidate_node_services(NodeId(400));
        assert!(!nd.is_node_ok(NodeId(400)));
    }
}
