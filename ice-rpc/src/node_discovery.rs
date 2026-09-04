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

#[derive(Debug, Clone)]
pub struct NodeRecord {
    pub node_id: NodeId,
    pub status: u8,
    pub last_seen: std::time::Instant,
}

impl NodeRecord {
    pub const STATUS_DEAD: u8 = 0;
    pub const STATUS_OK: u8 = 1;
}

// ---------------------------------------------------------------------------
// NodeDiscovery
// ---------------------------------------------------------------------------

pub struct NodeDiscovery {
    records: std::sync::Mutex<HashMap<u32, NodeRecord>>,
    service_map: std::sync::RwLock<HashMap<String, NodeId>>,
    pub(crate) registry_listener_started: AtomicBool,
    pending_events: std::sync::Mutex<Vec<DiscoveryEvent>>,
}

impl NodeDiscovery {
    pub fn new() -> Self {
        Self {
            records: std::sync::Mutex::new(HashMap::new()),
            service_map: std::sync::RwLock::new(HashMap::new()),
            registry_listener_started: AtomicBool::new(false),
            pending_events: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn upsert(&self, node_id: NodeId, status: u8, service_name: &str) {
        let is_new_node = {
            let mut map = self.records.lock().expect("records lock poisoning");
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
                let smap = self
                    .service_map
                    .read()
                    .expect("service_map read lock poisoning");
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
            let mut smap = self
                .service_map
                .write()
                .expect("service_map write lock poisoning");
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
            if let Ok(mut pending) = self.pending_events.lock() {
                pending.extend(events);
            }
        }
    }

    pub fn drain_events(&self) -> Vec<DiscoveryEvent> {
        if let Ok(mut pending) = self.pending_events.lock() {
            std::mem::take(&mut *pending)
        } else {
            Vec::new()
        }
    }

    pub fn all_known_services(&self) -> Vec<String> {
        let smap = self
            .service_map
            .read()
            .expect("service_map read lock poisoning");
        smap.keys().cloned().collect()
    }

    /// Discovers the **live** nodes from the registry.
    ///
    /// 1. `list_nodes()` → all candidate NodeIds (including dead ones).
    /// 2. `is_node_alive()` → filters the nodes whose kernel lock is absent.
    /// 3. `list_services()` → reads the services of the live nodes.
    pub fn discover_live_nodes(&self) -> HashMap<NodeId, Vec<String>> {
        let mut result: HashMap<NodeId, Vec<String>> = HashMap::new();
        for nid_raw in crate::blackboard::list_nodes() {
            let lock_name = format!("{}{}", crate::node_lock::LOCK_NAME_PREFIX, nid_raw);
            if crate::node_lock::is_node_alive(&lock_name) {
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

    pub fn discover_live_services(&self) -> Vec<String> {
        self.discover_live_nodes().into_values().flatten().collect()
    }

    pub fn remove(&self, node_id: NodeId) {
        let mut map = self.records.lock().expect("records lock poisoning");
        map.remove(&node_id.0);
        drop(map);
        let mut smap = self
            .service_map
            .write()
            .expect("service_map write lock poisoning");
        smap.retain(|_, v| v.0 != node_id.0);
    }

    pub fn active_nodes(&self) -> Vec<NodeId> {
        let map = self.records.lock().expect("records lock poisoning");
        map.values()
            .filter(|r| r.status == NodeRecord::STATUS_OK)
            .map(|r| r.node_id)
            .collect()
    }

    pub fn is_node_ok(&self, node_id: NodeId) -> bool {
        let map = self.records.lock().expect("records lock poisoning");
        map.get(&node_id.0)
            .map(|r| r.status == NodeRecord::STATUS_OK)
            .unwrap_or(false)
    }

    /// Looks for the NodeId hosting a service: cache → registry → None.
    pub fn locate_service(&self, service_name: &str) -> Option<NodeId> {
        {
            let smap = self
                .service_map
                .read()
                .expect("service_map read lock poisoning");
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
            // Starts a lock watcher to detect crashes.
            let lock_name = format!("{}{}", crate::node_lock::LOCK_NAME_PREFIX, node_id.0);
            crate::node_lock::register_node_lock_watcher(*node_id, lock_name);
        }
        let smap = self
            .service_map
            .read()
            .expect("service_map read lock poisoning");
        smap.get(service_name).copied()
    }

    pub fn snapshot(&self) -> Vec<NodeRecord> {
        let map = self.records.lock().expect("records lock poisoning");
        map.values().cloned().collect()
    }

    pub fn invalidate_service(&self, service_name: &str) {
        let mut smap = self
            .service_map
            .write()
            .expect("service_map write lock poisoning");
        smap.remove(service_name);
        log::info!(
            "Cache invalidated for service '{}' (reconnecting)",
            service_name
        );
    }

    pub fn invalidate_node_services(&self, node_id: NodeId) {
        {
            let mut rmap = self.records.lock().expect("records lock poisoning");
            rmap.remove(&node_id.0);
        }
        {
            let mut smap = self
                .service_map
                .write()
                .expect("service_map write lock poisoning");
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

#[derive(Debug, Clone)]
pub enum DiscoveryEvent {
    NodeUp {
        node_id: NodeId,
        services: Vec<String>,
    },
    NodeDown {
        node_id: NodeId,
        services_lost: Vec<String>,
    },
    ServiceAppeared {
        service_name: String,
        node_id: Option<NodeId>,
    },
    ServiceDisappeared {
        service_name: String,
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
    fn node_discovery_remove() {
        let nd = NodeDiscovery::new();
        nd.upsert(NodeId(100), NodeRecord::STATUS_OK, "Svc1");
        nd.upsert(NodeId(100), NodeRecord::STATUS_OK, "Svc2");
        nd.upsert(NodeId(200), NodeRecord::STATUS_OK, "Svc3");
        nd.remove(NodeId(100));
        assert!(!nd.is_node_ok(NodeId(100)));
        assert!(nd.is_node_ok(NodeId(200)));
        assert_eq!(nd.locate_service("Svc1"), None);
        assert_eq!(nd.locate_service("Svc2"), None);
        assert_eq!(nd.locate_service("Svc3"), Some(NodeId(200)));
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
