//! Centralized reconnection manager.
//!
//! When a remote node dies, every subscribed `ClientCore` used to spawn its
//! own retry thread (`std::thread::spawn` + `locate_service` polling), which
//! produced `N services × N instances` threads. This module replaces that
//! pattern with a single background worker that polls the discovery once per
//! interval for every service waiting for reconnection.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

/// State of a service waiting for reconnection.
pub struct PendingService {
    service_name: &'static str,
    cached_target_node: Arc<AtomicU64>,
    server_ready: Arc<AtomicBool>,
    reconnecting: Arc<AtomicBool>,
}

impl PendingService {
    /// Creates a pending-service record sharing the client's atomics.
    pub fn new(
        service_name: &'static str,
        cached_target_node: Arc<AtomicU64>,
        server_ready: Arc<AtomicBool>,
        reconnecting: Arc<AtomicBool>,
    ) -> Self {
        Self {
            service_name,
            cached_target_node,
            server_ready,
            reconnecting,
        }
    }
}

/// Supervises pending reconnections and drives the unique retry worker.
pub struct ReconnectManager {
    pending: Mutex<HashMap<u32, Vec<Arc<PendingService>>>>,
}

impl ReconnectManager {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the process-wide reconnect manager.
    pub fn global() -> &'static ReconnectManager {
        static MANAGER: OnceLock<ReconnectManager> = OnceLock::new();
        MANAGER.get_or_init(ReconnectManager::new)
    }

    /// Schedules a service for reconnection on a dead node.
    ///
    /// The node discovery cache and publishers are invalidated only once per
    /// node (on the first service registered for that node).
    pub fn schedule(&self, node_id: u32, service: Arc<PendingService>) {
        let is_new_node = self.insert_pending(node_id, service);

        if is_new_node {
            let node = crate::NodeId(node_id);
            crate::ServiceLocator::global()
                .node_discovery()
                .invalidate_node_services(node);
            crate::ServiceLocator::global()
                .hub()
                .invalidate_publishers(node);
        }

        ensure_worker();
    }

    /// Inserts a pending service, deduplicated by `Arc` identity.
    ///
    /// Returns `true` when the node had no pending entry before the insert
    /// (i.e. this is the first service registered for that node).
    pub fn insert_pending(&self, node_id: u32, service: Arc<PendingService>) -> bool {
        let mut pending = self
            .pending
            .lock()
            .expect("reconnect manager lock poisoning");
        let existed = pending.contains_key(&node_id);
        let list = pending.entry(node_id).or_default();
        if !list.iter().any(|s| Arc::ptr_eq(s, &service)) {
            list.push(service);
        }
        !existed
    }
}

/// Starts the unique retry worker once and registers it for orderly shutdown.
fn ensure_worker() {
    static WORKER: OnceLock<()> = OnceLock::new();
    WORKER.get_or_init(|| {
        let handle = crate::rt::spawn_blocking(worker_loop);
        crate::ServiceLocator::global().register_shutdown_handle(handle);
    });
}

/// Polls the discovery for every pending service until it is found again.
fn worker_loop() {
    let poll = Duration::from_millis(crate::INIT_RETRY_INTERVAL_MS);
    let cancel = crate::global_cancel_token().clone();

    loop {
        if cancel.is_cancelled() {
            break;
        }

        let manager = ReconnectManager::global();
        let snapshot: HashMap<u32, Vec<Arc<PendingService>>> = manager
            .pending
            .lock()
            .expect("reconnect manager lock poisoning")
            .clone();

        if snapshot.is_empty() {
            std::thread::sleep(poll);
            continue;
        }

        let discovery = crate::ServiceLocator::global().node_discovery();
        let mut resolved: Vec<(u32, &'static str)> = Vec::new();

        for (&node_id, services) in &snapshot {
            for service in services {
                if let Some(nid) = discovery.locate_service(service.service_name) {
                    log::info!(
                        "[ReconnectManager] Service '{}' found again on Node {}",
                        service.service_name,
                        nid.0
                    );
                    service
                        .cached_target_node
                        .store(nid.0 as u64, Ordering::SeqCst);
                    service.server_ready.store(true, Ordering::SeqCst);
                    service.reconnecting.store(false, Ordering::SeqCst);
                    resolved.push((node_id, service.service_name));
                }
            }
        }

        if !resolved.is_empty() {
            let mut pending = manager
                .pending
                .lock()
                .expect("reconnect manager lock poisoning");
            for (node_id, name) in resolved {
                if let Some(list) = pending.get_mut(&node_id) {
                    list.retain(|s| s.service_name != name);
                    if list.is_empty() {
                        pending.remove(&node_id);
                    }
                }
            }
        }

        std::thread::sleep(poll);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending_service(name: &'static str) -> Arc<PendingService> {
        Arc::new(PendingService::new(
            name,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        ))
    }

    #[test]
    fn insert_pending_accepts_first_service() {
        let manager = ReconnectManager::new();
        assert!(manager.insert_pending(1, pending_service("a")));
        assert_eq!(manager.pending.lock().unwrap()[&1].len(), 1);
    }

    #[test]
    fn insert_pending_deduplicates_same_service() {
        let manager = ReconnectManager::new();
        let service = pending_service("a");
        assert!(manager.insert_pending(1, service.clone()));
        assert!(!manager.insert_pending(1, service));
        assert_eq!(manager.pending.lock().unwrap()[&1].len(), 1);
    }

    #[test]
    fn insert_pending_accepts_different_services_on_same_node() {
        let manager = ReconnectManager::new();
        assert!(manager.insert_pending(1, pending_service("a")));
        assert!(!manager.insert_pending(1, pending_service("b")));
        assert_eq!(manager.pending.lock().unwrap()[&1].len(), 2);
    }

    #[test]
    fn insert_pending_accepts_multiple_instances_of_same_service() {
        let manager = ReconnectManager::new();
        assert!(manager.insert_pending(1, pending_service("a")));
        assert!(!manager.insert_pending(1, pending_service("a")));
        assert_eq!(manager.pending.lock().unwrap()[&1].len(), 2);
    }
}
