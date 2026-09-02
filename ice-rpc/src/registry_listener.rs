//! Discovery event listener (iceoryx2 WaitSet loop).
//!
//! Listens to notifications on the `ice_rpc_registry_notify` event service.
//! On each notification, re-reads the Blackboard to update the local view
//! of nodes and services.

use std::sync::Arc;

use crate::try_or_log;

use iceoryx2::prelude::*;

use crate::locator::ServiceLocator;
use crate::node_discovery::{NodeDiscovery, NodeRecord};
use crate::registry_notify::REGISTRY_NOTIFY_TOPIC;
use crate::types::NodeId;

/// WaitSet timeout duration for the discovery listener (ms).
const REGISTRY_WAITSET_TIMEOUT_MS: u64 = 200;

/// Starts the event listener in a `spawn_blocking`.
///
/// Listens to notifications on the `ice_rpc_registry_notify` event service.
/// On each notification, re-reads the Blackboard via [`NodeDiscovery::discover_live_nodes`]
/// to update the local view.
///
/// Idempotent via `NodeDiscovery::registry_listener_started`.
pub fn spawn(discovery: Arc<NodeDiscovery>) {
    if discovery
        .registry_listener_started
        .swap(true, std::sync::atomic::Ordering::Relaxed)
    {
        return;
    }
    let node = try_or_log!(
        ServiceLocator::global().get_node_sync(),
        "get_node_sync",
        "failed"
    );

    let notify_topic_name = try_or_log!(
        ServiceName::new(REGISTRY_NOTIFY_TOPIC),
        "ServiceName notify",
        "failed"
    );
    let notify_svc = try_or_log!(
        node.service_builder(&notify_topic_name)
            .event()
            .event_id_max_value(65535)
            .open_or_create(),
        "event open_or_create",
        "failed"
    );
    let listener = try_or_log!(
        notify_svc.listener_builder().create(),
        "listener create",
        "failed"
    );

    let cancel = crate::registry_cancel_token().clone();
    let handle = crate::rt::spawn_blocking(move || {
        use iceoryx2::prelude::{CallbackProgression, WaitSetBuilder};
        let wait_set = try_or_log!(
            WaitSetBuilder::new().create::<iceoryx2::service::ipc_threadsafe::Service>(),
            "WaitSet create",
            "failed"
        );
        let _guard = try_or_log!(
            wait_set.attach_notification(&listener),
            "attach_notification",
            "failed"
        );

        loop {
            if cancel.is_cancelled() {
                break;
            }
            let result = wait_set.wait_and_process_once_with_timeout(
                |_| {
                    // Consumes all notifications and processes each NodeId.
                    while let Ok(Some(event_id)) = listener.try_wait_one() {
                        let node_id_raw = event_id.as_value() as u32;
                        if node_id_raw > 0 {
                            handle_node_event(&discovery, node_id_raw);
                        }
                    }
                    CallbackProgression::Continue
                },
                std::time::Duration::from_millis(REGISTRY_WAITSET_TIMEOUT_MS),
            );
            // Check Termination Request 
            if let Err(_) | Ok(iceoryx2::waitset::WaitSetRunResult::TerminationRequest) = result {
                break;
            }
        }
    });
    ServiceLocator::global().register_shutdown_handle(handle);
}

/// Processes an event for a specific NodeId.
///
/// Checks whether the node is alive, updates or cleans the cache.
fn handle_node_event(discovery: &NodeDiscovery, node_id: u32) {
    // Ignores events from our own PID (shutdown).
    if node_id == std::process::id() {
        return;
    }
    let lock_name = format!("{}{}", crate::node_lock::LOCK_NAME_PREFIX, node_id);
    if crate::node_lock::is_node_alive(&lock_name) {
        // Alive node: reads its services and updates the cache.
        let services = crate::blackboard::list_services(node_id);
        for svc in &services {
            discovery.upsert(NodeId(node_id), NodeRecord::STATUS_OK, svc);
        }
        log::debug!(
            "[listener] Node {} alive, {} service(s)",
            node_id,
            services.len()
        );
    } else {
        // Dead node: cleans the cache.
        log::warn!("[listener] Node {} DEAD, clearing cache", node_id);
        discovery.invalidate_node_services(NodeId(node_id));
        crate::node_lock::unregister_node_lock_watcher(NodeId(node_id));
        crate::reconnect::fire(node_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    const DEAD_NODE_ID: u32 = 0x0D1E_0001;

    #[test]
    fn handle_node_event_ignores_own_pid() {
        let discovery = Arc::new(NodeDiscovery::new());
        handle_node_event(&discovery, std::process::id());
    }

    #[test]
    fn handle_node_event_dead_node_clears_without_panic() {
        let discovery = Arc::new(NodeDiscovery::new());
        assert_ne!(DEAD_NODE_ID, std::process::id());
        handle_node_event(&discovery, DEAD_NODE_ID);
    }
}
