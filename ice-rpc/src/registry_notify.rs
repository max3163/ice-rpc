//! Topology change notifications through the iceoryx2 event.
//!
//! Every change (node up/down) is notified to the other processes
//! through a notifier on the `ice_rpc_registry_notify` event service.
//!
//! The registry (one Blackboard per node) is the source of truth;
//! this module only notifies changes to wake up the listeners of the
//! other processes.

use std::sync::OnceLock;

use iceoryx2::prelude::*;

use crate::locator::ServiceLocator;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const REGISTRY_NOTIFY_TOPIC: &str = "ice_rpc_registry_notify";

// ---------------------------------------------------------------------------
// Notifier
// ---------------------------------------------------------------------------

fn get_or_create_notifier() -> Option<
    &'static iceoryx2::port::notifier::Notifier<iceoryx2::service::ipc_threadsafe::Service>,
> {
    type RegistryNotifier =
        iceoryx2::port::notifier::Notifier<iceoryx2::service::ipc_threadsafe::Service>;
    static NOTIFIER: OnceLock<RegistryNotifier> = OnceLock::new();
    if let Some(n) = NOTIFIER.get() {
        return Some(n);
    }
    let node = ServiceLocator::global()
        .get_node_sync()
        .map_err(|e| {
            log::warn!("[notify] get_node_sync failed (shutdown?): {}", e);
        })
        .ok()?;
    let topic_name = ServiceName::new(REGISTRY_NOTIFY_TOPIC)
        .map_err(|e| {
            log::warn!("[notify] ServiceName failed: {:?}", e);
        })
        .ok()?;
    let svc = node
        .service_builder(&topic_name)
        .event()
        .event_id_max_value(65535)
        .open_or_create()
        .map_err(|e| {
            log::warn!("[notify] event open_or_create failed (shutdown?): {:?}", e);
        })
        .ok()?;
    let notifier = svc
        .notifier_builder()
        .create()
        .map_err(|e| {
            log::warn!("[notify] notifier create failed: {:?}", e);
        })
        .ok()?;
    let _ = NOTIFIER.set(notifier);
    NOTIFIER.get()
}

pub fn notify_change(node_id: u32) {
    let Some(notifier) = get_or_create_notifier() else {
        log::debug!("[notify] notifier unavailable — change notification skipped");
        return;
    };
    if let Err(e) = notifier.notify_with_custom_event_id(EventId::new(node_id as usize)) {
        log::warn!("notify_with_custom_event_id failed: {:?}", e);
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Notifies that the node is operational (its services are in the registry).
pub fn announce_node_ready(node_id: u32) {
    notify_change(node_id);
}

/// Notifies that a node is dead.
pub fn announce_dead_node(node_id: u32) {
    notify_change(node_id);
}
