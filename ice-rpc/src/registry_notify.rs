//! Topology change notifications through the iceoryx2 event.
//!
//! Every change (node up/down) is notified to the other processes
//! through a notifier on the `ice_rpc_registry_notify` event service.
//!
//! The registry (one Blackboard per node) is the source of truth;
//! this module only notifies changes to wake up the listeners of the
//! other processes.

use std::sync::Mutex;

use iceoryx2::prelude::*;

use crate::locator::ServiceLocator;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const REGISTRY_NOTIFY_TOPIC: &str = "ice_rpc_registry_notify";

// ---------------------------------------------------------------------------
// Notifier
// ---------------------------------------------------------------------------

type RegistryNotifier =
    iceoryx2::port::notifier::Notifier<iceoryx2::service::ipc_threadsafe::Service>;

static NOTIFIER: Mutex<Option<RegistryNotifier>> = Mutex::new(None);

fn create_notifier() -> Option<RegistryNotifier> {
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
        .open_or_create()
        .map_err(|e| {
            log::warn!("[notify] event open_or_create failed (shutdown?): {:?}", e);
        })
        .ok()?;
    svc.notifier_builder()
        .create()
        .map_err(|e| {
            log::warn!("[notify] notifier create failed: {:?}", e);
        })
        .ok()
}

fn with_notifier<R>(f: impl FnOnce(&RegistryNotifier) -> R) -> Option<R> {
    let mut guard = NOTIFIER.lock().ok()?;
    if guard.is_none() {
        *guard = create_notifier();
    }
    let notifier = guard.as_ref()?;
    Some(f(notifier))
}

pub fn notify_change(_node_id: u32) {
    let notified = with_notifier(|notifier| {
        if let Err(e) = notifier.notify() {
            log::warn!("notify failed: {:?}", e);
        }
    })
    .is_some();

    if !notified {
        log::debug!("[notify] notifier unavailable — change notification skipped");
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

/// Drops the cached registry notifier (called at shutdown).
pub fn clear_notifier() {
    if let Ok(mut guard) = NOTIFIER.lock() {
        if guard.take().is_some() {
            log::info!("[ice-rpc] registry notifier dropped.");
        }
    }
}
