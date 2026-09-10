//! Discovery event listener (iceoryx2 WaitSet loop).
//!
//! Listens to notifications on the `ice_rpc_registry_notify` event service.
//! On each notification, re-reads the Blackboard to update the local view
//! of nodes and services.

use std::sync::Arc;

use crate::macros::try_or_log;

use iceoryx2::prelude::*;

use crate::locator::ServiceLocator;
use crate::node_discovery::NodeDiscovery;
use crate::registry_notify::REGISTRY_NOTIFY_TOPIC;

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
    // No `event_id_max_value` override: the notification is a payload-free
    // wake-up (see `registry_notify::notify_change`), so the iceoryx2 default
    // bound (255) is enough.
    let notify_svc = try_or_log!(
        node.service_builder(&notify_topic_name)
            .event()
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
                    // The notification is only a "the topology may have
                    // changed" wake-up: the blackboard and native liveness are
                    // the source of truth, so reconcile instead of trusting the
                    // event payload (which no longer carries a node id).
                    let mut notified = false;
                    while let Ok(Some(_)) = listener.try_wait_one() {
                        notified = true;
                    }
                    if notified {
                        reconcile_topology(&discovery);
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

/// Re-synchronizes the local cache with the live nodes and fires the
/// reconnection callbacks for the nodes that disappeared.
fn reconcile_topology(discovery: &NodeDiscovery) {
    let live = discovery.discover_live_nodes();
    for node_id in discovery.reconcile(&live) {
        log::warn!("[listener] Node {} DEAD, clearing cache", node_id);
        crate::node_liveness::unregister_node_liveness_watcher(node_id);
        crate::node_supervisor::fire(node_id.0);
    }
}
