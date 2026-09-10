//! Contract test for the `#[doc(hidden)] pub mod gen` facade of `ice-rpc`.
//!
//! `ice-rpc-macros` emits code that references every symbol below, so `gen` is
//! an internal contract between the macro crate and the runtime (see the
//! "Versioning contract" note in `ice-rpc/src/gen.rs`). Importing each symbol
//! explicitly makes the test fail to compile as soon as one of them is renamed
//! or removed, which is exactly the compatibility guarantee we want to pin:
//! `gen` is semver-exempt for the consumers of `ice-rpc`, but **not** for
//! `ice-rpc-macros`.

#[test]
fn gen_facade_symbols_resolve() {
    // The compilation of this explicit import list *is* the assertion: an
    // unknown or renamed path is a hard error. `allow(unused_imports)` is
    // required because pinning the symbols, not using them, is the point.
    #[allow(unused_imports)]
    use ice_rpc::gen::{
        acquire_global_node_lock, announce_dead_node, announce_node_ready, clear_ipc_cleanup,
        collect_values, create_node_blackboard, fire_reconnect_callbacks, first_event,
        is_node_alive, list_services, register_ipc_cleanup, register_node_lock_watcher,
        release_global_node_lock, spawn_node_registry_listener, unregister_node_lock_watcher,
        ClientCore, ConnectionState, DiscoveryEvent, NodeDiscovery, NodeHub, NodeLockWatcher,
        NodeRecord, NodeSupervisor, PendingService, ReconnectCallback, ReconnectManager,
        RequestHandler, ResponseHandler, SubscriberId, Subscription, LOCK_WATCHER_POLL_MS,
    };

    // The symbols above are only valid as a set; no runtime behaviour is
    // asserted here on purpose.
}
