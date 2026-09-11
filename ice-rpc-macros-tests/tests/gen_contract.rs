//! Contract test for the `#[doc(hidden)] pub mod gen` facade of `ice-rpc`.
//!
//! `ice-rpc-macros` emits code that references every symbol below, and
//! `ice-rpc-rx` uses the Rx primitives (`channel`, `Sender`, `WireEvent`, …).
//! `gen` is therefore an internal contract between those crates and the runtime
//! (see the "Versioning contract" note in `ice-rpc/src/gen.rs`).
//!
//! Importing/mentioning each symbol explicitly makes the test fail to compile as
//! soon as one of them is renamed or removed — which is exactly the
//! compatibility guarantee we want to pin: `gen` is semver-exempt for the
//! consumers of `ice-rpc`, but **not** for `ice-rpc-macros` / `ice-rpc-rx`.

#![allow(clippy::unwrap_used)] // tests/examples/benches may panic; production libs keep the deny, see [workspace.lints]

/// Referencing each symbol pins it in the compilation unit: a rename or a
/// removal in `ice_rpc::gen` turns this function into a compile error.
#[test]
fn gen_facade_symbols_resolve() {
    // The compilation of this explicit import list *is* the assertion: an
    // unknown or renamed path is a hard error. `allow(unused_imports)` is
    // required because pinning the symbols, not using them, is the point.
    #[allow(unused_imports)]
    use ice_rpc::gen::{
        _ProviderService,
        // Plumbing invoked by the generated entry points
        channel,
        clear_ipc_cleanup,
        collect_values,
        decode_aligned,
        first_event,
        fmt_correlation_id,
        global_cancel_token,
        init,
        init_without_ctrl_c,
        is_pid_alive,
        is_provider,
        mark_provider,
        native_call,
        next_correlation_id,
        observable_to_responses,
        raw_pid_to_u32,
        register_ipc_cleanup,
        register_node_liveness_watcher,
        registry_cancel_token,
        run_provider_inner,
        setup_iceoryx2_global_config,
        shutdown_and_release,
        spawn_native_service,
        unbounded_channel,
        unregister_node_liveness_watcher,
        wait_for_shutdown,
        EventKind,
        HttpCallable,
        NodeId,
        ResponseIter,
        RpcHeader,
        Sender,
        ServiceConsumer,
        ServiceDispatcher,
        ServiceLifecycle,
        ServiceNamed,
        ShutdownGuard,
        WireEvent,
        LIVENESS_POLL_MS,
        METHOD_NAME_LEN,
        PROTOCOL_VERSION,
        SERVICE_NAME_LEN,
    };

    // The symbols above are only valid as a set; no runtime behaviour is
    // asserted here on purpose.
}

/// The dependency re-exports exist so the generated code need not declare
/// `rkyv`, `serde_json`, `base64`, `async_channel`, … itself.
#[test]
fn gen_dependency_reexports_resolve() {
    let _ = core::any::type_name::<ice_rpc::gen::async_channel::Sender<u8>>();
    let _ = core::any::type_name::<ice_rpc::gen::async_lock::Mutex<u8>>();
    let _ = core::any::type_name::<ice_rpc::gen::futures::channel::oneshot::Sender<u8>>();
    let _ = core::any::type_name::<ice_rpc::gen::futures_lite::future::Pending<()>>();
    let _ = core::any::type_name::<ice_rpc::gen::iceoryx2::config::Config>();
    let _ = core::any::type_name::<ice_rpc::gen::log::Level>();
    let _ = core::any::type_name::<ice_rpc::gen::rkyv::rancor::Error>();
    let _ = core::any::type_name::<ice_rpc::gen::serde_json::Value>();
    let _ = ice_rpc::gen::base64::engine::general_purpose::STANDARD;
}
