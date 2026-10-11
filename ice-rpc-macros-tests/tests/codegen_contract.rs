//! Contract test for the `#[doc(hidden)] pub mod codegen` facade of `ice-rpc`.
//!
//! `ice-rpc-macros` emits code that references every symbol below. Importing each
//! one explicitly makes the test fail to compile as soon as one is renamed or
//! removed: `codegen` is semver-exempt for the consumers of `ice-rpc`, but **not**
//! for `ice-rpc-macros`.

#![allow(clippy::unwrap_used)] // tests/examples/benches may panic

/// Referencing each symbol pins it in the compilation unit: a rename or a
/// removal in `ice_rpc::codegen` turns this function into a compile error.
#[test]
fn gen_facade_symbols_resolve() {
    // Compiling this import list *is* the assertion: `allow(unused_imports)` is
    // required because pinning the symbols, not using them, is the point.
    #[allow(unused_imports)]
    use ice_rpc::codegen::{
        _ProviderService,
        CollectEmitter,
        DEFAULT_MAX_SLICE_LEN,
        EventKind,
        JSON_CALL_TIMEOUT,
        JsonCallError,
        JsonCallEvent,
        JsonCallStream,
        JsonInvoker,
        JsonOutcome,
        JsonResult,
        LIVENESS_POLL_MS,
        NodeId,
        PROTOCOL_VERSION,
        ReadMode,
        ResponseEmitter,
        RpcHeader,
        SERVICE_NAME_LEN,
        Sender,
        ServiceConsumer,
        ServiceDispatcher,
        ServiceLifecycle,
        ServiceNamed,
        ShutdownGuard,
        WireEvent,
        // Plumbing invoked by the generated entry points
        channel,
        clear_ipc_cleanup,
        collect_values,
        declare_channel_max_slice_len,
        decode_aligned,
        first_event,
        fmt_correlation_id,
        global_cancel_token,
        init,
        init_without_ctrl_c,
        is_pid_alive,
        is_provider,
        local_call_scoped,
        mark_provider,
        native_call,
        next_correlation_id,
        observable_to_responses,
        raw_pid_to_u32,
        read_json,
        register_ipc_cleanup,
        register_native_service,
        register_node_liveness_watcher,
        registry_cancel_token,
        run_provider_inner,
        service_id_of,
        shutdown_and_release,
        spawn_native_service,
        start_registered_channels,
        unbounded_channel,
        unregister_node_liveness_watcher,
        wait_for_shutdown,
    };

    // The symbols above are only valid as a set; no runtime behaviour is
    // asserted here on purpose.
}

/// The dependency re-exports exist so the generated code need not declare
/// `rkyv`, `serde_json`, `base64`, `async_channel`, … itself.
#[test]
fn gen_dependency_reexports_resolve() {
    let _ = core::any::type_name::<ice_rpc::codegen::async_channel::Sender<u8>>();
    let _ = core::any::type_name::<ice_rpc::codegen::async_lock::Mutex<u8>>();
    let _ = core::any::type_name::<ice_rpc::codegen::futures_lite::future::Pending<()>>();
    let _ = core::any::type_name::<ice_rpc::codegen::iceoryx2::config::Config>();
    let _ = core::any::type_name::<ice_rpc::codegen::log::Level>();
    let _ = core::any::type_name::<ice_rpc::codegen::rkyv::rancor::Error>();
    let _ = core::any::type_name::<ice_rpc::codegen::serde_json::Value>();
    let _ = ice_rpc::codegen::base64::engine::general_purpose::STANDARD;
}
