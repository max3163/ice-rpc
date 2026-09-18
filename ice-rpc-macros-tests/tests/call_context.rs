//! The call context: the transport hands the request header to the handler, and
//! the implementation reads it as an **ambient** value.
//!
//! Increment 1 of `plans/call-context-tracing.md`. No wire format changes:
//! everything asserted here is already transported today.

#![allow(missing_docs)]
#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ice_rpc::gen::{rkyv, service_id_of, EventKind, ServiceDispatcher, ServiceRef, WireEvent};
use ice_rpc::transport::{native_call, spawn_native_service};
use ice_rpc::{service, CallContext, CancellationToken, Event, Observable, TraceContext};

/// Boots ice-rpc once for the whole test binary.
fn init_global() {
    static GUARD: std::sync::OnceLock<ice_rpc::gen::ShutdownGuard> = std::sync::OnceLock::new();
    GUARD.get_or_init(ice_rpc::gen::init_without_ctrl_c);
}

/// What a handler observed, for the test to assert on.
#[derive(Clone, Debug)]
struct Observed {
    method: &'static str,
    correlation: String,
    service_id: u32,
    service_version: u16,
    received_at_ns: u64,
    trace: TraceContext,
}

// ── 1. The transport socle: the handler receives the request header ──────────

/// A hand-built dispatcher, as the transport uses one.
#[test]
fn the_handler_receives_the_request_header() {
    ice_rpc::gen::setup_iceoryx2_global_config();

    let channel = format!("CallContextSocle{}", std::process::id());
    let service_id = service_id_of(&channel);
    let stop = CancellationToken::new();

    let seen: Arc<Mutex<Option<Observed>>> = Arc::new(Mutex::new(None));
    let captured = Arc::clone(&seen);

    let mut dispatcher = ServiceDispatcher::new(ServiceRef::new(service_id, 1));
    dispatcher.method("echo", move |header, _payload, emitter| {
        // Exactly what a generated provider handler does with the header.
        let ctx = CallContext::new(header, "echo");
        *captured.lock().unwrap() = Some(Observed {
            method: ctx.method(),
            correlation: ctx.correlation(),
            service_id: ctx.service_id(),
            service_version: ctx.service_version(),
            received_at_ns: ctx.received_at_ns(),
            trace: ctx.trace(),
        });

        let sample = rkyv::to_bytes::<rkyv::rancor::Error>(&WireEvent::<i32, String>::Complete)
            .expect("encode the terminal event");
        let _ = emitter.emit(EventKind::Complete, &sample);
    });

    let server = spawn_native_service(&channel, vec![dispatcher], stop.clone());
    std::thread::sleep(Duration::from_millis(300));

    let stream =
        native_call::<i32, String>(&channel, ServiceRef::new(service_id, 1), "echo", b"go")
            .expect("native_call must open the native service");
    let _ = ice_rpc::rt::block_on(stream.collect());

    let seen = seen.lock().unwrap().clone().expect("the handler ran");
    assert_eq!(seen.method, "echo");
    assert_eq!(seen.correlation.len(), 36, "uuid-shaped");
    assert_eq!(seen.service_id, service_id);
    assert_eq!(seen.service_version, 1);
    assert!(
        seen.received_at_ns > 0,
        "the request timestamp travels with the call"
    );
    assert!(
        seen.trace.is_present(),
        "the client injects a trace even when the caller has none"
    );
    assert_eq!(
        seen.trace.parent_span_id, 0,
        "a call emitted outside any trace starts one: it has no parent"
    );

    stop.cancel();
    let _ = server.join();
}

// ── 2. The ambient context, through the macro-generated handler ──────────────

static SEEN: Mutex<Option<Observed>> = Mutex::new(None);

#[service("AmbientContextDemo")]
#[async_trait::async_trait]
pub trait AmbientContextDemo: Send + Sync + 'static {
    /// Note the signature: **no context parameter**. The implementation reads it.
    async fn probe(&self, value: i32) -> Observable<i32, String>;
}

struct Impl;

#[async_trait::async_trait]
impl AmbientContextDemo for Impl {
    async fn probe(&self, value: i32) -> Observable<i32, String> {
        let ctx = CallContext::current().expect("the generated handler installs the context");
        *SEEN.lock().unwrap() = Some(Observed {
            method: ctx.method(),
            correlation: ctx.correlation(),
            service_id: ctx.service_id(),
            service_version: ctx.service_version(),
            received_at_ns: ctx.received_at_ns(),
            trace: ctx.trace(),
        });
        Observable::from_events([Event::Next(value + 1), Event::Complete])
    }
}

#[test]
fn the_implementation_reads_the_ambient_context() {
    init_global();

    let locator = ice_rpc::locator();
    let provider = AmbientContextDemoProxy::provide(Impl);
    ice_rpc::rt::block_on(async {
        locator.register(provider).await;
        locator.initialize_all().await.expect("initialize_all");
    });

    // Let the native service thread create the iceoryx2 service.
    std::thread::sleep(Duration::from_millis(500));

    let consumer = AmbientContextDemoProxy::consume();
    // The call site is unchanged, on both sides.
    let stream = ice_rpc::rt::block_on(consumer.probe(41));
    let values = ice_rpc::rt::block_on(stream.collect()).expect("collect");
    assert_eq!(values, vec![42]);

    let seen = SEEN.lock().unwrap().clone().expect("the handler ran");
    assert_eq!(seen.method, "probe");
    assert_eq!(seen.service_version, 1);
    assert_eq!(seen.correlation.len(), 36, "uuid-shaped");
    assert_ne!(seen.service_id, 0, "the routed service id is exposed");
    assert!(seen.received_at_ns > 0);
    assert!(
        seen.trace.is_present(),
        "the trace reaches the implementation"
    );
}
