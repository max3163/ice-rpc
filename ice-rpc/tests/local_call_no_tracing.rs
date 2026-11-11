//! A service declared `no_tracing` is served with **no** span.
//!
//! The `tracing` feature is on and a collector records every span created, so a
//! span would show. None must: the call context is still installed — the callee
//! keeps its own identity and the caller's trace keeps propagating — and only the
//! span of the hop disappears.
//!
//! The traced counterpart is `local_call_span.rs`.

#![cfg(feature = "tracing")]
#![allow(missing_docs)] // test target: documented by the module header, not part of a published API
#![allow(clippy::unwrap_used)] // tests may panic

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ice_rpc::codegen::RpcHeader;
use ice_rpc::rt::block_on;
use ice_rpc::{CallContext, Observable, TraceContext, service};

/// The leaf the caller delegates to: opted out of spans.
#[service("DirectNoTracingLeaf", no_tracing)]
#[async_trait::async_trait]
pub trait DirectNoTracingLeaf: Send + Sync + 'static {
    /// Returns its argument, incremented.
    async fn reserve(&self, value: i32) -> Observable<i32, String>;
}

/// What the leaf read about the call it was serving.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Seen {
    service_id: u32,
    method: &'static str,
    trace: TraceContext,
}

/// Last observation of the leaf, for the test to assert on.
static SEEN: Mutex<Option<Seen>> = Mutex::new(None);

/// Leaf implementation: records the context, then answers.
struct Leaf;

#[async_trait::async_trait]
impl DirectNoTracingLeaf for Leaf {
    async fn reserve(&self, value: i32) -> Observable<i32, String> {
        let ctx = CallContext::current().expect("the direct call installs its own context");
        *SEEN.lock().unwrap() = Some(Seen {
            service_id: ctx.service_id(),
            method: ctx.method(),
            trace: ctx.trace(),
        });

        ice_rpc::of(value + 1)
    }
}

/// Minimal subscriber: it records the spans created and counts the entries.
#[derive(Default)]
struct Recorder {
    names: Mutex<Vec<&'static str>>,
    entered: AtomicUsize,
}

impl tracing::Subscriber for Recorder {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::Id {
        let mut names = self.names.lock().unwrap_or_else(|e| e.into_inner());
        names.push(span.metadata().name());
        tracing::Id::from_u64(names.len() as u64)
    }

    fn record(&self, _span: &tracing::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::Id, _follows: &tracing::Id) {}
    fn event(&self, _event: &tracing::Event<'_>) {}

    fn enter(&self, _span: &tracing::Id) {
        self.entered.fetch_add(1, Ordering::Relaxed);
    }

    fn exit(&self, _span: &tracing::Id) {}
}

/// The delegation keeps the callee's context and the caller's trace, and opens no
/// span at all.
#[test]
fn a_direct_call_of_an_opted_out_service_emits_no_span() {
    let recorder = Arc::new(Recorder::default());
    let dispatch = tracing::Dispatch::new(Arc::clone(&recorder));

    let parent = CallContext::new(
        &RpcHeader::request("place_order", 1, 1).with_trace(TraceContext::new_root()),
        "OrderService",
        "place_order",
    );

    // The proxy of a service provided in this process: calling it is exactly the
    // in-process delegation a provider makes to another of its services.
    let leaf = DirectNoTracingLeafProxy::provide(Leaf);

    let value = tracing::dispatcher::with_default(&dispatch, || {
        let _scope = parent.enter_ambient();
        let stream = block_on(leaf.reserve(41));
        block_on(stream.first_value()).expect("the leaf answers")
    });
    assert_eq!(value, 42, "the delegation returned the callee's value");

    let seen = SEEN.lock().unwrap().expect("the leaf ran");
    assert_eq!(
        seen.service_id,
        ice_rpc::codegen::service_id_of("DirectNoTracingLeaf"),
        "the callee still reads its own identity"
    );
    assert_eq!(seen.method, "reserve", "and the callee's method");
    assert_eq!(
        seen.trace.trace_id,
        parent.trace().trace_id,
        "the trace keeps propagating: only the span is gone"
    );

    let names = recorder
        .names
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    assert!(
        names.is_empty(),
        "an opted-out service must open no span, got {names:?}"
    );
    assert_eq!(
        recorder.entered.load(Ordering::Relaxed),
        0,
        "and enter nothing"
    );
}
