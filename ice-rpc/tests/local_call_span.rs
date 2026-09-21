//! The span of a **direct** (in-process) call, with the `tracing` feature on.
//!
//! A provider that hosts two services and calls one from the other goes through
//! the generated proxy, whose `Provider` mode used to call the local
//! implementation with no context and no span. This asserts what it gets now —
//! and the whole point of the envelope: the callee sees **its own** identity, the
//! delegation lands in the caller's trace, and both happen inside one span named
//! after the RPC.
//!
//! The feature-off counterpart is `local_call_no_span.rs`: same call, and nothing
//! is built at all.
//!
//! See `plans/spans-appels-internes-provider.md`.

#![cfg(feature = "tracing")]
#![allow(missing_docs)] // test target: documented by the plan, not part of a published API
#![allow(clippy::unwrap_used)] // tests may panic

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ice_rpc::gen::RpcHeader;
use ice_rpc::rt::block_on;
use ice_rpc::{service, CallContext, Observable, TraceContext};

/// The leaf the caller delegates to.
#[service("DirectSpanLeaf")]
#[async_trait::async_trait]
pub trait DirectSpanLeaf: Send + Sync + 'static {
    /// Returns its argument, incremented.
    async fn reserve(&self, value: i32) -> Observable<i32, String>;
}

/// What the leaf read about the call it was serving.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Seen {
    service_id: u32,
    method: &'static str,
    received_at_ns: u64,
    trace: TraceContext,
    span_id: u64,
}

/// Last observation of the leaf, for the test to assert on.
static SEEN: Mutex<Option<Seen>> = Mutex::new(None);

/// Leaf implementation: records the context, then yields.
struct Leaf;

#[async_trait::async_trait]
impl DirectSpanLeaf for Leaf {
    async fn reserve(&self, value: i32) -> Observable<i32, String> {
        let ctx = CallContext::current().expect("the direct call installs its own context");
        *SEEN.lock().unwrap() = Some(Seen {
            service_id: ctx.service_id(),
            method: ctx.method(),
            received_at_ns: ctx.received_at_ns(),
            trace: ctx.trace(),
            span_id: ctx.span_id(),
        });

        // The context is installed around **every** poll, not around the call: a
        // yield must not cost the leaf the identity of the call it is serving.
        ice_rpc::gen::futures_lite::future::yield_now().await;
        assert_eq!(
            CallContext::current().map(|ctx| ctx.method()),
            Some("reserve"),
            "the context must survive a yield inside a direct call"
        );

        ice_rpc::of(value + 1)
    }
}

/// Renders the fields one span carries, so the test asserts on what a collector
/// would actually see — the `service` field included.
#[derive(Default)]
struct Fields(Vec<String>);

impl tracing::field::Visit for Fields {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.push(format!("{}={value:?}", field.name()));
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.push(format!("{}={value:?}", field.name()));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.push(format!("{}={value}", field.name()));
    }
}

/// Minimal subscriber: it records the spans created, their fields, and the entries.
#[derive(Default)]
struct Recorder {
    names: Mutex<Vec<&'static str>>,
    fields: Mutex<Vec<Vec<String>>>,
    entered: AtomicUsize,
    exited: AtomicUsize,
}

impl tracing::Subscriber for Recorder {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::Id {
        let mut fields = Fields::default();
        span.record(&mut fields);
        self.fields
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(fields.0);

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

    fn exit(&self, _span: &tracing::Id) {
        self.exited.fetch_add(1, Ordering::Relaxed);
    }
}

/// A direct call is a traced hop: own identity, caller's trace, one span.
#[test]
fn a_direct_call_gets_its_own_context_and_a_span_parented_on_the_caller() {
    let recorder = Arc::new(Recorder::default());
    let dispatch = tracing::Dispatch::new(Arc::clone(&recorder));

    let parent = CallContext::new(
        &RpcHeader::request("place_order", 1, 1).with_trace(TraceContext::new_root()),
        "OrderService",
        "place_order",
    );

    // The proxy of a service provided in this process: calling it is exactly the
    // in-process delegation a provider makes to another of its services.
    let leaf = DirectSpanLeafProxy::provide(Leaf);

    let value = tracing::dispatcher::with_default(&dispatch, || {
        let _scope = parent.enter_ambient();
        let stream = block_on(leaf.reserve(41));
        block_on(stream.first_value()).expect("the leaf answers")
    });
    assert_eq!(value, 42, "the delegation returned the callee's value");

    // The callee, not the caller.
    let seen = SEEN.lock().unwrap().expect("the leaf ran");
    assert_eq!(
        seen.service_id,
        ice_rpc::gen::service_id_of("DirectSpanLeaf"),
        "the context names the callee's service"
    );
    assert_eq!(seen.method, "reserve", "and the callee's method");
    assert_eq!(
        seen.received_at_ns, 0,
        "a direct call has no wire emission instant, so no clock is read"
    );

    // The caller's trace, parented on the caller's span.
    assert_eq!(seen.trace.trace_id, parent.trace().trace_id, "same trace");
    assert_eq!(
        seen.trace.parent_span_id,
        parent.span_id(),
        "the delegation is a hop of the caller's trace"
    );

    // The span itself: one `rpc` span for the delegation, entered while it runs.
    let names = recorder
        .names
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    assert_eq!(names, ["rpc"], "one span for the direct call");

    // And what that span carries: the callee's **name** in plain text. The header
    // only transports its 4-byte hash, so a span that showed a number would be
    // unreadable — this is the assertion that pins the name.
    let fields = recorder
        .fields
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    assert_eq!(fields.len(), 1, "one span, one set of fields");
    let fields = &fields[0];
    assert!(
        fields.iter().any(|f| f == "service=\"DirectSpanLeaf\""),
        "the span names the callee instead of showing its hash: {fields:?}"
    );
    assert!(
        fields.iter().any(|f| f == "method=\"reserve\""),
        "and the method it served: {fields:?}"
    );
    assert!(
        fields.iter().any(|f| f == "kind=\"local\""),
        "and marks the hop as in-process: {fields:?}"
    );
    assert!(
        recorder.entered.load(Ordering::Relaxed) >= 1,
        "the span is entered while the call runs"
    );
    assert!(
        recorder.exited.load(Ordering::Relaxed) >= 1,
        "and left again, so the next call is not labelled with it"
    );
}
