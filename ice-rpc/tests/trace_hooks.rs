//! The propagation seam seen from the outside, with a collector installed.
//!
//! This file is its own test binary, so `install` runs once and every assertion
//! below observes the same hooks. What is checked is the contract, not the wire:
//! a call emitted outside a handler takes the collector's ids, a served call
//! hands its incoming context to the collector — which mints the span id of the
//! hop itself — when the collector's filter accepts that span, and a sample that
//! carries no trace, or no parent, is never adopted.

#![cfg(feature = "tracing")]
#![allow(missing_docs)] // test target: documented by the module header, not part of a published API
#![allow(clippy::unwrap_used)] // tests may panic

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ice_rpc::codegen::{
    RpcHeader, ServiceRef, call_scoped, call_scoped_untraced, local_call_scoped,
};
use ice_rpc::rt::block_on;
use ice_rpc::{CallContext, TraceContext};

/// Contexts the extraction hook below received, in call order.
static EXTRACTED: Mutex<Vec<TraceContext>> = Mutex::new(Vec::new());

/// The trace the fake collector reports as the caller's current span.
const OUTGOING: TraceContext = TraceContext {
    trace_id: [0x11; 16],
    parent_span_id: 0xabcd,
    flags: 1,
};

/// A collector that always reports the same current span.
fn inject() -> Option<TraceContext> {
    Some(OUTGOING)
}

/// A collector that records what it is told to adopt.
fn extract(trace: &TraceContext) {
    EXTRACTED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(*trace);
}

/// The contexts the extraction hook received so far.
fn extracted() -> Vec<TraceContext> {
    EXTRACTED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

/// A collector that accepts spans and events up to a level, counting the spans it
/// actually creates.
///
/// Every `rpc` span is a `debug_span!`, so `INFO` rejects the span of a hop while
/// `DEBUG` accepts it. `spans` is what tells an accepted hop from a filtered one.
struct LevelLimited {
    max_level: tracing::Level,
    spans: AtomicUsize,
}

impl LevelLimited {
    /// A collector whose filter accepts `max_level` and everything above it.
    fn accepting(max_level: tracing::Level) -> Self {
        Self {
            max_level,
            spans: AtomicUsize::new(0),
        }
    }
}

impl tracing::Subscriber for LevelLimited {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() <= self.max_level
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::Id {
        self.spans.fetch_add(1, Ordering::Relaxed);
        tracing::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::Id, _follows: &tracing::Id) {}
    fn event(&self, _event: &tracing::Event<'_>) {}
    fn enter(&self, _span: &tracing::Id) {}
    fn exit(&self, _span: &tracing::Id) {}
}

/// One test rather than several: `install` is process-wide, and the recorder is
/// shared, so a single sequential scenario is what keeps the assertions exact.
#[test]
fn an_installed_collector_feeds_and_receives_the_trace_ids() {
    ice_rpc::trace::install(inject, extract);

    // Outside a handler, a call takes the collector's context, ids included.
    let local = CallContext::local(ServiceRef::new(7, 1), "Svc", "method");
    assert_eq!(
        local.trace().trace_id,
        OUTGOING.trace_id,
        "the call continues the collector's trace"
    );
    assert_eq!(
        local.trace().parent_span_id,
        OUTGOING.parent_span_id,
        "and parents on the span that emitted it"
    );
    assert_ne!(
        local.span_id(),
        OUTGOING.parent_span_id,
        "the receiver mints its own span"
    );

    // A served call whose span the collector accepts hands its incoming context
    // over. The span id of the hop is the collector's to mint, so it is not part
    // of what travels.
    let header = RpcHeader::request("method", 7, 1).with_trace(OUTGOING);
    let accepting = Arc::new(LevelLimited::accepting(tracing::Level::DEBUG));
    let ctx = CallContext::new(&header, "Svc", "method");
    tracing::subscriber::with_default(Arc::clone(&accepting), || {
        block_on(call_scoped(ctx, async {}));
    });
    assert_eq!(
        accepting.spans.load(Ordering::Relaxed),
        1,
        "the collector accepts the DEBUG span of the hop"
    );
    assert_eq!(
        extracted().last().copied(),
        Some(OUTGOING),
        "the collector receives the incoming context, with no span id added"
    );

    // A sample with no trace is not adopted, even though the hop still has a
    // span: there is no parent to continue.
    let untraced = CallContext::new(&RpcHeader::request("method", 7, 1), "Svc", "method");
    let before = extracted().len();
    let accepted = Arc::new(LevelLimited::accepting(tracing::Level::DEBUG));
    tracing::subscriber::with_default(Arc::clone(&accepted), || {
        block_on(call_scoped(untraced, async {}));
    });
    assert_eq!(
        accepted.spans.load(Ordering::Relaxed),
        1,
        "a hop whose sample carries no trace still opens its span"
    );
    assert_eq!(
        extracted().len(),
        before,
        "an absent trace must not reach the collector"
    );

    // A hop of a `no_tracing` service opens no span, so it must not hand its
    // context over either: the adoption is a one-shot signal that the span of the
    // hop consumes, and leaving it pending would decorate the next span created.
    let opted_out = CallContext::new(&header, "Svc", "method");
    let accepted = Arc::new(LevelLimited::accepting(tracing::Level::DEBUG));
    tracing::subscriber::with_default(Arc::clone(&accepted), || {
        block_on(call_scoped_untraced(opted_out, async {}));
    });
    assert_eq!(
        accepted.spans.load(Ordering::Relaxed),
        0,
        "the delegation of an opted-out service opens no span"
    );
    assert_eq!(
        extracted().len(),
        before,
        "an opted-out hop must not reach the collector"
    );

    // Nor must a span the collector's filter rejects consume the signal: handing
    // it over when that span will not exist leaves it pending for the next span
    // the collector creates on the thread.
    let filtered = CallContext::new(&header, "Svc", "method");
    let rejecting = Arc::new(LevelLimited::accepting(tracing::Level::INFO));
    tracing::subscriber::with_default(Arc::clone(&rejecting), || {
        block_on(call_scoped(filtered, async {}));
    });
    assert_eq!(
        rejecting.spans.load(Ordering::Relaxed),
        0,
        "the filter rejects the span of the hop, so the collector creates none"
    );
    assert_eq!(
        extracted().len(),
        before,
        "a filtered-out span must not hand its context over either"
    );

    // The same rule holds for a direct (in-process) hop, whose span is also a
    // `DEBUG` span: a filter that rejects it must not see its context adopted.
    let rejecting = Arc::new(LevelLimited::accepting(tracing::Level::INFO));
    tracing::subscriber::with_default(Arc::clone(&rejecting), || {
        block_on(local_call_scoped(
            ServiceRef::new(7, 1),
            "Svc",
            "method",
            async {},
        ));
    });
    assert_eq!(
        rejecting.spans.load(Ordering::Relaxed),
        0,
        "the filter rejects the span of the direct hop too"
    );
    assert_eq!(
        extracted().len(),
        before,
        "a filtered-out direct hop must not hand its context over either"
    );
}
