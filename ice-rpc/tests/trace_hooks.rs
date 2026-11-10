//! The propagation seam seen from the outside, with a collector installed.
//!
//! This file is its own test binary, so `install` runs once and every assertion
//! below observes the same hooks. What is checked is the contract, not the wire:
//! a call emitted outside a handler takes the collector's ids, a served call
//! hands its incoming context to the collector — which mints the span id of the
//! hop itself — and a sample that carries no trace, or no parent, is never
//! adopted.

#![cfg(feature = "tracing")]
#![allow(missing_docs)] // test target: documented by the module header, not part of a published API
#![allow(clippy::unwrap_used)] // tests may panic

use std::sync::Mutex;

use ice_rpc::codegen::{RpcHeader, ServiceRef, call_scoped};
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

    // A served call hands its incoming context to the collector. The span id of
    // the hop is the collector's to mint, so it is not part of what travels.
    let header = RpcHeader::request("method", 7, 1).with_trace(OUTGOING);
    let ctx = CallContext::new(&header, "Svc", "method");
    block_on(call_scoped(ctx, async {}));

    let seen = EXTRACTED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    assert_eq!(
        seen.last().copied(),
        Some(OUTGOING),
        "the collector receives the incoming context, with no span id added"
    );

    // A sample with no trace is not adopted: there is no parent to continue.
    let untraced = CallContext::new(&RpcHeader::request("method", 7, 1), "Svc", "method");
    let before = seen.len();
    block_on(call_scoped(untraced, async {}));

    let after = EXTRACTED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .len();
    assert_eq!(
        after, before,
        "an absent trace must not reach the collector"
    );
}
