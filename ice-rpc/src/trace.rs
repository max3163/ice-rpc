//! Propagation seam between a call collector and the wire.
//!
//! ice-rpc carries a W3C-shaped [`TraceContext`] in every request, but it does not
//! decide where the ids come from: a collector (`tracing`, `log`, a custom
//! exporter) owns the spans, and this module lets it feed the transport in both
//! directions without the transport depending on it.
//!
//! # The two directions
//!
//! - **Injection** — the [`InjectHook`](crate::trace::InjectHook) returns the
//!   context of the caller's current span. `parent_span_id` is the id of the span
//!   that emits the call, which is what the transport copies into the request
//!   header as the W3C `parent-id`.
//! - **Extraction** — when a provider serves a call, the
//!   [`ExtractHook`](crate::trace::ExtractHook) is called once, immediately before
//!   the span of that hop is created, with the incoming context by reference. The
//!   collector learns the trace to continue and the span to parent on; it mints
//!   the span id of the hop itself, as W3C requires.
//!
//! # Who owns the span id of a hop
//!
//! The receiver mints the span id of the span it creates, so a collector that
//! records spans is the only authority on it. `CallContext::span_id` therefore
//! remains the **fallback** identity of a hop — the one a deployment without a
//! collector puts on the wire, and the one the out-of-band monitor reads — and it
//! is never substituted for the collector's id when a collector is installed.
//! That rule is applied by the injection side: inside a handler, the collector's
//! context wins **only** when it carries the trace of the call being served, which
//! is exactly the case where its span id belongs to this trace. A collector span
//! that sits in another trace belongs to the application around the call, not to
//! the call, and the ambient call context is used instead.
//!
//! # Contract
//!
//! - The extraction hook is a one-shot signal. A collector must consume it when it
//!   creates the span of the hop and clear it, because the next span created on the
//!   same thread would otherwise adopt a parent that is not its own.
//! - The signal is only handed over when the span of the hop will exist: before
//!   adopting, ice-rpc asks the collector whether the `DEBUG` span it is about to
//!   create — same target, same level, with the field names every hop carries —
//!   would be enabled, and skips the adoption when the answer is no. A rejected
//!   span would consume nothing, and the pending context would decorate the next
//!   span the collector creates on the thread. A filter more specific than that
//!   pre-check (one that matches field *values*, a span name, a file or a line)
//!   can still disagree; the hop then goes unadopted instead of leaving a signal
//!   behind.
//! - The extraction hook is never called for a sample whose trace id is zero or
//!   whose parent id is zero: the first carries no trace, the second is malformed
//!   per W3C (a `traceparent` `parent-id` is never zero).
//! - Without [`install`](crate::trace::install), every call behaves exactly as
//!   before: a root trace is minted per call and every hop mints its own span id.
//!
//! # Cost
//!
//! One atomic load per emitted call and per served call, then a function-pointer
//! call; a served call also asks the collector's filter about its span before
//! adopting, which is one more subscriber call on the same path. Nothing is
//! allocated and no lock is taken.
//!
//! [`TraceContext`]: crate::TraceContext

use std::sync::atomic::{AtomicBool, Ordering};

use crate::global::Global;
use crate::types::TraceContext;

/// Returns the trace context of the caller's current span.
///
/// `parent_span_id` must be the id of the span that emits the call: the transport
/// copies it into the request header, where the receiver reads it as the W3C
/// `parent-id`.
pub type InjectHook = fn() -> Option<TraceContext>;

/// Adopts the context of a received call.
///
/// The context is borrowed, and carries no span id for the hop: the collector
/// mints that id on the span it creates, which is what makes its export the single
/// source of span identity.
pub type ExtractHook = fn(trace: &TraceContext);

/// The installed hooks, read through one [`Global`].
///
/// Both are plain function pointers: no trait, no allocation and no dispatch
/// table, because a process has exactly one collector.
#[derive(Clone, Copy)]
pub(crate) struct Hooks {
    pub(crate) inject: InjectHook,
    pub(crate) extract: ExtractHook,
}

/// The hooks of this process, initialized by the first [`install`].
static HOOKS: Global<Hooks> = Global::new();

/// Whether [`install`] already ran, so a second call can be diagnosed in tests
/// and debug builds without changing the outcome.
static INSTALLED: AtomicBool = AtomicBool::new(false);

/// Installs the propagation hooks, once per process.
///
/// The first call wins; a later one is ignored, and a debug build asserts that it
/// does not happen. Install before the first RPC, next to the code that installs
/// the collector itself.
///
/// Both parameters are function pointers, so a closure passed here must capture
/// nothing and coerce to one. That is deliberate: it keeps the call path free of
/// allocation and of a dispatch table.
pub fn install(inject: InjectHook, extract: ExtractHook) {
    HOOKS.get_or_init(|| Hooks { inject, extract });

    let already_installed = INSTALLED.swap(true, Ordering::Relaxed);
    debug_assert!(
        !already_installed,
        "ice_rpc::trace::install must be called at most once per process; the first call wins"
    );
}

/// Returns the installed hooks, if any.
#[inline]
pub(crate) fn hooks() -> Option<&'static Hooks> {
    HOOKS.get()
}

/// Asks the installed collector for the caller's trace context.
#[inline]
pub(crate) fn inject() -> Option<TraceContext> {
    inject_with(hooks())
}

/// Hands a received context to the installed collector.
#[inline]
pub(crate) fn extract(trace: &TraceContext) {
    extract_with(hooks(), trace);
}

/// The dispatch of [`inject`], with the hooks passed in.
///
/// Taking them as a parameter is what makes the contract testable without
/// installing process-wide state.
#[inline]
pub(crate) fn inject_with(hooks: Option<&Hooks>) -> Option<TraceContext> {
    hooks.and_then(|hooks| (hooks.inject)())
}

/// The dispatch of [`extract`], with the hooks passed in.
///
/// A context with no trace id, or with a zero parent id, stops here: there is no
/// parent to continue, and W3C forbids a zero `parent-id`, so the sample is
/// treated as untraced rather than trusted.
#[inline]
pub(crate) fn extract_with(hooks: Option<&Hooks>, trace: &TraceContext) {
    if !trace.is_present() || trace.parent_span_id == 0 {
        return;
    }
    if let Some(hooks) = hooks {
        (hooks.extract)(trace);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    /// Contexts the extraction hook below received.
    static EXTRACTED: Mutex<Vec<TraceContext>> = Mutex::new(Vec::new());

    /// A collector that always reports the same current span.
    fn inject() -> Option<TraceContext> {
        Some(TraceContext {
            trace_id: [7u8; 16],
            parent_span_id: 0xabc,
            flags: 1,
        })
    }

    /// A collector that records what it is told to adopt.
    fn extract(trace: &TraceContext) {
        EXTRACTED
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(*trace);
    }

    /// The injection side answers unchanged, and no hook means no context.
    #[test]
    fn the_injection_hook_answers_unchanged() {
        let hooks = Hooks { inject, extract };

        assert_eq!(
            inject_with(Some(&hooks)),
            inject(),
            "the injection hook answers unchanged"
        );
        assert_eq!(inject_with(None), None, "no hook, no injected context");
    }

    /// The extraction side adopts a well-formed context and nothing else: no
    /// trace id, or a zero parent, is a sample without a usable parent.
    #[test]
    fn the_extraction_hook_receives_only_a_well_formed_context() {
        let hooks = Hooks { inject, extract };

        extract_with(Some(&hooks), &TraceContext::default());
        extract_with(
            Some(&hooks),
            &TraceContext {
                trace_id: [1u8; 16],
                parent_span_id: 0,
                flags: 1,
            },
        );
        assert!(
            EXTRACTED
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_empty(),
            "a zero trace id or a zero parent id must not reach the collector"
        );

        let trace = TraceContext {
            trace_id: [1u8; 16],
            parent_span_id: 3,
            flags: 0,
        };
        extract_with(Some(&hooks), &trace);

        let seen = EXTRACTED
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(
            seen,
            vec![trace],
            "a well-formed context is adopted as-is, with no span id added"
        );
    }
}
