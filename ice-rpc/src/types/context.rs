//! Read-only context of the call being served.
//!
//! The generated provider handler installs it around the implementation, which
//! therefore reads it without changing its signature:
//!
//! ```rust,ignore
//! #[service("Db")]
//! pub trait DbService {
//!     async fn get_user_age(&self, name: String) -> Observable<i32, DbError>;
//! }
//!
//! #[async_trait]
//! impl DbService for Db {
//!     async fn get_user_age(&self, name: String) -> Observable<i32, DbError> {
//!         let call = CallContext::current();
//!         // ...
//!     }
//! }
//! ```
//!
//! Everything a context carries is already transported by the request header, so
//! building one costs no allocation and no decode. The value is **ambient** for
//! the duration of one invocation of one handler (see [`CallContext::enter`]):
//! [`CallContext::current`] returns `None` outside a handler, and inside a task
//! the implementation spawned itself.
//!
//! The tracing fields (see [`TraceContext`]) are zero until the wire carries
//! them; callers must read [`TraceContext::is_present`] rather than assume a
//! trace exists.

use std::cell::Cell;
use std::marker::PhantomData;

use super::header::{fmt_correlation_id, next_correlation_id, RpcHeader, CORRELATION_ID_LEN};

thread_local! {
    /// Context of the call being served on this thread, if any.
    ///
    /// One slot per thread, installed by [`CallContext::enter`] for the duration
    /// of one handler call. The dispatch thread of a channel is dedicated, so the
    /// slot cannot be shared with an unrelated call.
    static CURRENT: Cell<Option<CallContext>> = const { Cell::new(None) };
}

/// Trace ids of one call.
///
/// Zeroed when the caller propagated no trace: this is the "no trace" value, not
/// a valid trace id, which is why [`TraceContext::is_present`] exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TraceContext {
    /// W3C trace id, shared by every hop of the call tree.
    pub trace_id: [u8; 16],
    /// Span of the caller, onto which the receiver parents its own span.
    pub parent_span_id: u64,
    /// W3C trace flags (bit 0: sampled).
    pub flags: u8,
}

impl TraceContext {
    /// Returns `true` when the caller propagated a trace.
    #[inline]
    pub fn is_present(&self) -> bool {
        self.trace_id != [0u8; 16]
    }

    /// Returns `true` when the emitter asked for the call to be sampled.
    #[inline]
    pub fn is_sampled(&self) -> bool {
        self.flags & 1 == 1
    }

    /// Trace id as lowercase hexadecimal, for logs and trace records.
    ///
    /// The wire carries raw bytes; this is for the places that must **render**
    /// them — a span field, an observer's trace record. Never the hot path.
    pub fn trace_id_hex(&self) -> String {
        hex_trace_id(&self.trace_id)
    }

    /// Starts a new trace: a fresh id, no parent.
    ///
    /// Used by a call made outside any traced work. The id is the same
    /// `pid ++ counter` shape as a correlation id, which makes it unique on a
    /// machine without pulling in a random generator — and this framework is
    /// single-machine by construction.
    #[inline]
    pub fn new_root() -> Self {
        Self {
            trace_id: next_correlation_id(),
            parent_span_id: 0,
            flags: 0,
        }
    }

    /// Continues this trace, parenting on `span_id`.
    #[inline]
    pub fn child_of(&self, span_id: u64) -> Self {
        Self {
            trace_id: self.trace_id,
            parent_span_id: span_id,
            flags: self.flags,
        }
    }

    /// Continues this trace if it is present, or starts a new one otherwise.
    ///
    /// This is the rule a hop applies: a call emitted while serving a traced
    /// call stays in that trace, a call emitted outside one becomes a root.
    #[inline]
    pub fn continued_or_root(&self, span_id: u64) -> Self {
        if self.is_present() {
            self.child_of(span_id)
        } else {
            Self::new_root()
        }
    }
}

/// Lowercase hexadecimal rendering of a trace id.
fn hex_trace_id(bytes: &[u8; 16]) -> String {
    use std::fmt::Write as _;

    let mut out = String::with_capacity(32);
    for byte in bytes {
        // Writing to a `String` cannot fail.
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Derives the span id of one hop from the call id.
///
/// Unique per call and stable for its whole duration, so a downstream call can
/// parent on it. No randomness, no extra wire field: the correlation id already
/// carries a process-unique `pid ++ counter`.
#[inline]
fn span_id_of(correlation_id: &[u8; CORRELATION_ID_LEN]) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&correlation_id[..8]);
    u64::from_be_bytes(bytes)
}

/// Read-only view of the call being served, built from its request header.
///
/// `Copy`: passing it as the first parameter of every RPC method costs nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallContext {
    correlation_id: [u8; CORRELATION_ID_LEN],
    service_id: u32,
    service_version: u16,
    method: &'static str,
    received_at_ns: u64,
    trace: TraceContext,
    /// Span id of **this** hop, which a downstream call parents on.
    span_id: u64,
}

impl CallContext {
    /// Builds the context of one request.
    ///
    /// `method` is the name the dispatcher routed on. The generated handler knows
    /// it at compile time, which is why it can be a `&'static str` rather than a
    /// slice borrowed from the header.
    #[inline]
    pub fn new(header: &RpcHeader, method: &'static str) -> Self {
        Self {
            correlation_id: header.correlation_id,
            service_id: header.service_id,
            service_version: header.service_version,
            method,
            received_at_ns: header.timestamp_ns,
            trace: header.trace(),
            span_id: span_id_of(&header.correlation_id),
        }
    }

    /// Raw correlation id of the call.
    #[inline]
    pub fn correlation_id(&self) -> &[u8; CORRELATION_ID_LEN] {
        &self.correlation_id
    }

    /// Correlation id formatted as a UUID-like hexadecimal string.
    ///
    /// This is the value to log: it is the one an out-of-band observer reads from
    /// the header, so a business log line and the monitor's view can be joined.
    pub fn correlation(&self) -> String {
        fmt_correlation_id(&self.correlation_id)
    }

    /// Identifier of the service inside its channel.
    #[inline]
    pub fn service_id(&self) -> u32 {
        self.service_id
    }

    /// Interface version the caller asked for.
    #[inline]
    pub fn service_version(&self) -> u16 {
        self.service_version
    }

    /// Method being served.
    #[inline]
    pub fn method(&self) -> &'static str {
        self.method
    }

    /// Emission time of the request, in nanoseconds since the Unix epoch.
    ///
    /// Stamped by the caller on the shared host clock, so it is comparable with
    /// [`now_ns`](crate::types::now_ns) taken here.
    #[inline]
    pub fn received_at_ns(&self) -> u64 {
        self.received_at_ns
    }

    /// Trace ids of the call; all zero when the caller propagated none.
    #[inline]
    pub fn trace(&self) -> TraceContext {
        self.trace
    }

    /// Span id of this hop, derived from the call id.
    ///
    /// A downstream call from inside this handler parents on it, which is what
    /// reconstitutes the depth of the chain.
    #[inline]
    pub fn span_id(&self) -> u64 {
        self.span_id
    }

    /// Trace to send on a call emitted from inside this handler.
    ///
    /// Continues the incoming trace when there is one, and starts a new one
    /// otherwise — so a chain is never broken by a hop that has no incoming
    /// trace to propagate.
    #[inline]
    pub fn child_trace(&self) -> TraceContext {
        self.trace.continued_or_root(self.span_id)
    }

    /// The context of the call being served on this thread, if any.
    ///
    /// `None` outside a provider handler — in a unit test that calls the
    /// implementation directly, for instance — and inside a task the
    /// implementation spawned itself, which starts on another thread with an
    /// empty slot. Both are expected: this is an ambient value, not a global.
    #[inline]
    pub fn current() -> Option<CallContext> {
        CURRENT.with(Cell::get)
    }

    /// Installs this context as the ambient one for the current thread.
    ///
    /// The generated provider handler calls this around the implementation, so
    /// the implementation reads the context with [`CallContext::current`] instead
    /// of receiving a parameter. Dropping the returned scope restores the
    /// **previous** value rather than clearing the slot, which is what makes
    /// nested calls — a provider calling another service in-process — correct.
    ///
    /// With the `tracing` feature, the same call also enters the span of the
    /// call, so the implementation's own events are attached to it. The span is
    /// created here rather than in the generated code on purpose: the codegen has
    /// one call site, and a build without the feature carries neither the span
    /// nor the dependency.
    #[must_use = "the context is only ambient while the returned scope is alive"]
    pub fn enter(self) -> CallContextScope {
        let previous = CURRENT.with(|slot| slot.replace(Some(self)));

        #[cfg(feature = "tracing")]
        let _span = tracing::info_span!(
            "rpc",
            service = self.service_id,
            version = self.service_version,
            method = self.method,
            corr = ?span_fields::Correlation(&self.correlation_id),
            trace = ?span_fields::TraceId(&self.trace.trace_id),
            parent = self.trace.parent_span_id,
            span = self.span_id,
            sampled = self.trace.is_sampled(),
        )
        .entered();

        CallContextScope {
            previous,
            _not_send: PhantomData,
            #[cfg(feature = "tracing")]
            _span,
        }
    }
}

/// Lazy renderers for the span fields of a call.
///
/// `?value` records a `Debug` field, so the formatting below runs **only if a
/// subscriber records it** — a provider whose subscriber filters the field out
/// pays nothing, and no `String` is built on the hot path.
#[cfg(feature = "tracing")]
mod span_fields {
    use std::fmt;

    use crate::types::header::{fmt_correlation_id, CORRELATION_ID_LEN};

    /// Correlation id, rendered as the UUID-like string an observer logs.
    pub(super) struct Correlation<'a>(pub(super) &'a [u8; CORRELATION_ID_LEN]);

    impl fmt::Debug for Correlation<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&fmt_correlation_id(self.0))
        }
    }

    /// Trace id, rendered as hexadecimal.
    pub(super) struct TraceId<'a>(pub(super) &'a [u8; 16]);

    impl fmt::Debug for TraceId<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&super::hex_trace_id(self.0))
        }
    }
}

/// Scope that installs a [`CallContext`] for the current thread.
///
/// Created by [`CallContext::enter`]. Not `Send` on purpose: the scope is bound
/// to the thread whose slot it owns, and dropping it elsewhere would clobber
/// another thread's slot.
pub struct CallContextScope {
    previous: Option<CallContext>,
    _not_send: PhantomData<*const ()>,
    /// Entered span of the call, exited on drop — held for that side effect only.
    /// Also `!Send`, which reinforces the reason `_not_send` exists.
    #[cfg(feature = "tracing")]
    _span: tracing::span::EnteredSpan,
}

impl Drop for CallContextScope {
    fn drop(&mut self) {
        CURRENT.with(|slot| slot.set(self.previous));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::header::service_id_of;

    #[test]
    fn a_context_mirrors_the_request_header() {
        let id = service_id_of("Db");
        let header = RpcHeader::request("get_user_age", id, 3);

        let ctx = CallContext::new(&header, "get_user_age");

        assert_eq!(ctx.correlation_id(), &header.correlation_id);
        assert_eq!(ctx.service_id(), id);
        assert_eq!(ctx.service_version(), 3);
        assert_eq!(ctx.method(), "get_user_age");
        assert_eq!(ctx.received_at_ns(), header.timestamp_ns);
        // 8 + 4 + 4 + 4 + 12 digits and 4 dashes.
        assert_eq!(ctx.correlation().len(), 36);
    }

    #[test]
    fn no_trace_is_present_until_the_wire_carries_one() {
        let header = RpcHeader::request("ping", 7, 1);
        let ctx = CallContext::new(&header, "ping");

        assert!(!ctx.trace().is_present());
        assert!(!ctx.trace().is_sampled());
    }

    #[test]
    fn a_child_call_continues_the_trace_and_parents_on_our_span() {
        let header = RpcHeader::request("outer", 1, 1).with_trace(TraceContext::new_root());
        let ctx = CallContext::new(&header, "outer");
        assert!(ctx.trace().is_present());

        let out = ctx.child_trace();
        assert_eq!(out.trace_id, ctx.trace().trace_id, "same trace");
        assert_eq!(out.parent_span_id, ctx.span_id(), "parented on our span");

        // A second hop keeps the trace and chains a new parent.
        let header = RpcHeader::request("mid", 2, 1).with_trace(out);
        let ctx = CallContext::new(&header, "mid");
        let out = ctx.child_trace();
        assert_eq!(out.trace_id, header.trace().trace_id);
        assert_eq!(out.parent_span_id, ctx.span_id());
        assert_ne!(
            out.parent_span_id, 0,
            "a nested hop is not a root: it has a parent"
        );
    }

    #[test]
    fn a_call_outside_any_trace_starts_one() {
        let header = RpcHeader::request("root", 1, 1);
        let ctx = CallContext::new(&header, "root");
        assert!(!ctx.trace().is_present());

        let out = ctx.child_trace();
        assert!(out.is_present(), "a fresh trace is started");
        assert_eq!(out.parent_span_id, 0, "a root has no parent");
        assert_ne!(
            out.trace_id,
            ctx.trace().trace_id,
            "the fresh id replaces the absent one rather than reusing it"
        );
    }

    #[test]
    fn the_context_is_ambient_only_within_its_scope() {
        assert!(CallContext::current().is_none(), "nothing before entering");

        let outer = CallContext::new(&RpcHeader::request("outer", 1, 1), "outer");
        let _outer_scope = outer.enter();
        assert_eq!(CallContext::current().unwrap().method(), "outer");

        {
            let inner = CallContext::new(&RpcHeader::request("inner", 2, 1), "inner");
            let _inner_scope = inner.enter();
            assert_eq!(CallContext::current().unwrap().method(), "inner");
        }

        // The inner scope restored the outer one: it did not clear the slot.
        assert_eq!(CallContext::current().unwrap().method(), "outer");

        drop(_outer_scope);
        assert!(
            CallContext::current().is_none(),
            "the slot is restored after"
        );
    }

    /// Records what `tracing` hands it, so the test can assert on it.
    ///
    /// A hand-written `Subscriber` on purpose: the alternative is pulling
    /// `tracing-subscriber` in as a dev-dependency for three assertions. Note it
    /// does **not** implement `current_span()`, which is optional and defaults to
    /// "none" — so this test asserts on span creation and entry, not on
    /// `Span::current()`.
    #[cfg(feature = "tracing")]
    #[derive(Default)]
    struct SpanRecorder {
        names: std::sync::Mutex<Vec<String>>,
        entered: std::sync::atomic::AtomicUsize,
        exited: std::sync::atomic::AtomicUsize,
    }

    #[cfg(feature = "tracing")]
    impl tracing::Subscriber for SpanRecorder {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::Id {
            let mut names = self.names.lock().unwrap_or_else(|e| e.into_inner());
            names.push(span.metadata().name().to_owned());
            tracing::Id::from_u64(names.len() as u64)
        }

        fn record(&self, _span: &tracing::Id, _values: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _span: &tracing::Id, _follows: &tracing::Id) {}
        fn event(&self, _event: &tracing::Event<'_>) {}

        fn enter(&self, _span: &tracing::Id) {
            self.entered
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        fn exit(&self, _span: &tracing::Id) {
            self.exited
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// With the feature on, entering a call context creates and enters the span
    /// of the call — which is what attaches the implementation's events to it.
    #[cfg(feature = "tracing")]
    #[test]
    fn the_call_scope_emits_and_enters_a_span() {
        use std::sync::atomic::Ordering;
        use std::sync::Arc;

        let recorder = Arc::new(SpanRecorder::default());
        let dispatch = tracing::Dispatch::new(Arc::clone(&recorder));

        tracing::dispatcher::with_default(&dispatch, || {
            let header = RpcHeader::request("ping", 7, 1);
            let _scope = CallContext::new(&header, "ping").enter();
            assert_eq!(
                recorder.entered.load(Ordering::Relaxed),
                1,
                "the span is entered while the call runs"
            );
        });

        let names = recorder.names.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(*names, ["rpc"], "one span per call, named after the RPC");
        assert_eq!(recorder.entered.load(Ordering::Relaxed), 1, "entered once");
        assert_eq!(
            recorder.exited.load(Ordering::Relaxed),
            1,
            "exited when the scope drops"
        );
    }
}
