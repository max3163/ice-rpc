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
//! one invocation of one handler, installed around **every poll** of its task
//! (see [`call_scoped`]): [`CallContext::current`] returns `None` outside a
//! handler, and inside a task the implementation spawned itself.
//!
//! The tracing fields (see [`TraceContext`]) are zero until the wire carries
//! them; callers must read [`TraceContext::is_present`] rather than assume a
//! trace exists.
//!
//! [`CallContext::cancellation`] is the **second ambient value** of a handler. It
//! does not come from the header: the transport creates one token per call,
//! installs it around every poll of the task and fires it when a remote Cancel
//! names that call. The implementation reads it to stay interruptible — the
//! provider drops the handler's future as soon as the token is cancelled.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::header::{
    fmt_correlation_id, next_correlation_id, RpcHeader, ServiceRef, CORRELATION_ID_LEN,
};
use crate::CancellationToken;

thread_local! {
    /// Context of the call being **polled** on this thread, if any.
    ///
    /// One slot per thread, installed around every poll of a handler task by
    /// [`call_scoped`] — not for the whole call. A handler runs as a task, and
    /// several calls are polled interleaved on the same thread, so a slot held
    /// across an await would leak into whichever call is polled next.
    static CURRENT: Cell<Option<CallContext>> = const { Cell::new(None) };

    /// Cancellation token of the call being **polled** on this thread, if any.
    ///
    /// Same discipline as [`CURRENT`], for the same reason: installed around one
    /// poll, restored — not cleared — on the way out. A `RefCell` rather than a
    /// `Cell`, because a [`CancellationToken`] is not `Copy` and reading the slot
    /// must not consume it.
    static CURRENT_CANCEL: RefCell<Option<CancellationToken>> = const { RefCell::new(None) };
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

/// Builds the `rpc` span of a call, optionally carrying a `kind` field.
///
/// One macro rather than two copies of the field list: the fields are the
/// contract with the subscriber, and two lists would drift apart. `kind` is
/// emitted only for the calls that pass it — a transported call has no such
/// field, so an observer's queries keep meaning what they meant.
#[cfg(feature = "tracing")]
macro_rules! rpc_span {
    ($ctx:expr $(, kind = $kind:expr)?) => {
        tracing::info_span!(
            "rpc",
            $(kind = $kind,)?
            service = $ctx.service_name,
            version = $ctx.service_version,
            method = $ctx.method,
            corr = ?span_fields::Correlation(&$ctx.correlation_id),
            trace = ?span_fields::TraceId(&$ctx.trace.trace_id),
            parent = $ctx.trace.parent_span_id,
            span = $ctx.span_id,
            sampled = $ctx.trace.is_sampled(),
        )
    };
}

/// Read-only view of the call being served, built from its request header.
///
/// `Copy`: passing it as the first parameter of every RPC method costs nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallContext {
    correlation_id: [u8; CORRELATION_ID_LEN],
    /// Logical name of the served service.
    ///
    /// Not on the wire: the header carries only its 4-byte hash
    /// ([`service_id_of`](super::header::service_id_of)), which cannot be inverted.
    /// The name comes from the generated handler, which knows it as a constant, so
    /// a span can show `service = "OrderService"` instead of a number.
    service_name: &'static str,
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
    /// `service_name` and `method` are compile-time knowledge of the generated
    /// handler, which is why they are `&'static str` rather than slices borrowed
    /// from the header: the wire carries the service **id** (the 4-byte hash of the
    /// name) and the routed method name, but never the logical name itself.
    ///
    /// The identity therefore comes from two places, and they must agree: the id
    /// and the version from the header, the name from the code that declares the
    /// service. A mismatch would be a routing bug, not a wire one.
    #[inline]
    pub fn new(header: &RpcHeader, service_name: &'static str, method: &'static str) -> Self {
        Self {
            correlation_id: header.correlation_id,
            service_name,
            service_id: header.service_id,
            service_version: header.service_version,
            method,
            received_at_ns: header.timestamp_ns,
            trace: header.trace(),
            span_id: span_id_of(&header.correlation_id),
        }
    }

    /// Builds the context of a **direct** (in-process) call.
    ///
    /// A proxy in `Provider` mode calls the local implementation without touching
    /// the wire: there is no header to mirror, so the correlation id is minted
    /// here, the service identity is the callee's own ([`ServiceRef`] plus its
    /// declared name), and the trace continues the ambient one — parenting on the
    /// span of the call being served — or starts a root when there is none.
    ///
    /// `received_at_ns` stays `0` on purpose: nothing was emitted on the wire, so
    /// there is no emission instant to compare with a local clock — and reading
    /// that clock would be the most expensive part of the whole envelope
    /// (measured: 31 ns out of the 49 ns a context costs). `0` is the documented
    /// value of "not transported"; an `Option` would cost `CallContext` its
    /// `Copy`, which the per-poll installation relies on.
    #[inline]
    pub fn local(service: ServiceRef, service_name: &'static str, method: &'static str) -> Self {
        let correlation_id = next_correlation_id();
        let trace = match Self::current() {
            Some(parent) => parent.child_trace(),
            None => TraceContext::new_root(),
        };
        Self {
            correlation_id,
            service_name,
            service_id: service.id,
            service_version: service.version,
            method,
            received_at_ns: 0,
            trace,
            span_id: span_id_of(&correlation_id),
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

    /// Logical name of the service being served, as `#[service]` declared it.
    ///
    /// Carried by the generated code, never by the wire: the header keeps only the
    /// 4-byte [`service_id_of`](super::header::service_id_of) hash, which no
    /// observer can invert. This is the name a span and a business log line show —
    /// the id stays available through [`service_id`](Self::service_id) for the
    /// observers that key on it.
    #[inline]
    pub fn service_name(&self) -> &'static str {
        self.service_name
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

    /// Cancellation token of the call being served on this thread, if any.
    ///
    /// Installed by the transport around **every poll** of a handler task, exactly
    /// like the context itself, so an implementation reads it the same way. It is
    /// the token a remote Cancel fires: dropping the future that awaits it,
    /// returning early, or racing a `cancelled()` future is what makes the
    /// cancellation effective — cooperative, and therefore the implementation's
    /// decision.
    ///
    /// `None` outside a provider handler, for the same reasons as
    /// [`CallContext::current`].
    #[inline]
    pub fn cancellation() -> Option<CancellationToken> {
        CURRENT_CANCEL.with(|slot| slot.borrow().clone())
    }

    /// Installs this context as the thread-local ambient one, and nothing else.
    ///
    /// Dropping the returned scope restores the **previous** value rather than
    /// clearing the slot, which is what makes nested calls — a provider calling
    /// another service in-process — correct.
    ///
    /// This is the half [`call_scoped`] uses, once per poll: it is a plain
    /// thread-local swap, so re-installing it thousands of times per call costs
    /// nothing and creates no span.
    #[must_use = "the context is only ambient while the returned scope is alive"]
    pub fn enter_ambient(self) -> AmbientScope {
        AmbientScope {
            previous: CURRENT.with(|slot| slot.replace(Some(self))),
            _not_send: PhantomData,
        }
    }

    /// The `tracing` span of this call, **created once per call**.
    ///
    /// A task cannot hold an `EnteredSpan` across its awaits: entering is
    /// thread-bound, and the tasks polled between two of its awaits would be
    /// recorded inside it. The span is therefore stored in the task and entered
    /// around every poll — see [`call_scoped`].
    #[cfg(feature = "tracing")]
    pub fn span(&self) -> tracing::Span {
        rpc_span!(self)
    }

    /// The span of a **direct** (in-process) call, marked as such.
    ///
    /// Same fields as [`CallContext::span`], plus `kind = "local"`: an observer
    /// that counts hops must not count an in-process delegation as a network
    /// round trip. The field costs nothing of its own — `tracing` records a field
    /// only if the subscriber asks for it.
    #[cfg(feature = "tracing")]
    fn span_local(&self) -> tracing::Span {
        rpc_span!(self, kind = "local")
    }

    /// Installs this context as the ambient one for the current thread, tracing
    /// span included, for the duration of one **synchronous** scope.
    ///
    /// Reserved for the callers that are not polled by an executor — the tests,
    /// and any synchronous integration. A provider handler runs as a task,
    /// native or Node.js, and therefore uses [`call_scoped`].
    ///
    /// Dropping the returned scope restores the **previous** value rather than
    /// clearing the slot, which is what makes nested calls — a provider calling
    /// another service in-process — correct.
    #[must_use = "the context is only ambient while the returned scope is alive"]
    pub fn enter(self) -> CallContextScope {
        let _ambient = self.enter_ambient();

        #[cfg(feature = "tracing")]
        let _span = self.span().entered();

        CallContextScope {
            _ambient,
            #[cfg(feature = "tracing")]
            _span,
        }
    }
}

/// A handler's future, erased: what [`ServiceDispatcher`](crate::transport)
/// spawns per request.
pub type BoxResponseFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Wraps a handler's future so the call context is installed around **each poll**.
///
/// This is the replacement for installing the context once, around the whole
/// handler: a handler now runs as a task, several calls are polled interleaved on
/// the same thread, and a thread-local held across an await would attribute the
/// events of one call to another.
///
/// The scope never crosses an await point — it is created and dropped inside
/// `poll` — so it never becomes part of the future's state, which is what keeps
/// the task `Send`.
pub fn call_scoped<F>(ctx: CallContext, future: F) -> BoxResponseFuture
where
    F: Future<Output = ()> + Send + 'static,
{
    #[cfg(feature = "tracing")]
    let span = ctx.span();

    Box::pin(CallScope {
        ctx,
        inner: Box::pin(future),
        #[cfg(feature = "tracing")]
        span,
    })
}

/// Serves a **direct** (in-process) call: installs the callee's context and its
/// span around each poll, or forwards the future untouched when nothing is
/// collected.
///
/// The generated proxy calls the local implementation through this helper in its
/// `Provider` mode. Unlike [`call_scoped`], the whole envelope is conditioned by
/// the `tracing` feature, and that is deliberate:
///
/// - **off** (the default): `future.await` and nothing else — no correlation id,
///   no clock read, no thread-local, no allocation. A deployment that collects
///   nothing keeps paying exactly what it paid before;
/// - **on**: the callee gets its own [`CallContext`], so [`CallContext::current`]
///   tells it *its* service and method instead of the caller's, and the span —
///   parented on the caller's, marked `kind = "local"` — makes the delegation
///   visible in the trace.
///
/// A transported call installs its context unconditionally because the header
/// carries a protocol contract the monitor, the logs and the outgoing calls rely
/// on. A direct call has no wire: its context only serves the trace, so the
/// condition belongs here. It cannot live in the generated code either: `tracing`
/// is a feature of this crate, and a `#[cfg]` emitted by `#[service]` would be
/// evaluated in the consumer's crate, which does not carry it.
///
/// The context is entered around each poll, never held across an `await`: that is
/// what keeps several tasks polled on one thread from labelling each other, and
/// what keeps the returned future `Send`.
///
/// `service_name` is the callee's logical name, as `#[service]` declared it: it is
/// what the span shows, since no header carries it and the id it *does* carry is a
/// hash no observer can invert.
pub async fn local_call_scoped<F>(
    service: ServiceRef,
    service_name: &'static str,
    method: &'static str,
    future: F,
) -> F::Output
where
    F: Future,
{
    #[cfg(feature = "tracing")]
    {
        let ctx = CallContext::local(service, service_name, method);
        let span = ctx.span_local();
        let mut future = std::pin::pin!(future);
        return std::future::poll_fn(move |cx| {
            let _ambient = ctx.enter_ambient();
            let _span = span.enter();
            future.as_mut().poll(cx)
        })
        .await;
    }

    // No collector: there is nothing to analyse, so nothing is built.
    #[cfg(not(feature = "tracing"))]
    {
        let _ = (service, service_name, method);
        future.await
    }
}

/// Future wrapping a handler, installing its context around each poll.
///
/// Every field is `Unpin`, so no pin projection is needed.
struct CallScope {
    ctx: CallContext,
    inner: BoxResponseFuture,
    #[cfg(feature = "tracing")]
    span: tracing::Span,
}

impl Future for CallScope {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        // Installed for this poll only: the next task polled on this thread must
        // not inherit it.
        let _ambient = this.ctx.enter_ambient();
        #[cfg(feature = "tracing")]
        let _span = this.span.enter();
        this.inner.as_mut().poll(cx)
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

/// Installs `token` as the cancellation token of the call being polled on this
/// thread, returning the scope that restores the previous one.
///
/// Called by the transport around every poll of a handler task, next to the
/// context installation the generated handler performs: one is the identity of
/// the call, the other is the ability to abandon it. Restoring the **previous**
/// token — rather than clearing the slot — is what makes nested calls correct.
#[must_use = "the token is only ambient while the returned scope is alive"]
pub(crate) fn install_call_cancellation(token: CancellationToken) -> AmbientCancelScope {
    AmbientCancelScope {
        previous: CURRENT_CANCEL.with(|slot| slot.replace(Some(token))),
    }
}

/// Scope that installs a [`CancellationToken`] as the ambient one of the current
/// thread.
///
/// Created by [`install_call_cancellation`]. Not `Send`, like [`AmbientScope`]:
/// it is bound to the thread whose slot it owns.
pub(crate) struct AmbientCancelScope {
    previous: Option<CancellationToken>,
}

impl Drop for AmbientCancelScope {
    fn drop(&mut self) {
        CURRENT_CANCEL.with(|slot| *slot.borrow_mut() = self.previous.take());
    }
}

/// Scope that installs a [`CallContext`] as the ambient value of the current
/// thread, tracing excluded.
///
/// Created by [`CallContext::enter_ambient`], and by [`call_scoped`] around every
/// poll. Not `Send` on purpose: the scope is bound to the thread whose slot it
/// owns, and dropping it elsewhere would clobber another thread's slot.
pub struct AmbientScope {
    previous: Option<CallContext>,
    _not_send: PhantomData<*const ()>,
}

impl Drop for AmbientScope {
    fn drop(&mut self) {
        CURRENT.with(|slot| slot.set(self.previous));
    }
}

/// Scope that installs a [`CallContext`] for the current thread, tracing span
/// included.
///
/// Created by [`CallContext::enter`], for a synchronous scope. Not `Send` on
/// purpose: the scope is bound to the thread whose slot it owns, and dropping it
/// elsewhere would clobber another thread's slot.
pub struct CallContextScope {
    /// Restores the previous ambient value on drop, and does nothing else.
    _ambient: AmbientScope,
    /// Entered span of the call, exited on drop — held for that side effect only.
    /// Also `!Send`, which reinforces the reason `AmbientScope` is `!Send`.
    #[cfg(feature = "tracing")]
    _span: tracing::span::EnteredSpan,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::header::service_id_of;

    #[test]
    fn a_context_mirrors_the_request_header() {
        let id = service_id_of("Db");
        let header = RpcHeader::request("get_user_age", id, 3);

        let ctx = CallContext::new(&header, "Db", "get_user_age");

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
        let ctx = CallContext::new(&header, "Ping", "ping");

        assert!(!ctx.trace().is_present());
        assert!(!ctx.trace().is_sampled());
    }

    #[test]
    fn a_child_call_continues_the_trace_and_parents_on_our_span() {
        let header = RpcHeader::request("outer", 1, 1).with_trace(TraceContext::new_root());
        let ctx = CallContext::new(&header, "Outer", "outer");
        assert!(ctx.trace().is_present());

        let out = ctx.child_trace();
        assert_eq!(out.trace_id, ctx.trace().trace_id, "same trace");
        assert_eq!(out.parent_span_id, ctx.span_id(), "parented on our span");

        // A second hop keeps the trace and chains a new parent.
        let header = RpcHeader::request("mid", 2, 1).with_trace(out);
        let ctx = CallContext::new(&header, "Mid", "mid");
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
        let ctx = CallContext::new(&header, "Root", "root");
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

    /// Same contract as the context slot, for the token: ambient for one scope,
    /// and the previous value is restored rather than cleared.
    #[test]
    fn the_cancellation_token_is_ambient_only_within_its_scope() {
        assert!(
            CallContext::cancellation().is_none(),
            "nothing before entering"
        );

        let outer = CancellationToken::new();
        let outer_scope = install_call_cancellation(outer.clone());
        assert!(
            !CallContext::cancellation()
                .expect("the outer token is ambient")
                .is_cancelled(),
            "a fresh token is not cancelled"
        );

        {
            let inner = CancellationToken::new();
            let _inner_scope = install_call_cancellation(inner.clone());
            // Cancelling the inner token is what proves the slot holds it, and
            // `CancellationToken` needs no `PartialEq` for that.
            inner.cancel();
            assert!(
                CallContext::cancellation().unwrap().is_cancelled(),
                "the inner token is the ambient one"
            );
        }

        // The inner scope restored the outer token: it did not clear the slot.
        assert!(
            !CallContext::cancellation().unwrap().is_cancelled(),
            "the outer token is back"
        );

        drop(outer_scope);
        assert!(
            CallContext::cancellation().is_none(),
            "the slot is restored after"
        );
    }

    /// The generated handler installs the context; the transport installs the
    /// token. A task wrapped by `call_scoped` alone therefore has a context and no
    /// token — the two slots are independent.
    #[test]
    fn a_context_without_a_transport_wrapper_has_no_token() {
        let header = RpcHeader::request("ping", 7, 1);
        let task = call_scoped(CallContext::new(&header, "Ping", "ping"), async {
            assert!(CallContext::current().is_some(), "the context is installed");
            assert!(
                CallContext::cancellation().is_none(),
                "no token was installed by the transport"
            );
        });

        futures_lite::future::block_on(task);
    }

    #[test]
    fn the_context_is_ambient_only_within_its_scope() {
        assert!(CallContext::current().is_none(), "nothing before entering");

        let outer = CallContext::new(&RpcHeader::request("outer", 1, 1), "Outer", "outer");
        let _outer_scope = outer.enter();
        assert_eq!(CallContext::current().unwrap().method(), "outer");

        {
            let inner = CallContext::new(&RpcHeader::request("inner", 2, 1), "Inner", "inner");
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
            let _scope = CallContext::new(&header, "Ping", "ping").enter();
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

    /// With the feature on, the span of a call is created **once** and entered
    /// around **every poll** of the task.
    ///
    /// A task is polled several times, on a thread that may change between two
    /// polls, and a thread-bound `EnteredSpan` cannot be held across an await — so
    /// the span is stored in the task and entered per poll. Creating it per poll
    /// instead would mint a new span on every wake-up, which would break the very
    /// thing a span is for: joining the events of one call.
    #[cfg(feature = "tracing")]
    #[test]
    fn the_call_scope_enters_one_span_per_poll() {
        use std::sync::atomic::Ordering;
        use std::sync::Arc;

        /// Yields once: the task is polled twice, like one that awaits.
        async fn yields_once() {
            let mut yielded = false;
            futures_lite::future::poll_fn(|_cx| {
                if yielded {
                    Poll::Ready(())
                } else {
                    yielded = true;
                    Poll::Pending
                }
            })
            .await;
        }

        let recorder = Arc::new(SpanRecorder::default());
        let dispatch = tracing::Dispatch::new(Arc::clone(&recorder));

        // Built inside the dispatcher: the span of a call is created when the task
        // is built, not when it is polled.
        tracing::dispatcher::with_default(&dispatch, || {
            let header = RpcHeader::request("ping", 7, 1);
            let mut task = call_scoped(CallContext::new(&header, "Ping", "ping"), yields_once());

            let mut cx = Context::from_waker(std::task::Waker::noop());
            assert!(task.as_mut().poll(&mut cx).is_pending());
            assert!(task.as_mut().poll(&mut cx).is_ready());
        });

        let names = recorder
            .names
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        assert_eq!(
            names,
            ["rpc"],
            "one span for the whole call, not one per poll"
        );
        assert_eq!(
            recorder.entered.load(Ordering::Relaxed),
            2,
            "entered around each of the two polls"
        );
        assert_eq!(
            recorder.exited.load(Ordering::Relaxed),
            2,
            "and left after each"
        );
    }

    /// The context must follow the **task** being polled, not the thread.
    ///
    /// Two calls of one channel are polled interleaved on the same thread by the
    /// same executor. A scope installed for the whole call would still be the
    /// first one's when the second is polled — which is exactly what this wrapper
    /// prevents.
    #[test]
    fn the_context_follows_the_task_being_polled() {
        use std::sync::{Arc, Mutex};

        /// Yields once, so a second task can be polled in between.
        async fn yield_once() {
            let mut yielded = false;
            futures_lite::future::poll_fn(|_cx| {
                if yielded {
                    Poll::Ready(())
                } else {
                    yielded = true;
                    Poll::Pending
                }
            })
            .await;
        }

        type Seen = Vec<(&'static str, Option<&'static str>)>;
        let seen: Arc<Mutex<Seen>> = Arc::default();

        let task = |method: &'static str, seen: Arc<Mutex<Seen>>| {
            let ctx = CallContext::new(&RpcHeader::request(method, 1, 1), "Svc", method);
            call_scoped(ctx, async move {
                let observed = CallContext::current().map(|ctx| ctx.method());
                seen.lock().unwrap().push((method, observed));
                yield_once().await;
                let observed = CallContext::current().map(|ctx| ctx.method());
                seen.lock().unwrap().push((method, observed));
            })
        };

        let mut first = std::pin::pin!(task("first", Arc::clone(&seen)));
        let mut second = std::pin::pin!(task("second", Arc::clone(&seen)));

        let mut cx = Context::from_waker(std::task::Waker::noop());

        assert!(first.as_mut().poll(&mut cx).is_pending());
        assert!(second.as_mut().poll(&mut cx).is_pending());
        assert!(first.as_mut().poll(&mut cx).is_ready());
        assert!(second.as_mut().poll(&mut cx).is_ready());

        assert_eq!(
            &*seen.lock().unwrap(),
            &[
                ("first", Some("first")),
                ("second", Some("second")),
                ("first", Some("first")),
                ("second", Some("second")),
            ],
            "each poll must observe the context of its own call"
        );
    }

    /// A direct call is a hop of its own: it names the **callee**, and it reads no
    /// clock.
    #[test]
    fn a_local_context_names_the_callee_and_carries_no_clock() {
        let ctx = CallContext::local(ServiceRef::new(7, 3), "Leaf", "reserve");

        assert_eq!(
            ctx.service_id(),
            7,
            "the callee's identity, not the caller's"
        );
        assert_eq!(ctx.service_version(), 3);
        assert_eq!(ctx.method(), "reserve");
        assert_eq!(
            ctx.received_at_ns(),
            0,
            "nothing was emitted on the wire, so there is no emission instant"
        );
        assert_eq!(
            ctx.correlation().len(),
            36,
            "a correlation id is still minted: it is what joins a log line to the call"
        );
        assert!(
            ctx.trace().is_present(),
            "outside any ambient call, a direct call starts a trace"
        );
        assert_eq!(ctx.trace().parent_span_id, 0, "a root has no parent");
    }

    /// The delegation stays in the caller's trace and parents on its span.
    #[test]
    fn a_local_context_continues_the_ambient_trace_and_parents_on_it() {
        let header = RpcHeader::request("place_order", 1, 1).with_trace(TraceContext::new_root());
        let parent = CallContext::new(&header, "OrderService", "place_order");

        let child = {
            let _scope = parent.enter_ambient();
            assert_eq!(
                CallContext::current(),
                Some(parent),
                "inside the scope, the caller is the ambient context"
            );
            CallContext::local(ServiceRef::new(7, 1), "Leaf", "reserve")
        };

        assert_eq!(
            child.trace().trace_id,
            parent.trace().trace_id,
            "same trace"
        );
        assert_eq!(
            child.trace().parent_span_id,
            parent.span_id(),
            "the delegation parents on the caller's span"
        );
        assert_eq!(
            CallContext::current(),
            None,
            "leaving the scope restores the previous value, which was none"
        );
    }

    /// The whole point of the envelope: what is not collected is not built.
    #[test]
    fn a_direct_call_is_a_plain_await_unless_the_deployment_collects() {
        let observed = pollster::block_on(local_call_scoped(
            ServiceRef::new(7, 3),
            "Leaf",
            "reserve",
            async { CallContext::current() },
        ));

        #[cfg(feature = "tracing")]
        {
            let ctx = observed.expect("with a collector, the callee sees its own context");
            assert_eq!(ctx.service_id(), 7);
            assert_eq!(
                ctx.service_name(),
                "Leaf",
                "the declared name travels with the call, for the span and the logs"
            );
            assert_eq!(ctx.method(), "reserve");
        }

        #[cfg(not(feature = "tracing"))]
        {
            assert!(
                observed.is_none(),
                "with no collector, no context is built and none is installed"
            );
        }
    }
}
