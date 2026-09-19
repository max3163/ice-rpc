//! Zero-copy RPC header carried in iceoryx2's `user_header`.
//!
//! The header is `ZeroCopySend` and lives *next to* the payload in the shared
//! memory sample, not inside it: reading the correlation id, the method name or
//! the protocol version never deserializes the payload.
//!
//! The layout is `#[repr(C)]` and part of the wire contract shared by every
//! process on the machine: it must not drift silently.

use std::time::{SystemTime, UNIX_EPOCH};

use iceoryx2::prelude::ZeroCopySend;
use iceoryx2_bb_container::string::StaticString;

use super::context::TraceContext;
use crate::labels::impl_labels;
use crate::types::consts::{METHOD_NAME_LEN, PROTOCOL_VERSION};

/// Correlation id prefixing every request/response pair.
pub const CORRELATION_ID_LEN: usize = 16;

/// Returns the current wall-clock time as nanoseconds since the Unix epoch.
///
/// Every process on the same host shares this clock, so two timestamps taken by
/// different processes are comparable. This is what lets an out-of-band observer
/// compute an exact latency from the `timestamp_ns` fields of a request and of
/// its response.
///
/// A clock adjustment (NTP step or slew) can make a difference between two
/// readings negative: callers must subtract with saturation and count the
/// occurrences separately.
#[inline]
pub fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Kind of the sample carried by a [`RpcHeader`].
///
/// Stored as a `u8` in the header so the zero-copy layout stays plain data; the
/// values are part of the wire contract.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EventKind {
    /// Request emitted by a client (non-terminal).
    #[default]
    Request = 0,
    /// Intermediate event carrying a business value (non-terminal).
    Next = 1,
    /// Normal end of the stream (terminal).
    Complete = 2,
    /// Terminal business error, whose payload is the service's `WireEvent<T, E>`.
    Error = 3,
    /// Terminal **transport-level** technical error, whose payload is a bare
    /// [`RpcError`](crate::types::RpcError).
    ///
    /// Distinct from [`EventKind::Error`] on purpose: a rejection (unknown
    /// method, unknown service, protocol or interface version mismatch) says
    /// nothing about the service types, and the provider cannot name them at all
    /// for a method it does not have. Framing it as a bare `RpcError` is what
    /// lets any request be answered instead of silently timing out.
    RpcError = 4,
    /// Client to provider signal asking the provider to abandon the call named by
    /// the correlation id it carries (non-terminal).
    ///
    /// Published on the **request** channel, because that is the direction the
    /// caller already publishes on: no extra service, no extra port, and the
    /// correlation id travels in the zero-copy header like every other sample.
    ///
    /// It is not an answer: a provider that does not know the id ignores it, and
    /// an older build decodes it as [`EventKind::Error`] through the fail-closed
    /// [`EventKind::from_u8`], so mixed builds degrade instead of breaking.
    /// Being non-terminal matters to the out-of-band observer too: a Cancel ends a
    /// call's tail, it does not close the call as answered.
    Cancel = 5,
}

impl EventKind {
    /// Returns `true` if this kind terminates the stream.
    #[inline]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            EventKind::Complete | EventKind::Error | EventKind::RpcError
        )
    }

    /// Returns the wire value of this kind.
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Decodes a wire value; unknown values map to [`EventKind::Error`].
    ///
    /// Deliberately fail-closed: an unknown value must never read as a
    /// successful completion. `Complete` is terminal, so a corrupt kind decoded
    /// that way would close a call as if it had succeeded — inflating the
    /// completion counters and hiding the very corruption it should report.
    #[inline]
    pub const fn from_u8(value: u8) -> Self {
        match value {
            0 => EventKind::Request,
            1 => EventKind::Next,
            2 => EventKind::Complete,
            3 => EventKind::Error,
            4 => EventKind::RpcError,
            5 => EventKind::Cancel,
            _ => EventKind::Error,
        }
    }
}

// The labels an observer reads back from a sample (`kind="complete"`), declared
// next to the kind they describe.
impl_labels!(EventKind {
    EventKind::Request => "request",
    EventKind::Next => "next",
    EventKind::Complete => "complete",
    EventKind::Error => "error",
    EventKind::RpcError => "rpc-error",
    EventKind::Cancel => "cancel",
});

/// Zero-copy RPC header attached to every request and response sample.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, ZeroCopySend)]
pub struct RpcHeader {
    /// Request↔response correlation id (16 bytes).
    pub correlation_id: [u8; CORRELATION_ID_LEN],
    /// Emission timestamp, in nanoseconds since the Unix epoch.
    ///
    /// Stamped by the emitter (consumer for a request, provider for a response),
    /// so an observer computes an exact latency with
    /// `response.timestamp_ns - request.timestamp_ns`.
    ///
    /// Declared early so the two 8-byte monitoring fields pack tightly: the
    /// struct must stay within the 128-byte wire budget.
    pub timestamp_ns: u64,
    /// Per-publisher, per-channel monotonic sample counter.
    ///
    /// A publisher is single-threaded for a given channel, so a gap in the
    /// observed sequence proves that samples were lost. Set by the emitter at
    /// publication time.
    pub seq: u64,
    /// W3C trace id of the call, shared by every hop of its call tree.
    ///
    /// Zero when the caller propagated no trace — that is the "absent" value,
    /// not a valid id — which is why [`TraceContext::is_present`] exists.
    pub trace_id: [u8; 16],
    /// Span of the caller, onto which the receiver parents its own span.
    pub parent_span_id: u64,
    /// Identifier of the target service inside the channel it is published on.
    ///
    /// Several services can share one channel (their *group*); this id —
    /// [`service_id_of`] of the service name — selects the provider dispatcher.
    ///
    /// The **emitter** identity is deliberately absent: the native iceoryx2
    /// sample header already carries the source `node_id` (hence the PID) and the
    /// `publisher_id`, so duplicating it here would only risk a divergence. An
    /// observer reads it from [`Sample::header()`], and the `publisher_id` is what
    /// scopes [`seq`](Self::seq).
    ///
    /// [`Sample::header()`]: iceoryx2::sample::Sample::header
    pub service_id: u32,
    /// Method invoked by the request (left empty on a response).
    pub method_name: StaticString<METHOD_NAME_LEN>,
    /// Kind of the sample (request / next / complete / error), as a wire value.
    pub event_kind: u8,
    /// ice-rpc wire protocol version of the emitter.
    pub protocol_version: u16,
    /// Service interface version of the emitter.
    pub service_version: u16,
    /// W3C trace flags (bit 0: sampled), propagated with the trace.
    pub flags: u8,
}

impl RpcHeader {
    /// Creates a **request** header with a fresh correlation id.
    ///
    /// A method name longer than [`METHOD_NAME_LEN`] is truncated; the
    /// `#[service]` macro already rejects such names at compile time.
    #[inline]
    pub fn request(method: &str, service_id: u32, service_version: u16) -> Self {
        Self {
            correlation_id: next_correlation_id(),
            service_id,
            method_name: StaticString::try_from(method).unwrap_or_default(),
            event_kind: EventKind::Request.as_u8(),
            protocol_version: PROTOCOL_VERSION,
            service_version,
            // No trace until the caller attaches one with `with_trace`.
            trace_id: [0u8; 16],
            parent_span_id: 0,
            flags: 0,
            // Stamped by the caller with the per-channel sequence.
            seq: 0,
            timestamp_ns: now_ns(),
        }
    }

    /// Attaches the trace context propagated with this sample (builder style).
    ///
    /// A caller that is itself serving a traced call passes the context it
    /// received, so the chain survives the hop; a caller with no ambient trace
    /// leaves the three fields zeroed, which is the "absent" value.
    #[inline]
    pub fn with_trace(mut self, trace: TraceContext) -> Self {
        self.trace_id = trace.trace_id;
        self.parent_span_id = trace.parent_span_id;
        self.flags = trace.flags;
        self
    }

    /// Trace context carried by this sample.
    #[inline]
    pub fn trace(&self) -> TraceContext {
        TraceContext {
            trace_id: self.trace_id,
            parent_span_id: self.parent_span_id,
            flags: self.flags,
        }
    }

    /// Sets the per-publisher sample sequence (builder style).
    ///
    /// The counter is owned by the publisher (one per channel), not by this
    /// header constructor, so the emitter stamps it just before publication.
    #[inline]
    pub fn with_seq(mut self, seq: u64) -> Self {
        self.seq = seq;
        self
    }

    /// Builds the response header of `request`.
    #[inline]
    pub fn response_from(request: &RpcHeader, event_kind: EventKind, service_version: u16) -> Self {
        Self {
            correlation_id: request.correlation_id,
            service_id: request.service_id,
            method_name: StaticString::default(),
            event_kind: event_kind.as_u8(),
            protocol_version: PROTOCOL_VERSION,
            service_version,
            // A response echoes the trace of the request it answers.
            trace_id: request.trace_id,
            parent_span_id: request.parent_span_id,
            flags: request.flags,
            // Stamped by the caller with the per-channel sequence.
            seq: 0,
            timestamp_ns: now_ns(),
        }
    }

    /// Builds the header of the Cancel that abandons `request`.
    ///
    /// Mirrors [`response_from`](Self::response_from): same correlation id, same
    /// service identity and same trace, so the provider finds the call it must
    /// stop and an out-of-band observer joins the Cancel to its request. The
    /// method name is left empty on purpose — the correlation id names the call
    /// on its own, and a provider routes a Cancel without looking up a service,
    /// which is exactly what lets it be answered for a call it never dispatched.
    #[inline]
    pub fn cancel_from(request: &RpcHeader) -> Self {
        Self {
            correlation_id: request.correlation_id,
            service_id: request.service_id,
            method_name: StaticString::default(),
            event_kind: EventKind::Cancel.as_u8(),
            protocol_version: PROTOCOL_VERSION,
            service_version: request.service_version,
            // A Cancel stays in the trace of the call it abandons.
            trace_id: request.trace_id,
            parent_span_id: request.parent_span_id,
            flags: request.flags,
            // Stamped by the emitter with the per-channel sequence, like every
            // other sample: a gap in `seq` is how the observer detects a loss.
            seq: 0,
            timestamp_ns: now_ns(),
        }
    }

    /// Returns the method name carried by the header.
    #[inline]
    pub fn method(&self) -> &str {
        std::str::from_utf8(self.method_name.as_bytes_const()).unwrap_or("")
    }

    /// Returns the kind of the sample.
    #[inline]
    pub fn event_kind(&self) -> EventKind {
        EventKind::from_u8(self.event_kind)
    }
}

/// Computes the stable identifier of a service inside its channel (FNV-1a).
///
/// A `const fn`, so the generated code can use the result as a constant, and
/// every process derives the same value from the same name without discovery.
/// A collision between two co-located services is detected at channel
/// registration.
#[inline]
pub const fn service_id_of(name: &str) -> u32 {
    const OFFSET_BASIS: u32 = 0x811c_9dc5;
    const PRIME: u32 = 0x0100_0193;

    let bytes = name.as_bytes();
    let mut hash = OFFSET_BASIS;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u32;
        hash = hash.wrapping_mul(PRIME);
        i += 1;
    }
    hash
}

/// Identity of a service contract: its id inside the channel and its interface
/// version.
///
/// The two always travel together and are generated **once** per service, so an
/// id can never be paired with a foreign version. Threading this single value
/// through the transport is what makes dropping the version a compile error
/// rather than a silent `1`.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ServiceRef {
    /// Identifier of the service inside its channel ([`service_id_of`]).
    pub id: u32,
    /// Service interface version declared by `#[service(..., version = N)]`.
    pub version: u16,
}

impl ServiceRef {
    /// Builds a service identity; usable in a `const` item.
    #[inline]
    pub const fn new(id: u32, version: u16) -> Self {
        Self { id, version }
    }
}

/// Allocates a process-unique correlation id: `pid ++ counter`.
#[inline]
pub fn next_correlation_id() -> [u8; CORRELATION_ID_LEN] {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let pid = std::process::id() as u64;
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut out = [0u8; CORRELATION_ID_LEN];
    out[..8].copy_from_slice(&pid.to_be_bytes());
    out[8..].copy_from_slice(&counter.to_be_bytes());
    out
}

/// Formats a correlation id as a UUID-like hexadecimal string.
pub fn fmt_correlation_id(cid: &[u8; CORRELATION_ID_LEN]) -> String {
    let [b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15] = cid;
    format!(
        "{b0:02x}{b1:02x}{b2:02x}{b3:02x}-{b4:02x}{b5:02x}-{b6:02x}{b7:02x}-\
         {b8:02x}{b9:02x}-{b10:02x}{b11:02x}{b12:02x}{b13:02x}{b14:02x}{b15:02x}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_header_carries_the_method_and_a_fresh_id() {
        let id = service_id_of("DatabaseService");
        let a = RpcHeader::request("get_user_age", id, 2);
        let b = RpcHeader::request("get_user_age", id, 2);
        assert_eq!(a.method(), "get_user_age");
        assert_eq!(a.event_kind(), EventKind::Request);
        assert_eq!(a.service_id, id);
        assert_eq!(a.service_version, 2);
        assert_ne!(a.correlation_id, b.correlation_id);
    }

    #[test]
    fn response_header_reuses_the_request_id_and_service() {
        let id = service_id_of("GetPerson");
        let request = RpcHeader::request("ping", id, 1);
        let response = RpcHeader::response_from(&request, EventKind::Complete, 1);
        assert_eq!(response.correlation_id, request.correlation_id);
        assert_eq!(response.service_id, id);
        assert_eq!(response.event_kind(), EventKind::Complete);
        assert!(response.method().is_empty());
    }

    #[test]
    fn a_name_longer_than_the_capacity_falls_back_to_empty() {
        // `#[service]` rejects this at compile time: the fallback is a safety net.
        let long = "x".repeat(METHOD_NAME_LEN + 20);
        let header = RpcHeader::request(&long, 0, 1);
        assert!(header.method().is_empty());
    }

    #[test]
    fn service_id_is_stable_and_distinguishes_names() {
        // Offset basis of FNV-1a: pins the algorithm.
        assert_eq!(service_id_of(""), 0x811c_9dc5);

        // Usable in a constant expression, which is how the macro passes it.
        const ID: u32 = service_id_of("DatabaseService");
        assert_eq!(ID, service_id_of("DatabaseService"));
        assert_ne!(
            service_id_of("DatabaseService"),
            service_id_of("ConfigService")
        );
    }

    #[test]
    fn the_header_layout_stays_bounded_and_aligned() {
        // Part of the wire contract: the header is copied per sample and its size
        // is validated by iceoryx2 when a service is opened, so every process on
        // the machine must agree on it. Pinning the exact size makes any layout
        // drift a deliberate, reviewed change.
        assert_eq!(std::mem::align_of::<RpcHeader>(), 8);
        assert_eq!(std::mem::size_of::<RpcHeader>(), 120);

        assert_eq!(std::mem::offset_of!(RpcHeader, correlation_id), 0);
        assert_eq!(std::mem::offset_of!(RpcHeader, timestamp_ns), 16);
        assert_eq!(std::mem::offset_of!(RpcHeader, seq), 24);
        assert_eq!(std::mem::offset_of!(RpcHeader, trace_id), 32);
        assert_eq!(std::mem::offset_of!(RpcHeader, parent_span_id), 48);
        assert_eq!(std::mem::offset_of!(RpcHeader, service_id), 56);
        assert_eq!(std::mem::offset_of!(RpcHeader, method_name), 64);
        assert_eq!(std::mem::offset_of!(RpcHeader, event_kind), 112);
        assert_eq!(std::mem::offset_of!(RpcHeader, protocol_version), 114);
        assert_eq!(std::mem::offset_of!(RpcHeader, service_version), 116);
        assert_eq!(std::mem::offset_of!(RpcHeader, flags), 118);
    }

    /// The trace context survives a header round-trip, and the response echoes
    /// the request's.
    #[test]
    fn the_trace_context_travels_with_the_header() {
        let trace = TraceContext {
            trace_id: [7u8; 16],
            parent_span_id: 42,
            flags: 1,
        };

        let request = RpcHeader::request("ping", service_id_of("Ping"), 1).with_trace(trace);
        assert_eq!(request.trace(), trace);
        assert!(request.trace().is_present());
        assert!(request.trace().is_sampled());

        let response = RpcHeader::response_from(&request, EventKind::Complete, 1);
        assert_eq!(response.trace(), trace, "the response echoes the trace");
    }

    /// With no trace attached, the three fields stay at their "absent" value.
    #[test]
    fn a_request_without_trace_carries_zeros() {
        let request = RpcHeader::request("ping", service_id_of("Ping"), 1);
        assert!(!request.trace().is_present());
        assert_eq!(request.trace().parent_span_id, 0);
    }

    #[test]
    fn request_stamps_a_timestamp() {
        let id = service_id_of("Ping");
        let before = now_ns();
        let header = RpcHeader::request("ping", id, 1);
        let after = now_ns();

        assert!(header.timestamp_ns >= before);
        assert!(header.timestamp_ns <= after);
        // `seq` is owned by the publisher, not by the constructor.
        assert_eq!(header.seq, 0);
        assert_eq!(header.with_seq(7).seq, 7);
    }

    #[test]
    fn response_stamps_its_own_emission_time() {
        let request = RpcHeader::request("ping", service_id_of("Ping"), 1);
        let response = RpcHeader::response_from(&request, EventKind::Complete, 1);

        assert_eq!(response.correlation_id, request.correlation_id);
        // The response carries its own emission time, distinct from the request's.
        assert!(response.timestamp_ns >= request.timestamp_ns);
        assert_eq!(response.with_seq(3).seq, 3);
    }

    #[test]
    fn fmt_correlation_id_is_uuid_shaped() {
        let cid = [
            0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe, 0xba, 0xbe, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55,
            0x66, 0x77,
        ];
        assert_eq!(
            fmt_correlation_id(&cid),
            "deadbeef-cafe-babe-0011-223344556677"
        );
    }

    /// An unknown kind must decode to `Error`, never to the terminal `Complete`.
    #[test]
    fn an_unknown_event_kind_decodes_as_error_not_complete() {
        for value in 6u8..=u8::MAX {
            assert_eq!(
                EventKind::from_u8(value),
                EventKind::Error,
                "the unknown kind {value} must not read as a successful completion"
            );
        }

        // Every declared value round-trips, `Complete` included.
        for kind in [
            EventKind::Request,
            EventKind::Next,
            EventKind::Complete,
            EventKind::Error,
            EventKind::RpcError,
            EventKind::Cancel,
        ] {
            assert_eq!(EventKind::from_u8(kind.as_u8()), kind);
        }
    }

    /// A Cancel abandons a call, it does not answer it: reading it as terminal
    /// would let an observer close a call the provider never finished.
    #[test]
    fn a_cancel_is_not_terminal_and_has_its_own_label() {
        assert!(!EventKind::Cancel.is_terminal());
        assert_eq!(EventKind::Cancel.label(), "cancel");
        assert_eq!(EventKind::from_u8(5), EventKind::Cancel);
    }

    /// The Cancel names the call and nothing else: same identity as the request,
    /// no method, and its own emission time.
    #[test]
    fn cancel_header_reuses_the_request_identity_without_a_method() {
        let id = service_id_of("GetPerson");
        let request = RpcHeader::request("get_person", id, 3).with_trace(TraceContext::new_root());
        let cancel = RpcHeader::cancel_from(&request);

        assert_eq!(cancel.correlation_id, request.correlation_id);
        assert_eq!(cancel.service_id, id);
        assert_eq!(cancel.service_version, 3);
        assert_eq!(cancel.event_kind(), EventKind::Cancel);
        assert!(cancel.method().is_empty(), "a Cancel names no method");
        assert_eq!(cancel.trace(), request.trace(), "the Cancel stays traced");
        // `seq` belongs to the emitter, exactly like a response's.
        assert_eq!(cancel.seq, 0);
        assert_eq!(cancel.with_seq(9).seq, 9);
        assert!(cancel.timestamp_ns >= request.timestamp_ns);
    }
}
