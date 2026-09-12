//! Zero-copy RPC header carried in iceoryx2's `user_header`.
//!
//! The header is `ZeroCopySend` and lives *next to* the payload in the shared
//! memory sample, not inside it: reading the correlation id, the method name or
//! the protocol version never deserializes the payload. It is also what makes
//! the payload start at the beginning of the sample, so the alignment requested
//! from iceoryx2 is the alignment rkyv sees.
//!
//! The layout is `#[repr(C)]` and is part of the wire contract shared by every
//! process on the machine: it must not drift silently.

use iceoryx2::prelude::ZeroCopySend;
use iceoryx2_bb_container::string::StaticString;

use crate::types::consts::{METHOD_NAME_LEN, PROTOCOL_VERSION};

/// Correlation id prefixing every request/response pair.
pub const CORRELATION_ID_LEN: usize = 16;

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
    /// Terminal error (business or technical).
    Error = 3,
}

impl EventKind {
    /// Returns `true` if this kind terminates the stream.
    #[inline]
    pub fn is_terminal(self) -> bool {
        matches!(self, EventKind::Complete | EventKind::Error)
    }

    /// Returns the wire value of this kind.
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Decodes a wire value; unknown values map to [`EventKind::Error`].
    #[inline]
    pub const fn from_u8(value: u8) -> Self {
        match value {
            0 => EventKind::Request,
            1 => EventKind::Next,
            3 => EventKind::Error,
            _ => EventKind::Complete,
        }
    }
}

/// Zero-copy RPC header attached to every request and response sample.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, ZeroCopySend)]
pub struct RpcHeader {
    /// Request↔response correlation id (16 bytes).
    pub correlation_id: [u8; CORRELATION_ID_LEN],
    /// Identifier of the target service inside the channel it is published on.
    ///
    /// Several services can share one request/response channel (their *group*);
    /// this id — [`service_id_of`] of the service name — is what the provider
    /// uses to pick the right dispatcher. Zero on a response that does not
    /// belong to a registered service.
    pub service_id: u32,
    /// Method invoked by the request (left empty on a response).
    pub method_name: StaticString<METHOD_NAME_LEN>,
    /// Kind of the sample (request / next / complete / error), as a wire value.
    pub event_kind: u8,
    /// ice-rpc wire protocol version of the emitter.
    pub protocol_version: u16,
    /// Service interface version of the emitter.
    pub service_version: u16,
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
        }
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

/// Computes the stable identifier of a service inside its channel.
///
/// FNV-1a, chosen because it is a `const fn`: the generated code can pass the
/// result of `service_id_of("DatabaseService")` as a constant, and **every**
/// process — as well as `ice-rpc-macros` — derives the same value from the same
/// name without any coordination or discovery step.
///
/// The 32-bit space keeps the id in the header; a collision between two
/// co-located services is detected when the channel is registered.
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
        // `#[service]` rejects this at compile time, so the runtime fallback is
        // only a safety net.
        let long = "x".repeat(METHOD_NAME_LEN + 20);
        let header = RpcHeader::request(&long, 0, 1);
        assert!(header.method().is_empty());
    }

    #[test]
    fn service_id_is_stable_and_distinguishes_names() {
        // Offset basis of FNV-1a: pins the algorithm, so a change of hash would
        // be caught here instead of silently breaking the routing.
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
        // The layout is part of the wire contract shared by every process: it
        // must stay small (it is copied per sample) and 8-byte aligned.
        assert_eq!(std::mem::align_of::<RpcHeader>(), 8);
        assert!(std::mem::size_of::<RpcHeader>() <= 128);
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
}
