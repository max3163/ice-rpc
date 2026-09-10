//! Zero-copy RPC header and the correlation-id helpers.

use iceoryx2::prelude::*;

use super::consts::{METHOD_NAME_LEN, PROTOCOL_VERSION, SERVICE_NAME_LEN};
use super::wire::EventKind;
use super::StaticString;

/// Zero-copy iceoryx2 header carried in the `user_header` of each sample.
///
/// Multiplexes several services on the same topics via the `service_name` field.
#[repr(C)]
#[derive(Debug, Clone, Copy, ZeroCopySend, Default)]
pub struct RpcHeader {
    /// Request↔response correlation UUID (128 bits).
    pub correlation_id: [u8; 16],
    /// Emission timestamp in nanoseconds since `UNIX_EPOCH`.
    pub sent_at_ns: u64,
    /// Name of the target service.
    pub service_name: StaticString<SERVICE_NAME_LEN>,
    /// Name of the RPC method.
    pub method_name: StaticString<METHOD_NAME_LEN>,
    /// Type of the carried event.
    pub event_kind: EventKind,
    /// Version of the ice-rpc wire protocol.
    pub protocol_version: u16,
    /// Version of the service interface (methods + request enum).
    pub service_version: u16,
}

impl RpcHeader {
    /// Creates a **request** header with a unique correlation_id and the current timestamp.
    pub fn new(service: &str, method: &str) -> Self {
        Self {
            correlation_id: RpcHeader::next_correlation_id(),
            sent_at_ns: RpcHeader::now_ns(),
            service_name: StaticString::from_bytes_truncated(service.as_bytes())
                .unwrap_or_default(),
            method_name: StaticString::from_bytes_truncated(method.as_bytes()).unwrap_or_default(),
            event_kind: EventKind::Request,
            protocol_version: PROTOCOL_VERSION,
            service_version: 0,
        }
    }

    /// Sets the service interface version carried in the header.
    #[inline]
    pub fn with_service_version(mut self, service_version: u16) -> Self {
        self.service_version = service_version;
        self
    }

    /// Creates a **request** header with a service interface version.
    #[inline]
    pub fn request(service: &str, method: &str, service_version: u16) -> Self {
        Self::new(service, method).with_service_version(service_version)
    }

    /// Creates a **response** header from a request header.
    ///
    /// Reuses the correlation id, service and method names of the request and
    /// stamps the protocol/service versions and the current timestamp.
    #[inline]
    pub fn response_from(request: &RpcHeader, event_kind: EventKind, service_version: u16) -> Self {
        Self {
            correlation_id: request.correlation_id,
            sent_at_ns: RpcHeader::now_ns(),
            service_name: request.service_name,
            method_name: request.method_name,
            event_kind,
            protocol_version: PROTOCOL_VERSION,
            service_version,
        }
    }

    /// Returns `true` if this header is a request (client → server).
    #[inline]
    pub fn is_request(&self) -> bool {
        self.event_kind == EventKind::Request
    }

    /// Returns `true` if this header is a response (server → client).
    #[inline]
    pub fn is_response(&self) -> bool {
        self.event_kind != EventKind::Request
    }

    /// Returns the service name as a `&str`.
    #[inline]
    pub fn service(&self) -> &str {
        core::str::from_utf8(self.service_name.as_bytes_const()).unwrap_or("")
    }

    /// Generates a unique correlation id without a CSPRNG syscall.
    ///
    /// Layout of the 16 bytes:
    /// - `[0..4]`   : PID (u32 LE) — cross-process uniqueness.
    /// - `[4..12]`  : atomic counter u64 LE — intra-process uniqueness.
    /// - `[12..16]` : padding (zeros).
    pub fn next_correlation_id() -> [u8; 16] {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let mut id = [0u8; 16];
        id[0..4].copy_from_slice(&pid.to_le_bytes());
        id[4..12].copy_from_slice(&seq.to_le_bytes());
        id
    }

    /// Returns the method name as a `&str`.
    #[inline]
    pub fn method(&self) -> &str {
        core::str::from_utf8(self.method_name.as_bytes_const()).unwrap_or("")
    }

    /// Returns the current timestamp in nanoseconds since `UNIX_EPOCH`.
    pub fn now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }
}

/// Extracts the emitting PID from a raw `correlation_id`.
#[inline]
pub fn caller_pid_from_cid(cid: &[u8; 16]) -> u32 {
    u32::from_le_bytes([cid[0], cid[1], cid[2], cid[3]])
}

/// Formats a correlation id as hexadecimal (UUID-like format).
pub fn fmt_correlation_id(cid: &[u8; 16]) -> String {
    let [b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15] = cid;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15,
    )
}

/// Formats the first 4 bytes of a correlation_id in hex (8 characters).
///
/// These 4 bytes contain the PID of the emitting process.
pub fn fmt_correlation_id_short(cid: &[u8; 16]) -> String {
    format!("{:02x}{:02x}{:02x}{:02x}", cid[0], cid[1], cid[2], cid[3])
}
