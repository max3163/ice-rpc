//! Technical RPC errors.

use rkyv::{Archive, Deserialize, Serialize};

/// Technical RPC error, categorized so callers can choose a policy
/// (retry / fallback / log / fatal) via [`RpcError::is_retryable`].
#[derive(Debug, Clone, Archive, Serialize, Deserialize, thiserror::Error)]
pub enum RpcError {
    /// Payload serialization/deserialization failure.
    #[error("RPC error: serialization failure")]
    SerializationError,
    /// iceoryx2 transport failure (loan, send, receive, publisher creation).
    #[error("RPC transport error: {0}")]
    TransportError(String),
    /// Service discovery failed (registry/blackboard unavailable).
    #[error("RPC discovery error: {0}")]
    DiscoveryError(String),
    /// The requested service is not registered on any node.
    #[error("RPC service not found: {service}")]
    ServiceNotFound {
        /// Name of the service that could not be resolved.
        service: String,
    },
    /// The provider node is unreachable (publishers missing/invalidated).
    #[error("RPC provider unavailable (node {node})")]
    ProviderUnavailable {
        /// Raw node id (PID) of the unreachable provider.
        node: u32,
    },
    /// Waiting deadline exceeded.
    #[error("RPC error: timeout exceeded")]
    Timeout,
    /// The call was cancelled by a global shutdown (Ctrl+C).
    #[error("RPC error: call cancelled")]
    Cancelled,
    /// Payload exceeds the configured shared-memory limit.
    #[error("RPC payload too large: {size} bytes (limit {limit})")]
    PayloadTooLarge {
        /// Size of the payload the caller tried to send, in bytes.
        size: usize,
        /// Maximum payload size allowed by the shared-memory segment.
        limit: usize,
    },
    /// Peer uses an incompatible protocol or service version.
    #[error(
        "RPC protocol mismatch (protocol {received_protocol} != {expected_protocol}, service {received_service} != {expected_service})"
    )]
    ProtocolMismatch {
        /// Protocol version this build speaks.
        expected_protocol: u16,
        /// Protocol version declared by the peer.
        received_protocol: u16,
        /// Service version this build expects for this service.
        expected_service: u16,
        /// Service version declared by the peer.
        received_service: u16,
    },
    /// Unexpected internal error / invariant violation.
    #[error("RPC internal error: {0}")]
    Internal(String),
}

impl RpcError {
    /// Returns `true` when retrying the same call may succeed.
    ///
    /// Transient failures (transport, discovery, provider availability,
    /// timeout) are retryable. Deterministic failures (serialization, missing
    /// service, payload size, protocol mismatch) and internal errors are not.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            RpcError::TransportError(_)
                | RpcError::DiscoveryError(_)
                | RpcError::ProviderUnavailable { .. }
                | RpcError::Timeout
        )
    }
}
