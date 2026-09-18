//! Technical RPC errors.

use rkyv::{Archive, Deserialize, Serialize};

/// Technical RPC error, categorized so callers can choose a policy
/// (retry / fallback / log / fatal) via [`RpcError::is_retryable`].
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize, thiserror::Error)]
pub enum RpcError {
    /// Payload serialization/deserialization failure.
    #[error("RPC error: serialization failure")]
    SerializationError,
    /// iceoryx2 transport failure (loan, send, receive, port creation).
    #[error("RPC transport error: {0}")]
    TransportError(String),
    /// Waiting deadline exceeded.
    #[error("RPC error: timeout exceeded")]
    Timeout,
    /// The call was cancelled by a global shutdown (Ctrl+C).
    #[error("RPC error: call cancelled")]
    Cancelled,
    /// Unexpected internal error / invariant violation.
    #[error("RPC internal error: {0}")]
    Internal(String),
    /// The service on the bus is not the one this build asks for.
    #[error("RPC error: incompatible service on the bus: {0}")]
    ProtocolMismatch(String),
    /// The provider has no method of that name on the requested service.
    ///
    /// Answered immediately by the provider instead of leaving the call to time
    /// out, so a typo or a stale client is diagnosable.
    #[error("RPC error: unknown method '{0}'")]
    UnknownMethod(String),
    /// No service is registered under the requested id on that channel.
    #[error("RPC error: unknown service id {0}")]
    UnknownService(String),
    /// The provider's service interface version differs from the requested one.
    ///
    /// Emitted by the provider before dispatching, so the caller learns the
    /// contract mismatch instead of waiting for a timeout. No amount of retrying
    /// resolves it: both peers must be rebuilt with the same `#[service(version)]`.
    #[error(
        "RPC error: incompatible service version: provider is v{expected}, requested v{actual}"
    )]
    IncompatibleVersion {
        /// Interface version the provider was built with.
        expected: u16,
        /// Interface version the request asked for.
        actual: u16,
    },
}

impl RpcError {
    /// Returns `true` when retrying the same call may succeed.
    ///
    /// Transient failures (transport, timeout) are retryable; serialization
    /// failures, internal errors, [`RpcError::ProtocolMismatch`],
    /// [`RpcError::IncompatibleVersion`], [`RpcError::UnknownMethod`] and
    /// [`RpcError::UnknownService`] — which no amount of retrying resolves — are
    /// not.
    pub fn is_retryable(&self) -> bool {
        matches!(self, RpcError::TransportError(_) | RpcError::Timeout)
    }
}
