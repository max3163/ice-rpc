//! Technical RPC errors.

use rkyv::{Archive, Deserialize, Serialize};

/// Technical RPC error, categorized so callers can choose a policy
/// (retry / fallback / log / fatal) via [`RpcError::is_retryable`].
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub enum RpcError {
    /// Payload serialization/deserialization failure.
    SerializationError,
    /// iceoryx2 transport failure (loan, send, receive, port creation).
    TransportError(String),
    /// Waiting deadline exceeded.
    Timeout,
    /// The call was cancelled by a global shutdown (Ctrl+C).
    Cancelled,
    /// Unexpected internal error / invariant violation.
    Internal(String),
    /// The service on the bus is not the one this build asks for.
    ProtocolMismatch(String),
    /// The provider has no method of that name on the requested service.
    ///
    /// Answered immediately by the provider instead of leaving the call to time
    /// out, so a typo or a stale client is diagnosable.
    UnknownMethod(String),
    /// No service is registered under the requested id on that channel.
    UnknownService(String),
    /// The provider's service interface version differs from the requested one.
    ///
    /// Emitted by the provider before dispatching, so the caller learns the
    /// contract mismatch instead of waiting for a timeout. No amount of retrying
    /// resolves it: both peers must be rebuilt with the same `#[service(version)]`.
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

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcError::SerializationError => f.write_str("RPC error: serialization failure"),
            RpcError::TransportError(message) => write!(f, "RPC transport error: {message}"),
            RpcError::Timeout => f.write_str("RPC error: timeout exceeded"),
            RpcError::Cancelled => f.write_str("RPC error: call cancelled"),
            RpcError::Internal(message) => write!(f, "RPC internal error: {message}"),
            RpcError::ProtocolMismatch(message) => {
                write!(f, "RPC error: incompatible service on the bus: {message}")
            }
            RpcError::UnknownMethod(method) => {
                write!(f, "RPC error: unknown method '{method}'")
            }
            RpcError::UnknownService(id) => write!(f, "RPC error: unknown service id {id}"),
            RpcError::IncompatibleVersion { expected, actual } => write!(
                f,
                "RPC error: incompatible service version: provider is v{expected}, requested v{actual}"
            ),
        }
    }
}

impl std::error::Error for RpcError {}
