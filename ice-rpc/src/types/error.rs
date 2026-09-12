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
}

impl RpcError {
    /// Returns `true` when retrying the same call may succeed.
    ///
    /// Transient failures (transport, timeout) are retryable; serialization
    /// failures and internal errors are not.
    pub fn is_retryable(&self) -> bool {
        matches!(self, RpcError::TransportError(_) | RpcError::Timeout)
    }
}
