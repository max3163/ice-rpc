//! Key-value context service shared between services.
//!
//! Stores metadata, environment variables and session tokens.
//! Implemented in Node.js through the `ProviderNodeJs` mode of the ice-rpc Proxy.

use ice_rpc::{service, Observable};
use rkyv::{Archive, Deserialize, Serialize};

/// Context entry associating a key with its value.
#[derive(Debug, Clone, Archive, Deserialize, Serialize, serde::Serialize, serde::Deserialize)]
pub struct ContextEntry {
    /// Key of the entry.
    pub key: String,
    /// Value associated with the key.
    pub value: String,
}

/// Error returned by the [`ContextService`] operations.
#[derive(Debug, Archive, Deserialize, Serialize, serde::Serialize, serde::Deserialize)]
pub enum ContextError {
    /// The requested key does not exist in the context.
    KeyNotFound,
    /// The key is invalid (empty or contains forbidden characters).
    InvalidKey,
    /// The provided value is invalid.
    InvalidValue,
}

impl std::fmt::Display for ContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContextError::KeyNotFound => write!(f, "key not found"),
            ContextError::InvalidKey => write!(f, "invalid key"),
            ContextError::InvalidValue => write!(f, "invalid value"),
        }
    }
}

/// Key-value storage service shared between ice-rpc services.
///
/// Allows sharing metadata (tokens, environment variables, etc.)
/// between the different services of an ice-rpc node.
#[service("ContextService")]
pub trait ContextService {
    /// Returns the value associated with a key.
    ///
    /// # Returns
    /// * `Ok(stream)` emitting `Next(value)` then `Complete`.
    /// * `Err(ContextError::KeyNotFound)` if the key is absent.
    async fn get(&self, key: String) -> Observable<String, ContextError>;

    /// Sets the value of a key (creation or update).
    ///
    /// # Returns
    /// * `Ok(stream)` emitting `Next(true)` then `Complete` on success.
    /// * `Err(ContextError::InvalidKey)` if the key is invalid.
    /// * `Err(ContextError::InvalidValue)` if the value is invalid.
    async fn set(&self, key: String, value: String) -> Observable<bool, ContextError>;

    /// Deletes a key from the context.
    ///
    /// # Returns
    /// * `Ok(stream)` emitting `Next(true)` then `Complete` if the key existed.
    /// * `Ok(stream)` emitting `Next(false)` then `Complete` if the key did not exist.
    async fn delete(&self, key: String) -> Observable<bool, ContextError>;

    /// Returns the list of all entries in the context.
    ///
    /// # Returns
    /// * `Ok(stream)` emitting one `Next(ContextEntry)` per entry, then `Complete`.
    async fn list(&self) -> Observable<ContextEntry, ContextError>;
}
