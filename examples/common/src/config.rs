//! Key-value configuration service based on a TOML file.
//!
//! Exposes a single method [`ConfigService::get`] returning the value
//! associated with a key of the form `"section.field"` (e.g. `"database.url"`).

use ice_rpc::{cache, service, Observable};
use rkyv::{Archive, Deserialize, Serialize};

/// Error returned when a key is not found in the configuration.
#[derive(Debug, Archive, Deserialize, Serialize, Clone, serde::Serialize, serde::Deserialize)]
pub enum ConfigError {
    /// The requested key does not exist in the configuration.
    KeyNotFound,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::KeyNotFound => write!(f, "key not found in configuration"),
        }
    }
}

/// Key-value configuration service based on a TOML file.
#[service("ConfigService")]
pub trait ConfigService {
    /// Returns the value associated with a configuration key.
    ///
    /// # Arguments
    /// * `key` — Key in the `"section.field"` format (e.g. `"database.url"`).
    ///
    /// # Returns
    /// * `Ok(stream)` emitting `Next(value)` then `Complete`.
    /// * `Err(KeyNotFound)` if the key is absent.
    #[cache(ttl = "60s", max_entries = 128)]
    async fn get(&self, key: String) -> Observable<String, ConfigError>;
}
