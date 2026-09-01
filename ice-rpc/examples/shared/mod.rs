//! Self-contained demo services shared by the ice-rpc examples.
//!
//! These definitions are inlined in the examples so that they stay compilable
//! when the `ice-rpc` crate is packaged and published on crates.io: the
//! examples must not depend on the local, unpublished `common` crate.
//!
//! Each `#[service]`-annotated trait automatically generates its Proxy,
//! Client, Server and lifecycle implementations.

// Each example binary only uses a subset of these demo services, and the
// generated code conditionally emits `#[cfg(feature = "napi")]` items that
// the examples never enable.
#![allow(dead_code)]

use ice_rpc::{cache, service, timeout, Observable};
use rkyv::{Archive, Deserialize, Serialize};

// ── ConfigService ───────────────────────────────────────────────────

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
    #[cache(ttl = "60s", max_entries = 128)]
    async fn get(&self, key: String) -> Observable<String, ConfigError>;
}

// ── ContextService ──────────────────────────────────────────────────

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
#[service("ContextService")]
pub trait ContextService {
    /// Returns the value associated with a key.
    async fn get(&self, key: String) -> Observable<String, ContextError>;

    /// Sets the value of a key (creation or update).
    async fn set(&self, key: String, value: String) -> Observable<bool, ContextError>;

    /// Deletes a key from the context.
    async fn delete(&self, key: String) -> Observable<bool, ContextError>;

    /// Returns the list of all entries in the context.
    async fn list(&self) -> Observable<ContextEntry, ContextError>;
}

// ── DatabaseService ─────────────────────────────────────────────────

/// Error returned by the [`DatabaseService`] operations.
#[derive(Debug, Archive, Deserialize, Serialize, serde::Serialize, serde::Deserialize)]
pub enum DatabaseError {
    /// No record found for the query.
    NotFound,
    /// Internal database error.
    Error,
}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabaseError::NotFound => write!(f, "record not found"),
            DatabaseError::Error => write!(f, "internal database error"),
        }
    }
}

/// Search criteria for a person by their identity.
#[derive(Debug, Clone, Archive, Deserialize, Serialize, serde::Serialize, serde::Deserialize)]
pub struct PersonneQuery {
    /// Last name of the searched person.
    pub nom: String,
    /// First name of the searched person.
    pub prenom: String,
}

/// Full information about a person.
#[derive(Debug, Clone, Archive, Deserialize, Serialize, serde::Serialize, serde::Deserialize)]
pub struct PersonneInfo {
    /// Last name.
    pub nom: String,
    /// First name.
    pub prenom: String,
    /// Age in years.
    pub age: u32,
    /// Email address.
    pub email: String,
    /// Phone number.
    pub telephone: String,
    /// City of residence.
    pub ville: String,
    /// Current occupation.
    pub profession: String,
}

/// Business query service over a database.
#[service("DatabaseService")]
pub trait DatabaseService {
    /// Returns the age associated with a person's name.
    #[timeout("10s")]
    async fn get_user_age(&self, name: String) -> Observable<i32, DatabaseError>;

    /// Returns the full information of a person.
    async fn get_person(&self, query: PersonneQuery) -> Observable<PersonneInfo, DatabaseError>;
}

// ── HttpService ─────────────────────────────────────────────────────

/// Parameters of an HTTP request to transmit.
#[derive(Debug, Archive, Deserialize, Serialize, Clone, serde::Serialize, serde::Deserialize)]
pub struct HttpRequestParams {
    /// HTTP method (`GET`, `POST`, `PUT`, etc.).
    pub method: String,
    /// Target URL of the request.
    pub url: String,
    /// HTTP headers as (key, value) pairs.
    pub headers: Vec<(String, String)>,
    /// Request body — may reach several MB.
    pub body: Vec<u8>,
}

/// HTTP response received after processing.
#[derive(Debug, Archive, Deserialize, Serialize, Clone, serde::Serialize, serde::Deserialize)]
pub struct HttpResponseParams {
    /// HTTP status code (`200`, `404`, `500`, etc.).
    pub status_code: u16,
    /// Status text (`"OK"`, `"Not Found"`, etc.).
    pub status_text: String,
    /// Response headers.
    pub headers: Vec<(String, String)>,
    /// Response body — may reach several MB.
    pub body: Vec<u8>,
}

/// Error returned by the [`HttpService`] operations.
#[derive(Debug, Archive, Deserialize, Serialize, serde::Serialize, serde::Deserialize)]
pub enum HttpError {
    /// Internal error while processing the request.
    InternalError(String),
    /// The payload exceeds the maximum allowed size.
    PayloadTooLarge {
        /// Maximum allowed size in bytes.
        max_bytes: u64,
        /// Actual size of the received payload.
        actual_bytes: u64,
    },
}

impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpError::InternalError(msg) => write!(f, "Internal error: {}", msg),
            HttpError::PayloadTooLarge {
                max_bytes,
                actual_bytes,
            } => write!(
                f,
                "Payload too large: {} max, {} received",
                max_bytes, actual_bytes
            ),
        }
    }
}

/// HTTP transfer service with large zero-copy payloads.
#[service("HttpService", allow_large_payload = true)]
pub trait HttpService {
    /// Sends an HTTP request and returns the response.
    async fn send_request(
        &self,
        request: HttpRequestParams,
    ) -> Observable<HttpResponseParams, HttpError>;
}
