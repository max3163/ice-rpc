//! Demonstration HTTP service for transferring large payloads.
//!
//! Illustrates iceoryx2's ability to transfer several megabytes through
//! shared memory without any superfluous copy (zero-copy).

use ice_rpc::{service, Observable};
use rkyv::{Archive, Deserialize, Serialize};

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
///
/// Demonstrates iceoryx2's ability to transmit multi-megabyte buffers
/// without memory copies, through inter-process shared memory.
#[service("HttpService")]
pub trait HttpService {
    /// Sends an HTTP request and returns the response.
    ///
    /// The request body may reach several MB thanks to the zero-copy
    /// transport of iceoryx2.
    ///
    /// # Returns
    /// * `Ok(stream)` emitting `Next(HttpResponseParams)` then `Complete`.
    /// * `Err(HttpError::PayloadTooLarge)` if the body is too large.
    /// * `Err(HttpError::InternalError)` on processing error.
    async fn send_request(
        &self,
        request: HttpRequestParams,
    ) -> Observable<HttpResponseParams, HttpError>;
}
