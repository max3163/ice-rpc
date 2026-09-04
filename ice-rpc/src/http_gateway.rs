//! HTTP REST gateway — Automatic exposure of ice-rpc services.
//!
//! This module provides a [trillium]-based HTTP server that exposes the ice-rpc
//! services through a REST API. Each service method is accessible at the
//! URL `/{service}/{method}` with the parameters passed as a query string
//! (GET) or as a JSON body (POST).
//!
//! The server is runtime-agnostic: it runs on `async-global-executor` through
//! the `trillium-smol` adapter, so **no tokio runtime is required**.
//!
//! # Quick start
//!
//! ```rust,ignore
//! fn main() {
//!     // Initializes the framework (without a global service registry).
//!     ice_rpc::init();
//!
//!     smol::block_on(async {
//!         // Starts the HTTP gateway exposing the chosen services.
//!         ice_rpc::start_http_gateway!(8080, DatabaseServiceProxy, ConfigServiceProxy).await;
//!
//!         // The server runs until Ctrl+C
//!         ice_rpc::wait_for_shutdown().await;
//!     });
//! }
//! ```
//!
//! # URL format
//!
//! | HTTP method | URL | Parameters |
//! |---|---|---|
//! | GET | `/{service}/{method}?arg1=val1&arg2=val2` | Query string |
//! | POST | `/{service}/{method}` | JSON body |
//!
//! # Response format
//!
//! ```json
//! {"status":"ok","data":{...}}
//! {"status":"error","error":"error message"}
//! ```

use crate::service_traits::HttpCallable;
use async_lock::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use trillium::{Conn, Handler, Method};

/// Shared state of the HTTP gateway.
///
/// Contains the cache of [`HttpCallable`] proxies indexed by service name.
/// Proxies are created lazily on the first call and reused afterwards.
#[derive(Clone)]
struct HttpGatewayState {
    /// HTTP proxy factories (logical name → factory).
    factories: Arc<HashMap<&'static str, fn() -> Arc<dyn HttpCallable>>>,
    /// HTTP proxy cache (name → proxy).
    cache: Arc<RwLock<HashMap<String, Arc<dyn HttpCallable>>>>,
}

impl HttpGatewayState {
    fn new(factories: HashMap<&'static str, fn() -> Arc<dyn HttpCallable>>) -> Self {
        Self {
            factories: Arc::new(factories),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Retrieves or creates an [`HttpCallable`] proxy for the requested service.
    async fn get_or_create(&self, service_name: &str) -> Option<Arc<dyn HttpCallable>> {
        // Fast-path: already in the cache.
        {
            let cache = self.cache.read().await;
            if let Some(proxy) = cache.get(service_name) {
                return Some(proxy.clone());
            }
        }

        // Slow-path: creates the proxy through the service factory.
        let factory = self.factories.get(service_name)?;
        let proxy = factory();

        // Caches for subsequent calls.
        let mut cache = self.cache.write().await;
        cache
            .entry(service_name.to_string())
            .or_insert_with(|| proxy.clone());

        Some(proxy)
    }
}

/// Trillium handler implementing the `/{service}/{method}` REST routes.
struct HttpGateway {
    state: HttpGatewayState,
}

impl Handler for HttpGateway {
    async fn run(&self, mut conn: Conn) -> Conn {
        // Origin check (same behavior as the previous axum middleware).
        let origin_header = conn.request_headers().get_str("origin").map(str::to_string);
        if let Some(origin) = origin_header {
            if !is_origin_allowed(&origin) {
                log::warn!(
                    "Origin rejected: '{}' (allowed domain: *.{})",
                    origin,
                    allowed_origin_domain()
                );
                return json_response(
                    conn,
                    403,
                    serde_json::json!({
                        "status": "error",
                        "error": format!(
                            "Origin '{}' is not allowed. Only the domain '{}' is accepted.",
                            origin,
                            allowed_origin_domain()
                        )
                    }),
                );
            }
        }

        let Some((service, method)) = split_path(conn.path()) else {
            return json_response(
                conn,
                404,
                serde_json::json!({
                    "status": "error",
                    "error": "Invalid route. Expected URL format: /{service}/{method}"
                }),
            );
        };

        match conn.method() {
            Method::Get => {
                // Converts the query params into a JSON object.
                let params = parse_query_string(conn.querystring());
                let json_params = params_to_json(&params);
                invoke_service(&self.state, &service, &method, json_params, conn).await
            }
            Method::Post => {
                let body = conn.request_body_string().await.unwrap_or_default();
                let json_params = serde_json::from_str(&body).unwrap_or(Value::Null);
                invoke_service(&self.state, &service, &method, json_params, conn).await
            }
            _ => json_response(
                conn,
                405,
                serde_json::json!({
                    "status": "error",
                    "error": "Method not allowed. Use GET or POST."
                }),
            ),
        }
    }
}

/// Splits the request path into `(service, method)`.
///
/// Expects exactly two non-empty segments: `/{service}/{method}`.
fn split_path(path: &str) -> Option<(String, String)> {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    let mut segments = trimmed.split('/');
    let service = segments.next()?.to_string();
    let method = segments.next()?.to_string();
    if segments.next().is_some() {
        // More than two segments.
        return None;
    }
    if service.is_empty() || method.is_empty() {
        return None;
    }
    Some((service, method))
}

/// Parses a raw query string (`a=b&c=d`) into a map of decoded strings.
///
/// Trillium does not ship a query-string parser; this minimal implementation
/// covers the subset used by the gateway: scalar values, `+` as space, and
/// percent-encoding.
fn parse_query_string(querystring: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();
    if querystring.is_empty() {
        return params;
    }
    for pair in querystring.split('&') {
        let (key, value) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        params.insert(percent_decode(key), percent_decode(value));
    }
    params
}

/// Minimal percent-decoder supporting `%XX` escapes and `+` as space.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                match (hex_value(bytes[i + 1]), hex_value(bytes[i + 2])) {
                    (Some(hi), Some(lo)) => {
                        out.push((hi << 4) | lo);
                    }
                    _ => out.extend_from_slice(&bytes[i..i + 3]),
                }
                i += 3;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Returns the numeric value of a hexadecimal digit.
fn hex_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Builds a JSON response with the given status code.
fn json_response(conn: Conn, status: u16, value: Value) -> Conn {
    conn.with_response_header("content-type", "application/json")
        .with_status(status)
        .with_body(value.to_string())
        .halt()
}

/// Converts the HTTP query params into a JSON object.
///
/// Scalar values are interpreted as JSON:
/// - `"true"` / `"false"` → boolean
/// - integers → integer number
/// - otherwise → string
fn params_to_json(params: &HashMap<String, String>) -> Value {
    if params.is_empty() {
        return Value::Null;
    }
    if params.len() == 1 {
        // Single parameter: pass the value directly.
        let (_, val) = params.iter().next().unwrap();
        return parse_scalar(val);
    }
    let map: serde_json::Map<String, Value> = params
        .iter()
        .map(|(k, v)| (k.clone(), parse_scalar(v)))
        .collect();
    Value::Object(map)
}

/// Tries to interpret a string as a JSON scalar.
fn parse_scalar(s: &str) -> Value {
    if s.is_empty() {
        return Value::String(String::new());
    }
    if s.eq_ignore_ascii_case("true") {
        return Value::Bool(true);
    }
    if s.eq_ignore_ascii_case("false") {
        return Value::Bool(false);
    }
    if s.eq_ignore_ascii_case("null") || s.eq_ignore_ascii_case("none") {
        return Value::Null;
    }
    if let Ok(n) = s.parse::<i64>() {
        return Value::Number(n.into());
    }
    if let Ok(n) = s.parse::<f64>() {
        if n.is_finite() {
            if let Some(num) = serde_json::Number::from_f64(n) {
                return Value::Number(num);
            }
        }
    }
    Value::String(s.to_string())
}

/// Calls the service through the [`HttpCallable`] proxy and formats the HTTP response.
async fn invoke_service(
    state: &HttpGatewayState,
    service: &str,
    method: &str,
    params: Value,
    conn: Conn,
) -> Conn {
    // Validates the service and method names.
    if service.is_empty() || method.is_empty() {
        return json_response(
            conn,
            400,
            serde_json::json!({
                "status": "error",
                "error": "Service and method names are required in the URL: /{service}/{method}"
            }),
        );
    }

    // Resolves the proxy.
    let proxy = match state.get_or_create(service).await {
        Some(p) => p,
        None => {
            return json_response(
                conn,
                404,
                serde_json::json!({
                    "status": "error",
                    "error": format!("Unknown service '{}'. Make sure this service is exposed via start_http_gateway!.", service)
                }),
            );
        }
    };

    // RPC call through the HTTP proxy.
    match proxy.http_invoke(method, params).await {
        Ok(result) => json_response(conn, 200, result),
        Err(err) => {
            // Determines whether it is an "unknown method" error (404) or another (400).
            if err.contains("Unknown method") {
                json_response(
                    conn,
                    404,
                    serde_json::json!({
                        "status": "error",
                        "error": format!("Unknown method '{}' for service '{}'", method, service)
                    }),
                )
            } else {
                json_response(
                    conn,
                    400,
                    serde_json::json!({
                        "status": "error",
                        "error": err
                    }),
                )
            }
        }
    }
}

/// Allowed domain for the HTTP `Origin` header.
///
/// If the `Origin` header is present, its value must match
/// `*.{domain}` or `{domain}` exactly. Default: `"my-domain.com"`.
/// Overridable via the `ICE_HTTP_ALLOWED_ORIGIN` environment variable.
fn allowed_origin_domain() -> String {
    std::env::var("ICE_HTTP_ALLOWED_ORIGIN")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "my-domain.com".to_string())
}

/// Checks whether an `Origin` header value matches the allowed domain.
///
///
/// Extracts the host from the URL and checks that it ends with `.domain` or
/// matches the domain exactly.
fn is_origin_allowed(origin: &str) -> bool {
    let allowed = allowed_origin_domain();

    // Extracts the host from the origin URL (e.g. "https://sub.my-domain.com:8080" → "sub.my-domain.com")
    let host = origin
        .strip_prefix("https://")
        .or_else(|| origin.strip_prefix("http://"))
        .unwrap_or(origin)
        .split(':')
        .next()
        .unwrap_or("")
        .split('/')
        .next()
        .unwrap_or("");

    if host.is_empty() {
        return false;
    }

    host == allowed || host.ends_with(&format!(".{}", allowed))
}

/// Starts the HTTP REST gateway server on the specified port.
///
/// Receives the mapping of the services to expose (`logical name → proxy factory`).
/// Prefer the [`start_http_gateway!`] macro which builds this mapping
/// automatically from the list of proxies.
///
/// This function is async and **blocks** until the server stops
/// (Ctrl+C or [`global_cancel_token`](crate::global_cancel_token)).
pub async fn start_http_server(
    port: u16,
    factories: HashMap<&'static str, fn() -> Arc<dyn HttpCallable>>,
) {
    let handler = HttpGateway {
        state: HttpGatewayState::new(factories),
    };

    log::info!(
        "🌐 ice-rpc HTTP REST gateway started on http://localhost:{}",
        port
    );
    log::info!(
        "   URL format : http://localhost:{}/{{service}}/{{method}}",
        port
    );
    log::info!(
        "   Example GET  : curl http://localhost:{}/DatabaseService/get_user_age?name=Alice",
        port
    );
    log::info!(
        "   Example POST : curl -X POST http://localhost:{}/DatabaseService/get_person -H 'Content-Type: application/json' -d '{{\"nom\":\"Dupont\",\"prenom\":\"Jean\"}}'",
        port
    );

    // Graceful shutdown driven by the ice-rpc global cancellation token
    // (Ctrl+C or programmatic cancellation). Trillium's own signal handling is
    // disabled so that ice-rpc keeps a single shutdown path.
    let swansong = trillium::Swansong::new();
    let signal_swansong = swansong.clone();
    let cancel = crate::global_cancel_token().clone();
    crate::rt::spawn(async move {
        cancel.cancelled().await;
        signal_swansong.shut_down().await;
    });

    // The runtime adapter follows the selected runtime feature:
    // - `tokio`             → trillium-tokio
    // - `smol` / no feature → trillium-smol (async-global-executor)
    #[cfg(feature = "tokio")]
    {
        trillium_tokio::config()
            .with_port(port)
            .with_host("127.0.0.1")
            .without_signals()
            .with_swansong(swansong)
            .run_async(handler)
            .await;
    }

    #[cfg(not(feature = "tokio"))]
    {
        trillium_smol::config()
            .with_port(port)
            .with_host("127.0.0.1")
            .without_signals()
            .with_swansong(swansong)
            .run_async(handler)
            .await;
    }

    log::info!("HTTP server stopped.");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_scalar_bool_true() {
        assert_eq!(parse_scalar("true"), Value::Bool(true));
        assert_eq!(parse_scalar("TRUE"), Value::Bool(true));
    }

    #[test]
    fn parse_scalar_bool_false() {
        assert_eq!(parse_scalar("false"), Value::Bool(false));
    }

    #[test]
    fn parse_scalar_null() {
        assert_eq!(parse_scalar("null"), Value::Null);
        assert_eq!(parse_scalar("none"), Value::Null);
    }

    #[test]
    fn parse_scalar_integer() {
        assert_eq!(parse_scalar("42"), Value::Number(42.into()));
        assert_eq!(parse_scalar("-1"), Value::Number((-1).into()));
    }

    #[test]
    fn parse_scalar_string() {
        assert_eq!(parse_scalar("Alice"), Value::String("Alice".into()));
    }

    #[test]
    fn parse_scalar_empty() {
        assert_eq!(parse_scalar(""), Value::String(String::new()));
    }

    #[test]
    fn params_to_json_empty() {
        let params = HashMap::new();
        assert_eq!(params_to_json(&params), Value::Null);
    }

    #[test]
    fn params_to_json_single() {
        let mut params = HashMap::new();
        params.insert("name".into(), "Alice".into());
        assert_eq!(params_to_json(&params), Value::String("Alice".into()));
    }

    #[test]
    fn params_to_json_multiple() {
        let mut params = HashMap::new();
        params.insert("name".into(), "Alice".into());
        params.insert("age".into(), "30".into());
        let result = params_to_json(&params);
        assert!(result.is_object());
        let obj = result.as_object().unwrap();
        assert_eq!(obj["name"], Value::String("Alice".into()));
        assert_eq!(obj["age"], Value::Number(30.into()));
    }

    #[test]
    fn split_path_two_segments() {
        assert_eq!(
            split_path("/DatabaseService/get_user_age"),
            Some(("DatabaseService".into(), "get_user_age".into()))
        );
    }

    #[test]
    fn split_path_invalid() {
        assert_eq!(split_path("/"), None);
        assert_eq!(split_path("/onlyone"), None);
        assert_eq!(split_path("/a/b/c"), None);
        assert_eq!(split_path("/a//b"), None);
    }

    #[test]
    fn parse_query_string_basic() {
        let params = parse_query_string("name=Alice&age=30");
        assert_eq!(params.get("name").map(String::as_str), Some("Alice"));
        assert_eq!(params.get("age").map(String::as_str), Some("30"));
    }

    #[test]
    fn parse_query_string_percent_decoding() {
        let params = parse_query_string("msg=hello+world%21");
        assert_eq!(params.get("msg").map(String::as_str), Some("hello world!"));
    }

    #[test]
    fn percent_decode_plain() {
        assert_eq!(percent_decode("hello"), "hello");
        assert_eq!(percent_decode("hello+world"), "hello world");
        assert_eq!(percent_decode("a%21b"), "a!b");
        assert_eq!(percent_decode("%C3%A9"), "é");
    }
}
