//! Singleton Node.js bridge for JS ↔ Rust communication via N-API.
//!
//! Receives the IPC calls through the handlers registered by the generated
//! code (`#[service]` macro), forwards them to the Node.js callback as native
//! JS objects (zero-copy via N-API serde-json), and sends the responses back to the IPC bus.
//!
//! # Flow
//! IPC (rkyv) → handler → deserialize → Value → call_async() → JS
//! JS → resolve() → Value → serialize → rkyv → IPC

use napi::bindgen_prelude::{Function, Unknown};
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use std::collections::HashMap;
use std::sync::Mutex;

type CorrelationId = [u8; 16];

struct PendingCall {
    sender: ice_rpc::rt::oneshot::Sender<serde_json::Value>,
    deadline: std::time::Instant,
}

/// Typed JS callback for the Node.js dispatcher.
///
/// Receives an object `{ correlationId, service, method, args }` where `args`
/// is a native JS object (not a JSON string). After processing, the JS code
/// must call [`resolve_nodejs_call`](crate::lib::resolve_nodejs_call).
pub type NodeJsCallback = ThreadsafeFunction<serde_json::Value>;

static BRIDGE: std::sync::OnceLock<NodeJsBridge> = std::sync::OnceLock::new();

/// Singleton bridge for JS ↔ Rust communication.
pub struct NodeJsBridge {
    callback: NodeJsCallback,
    pending: Mutex<HashMap<CorrelationId, PendingCall>>,
}

impl NodeJsBridge {
    /// Returns the global instance of the bridge.
    ///
    /// # Panics
    /// If the bridge has not been initialized via [`init`](Self::init).
    pub fn global() -> &'static Self {
        BRIDGE
            .get()
            .expect("NodeJsBridge not initialized — call NodeJsBridge::init() first")
    }

    /// Initializes the bridge with the JS callback.
    pub fn init(js_func: Function<'_, serde_json::Value, Unknown<'static>>) -> napi::Result<()> {
        let tsfn: NodeJsCallback = js_func
            .build_threadsafe_function::<serde_json::Value>()
            .callee_handled::<true>()
            .build()?;

        let bridge = NodeJsBridge {
            callback: tsfn,
            pending: Mutex::new(HashMap::new()),
        };

        BRIDGE
            .set(bridge)
            .map_err(|_| napi::Error::from_reason("NodeJsBridge already initialized"))?;

        log::info!("NodeJsBridge initialized (JS callback registered).");
        Ok(())
    }

    /// Calls the JS with the request data and waits for the response.
    ///
    /// Uses the runtime-agnostic oneshot channel and the 30s timeout
    /// provided by `ice_rpc::rt`.
    pub async fn call_async(
        &self,
        cid: CorrelationId,
        service: &str,
        method: &str,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let (tx, rx) = ice_rpc::rt::oneshot::channel::<serde_json::Value>();

        let call_data = serde_json::json!({
            "correlationId": ice_rpc::fmt_correlation_id(&cid),
            "service": service,
            "method": method,
            "args": args,
        });

        self.pending.lock().expect("pending lock poisoning").insert(
            cid,
            PendingCall {
                sender: tx,
                deadline: std::time::Instant::now() + std::time::Duration::from_secs(30),
            },
        );

        let status = self
            .callback
            .call(Ok(call_data), ThreadsafeFunctionCallMode::NonBlocking);
        if status != napi::Status::Ok {
            self.pending
                .lock()
                .expect("pending lock poisoning")
                .remove(&cid);
            return Err(format!("Failed to call the JS callback: {:?}", status));
        }

        match ice_rpc::rt::timeout(std::time::Duration::from_secs(30), rx).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(_)) => {
                self.pending
                    .lock()
                    .expect("pending lock poisoning")
                    .remove(&cid);
                Err("JS bridge: response channel closed (JS crash?)".into())
            }
            Err(_) => {
                self.pending
                    .lock()
                    .expect("pending lock poisoning")
                    .remove(&cid);
                Err("JS bridge: timeout (30s) — the JS callback did not respond".into())
            }
        }
    }

    /// Resolves a pending call through its hexadecimal correlation_id.
    pub fn resolve(&self, correlation_id_hex: &str, result: serde_json::Value) -> bool {
        let cid = match parse_correlation_id_hex(correlation_id_hex) {
            Some(c) => c,
            None => {
                log::warn!(
                    "resolve: invalid correlation_id hex '{}'",
                    correlation_id_hex
                );
                return false;
            }
        };

        let sender = self
            .pending
            .lock()
            .expect("pending lock poisoning")
            .remove(&cid)
            .map(|p| p.sender);

        match sender {
            Some(tx) => {
                let _ = tx.send(result);
                true
            }
            None => {
                log::warn!(
                    "resolve: correlation_id '{}' not found (expired or nonexistent)",
                    correlation_id_hex
                );
                false
            }
        }
    }

    /// Cleans up the expired calls (timeout).
    ///
    /// Kept for future use: a periodic call (timer/spawn)
    /// will free the memory of the expired correlations.
    #[allow(dead_code)]
    pub fn cleanup_expired(&self) {
        let now = std::time::Instant::now();
        if let Ok(mut map) = self.pending.lock() {
            map.retain(|_, call| call.deadline > now);
        }
    }
}

/// Resolves a pending call (called from [`lib.rs`] via N-API).
pub fn resolve_call(correlation_id_hex: String, result: serde_json::Value) -> bool {
    NodeJsBridge::global().resolve(&correlation_id_hex, result)
}

/// Parses a correlation_id in UUID-like hexadecimal format.
fn parse_correlation_id_hex(hex: &str) -> Option<CorrelationId> {
    if hex.len() != 36 {
        return None;
    }
    let bytes: Vec<u8> = hex
        .chars()
        .filter(|c| *c != '-')
        .collect::<Vec<char>>()
        .chunks(2)
        .filter_map(|chunk| {
            let s: String = chunk.iter().collect();
            u8::from_str_radix(&s, 16).ok()
        })
        .collect();

    if bytes.len() != 16 {
        return None;
    }

    let mut cid = [0u8; 16];
    cid.copy_from_slice(&bytes);
    Some(cid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_correlation_id() {
        let hex = "deadbeef-cafe-babe-0011-223344556677";
        let cid = parse_correlation_id_hex(hex).unwrap();
        assert_eq!(cid[0], 0xDE);
        assert_eq!(cid[1], 0xAD);
        assert_eq!(cid[15], 0x77);
    }

    #[test]
    fn parse_invalid_length() {
        assert!(parse_correlation_id_hex("too-short").is_none());
        assert!(parse_correlation_id_hex("").is_none());
    }

    #[test]
    fn parse_invalid_hex() {
        assert!(parse_correlation_id_hex("gggggggg-gggg-gggg-gggg-gggggggggggg").is_none());
    }

    #[test]
    fn roundtrip_correlation_id() {
        let cid_orig = ice_rpc::RpcHeader::next_correlation_id();
        let hex = ice_rpc::fmt_correlation_id(&cid_orig);
        let cid_parsed = parse_correlation_id_hex(&hex).unwrap();
        assert_eq!(cid_orig, cid_parsed);
    }
}
