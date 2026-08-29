//! Bridge between the `#[service]` macro and the native Node.js bridge (N-API).
//!
//! The macro generates code calling `call` for every service method
//! in `ProviderNodeJs` mode. The actual dispatch function is injected
//! by `gateway_nodejs` during runtime initialization.

use std::sync::OnceLock;

/// Signature of the dispatch callback to Node.js.
///
/// # Arguments
/// * `cid` — Correlation id (16 bytes).
/// * `service` — Logical name of the target service.
/// * `method` — Name of the method to call.
/// * `args` — Arguments in [`serde_json::Value`] format.
///
/// # Returns
/// * `Ok(Value)` — JSON response from the Node.js handler.
/// * `Err(String)` — Error message.
pub type DispatchFn = fn(
    cid: [u8; 16],
    service: &str,
    method: &str,
    args: serde_json::Value,
) -> Result<serde_json::Value, String>;

static DISPATCH: OnceLock<DispatchFn> = OnceLock::new();

/// Registers the Node.js dispatch function.
///
/// Called exactly once by `gateway_nodejs` during runtime initialization.
/// Subsequent calls are silently ignored.
pub fn set_dispatch(f: DispatchFn) {
    let _ = DISPATCH.set(f);
}

/// Calls the Node.js bridge through the registered dispatch function.
///
/// # Panics
/// If [`set_dispatch`] has not been called beforehand.
pub fn call(
    cid: [u8; 16],
    service: &str,
    method: &str,
    args: serde_json::Value,
) -> Result<serde_json::Value, String> {
    DISPATCH.get().expect(
        "NodeJS dispatch not initialized — call ice_rpc::nodejs_dispatch::set_dispatch() first",
    )(cid, service, method, args)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handler(
        cid: [u8; 16],
        service: &str,
        method: &str,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        assert_eq!(cid, [7u8; 16]);
        assert_eq!(service, "DatabaseService");
        assert_eq!(method, "get_user_age");
        Ok(serde_json::json!({ "echo": args }))
    }

    #[test]
    fn set_dispatch_then_call_roundtrip() {
        set_dispatch(handler);
        let result = call(
            [7u8; 16],
            "DatabaseService",
            "get_user_age",
            serde_json::json!({ "name": "Alice" }),
        )
        .expect("dispatch call should succeed");
        assert_eq!(result, serde_json::json!({ "echo": { "name": "Alice" } }));
    }
}
