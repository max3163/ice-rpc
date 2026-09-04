//! Consumer dispatch Node.js → IPC.
//!
//! Allows JavaScript code to call methods of remote IPC services
//! through a generic `call_ipc_method(service, method, args)` API.
//!
//! # Extensibility
//!
//! To add a new service or a new method, simply add
//! an arm in the `match (service, method)` of [`dispatch_consumer_call`].
//!
//! # Call flow
//!
//! 1. JS: `gateway.callService('DatabaseService', 'get_user_age', 'Alice')`
//! 2. N-API → [`call_ipc_method`] → [`dispatch_consumer_call`]
//! 3. Retrieval of the proxy via [`ice_rpc::ServiceLocator::get`]
//! 4. Call of the business method (IPC to the remote provider)
//! 5. Return of the first event (`Next`) as `serde_json::Value`

use common::{ConfigService, DatabaseService, HttpService};
use ice_rpc::{Event, ServiceLocator};
use serde_json::Value;

/// Calls a method of a remote IPC service and returns the result as JSON.
///
/// # Arguments
/// * `service` — Logical name of the service (e.g. `"DatabaseService"`).
/// * `method` — Name of the method (e.g. `"get_user_age"`).
/// * `args` — Arguments in [`serde_json::Value`] format.
///
/// # Returns
/// * `Ok(Value)` — The return value of the method (first `Next` event).
/// * `Err(String)` — Error message (unknown service, business error, etc.).
pub async fn call_ipc_method(
    service: &str,
    method: &str,
    args: serde_json::Value,
) -> Result<serde_json::Value, String> {
    dispatch_consumer_call(service, method, args).await
}

/// Main dispatch table.
///
/// Associates each `(service, method)` pair with the concrete proxy call.
/// Proxies are retrieved via [`ServiceLocator::get`] (lazy cache).
async fn dispatch_consumer_call(service: &str, method: &str, args: Value) -> Result<Value, String> {
    match (service, method) {
        // ── DatabaseService ──────────────────────────────────────────
        ("DatabaseService", "get_user_age") => {
            let name: String = extract_arg(args, "name")?;

            let proxy = ServiceLocator::global()
                .get::<common::DatabaseServiceProxy>()
                .await
                .ok_or_else(|| "DatabaseService not found (no provider detected)".to_string())?;

            let rx = proxy
                .get_user_age(name)
                .await
                .map_err(|e| format!("IPC error (get_user_age): {}", e))?;

            match rx.recv().await {
                Ok(Event::Next(age)) => Ok(serde_json::json!(age)),
                Ok(Event::Error(common::DatabaseError::NotFound)) => {
                    Err("NotFound: unknown name in database".to_string())
                }
                Ok(Event::Error(e)) => Err(format!("Database business error: {}", e)),
                Ok(Event::RpcError(e)) => Err(format!("RPC error: {}", e)),
                Ok(Event::Complete) | Err(_) => Err("Stream ended without a value".to_string()),
            }
        }
        ("DatabaseService", "get_person") => {
            let nom: String = extract_field(&args, "nom")?;
            let prenom: String = extract_field(&args, "prenom")?;
            let query = common::PersonneQuery { nom, prenom };

            let proxy = ServiceLocator::global()
                .get::<common::DatabaseServiceProxy>()
                .await
                .ok_or_else(|| "DatabaseService not found (no provider detected)".to_string())?;

            let rx = proxy
                .get_person(query)
                .await
                .map_err(|e| format!("IPC error (get_person): {}", e))?;

            match rx.recv().await {
                Ok(Event::Next(info)) => Ok(serde_json::to_value(&info)
                    .map_err(|e| format!("Failed to serialize PersonneInfo: {}", e))?),
                Ok(Event::Error(common::DatabaseError::NotFound)) => {
                    Err("NotFound: person not found".to_string())
                }
                Ok(Event::Error(e)) => Err(format!("Database business error: {}", e)),
                Ok(Event::RpcError(e)) => Err(format!("RPC error: {}", e)),
                Ok(Event::Complete) | Err(_) => Err("Stream ended without a value".to_string()),
            }
        }

        // ── ConfigService ────────────────────────────────────────────
        ("ConfigService", "get") => {
            let key: String = extract_arg(args, "key")?;

            let proxy = ServiceLocator::global()
                .get::<common::ConfigServiceProxy>()
                .await
                .ok_or_else(|| "ConfigService not found (no provider detected)".to_string())?;

            let rx = proxy
                .get(key)
                .await
                .map_err(|e| format!("IPC error (ConfigService::get): {}", e))?;

            match rx.recv().await {
                Ok(Event::Next(value)) => Ok(serde_json::json!(value)),
                Ok(Event::Error(common::ConfigError::KeyNotFound)) => {
                    Err("KeyNotFound: key not found".to_string())
                }
                Ok(Event::RpcError(e)) => Err(format!("RPC error: {}", e)),
                Ok(Event::Complete) | Err(_) => Err("Stream ended without a value".to_string()),
            }
        }

        // ── HttpService ──────────────────────────────────────────────
        ("HttpService", "send_request") => {
            let request: common::HttpRequestParams = serde_json::from_value(args)
                .map_err(|e| format!("Invalid arguments for send_request: {}", e))?;

            let proxy = ServiceLocator::global()
                .get::<common::HttpServiceProxy>()
                .await
                .ok_or_else(|| "HttpService not found (no provider detected)".to_string())?;

            let rx = proxy
                .send_request(request)
                .await
                .map_err(|e| format!("IPC error (send_request): {}", e))?;

            match rx.recv().await {
                Ok(Event::Next(response)) => Ok(serde_json::to_value(&response)
                    .map_err(|e| format!("Failed to serialize HttpResponseParams: {}", e))?),
                Ok(Event::Error(e)) => Err(format!("Http business error: {}", e)),
                Ok(Event::RpcError(e)) => Err(format!("RPC error: {}", e)),
                Ok(Event::Complete) | Err(_) => Err("Stream ended without a value".to_string()),
            }
        }

        _ => Err(format!(
            "Unknown service '{}' or method '{}'. \
             Available methods: DatabaseService(get_user_age, get_person), \
             ConfigService(get), HttpService(send_request).",
            service, method
        )),
    }
}

/// Extracts a single argument from a JSON value (string, number, etc.).
///
/// If `args` is an object with a single key matching the expected parameter
/// name, the value is extracted. Otherwise, `args` is used directly.
fn extract_arg<T: serde::de::DeserializeOwned>(
    args: Value,
    expected_field: &str,
) -> Result<T, String> {
    // If args is an object with the expected field, extract that field.
    if let Some(obj) = args.as_object() {
        if let Some(field_val) = obj.get(expected_field) {
            return serde_json::from_value(field_val.clone())
                .map_err(|e| format!("Invalid field '{}': {}", expected_field, e));
        }
        // If the object has a single key, use it directly (compatibility).
        if obj.len() == 1 {
            if let Some(val) = obj.values().next() {
                return serde_json::from_value(val.clone())
                    .map_err(|e| format!("Invalid argument: {}", e));
            }
        }
    }
    // Fallback: args is the value itself (e.g. a simple string).
    serde_json::from_value(args)
        .map_err(|e| format!("Invalid argument (expected '{}'): {}", expected_field, e))
}

/// Extracts a named field from a JSON object.
fn extract_field<T: serde::de::DeserializeOwned>(args: &Value, field: &str) -> Result<T, String> {
    args.get(field)
        .ok_or_else(|| format!("Missing field '{}' in the arguments", field))
        .and_then(|v| {
            serde_json::from_value(v.clone())
                .map_err(|e| format!("Invalid field '{}': {}", field, e))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_arg_from_object_with_field() {
        let args = serde_json::json!({"name": "Alice"});
        let result: String = extract_arg(args, "name").unwrap();
        assert_eq!(result, "Alice");
    }

    #[test]
    fn extract_arg_from_plain_string() {
        let args = serde_json::json!("Alice");
        let result: String = extract_arg(args, "name").unwrap();
        assert_eq!(result, "Alice");
    }

    #[test]
    fn extract_arg_from_single_key_object() {
        let args = serde_json::json!({"nom": "Dupont"});
        let result: String = extract_arg(args, "name").unwrap();
        assert_eq!(result, "Dupont");
    }

    #[test]
    fn extract_field_from_object() {
        let args = serde_json::json!({"nom": "Dupont", "prenom": "Jean"});
        let nom: String = extract_field(&args, "nom").unwrap();
        let prenom: String = extract_field(&args, "prenom").unwrap();
        assert_eq!(nom, "Dupont");
        assert_eq!(prenom, "Jean");
    }

    #[test]
    fn extract_field_missing() {
        let args = serde_json::json!({"nom": "Dupont"});
        let result: Result<String, _> = extract_field(&args, "prenom");
        assert!(result.is_err());
    }
}
