//! N-API entry point for the ice-rpc Node.js gateway.
//!
//! # Architecture
//!
//! - one shared iceoryx2 node, owned by the core ([`ice_rpc::locator`]);
//! - one `NodeJsBridge` per gateway, holding the
//!   single JS dispatcher and the calls JavaScript has not closed yet;
//! - one `ProviderJson` proxy per service registered from JavaScript;
//! - `state` owns the lifecycle, so `init` and `shutdown` can be replayed.
//!
//! # Node.js API
//!
//! ```javascript
//! const gateway = require('gateway-nodejs');
//!
//! gateway.registerService('ContextService');   // this process provides it
//! gateway.init((err, call) => {                // one dispatcher for all services
//!     // call = { correlationId, service, method, args }
//!     gateway.resolveNodejsCall(call.correlationId, { type: 'next', data: 42 });
//! });
//!
//! const age = await gateway.callService('DatabaseService', 'get_user_age', 'Alice');
//! const values = await gateway.callServiceStream('NotificationService', 'watch', 3);
//! await gateway.shutdown();
//! ```
//!
//! A served method may emit **several** events: `emitNodejsEvent` pushes an
//! intermediate one and keeps the call open, `resolveNodejsCall` sends the
//! terminal one and closes it. Each event becomes its own wire sample, so a
//! multi-value `Observable` is servable from JavaScript.
//!
//! The full contract (argument convention, event envelope, error codes and the
//! lifecycle state machine) is in `docs/nodejs-gateway-api-v2.md`; `Readme.md`
//! next to this crate is the user-facing guide.
//!
//! # Threading
//!
//! `callService` and `callServiceStream` run their IPC call on the libuv thread
//! pool, so they never block the Node.js event loop. The reverse direction does
//! block: the transport dispatch thread waits for the JS dispatcher, which is
//! why that wait is bounded
//! (`ice_rpc::json::JSON_CALL_TIMEOUT`) and the pending table is
//! capped.

#![cfg_attr(test, allow(clippy::unwrap_used))] // test code may panic

mod consumer;
mod error;
mod nodejs_bridge;
mod services;
mod state;

use error::GatewayError;
use napi::bindgen_prelude::{AsyncTask, Env, Function, Unknown};
use napi_derive::napi;
use serde_json::Value;

/// Registers a Node.js provider service by its logical name.
///
/// To call BEFORE [`init`]. The accepted names are the ones this gateway
/// maintains ([`services::maintained_services!`]), so a name it does not serve is
/// refused with `E_UNKNOWN_SERVICE`, and the list is checked against the generated
/// proxies when the gateway is built.
///
/// # JS signature
/// `registerService(serviceName: string): void`
///
/// # Errors
/// Throws `E_UNKNOWN_SERVICE` or `E_GATEWAY_STATE`.
#[napi]
pub fn register_service(service_name: String) -> napi::Result<()> {
    services::register_service(&service_name).map_err(napi::Error::from)
}

/// Initializes the gateway: bridge, dispatch loop, then service announcement.
///
/// # JS signature
/// `init(callback: (err, call) => void): void`
///
/// # Arguments
/// * `callback` — JS function called on each incoming IPC request. The Rust
///   side builds the ThreadsafeFunction with `callee_handled::<true>()`, so the
///   JavaScript signature is `(err, call)`.
///
/// # Errors
/// Throws `E_GATEWAY_STATE` when the gateway already runs, or propagates the
/// N-API failure if the callback cannot be made thread-safe.
#[napi]
pub fn init(callback: Function<'_, Value, Unknown<'static>>) -> napi::Result<()> {
    log::info!("Initializing the NodeJS gateway...");

    // Built (and possibly rejected) before any state change, so a failed
    // `init` leaves the previous run untouched.
    let bridge = std::sync::Arc::new(nodejs_bridge::NodeJsBridge::new(callback)?);

    // Reserve the transition BEFORE creating the guard: dropping a
    // `ShutdownGuard` cancels the ice-rpc tokens, which must not happen to a
    // gateway that is already running.
    state::begin_start().map_err(napi::Error::from)?;
    let guard = ice_rpc::gen::init_without_ctrl_c();
    state::complete_start(bridge, guard);

    // Registered once: the host reads the *running* bridge from the state on
    // every call, so a restart swaps it without re-registering. The generated
    // dispatcher runs on the transport dispatch thread, which then reads the
    // call's events until JavaScript closes it.
    ice_rpc::json::set_json_dispatcher(std::sync::Arc::new(nodejs_bridge::GatewayJsonDispatcher));

    log::info!(
        "iceoryx2 native transport ready (pid={}).",
        std::process::id()
    );

    services::spawn_initialize_all();

    log::info!("NodeJS gateway ready.");
    Ok(())
}

/// Wrapper letting an async task hand a `serde_json::Value` to JavaScript.
///
/// `serde_json::Value` implements `ToNapiValue` but not `TypeName`, while
/// `napi::Task::JsValue` requires both. The newtype supplies the missing one and
/// delegates the conversion, so the value is not re-encoded on the way out.
pub struct JsonValue(Value);

impl napi::bindgen_prelude::TypeName for JsonValue {
    fn type_name() -> &'static str {
        "any"
    }

    fn value_type() -> napi::ValueType {
        napi::ValueType::Unknown
    }
}

impl napi::bindgen_prelude::ToNapiValue for JsonValue {
    unsafe fn to_napi_value(
        env: napi::sys::napi_env,
        val: Self,
    ) -> napi::Result<napi::sys::napi_value> {
        // SAFETY: the caller upholds the `napi_env` contract documented on the
        // trait; this delegates to the very conversion the synchronous
        // `serde_json::Value` return path uses.
        unsafe { <Value as napi::bindgen_prelude::ToNapiValue>::to_napi_value(env, val.0) }
    }
}

/// Runs one `callService` on the libuv thread pool.
///
/// `compute` owns the blocking IPC call; `resolve` hands the value back to the
/// Node.js main thread. This split is what keeps the event loop free — the
/// synchronous version froze it for the whole call, including the 30 s failure
/// path measured in `plans/baseline/BASELINE.md`.
pub struct CallServiceTask {
    service: String,
    method: String,
    args: Option<Value>,
}

impl napi::Task for CallServiceTask {
    type Output = Result<Value, GatewayError>;
    type JsValue = JsonValue;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let args = self.args.take().unwrap_or(Value::Null);
        Ok(ice_rpc::rt::block_on(consumer::call_ipc_method(
            &self.service,
            &self.method,
            args,
        )))
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        output.map(JsonValue).map_err(napi::Error::from)
    }
}

/// Calls a method of a remote IPC service from Node.js.
///
/// The call is executed on the libuv thread pool through the runtime-agnostic
/// executor of `ice_rpc::rt`, so the Node.js event loop is never blocked.
///
/// # JS signature
/// `callService(serviceName: string, methodName: string, args: any): Promise<any>`
///
/// # Example
/// ```javascript
/// const age = await gateway.callService('DatabaseService', 'get_user_age', 'Alice');
/// ```
///
/// # Returns
/// A promise resolved with the first `Next` event, rejected with a coded
/// gateway error (see `docs/nodejs-gateway-api-v2.md`). For a method that
/// streams several values, use [`call_service_stream`]: this one answers as soon
/// as the first value is available, which is what lets it serve an endless
/// stream.
#[napi]
pub fn call_service(
    service_name: String,
    method_name: String,
    args: Value,
) -> napi::Result<AsyncTask<CallServiceTask>> {
    Ok(AsyncTask::new(CallServiceTask {
        service: service_name,
        method: method_name,
        args: Some(args),
    }))
}

/// Runs one `callServiceStream` on the libuv thread pool.
pub struct CallServiceStreamTask {
    service: String,
    method: String,
    args: Option<Value>,
}

impl napi::Task for CallServiceStreamTask {
    type Output = Result<Vec<Value>, GatewayError>;
    type JsValue = JsonValue;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let args = self.args.take().unwrap_or(Value::Null);
        Ok(ice_rpc::rt::block_on(consumer::call_ipc_method_stream(
            &self.service,
            &self.method,
            args,
        )))
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        // The array is handed over as a JSON array: `JsonValue` already knows how
        // to convert it, so the values are not re-encoded one by one.
        output
            .map(|values| JsonValue(Value::Array(values)))
            .map_err(napi::Error::from)
    }
}

/// Calls a method of a remote IPC service and waits for its stream to end.
///
/// # JS signature
/// `callServiceStream(serviceName: string, methodName: string, args: any): Promise<any[]>`
///
/// # Example
/// ```javascript
/// const values = await gateway.callServiceStream('NotificationService', 'watch', 3);
/// ```
///
/// # Returns
/// A promise resolved with **every** `Next` value the service emitted, rejected
/// with a coded gateway error. An empty array means the service completed
/// without a value, which is not an error here — unlike [`call_service`].
#[napi]
pub fn call_service_stream(
    service_name: String,
    method_name: String,
    args: Value,
) -> napi::Result<AsyncTask<CallServiceStreamTask>> {
    Ok(AsyncTask::new(CallServiceStreamTask {
        service: service_name,
        method: method_name,
        args: Some(args),
    }))
}

/// Releases the IPC resources, on the libuv thread pool.
pub struct ShutdownTask;

impl napi::Task for ShutdownTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> napi::Result<Self::Output> {
        // Take the guard out first: the gateway is no longer running, and
        // releasing the resources must not race a new `init`.
        let guard = state::stop();
        ice_rpc::rt::block_on(ice_rpc::gen::shutdown_and_release());
        drop(guard);
        Ok(())
    }

    fn resolve(&mut self, _env: Env, _output: ()) -> napi::Result<Self::JsValue> {
        Ok(())
    }
}

/// Stops the gateway and releases the IPC resources.
///
/// # JS signature
/// `shutdown(): Promise<void>`
///
/// # Returns
/// A promise resolved once the resources are released. `init` may then be
/// called again.
#[napi]
pub fn shutdown() -> napi::Result<AsyncTask<ShutdownTask>> {
    log::info!("Stopping the gateway...");
    Ok(AsyncTask::new(ShutdownTask))
}

/// Pushes an intermediate event of a call, keeping it open.
///
/// Every event becomes its own wire sample, so calling this `n` times before
/// [`resolve_nodejs_call`] turns one JavaScript answer into an `n`-value stream
/// — which is how a multi-value `Observable` is served from JavaScript.
///
/// # JS signature
/// `emitNodejsEvent(correlationId: string, event: { type, data }): void`
///
/// # Errors
/// Throws `E_INVALID_CID`, `E_UNKNOWN_CID` when the call is not open (already
/// answered, or its deadline passed), or `E_CALLBACK` when it was closed
/// meanwhile.
#[napi]
pub fn emit_nodejs_event(correlation_id_hex: String, event: Value) -> napi::Result<()> {
    state::bridge()
        .and_then(|bridge| bridge.emit_event(&correlation_id_hex, event))
        .map_err(napi::Error::from)
}

/// Answers a call with its terminal event and closes it.
///
/// # JS signature
/// `resolveNodejsCall(correlationId: string, event: { type, data }): void`
///
/// # Errors
/// Throws `E_INVALID_CID` on a malformed id and `E_UNKNOWN_CID` when no call
/// matches it (expired or already answered).
#[napi]
pub fn resolve_nodejs_call(correlation_id_hex: String, event: Value) -> napi::Result<()> {
    state::bridge()
        .and_then(|bridge| bridge.resolve(&correlation_id_hex, event))
        .map_err(napi::Error::from)
}

/// Returns the gateway, core and protocol versions actually compiled in.
///
/// Every value is read from the build, never written by hand: the previous
/// implementation reported a hardcoded `ice-rpc v0.1.0, iceoryx2 v0.9` while the
/// workspace was 0.3.2 and the manifest pinned `iceoryx2` 0.10.
#[napi]
pub fn version() -> String {
    format!(
        "gateway-nodejs v{} (ice-rpc v{}, protocol v{})",
        env!("CARGO_PKG_VERSION"),
        ice_rpc::gen::VERSION,
        ice_rpc::gen::PROTOCOL_VERSION,
    )
}
