//! The N-API implementations the `gateway!` macro wraps.
//!
//! Every function here is transport-facing but **not** N-API-annotated: the
//! `#[napi]` attribute belongs to the addon (the cdylib), where the macro emits
//! the thin wrappers. Keeping the bodies here means the addon carries no logic
//! of its own — it names its services and delegates.

use crate::error::GatewayError;
use crate::services::GatewayService;
use crate::{bridge, consumer, services, state};
use napi::bindgen_prelude::{AsyncTask, Env, Function, Unknown};
use serde_json::Value;

/// Wrapper letting an async task hand a `serde_json::Value` to JavaScript.
///
/// `serde_json::Value` implements `ToNapiValue` but not `TypeName`, while
/// `napi::Task::JsValue` requires both. The newtype supplies the missing one and
/// delegates the conversion, so the value is not re-encoded on the way out.
pub struct JsonValue(pub(crate) Value);

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

/// Registers a Node.js provider service by its logical name.
///
/// Backs the addon's `registerService(name)`.
///
/// # Errors
/// `E_UNKNOWN_SERVICE` when the name is absent from the table,
/// `E_GATEWAY_STATE` when the gateway is already running.
pub fn register_service_impl(
    table: &'static [GatewayService],
    service_name: String,
) -> napi::Result<()> {
    services::register_service(table, &service_name).map_err(napi::Error::from)
}

/// Initializes the gateway: bridge, dispatch loop, then service announcement.
///
/// Backs the addon's `init(callback)`.
///
/// # Errors
/// Throws `E_GATEWAY_STATE` when the gateway already runs, or propagates the
/// N-API failure if the callback cannot be made thread-safe.
pub fn init_impl(callback: Function<'_, Value, Unknown<'static>>) -> napi::Result<()> {
    log::info!("Initializing the NodeJS gateway...");

    // Built (and possibly rejected) before any state change, so a failed
    // `init` leaves the previous run untouched.
    let bridge = std::sync::Arc::new(bridge::NodeJsBridge::new(callback)?);

    // Reserve the transition BEFORE creating the guard: dropping a
    // `ShutdownGuard` cancels the ice-rpc tokens, which must not happen to a
    // gateway that is already running.
    state::begin_start().map_err(napi::Error::from)?;
    let guard = ice_rpc::codegen::init_without_ctrl_c();
    state::complete_start(bridge, guard);

    // Registered once: the host reads the *running* bridge from the state on
    // every call, so a restart swaps it without re-registering. The generated
    // dispatcher runs on the transport dispatch thread, which then reads the
    // call's events until JavaScript closes it.
    ice_rpc::json::set_json_dispatcher(std::sync::Arc::new(bridge::GatewayJsonDispatcher));

    log::info!(
        "iceoryx2 native transport ready (pid={}).",
        std::process::id()
    );

    services::spawn_initialize_all();

    log::info!("NodeJS gateway ready.");
    Ok(())
}

/// Runs one `callService` on the libuv thread pool.
///
/// `compute` owns the blocking IPC call; `resolve` hands the value back to the
/// Node.js main thread. This split is what keeps the event loop free.
pub struct CallServiceTask {
    table: &'static [GatewayService],
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
            self.table,
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
/// Backs the addon's `callService(service, method, args)`. Returns the first
/// value; the multi-value counterpart is [`call_service_stream_impl`].
pub fn call_service_impl(
    table: &'static [GatewayService],
    service_name: String,
    method_name: String,
    args: Value,
) -> napi::Result<AsyncTask<CallServiceTask>> {
    Ok(AsyncTask::new(CallServiceTask {
        table,
        service: service_name,
        method: method_name,
        args: Some(args),
    }))
}

/// Runs one `callServiceStream` on the libuv thread pool.
pub struct CallServiceStreamTask {
    table: &'static [GatewayService],
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
            self.table,
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
/// Backs the addon's `callServiceStream(service, method, args)`. An empty array
/// means the service completed without a value, which is not an error here.
pub fn call_service_stream_impl(
    table: &'static [GatewayService],
    service_name: String,
    method_name: String,
    args: Value,
) -> napi::Result<AsyncTask<CallServiceStreamTask>> {
    Ok(AsyncTask::new(CallServiceStreamTask {
        table,
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
        ice_rpc::rt::block_on(ice_rpc::codegen::shutdown_and_release());
        drop(guard);
        Ok(())
    }

    fn resolve(&mut self, _env: Env, _output: ()) -> napi::Result<Self::JsValue> {
        Ok(())
    }
}

/// Stops the gateway and releases the IPC resources.
///
/// Backs the addon's `shutdown()`. `init` may then be called again.
pub fn shutdown_impl() -> napi::Result<AsyncTask<ShutdownTask>> {
    log::info!("Stopping the gateway...");
    Ok(AsyncTask::new(ShutdownTask))
}

/// Pushes an intermediate event of a call, keeping it open.
///
/// Backs the addon's `emitNodejsEvent(correlationId, event)`. Every event
/// becomes its own wire sample, so calling this `n` times before
/// [`resolve_nodejs_call_impl`] turns one JavaScript answer into an `n`-value
/// stream.
pub fn emit_nodejs_event_impl(correlation_id_hex: String, event: Value) -> napi::Result<()> {
    state::bridge()
        .and_then(|bridge| bridge.emit_event(&correlation_id_hex, event))
        .map_err(napi::Error::from)
}

/// Answers a call with its terminal event and closes it.
///
/// Backs the addon's `resolveNodejsCall(correlationId, event)`.
pub fn resolve_nodejs_call_impl(correlation_id_hex: String, event: Value) -> napi::Result<()> {
    state::bridge()
        .and_then(|bridge| bridge.resolve(&correlation_id_hex, event))
        .map_err(napi::Error::from)
}

/// Builds the version banner from the addon's name and version.
///
/// The addon passes its own `env!("CARGO_PKG_NAME")` / `env!("CARGO_PKG_VERSION")`,
/// so the banner reports the **product** while this crate and `ice-rpc` supply
/// their compiled versions — never a hand-written literal.
pub fn version_string(name: &str, version: &str) -> String {
    format!(
        "{name} v{version} (ice-rpc v{}, protocol v{})",
        ice_rpc::codegen::VERSION,
        ice_rpc::codegen::PROTOCOL_VERSION,
    )
}
