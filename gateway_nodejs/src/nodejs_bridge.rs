//! The JS ↔ Rust bridge for one gateway process.
//!
//! A `#[service]` proxy in `ProviderJson` mode receives an IPC request on the
//! transport dispatch thread, converts it to a native JS value (serde-json, no
//! `JSON.parse` round trip) and hands it to the single JS dispatcher through a
//! ThreadsafeFunction. The dispatcher answers asynchronously, either with
//! [`emit`](NodeJsBridge::emit_event) for an intermediate event or with
//! [`resolve`](NodeJsBridge::resolve) for the terminal one, which closes the
//! call.
//!
//! # Flow
//!
//! ```text
//! IPC (rkyv) -> generated handler -> deserialize -> Value -> start_call() -> JS
//! JS -> emit()/resolve() -> Value -> serialize -> rkyv -> IPC (one sample per event)
//! ```
//!
//! The bridge holds no global: the gateway owns one instance
//! ([`crate::state`]) and hands out `Arc`s, so the pending table disappears with
//! the gateway instead of surviving a `shutdown`.

use crate::error::{GatewayError, GatewayErrorCode};
use ice_rpc::json::{JsonCallStream, JsonDispatcher};
use napi::bindgen_prelude::{Function, Unknown};
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

/// A correlation id: 16 bytes, rendered as a UUID-like hex string in JS.
pub type CorrelationId = [u8; 16];

/// Maximum number of calls waiting for their first JS event at the same time.
///
/// A JS dispatcher that never answers would otherwise grow the table without
/// bound. Reaching the limit is reported as `E_PENDING_LIMIT` rather than
/// silently dropping the oldest call, which would strand its caller.
pub const MAX_PENDING_CALLS: usize = 4096;

/// Typed JS callback for the Node.js dispatcher.
///
/// Built with `callee_handled::<true>()`, so JavaScript receives `(err, call)`
/// where `call` is `{ correlationId, service, method, args }` and `args` is a
/// native JS value.
pub type NodeJsCallback = ThreadsafeFunction<Value>;

type PendingSender = ice_rpc::gen::async_channel::Sender<Value>;

/// Bookkeeping of the calls JavaScript has not closed yet.
///
/// Kept separate from the ThreadsafeFunction so the admission rules (bounded
/// table, no duplicate id, one sender per call) are unit-testable without a JS
/// engine.
#[derive(Default)]
pub(crate) struct PendingCalls {
    entries: HashMap<CorrelationId, PendingSender>,
}

impl PendingCalls {
    /// Admits a new call.
    ///
    /// # Errors
    /// `E_PENDING_LIMIT` when the table is full, `E_DUPLICATE_CID` when this id
    /// is already open.
    pub(crate) fn insert(
        &mut self,
        cid: CorrelationId,
        sender: PendingSender,
    ) -> Result<(), GatewayError> {
        if self.entries.len() >= MAX_PENDING_CALLS {
            return Err(GatewayError::new(
                GatewayErrorCode::PendingLimit,
                format!("{MAX_PENDING_CALLS} calls already await a JavaScript answer"),
            ));
        }
        if self.entries.contains_key(&cid) {
            return Err(GatewayError::new(
                GatewayErrorCode::DuplicateCid,
                format!(
                    "correlation id '{}' is already in flight",
                    ice_rpc::gen::fmt_correlation_id(&cid)
                ),
            ));
        }
        self.entries.insert(cid, sender);
        Ok(())
    }

    /// Borrows the sender of an open call, without closing it.
    pub(crate) fn sender(&self, cid: &CorrelationId) -> Option<PendingSender> {
        self.entries.get(cid).cloned()
    }

    /// Removes the entry for `cid`, handing its sender back to the caller.
    ///
    /// Dropping that sender is what closes the call: the generated handler stops
    /// reading events and the transport answers.
    pub(crate) fn take(&mut self, cid: &CorrelationId) -> Option<PendingSender> {
        self.entries.remove(cid)
    }

    /// Number of calls currently open.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

/// The bridge of one gateway: one JS dispatcher, one table of open calls.
pub struct NodeJsBridge {
    callback: NodeJsCallback,
    pending: Mutex<PendingCalls>,
}

impl NodeJsBridge {
    /// Wraps the JS dispatcher in a ThreadsafeFunction.
    ///
    /// # Errors
    /// Propagates the N-API failure when the function cannot be made
    /// thread-safe (e.g. the JS runtime is shutting down).
    pub fn new(js_func: Function<'_, Value, Unknown<'static>>) -> napi::Result<Self> {
        let callback: NodeJsCallback = js_func
            .build_threadsafe_function::<Value>()
            .callee_handled::<true>()
            .build()?;

        Ok(Self {
            callback,
            pending: Mutex::new(PendingCalls::default()),
        })
    }

    /// Locks the pending table, recovering from a poisoned mutex.
    ///
    /// `panic = "abort"` would turn a poisoning `expect` into a process abort,
    /// and a bridge that cannot be read is worse than a bridge read in an
    /// unknown-but-consistent state.
    fn pending(&self) -> MutexGuard<'_, PendingCalls> {
        self.pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Hands a call to the JS dispatcher and returns the stream of its events.
    ///
    /// Does **not** wait: the events arrive later, through [`Self::emit_event`]
    /// and [`Self::resolve`]. The generated handler reads them and emits one
    /// wire sample per event, which is what makes a multi-value `Observable`
    /// servable from JavaScript. The deadline for the *first* event lives in
    /// `ice_rpc::json::JSON_CALL_TIMEOUT`.
    ///
    /// Runs on the transport dispatch thread (the caller is the generated
    /// `ProviderJson` handler), never on the Node.js main thread.
    ///
    /// # Errors
    /// `E_PENDING_LIMIT`, `E_DUPLICATE_CID`, or `E_CALLBACK` when the dispatcher
    /// cannot be reached.
    pub fn start_call(
        &self,
        cid: CorrelationId,
        service: &str,
        method: &str,
        args: Value,
    ) -> Result<JsonCallStream, GatewayError> {
        let (stream, sender) = JsonCallStream::channel();
        self.pending().insert(cid, sender)?;

        let call_data = serde_json::json!({
            "correlationId": ice_rpc::gen::fmt_correlation_id(&cid),
            "service": service,
            "method": method,
            "args": args,
        });

        let status = self
            .callback
            .call(Ok(call_data), ThreadsafeFunctionCallMode::NonBlocking);
        if status != napi::Status::Ok {
            self.pending().take(&cid);
            return Err(GatewayError::new(
                GatewayErrorCode::Callback,
                format!("the JS dispatcher is not reachable ({status:?})"),
            ));
        }

        Ok(stream)
    }

    /// Pushes one intermediate event, keeping the call open.
    ///
    /// # Errors
    /// `E_INVALID_CID`, `E_UNKNOWN_CID`, or `E_CALLBACK` when the call was
    /// already closed.
    pub fn emit_event(&self, correlation_id_hex: &str, event: Value) -> Result<(), GatewayError> {
        let cid = parse_cid(correlation_id_hex)?;
        let sender = self
            .pending()
            .sender(&cid)
            .ok_or_else(|| unknown_cid(correlation_id_hex))?;
        sender.try_send(event).map_err(|_| {
            GatewayError::new(
                GatewayErrorCode::Callback,
                "the call was closed while pushing an event",
            )
        })
    }

    /// Answers a pending call with its terminal event and closes it.
    ///
    /// # Errors
    /// `E_INVALID_CID`, `E_UNKNOWN_CID` when no call matches it (already
    /// answered or expired).
    pub fn resolve(&self, correlation_id_hex: &str, event: Value) -> Result<(), GatewayError> {
        let cid = parse_cid(correlation_id_hex)?;
        let sender = self
            .pending()
            .take(&cid)
            .ok_or_else(|| unknown_cid(correlation_id_hex))?;
        // Best effort: a closed receiver only means the caller already gave up
        // (deadline), which is not the answering side's problem.
        let _ = sender.try_send(event);
        Ok(())
    }
}

/// The bridge, seen by the generated JSON dispatcher.
///
/// Registered once, at startup, with
/// [`ice_rpc::json::set_json_dispatcher`]. The **running** bridge is
/// read from the state on every call, so a restart swaps it without
/// re-registering — and a dispatcher that outlives its gateway reports the state
/// error instead of touching a bridge that is gone.
pub(crate) struct GatewayJsonDispatcher;

#[async_trait::async_trait]
impl JsonDispatcher for GatewayJsonDispatcher {
    async fn dispatch_json(
        &self,
        cid: CorrelationId,
        service: &str,
        method: &str,
        args: Value,
    ) -> Result<JsonCallStream, String> {
        let bridge = crate::state::bridge().map_err(|error| error.rendered())?;
        bridge
            .start_call(cid, service, method, args)
            .map_err(|error| error.rendered())
    }
}

/// Parses a correlation id, reporting the failure with its own code.
fn parse_cid(correlation_id_hex: &str) -> Result<CorrelationId, GatewayError> {
    ice_rpc::gen::parse_correlation_id(correlation_id_hex).ok_or_else(|| {
        GatewayError::new(
            GatewayErrorCode::InvalidCid,
            format!("'{correlation_id_hex}' is not a hexadecimal correlation id"),
        )
    })
}

/// Builds the `E_UNKNOWN_CID` failure.
fn unknown_cid(correlation_id_hex: &str) -> GatewayError {
    GatewayError::new(
        GatewayErrorCode::UnknownCid,
        format!("no pending call for '{correlation_id_hex}' (expired or already answered)"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sender() -> PendingSender {
        let (tx, rx) = ice_rpc::gen::async_channel::unbounded::<Value>();
        // The receiver is kept alive by leaking it: `insert` only needs a live
        // sender, and a dropped receiver would turn every send into a no-op.
        Box::leak(Box::new(rx));
        tx
    }

    #[test]
    fn a_call_is_admitted_then_closed_once() {
        let mut pending = PendingCalls::default();
        let cid = [7u8; 16];
        pending.insert(cid, sender()).expect("fresh id is admitted");
        assert_eq!(pending.len(), 1);

        // Borrowing keeps the call open: that is what `emitNodejsEvent` does.
        assert!(pending.sender(&cid).is_some());
        assert_eq!(pending.len(), 1);

        assert!(pending.take(&cid).is_some());
        assert!(pending.take(&cid).is_none(), "a call closes only once");
        assert_eq!(pending.len(), 0);
    }

    #[test]
    fn a_duplicate_correlation_id_is_refused() {
        let mut pending = PendingCalls::default();
        let cid = [9u8; 16];
        pending.insert(cid, sender()).expect("first is admitted");
        let error = pending
            .insert(cid, sender())
            .expect_err("the duplicate must be refused");
        assert_eq!(error.code(), GatewayErrorCode::DuplicateCid);
        assert_eq!(pending.len(), 1, "the refused insert changed nothing");
    }

    #[test]
    fn the_table_is_bounded() {
        let mut pending = PendingCalls::default();
        for index in 0..MAX_PENDING_CALLS {
            let mut cid = [0u8; 16];
            cid[..8].copy_from_slice(&(index as u64).to_be_bytes());
            pending.insert(cid, sender()).expect("below the limit");
        }
        let error = pending
            .insert([0xffu8; 16], sender())
            .expect_err("the limit must be enforced");
        assert_eq!(error.code(), GatewayErrorCode::PendingLimit);
        assert_eq!(pending.len(), MAX_PENDING_CALLS);
    }

    #[test]
    fn a_malformed_correlation_id_is_an_invalid_cid() {
        let error = parse_cid("not-a-correlation-id").expect_err("malformed id");
        assert_eq!(error.code(), GatewayErrorCode::InvalidCid);
    }
}
