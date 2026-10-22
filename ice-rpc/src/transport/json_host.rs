//! The provider-side JSON handler, written once for every service.
//!
//! `#[service]` used to emit this body — the call context, the argument
//! conversion, the dispatcher lookup, the event loop and the four failure paths —
//! inside the closure of **every** method. The body is service-agnostic: only two
//! things vary per method, and both already exist as non-capturing associated
//! functions on the proxy. Passing them as function pointers keeps one copy of
//! the handler per binary instead of one per method, and turns each generated
//! closure into a call.

use rkyv::util::AlignedVec;

use super::bridge::{reject_json_dispatch, reject_json_request, reject_json_response};
use super::{BoxResponseFuture, OwnedEmitter, PAYLOAD_ALIGNMENT};
use crate::json::{JsonCallEvent, dispatch_json};
use crate::types::{CallContext, EventKind, RpcHeader};

/// `{Proxy}::deserialize_request_to_value`, as a plain function pointer.
///
/// Higher-ranked over both borrows: the request bytes are only valid during the
/// dispatch, and the method name is a `&'static str` in practice.
pub type RequestToValue = for<'a, 'b> fn(&'a str, &'b [u8]) -> Option<serde_json::Value>;

/// `{Proxy}::serialize_response_from_value`, as a plain function pointer.
pub type ValueToResponse =
    fn(&str, serde_json::Value) -> Option<(EventKind, AlignedVec<PAYLOAD_ALIGNMENT>)>;

/// Serves one JSON call, answering with the host's events.
///
/// A call may carry **several** events: the host pushes intermediate ones and
/// closes with the terminal one, so a multi-value `Observable` is servable. Each
/// event becomes its own wire sample.
///
/// The whole body runs inside `call_scoped`, so `CallContext::current()` works
/// inside a JSON-served method, and a call it emits continues the incoming trace
/// instead of starting a new root.
pub fn json_host_handler(
    service: &'static str,
    method: &'static str,
    header: RpcHeader,
    payload: &[u8],
    emitter: OwnedEmitter,
    request_to_value: RequestToValue,
    value_to_response: ValueToResponse,
) -> BoxResponseFuture {
    // Built before the coroutine: it is copied into the task, and the header
    // itself is not needed past this point.
    let ctx = CallContext::new(&header, service, method);
    // Decoded **synchronously**, straight from the received sample: the borrow
    // ends here, and only the JSON value enters the task, which outlives the
    // sample.
    let args = request_to_value(method, payload);
    let correlation_id = header.correlation_id;

    crate::types::call_scoped(ctx, async move {
        let mut emitter = emitter;
        let Some(args) = args else {
            reject_json_request(service, method, &mut *emitter);
            return;
        };
        // The correlation id is the real one now that the handler receives
        // the header: the host can correlate its logs.
        let mut events = match dispatch_json(correlation_id, service, method, args) {
            Ok(events) => events,
            Err(e) => {
                reject_json_dispatch(service, method, e, &mut *emitter);
                return;
            }
        };
        // The loop ends when the host closes the call.
        while let Some(event) = events.next().await {
            let value = match event {
                JsonCallEvent::Event(value) => value,
                JsonCallEvent::Failed(message) => {
                    reject_json_dispatch(service, method, message, &mut *emitter);
                    return;
                }
            };
            match value_to_response(method, value) {
                Some((kind, sample)) => {
                    emitter.emit(kind, &sample);
                }
                // An event that cannot be encoded would leave the call
                // unanswered: it is reported instead of dropped, and the
                // stream stops here.
                None => {
                    reject_json_response(service, method, &mut *emitter);
                    return;
                }
            }
        }
    })
}

/// Serves one **one-way** JSON call.
///
/// Nothing is published, by contract: the host is handed the call, and its
/// answer stream — if it opens one — is dropped, because there is no caller
/// waiting for it. The dispatcher is told the method is one-way, so it registers
/// no in-flight entry either.
pub fn json_host_oneway_handler(
    service: &'static str,
    method: &'static str,
    header: RpcHeader,
    payload: &[u8],
    request_to_value: RequestToValue,
) -> BoxResponseFuture {
    let ctx = CallContext::new(&header, service, method);
    let args = request_to_value(method, payload);
    let correlation_id = header.correlation_id;

    crate::types::call_scoped(ctx, async move {
        let Some(args) = args else {
            log::error!("[{service}::{method}] Failed to deserialize the one-way request");
            return;
        };
        match dispatch_json(correlation_id, service, method, args) {
            // Nothing is emitted, by contract: dropping the stream is how the
            // host learns the call is over.
            Ok(events) => drop(events),
            Err(e) => log::error!("[{service}::{method}] one-way JSON dispatch failed: {e}"),
        }
    })
}
