//! The provider-side native handler, written once for every service.
//!
//! `#[service]` used to emit this body inside the closure of **every** method:
//! the call context, the request decoding, the two decode-failure paths, the
//! scope around the polling task and the stream of responses. Only the decode
//! closure and the method call vary per method. Passing them as closures keeps
//! one copy of the handler per binary and turns each generated registration into
//! a call — the same shape [`json_host`](super::json_host) uses for the JSON
//! host.
//!
//! The decode stays synchronous, before the future is built: the payload is
//! borrowed from the received sample, and the future is `'static`.

use std::future::Future;

use rkyv::util::AlignedVec;

use super::bridge::{
    BoxResponseFuture, DecodeFailure, OwnedEmitter, observable_to_responses, reject_undecodable,
    reject_undecodable_oneway,
};
use crate::Observable;
use crate::types::{CallContext, RpcHeader, WireEvent, call_scoped, call_scoped_untraced};

/// Serves one request/response call.
///
/// `decode` turns the received payload into the method's arguments, and `call`
/// invokes the implementation. Every other step — context, scope, failure
/// reporting and response framing — lives here, once per binary.
pub fn native_handler<Req, Ok, Err, D, F, Fut>(
    service: &'static str,
    method: &'static str,
    header: RpcHeader,
    payload: &[u8],
    emitter: OwnedEmitter,
    decode: D,
    call: F,
) -> BoxResponseFuture
where
    Req: Send + 'static,
    D: FnOnce(&[u8]) -> Result<Req, DecodeFailure>,
    F: FnOnce(Req) -> Fut + Send + 'static,
    Fut: Future<Output = Observable<Ok, Err>> + Send + 'static,
    Ok: Send + 'static,
    Err: Send + 'static,
    for<'a> WireEvent<Ok, Err>: rkyv::Serialize<
            rkyv::rancor::Strategy<
                rkyv::ser::Serializer<
                    AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'a>,
                    rkyv::ser::sharing::Share,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    let request = match decode(payload) {
        Ok(request) => request,
        Err(failure) => return reject_undecodable(service, method, failure, emitter),
    };
    let ctx = CallContext::new(&header, service, method);
    call_scoped(ctx, native_body(emitter, request, call))
}

/// The [`native_handler`] of a service declared `#[service(..., no_tracing)]`.
pub fn native_handler_untraced<Req, Ok, Err, D, F, Fut>(
    service: &'static str,
    method: &'static str,
    header: RpcHeader,
    payload: &[u8],
    emitter: OwnedEmitter,
    decode: D,
    call: F,
) -> BoxResponseFuture
where
    Req: Send + 'static,
    D: FnOnce(&[u8]) -> Result<Req, DecodeFailure>,
    F: FnOnce(Req) -> Fut + Send + 'static,
    Fut: Future<Output = Observable<Ok, Err>> + Send + 'static,
    Ok: Send + 'static,
    Err: Send + 'static,
    for<'a> WireEvent<Ok, Err>: rkyv::Serialize<
            rkyv::rancor::Strategy<
                rkyv::ser::Serializer<
                    AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'a>,
                    rkyv::ser::sharing::Share,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    let request = match decode(payload) {
        Ok(request) => request,
        Err(failure) => return reject_undecodable(service, method, failure, emitter),
    };
    let ctx = CallContext::new(&header, service, method);
    call_scoped_untraced(ctx, native_body(emitter, request, call))
}

/// The body both request/response entry points share.
async fn native_body<Req, Ok, Err, F, Fut>(emitter: OwnedEmitter, request: Req, call: F)
where
    Req: Send + 'static,
    F: FnOnce(Req) -> Fut + Send + 'static,
    Fut: Future<Output = Observable<Ok, Err>> + Send + 'static,
    Ok: Send + 'static,
    Err: Send + 'static,
    for<'a> WireEvent<Ok, Err>: rkyv::Serialize<
            rkyv::rancor::Strategy<
                rkyv::ser::Serializer<
                    AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'a>,
                    rkyv::ser::sharing::Share,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    let mut emitter = emitter;
    let stream = call(request).await;
    observable_to_responses(stream, &mut *emitter).await;
}

/// Serves one **one-way** call.
///
/// Nothing is published, by contract: the notification is executed and its
/// result dropped. The emitter is not part of the signature — the transport
/// drops it with the generated closure.
pub fn native_oneway_handler<Req, D, F, Fut>(
    service: &'static str,
    method: &'static str,
    header: RpcHeader,
    payload: &[u8],
    decode: D,
    call: F,
) -> BoxResponseFuture
where
    Req: Send + 'static,
    D: FnOnce(&[u8]) -> Result<Req, DecodeFailure>,
    F: FnOnce(Req) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let request = match decode(payload) {
        Ok(request) => request,
        Err(failure) => return reject_undecodable_oneway(service, method, failure),
    };
    let ctx = CallContext::new(&header, service, method);
    call_scoped(ctx, async move { call(request).await })
}

/// The [`native_oneway_handler`] of a service declared
/// `#[service(..., no_tracing)]`.
pub fn native_oneway_handler_untraced<Req, D, F, Fut>(
    service: &'static str,
    method: &'static str,
    header: RpcHeader,
    payload: &[u8],
    decode: D,
    call: F,
) -> BoxResponseFuture
where
    Req: Send + 'static,
    D: FnOnce(&[u8]) -> Result<Req, DecodeFailure>,
    F: FnOnce(Req) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let request = match decode(payload) {
        Ok(request) => request,
        Err(failure) => return reject_undecodable_oneway(service, method, failure),
    };
    let ctx = CallContext::new(&header, service, method);
    call_scoped_untraced(ctx, async move { call(request).await })
}
