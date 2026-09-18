//! Bridge from a service `Observable` to the samples the transport publishes.

use std::collections::HashMap;

use rkyv::api::high::to_bytes_in;
use rkyv::util::AlignedVec;

use crate::types::{EventKind, Observable, RpcError, ServiceRef, WireEvent};

/// Sink of the encoded responses of one RPC method.
///
/// The producer **pushes** each sample into the sink instead of returning an
/// iterator: the encoded bytes stay in the producer's scratch buffer, so a
/// response reaches the transport with no allocation and no copy. An
/// `Iterator<Item = (EventKind, &[u8])>` cannot express that borrow, and
/// yielding owned vectors would cost one allocation plus one copy per response.
///
/// `emit` returns `false` when the sink asks the producer to stop, which the
/// transport uses when a response could not be published.
pub trait ResponseEmitter {
    /// Emits one sample, labelled with its real [`EventKind`].
    fn emit(&mut self, kind: EventKind, payload: &[u8]) -> bool;
}

/// A [`ResponseEmitter`] that collects the samples in memory.
///
/// For tests, examples and tooling that need the encoded samples rather than a
/// transport. Each sample is copied once, which is the price of collecting it.
#[derive(Default)]
pub struct CollectEmitter {
    samples: Vec<(EventKind, Vec<u8>)>,
}

impl CollectEmitter {
    /// Creates an empty collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes the collected samples, leaving the collector empty.
    pub fn take(&mut self) -> Vec<(EventKind, Vec<u8>)> {
        std::mem::take(&mut self.samples)
    }
}

impl ResponseEmitter for CollectEmitter {
    fn emit(&mut self, kind: EventKind, payload: &[u8]) -> bool {
        self.samples.push((kind, payload.to_vec()));
        true
    }
}

/// Drives `observable` into `emitter`, encoding each event as it arrives.
///
/// The events are taken raw (`recv_wire`), which preserves the `CompleteWith`
/// single-sample optimization. The [`EventKind`] of each sample is derived from
/// the wire variant before serialization, so the transport can stamp it in the
/// zero-copy header and an out-of-band observer can label the response without
/// decoding the payload.
///
/// A technical error is framed as a **bare [`RpcError`]** labelled
/// [`EventKind::RpcError`], like every transport-level rejection: it must be
/// decodable without knowing the service types. Every other kind carries the
/// method's `WireEvent<T, E>`.
///
/// One scratch buffer serves the whole stream: encoding `n` responses costs one
/// allocation, and the bytes reach the emitter without being copied. The buffer
/// belongs to this call and is never shared: `benches/concurrency.rs` measures a
/// shared, locked scratch as 20 to 45 times slower than a local one.
pub fn observable_to_responses<T, E>(
    mut observable: Observable<T, E>,
    emitter: &mut dyn ResponseEmitter,
) where
    T: Send + 'static,
    E: Send + 'static,
    for<'a> WireEvent<T, E>: rkyv::Serialize<
        rkyv::rancor::Strategy<
            rkyv::ser::Serializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::ser::sharing::Share,
            >,
            rkyv::rancor::Error,
        >,
    >,
{
    let mut scratch = AlignedVec::<16>::with_capacity(256);

    loop {
        let wire = match crate::rt::block_on(observable.recv_wire()) {
            Ok(wire) => wire,
            // The source is exhausted, or closed abruptly.
            Err(_) => return,
        };
        // The writer appends: the buffer must be emptied before each sample, and
        // it comes back from the call, so its allocation is reused. `AlignedVec`
        // is a pointer, a length and a capacity: passing it by value costs
        // nothing, and the encoded bytes reach the emitter without a copy.
        scratch.clear();

        // Two framings: a bare `RpcError` for a technical error, the service's
        // `WireEvent<T, E>` for everything else.
        let framed: (EventKind, Result<AlignedVec<16>, rkyv::rancor::Error>) = match &wire {
            WireEvent::RpcError(err) => (EventKind::RpcError, to_bytes_in(err, scratch)),
            _ => (wire.kind(), to_bytes_in(&wire, scratch)),
        };
        let (kind, result) = framed;

        match result {
            Ok(bytes) => {
                if !emitter.emit(kind, &bytes) {
                    return;
                }
                scratch = bytes;
            }
            Err(e) => {
                // The stream must end here: a response that cannot be encoded
                // would otherwise leave the call unanswered.
                log::error!("[bridge] response serialization failed: {e:?}");
                return;
            }
        }
    }
}

/// Handler of one RPC method: decoded payload plus the sink to push into.
pub type MethodHandler = Box<dyn Fn(&[u8], &mut dyn ResponseEmitter) + Send + Sync>;

/// Per-service table of method handlers, built by a generated provider.
#[derive(Default)]
pub struct ServiceDispatcher {
    /// Identity (id + interface version) of the service contract.
    service: ServiceRef,
    handlers: HashMap<&'static str, MethodHandler>,
}

impl ServiceDispatcher {
    /// Creates an empty dispatcher for `service`.
    pub fn new(service: ServiceRef) -> Self {
        Self {
            service,
            handlers: HashMap::new(),
        }
    }

    /// Returns the identity this dispatcher answers for.
    pub fn service(&self) -> ServiceRef {
        self.service
    }

    /// Registers the handler of one RPC method.
    pub fn method<F>(&mut self, name: &'static str, handler: F) -> &mut Self
    where
        F: Fn(&[u8], &mut dyn ResponseEmitter) + Send + Sync + 'static,
    {
        self.handlers.insert(name, Box::new(handler));
        self
    }

    /// Routes a request to its handler.
    ///
    /// Returns `false` when no handler is registered for `method`, which the
    /// transport turns into an immediate [`crate::types::RpcError::UnknownMethod`]
    /// — a silent drop would only reach the caller as a timeout. One lookup: the
    /// transport needs no separate `has_method` probe.
    pub fn dispatch(
        &self,
        method: &str,
        payload: &[u8],
        emitter: &mut dyn ResponseEmitter,
    ) -> bool {
        match self.handlers.get(method) {
            Some(handler) => {
                handler(payload, emitter);
                true
            }
            None => false,
        }
    }
}

/// Encodes a transport-level technical error and emits it.
///
/// The payload is the archived [`RpcError`] **alone**, labelled
/// [`EventKind::RpcError`] — never a `WireEvent<T, E>`. A rejection says nothing
/// about the service types, and the provider cannot name them at all for a
/// method it does not have, so a generic framing could not answer those calls.
pub fn emit_rpc_error(err: RpcError, emitter: &mut dyn ResponseEmitter) -> bool {
    let scratch = AlignedVec::<16>::with_capacity(256);
    let encoded: Result<AlignedVec<16>, rkyv::rancor::Error> = to_bytes_in(&err, scratch);
    match encoded {
        Ok(bytes) => emitter.emit(EventKind::RpcError, &bytes),
        Err(e) => {
            log::error!("[bridge] rpc error serialization failed: {e:?}");
            false
        }
    }
}
