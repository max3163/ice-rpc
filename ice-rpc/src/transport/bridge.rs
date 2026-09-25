//! Bridge from a service `Observable` to the samples the transport publishes.

use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};

use rkyv::api::high::to_bytes_in;
use rkyv::util::AlignedVec;

use super::{PAYLOAD_ALIGNMENT, REQUEST_SCRATCH_CAPACITY};
use crate::types::{Event, EventKind, Observable, RpcError, RpcHeader, ServiceRef, WireEvent};

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

pin_project_lite::pin_project! {
    /// Folds a stream of [`Event`] into the wire events the transport publishes.
    ///
    /// This is the **single** place where the local stream vocabulary becomes the
    /// wire one, and the **single** implementation of the `CompleteWith` rule: a
    /// `Next(value)` immediately followed by a `Complete` is one
    /// [`WireEvent::CompleteWith`] sample instead of two, so a single-response
    /// service travels as one iceoryx2 sample. The channels created by
    /// [`crate::types::channel`] carry plain events; the fold happens on the way
    /// out, where the wire format is known.
    ///
    /// The look-ahead is **non-blocking**: a value is never held back while
    /// waiting for the event that follows it, so a stream that emits a value and
    /// then stays silent is published immediately.
    struct WireFolding<T, E> {
        #[pin]
        source: Observable<T, E>,
        // Event read one step ahead while looking for the `Complete` that closes
        // a single-sample response.
        pending: Option<Event<T, E>>,
    }
}

impl<T, E> WireFolding<T, E> {
    /// Wraps the observable returned by one RPC method.
    fn new(source: Observable<T, E>) -> Self {
        Self {
            source,
            pending: None,
        }
    }

    /// Awaits the next wire event, or `None` when the source is exhausted.
    async fn next_wire(mut self: Pin<&mut Self>) -> Option<WireEvent<T, E>> {
        futures_lite::future::poll_fn(|cx| futures_lite::Stream::poll_next(self.as_mut(), cx)).await
    }
}

impl<T, E> futures_lite::Stream for WireFolding<T, E> {
    /// Wire event, ready to be framed and encoded.
    type Item = WireEvent<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // The event to emit: the one read one step ahead by the previous call, or
        // the next event of the source. Both go through the fold below, so a value
        // is never emitted without its look-ahead — otherwise the last `Next` of a
        // stream would never meet its `Complete`.
        let event = match this.pending.take() {
            Some(event) => event,
            None => match futures_lite::Stream::poll_next(this.source.as_mut(), cx) {
                Poll::Ready(Some(event)) => event,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            },
        };

        match event {
            Event::Next(value) => {
                // Best-effort peek with the same context: when the `Complete` is
                // already available, the pair is one sample.
                match futures_lite::Stream::poll_next(this.source.as_mut(), cx) {
                    Poll::Ready(Some(Event::Complete)) => {
                        Poll::Ready(Some(WireEvent::CompleteWith(value)))
                    }
                    Poll::Ready(other) => {
                        *this.pending = other;
                        Poll::Ready(Some(WireEvent::Next(value)))
                    }
                    Poll::Pending => Poll::Ready(Some(WireEvent::Next(value))),
                }
            }
            other => Poll::Ready(Some(other.into())),
        }
    }
}

/// Drives `observable` into `emitter`, encoding each event as it arrives.
///
/// The events go through `WireFolding`, which preserves the `CompleteWith`
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
///
/// **Async on purpose.** Awaiting the next wire event — instead of blocking on
/// it — is what lets the other calls served on the same executor make progress
/// while this one is silent, and what the emitter's own back-pressure needs.
pub async fn observable_to_responses<T, E, S>(observable: Observable<T, E>, emitter: &mut S)
where
    S: ResponseEmitter + ?Sized,
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
    let mut scratch = AlignedVec::<{ PAYLOAD_ALIGNMENT }>::with_capacity(REQUEST_SCRATCH_CAPACITY);
    // Pinned on the stack: `Observable` is not `Unpin`, and this costs no
    // allocation on the response path.
    let mut stream = std::pin::pin!(WireFolding::new(observable));

    loop {
        // Yields here when the source is silent: the executor runs the other
        // in-flight calls of the same channel meanwhile.
        let wire = match stream.as_mut().next_wire().await {
            Some(wire) => wire,
            // The source is exhausted, or closed abruptly.
            None => return,
        };
        // The writer appends: the buffer must be emptied before each sample, and
        // it comes back from the call, so its allocation is reused. `AlignedVec`
        // is a pointer, a length and a capacity: passing it by value costs
        // nothing, and the encoded bytes reach the emitter without a copy.
        scratch.clear();

        // Two framings: a bare `RpcError` for a technical error, the service's
        // `WireEvent<T, E>` for everything else.
        let framed: (
            EventKind,
            Result<AlignedVec<{ PAYLOAD_ALIGNMENT }>, rkyv::rancor::Error>,
        ) = match &wire {
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

/// Sink of one call's responses, owned by the task that serves it.
///
/// Type-erased and `Send` so a handler never names the transport: a test hands it
/// a [`CollectEmitter`], a provider hands it the channel's sink.
pub type OwnedEmitter = Box<dyn ResponseEmitter + Send>;

/// Handler of one RPC method: an owned request, and the sink of its responses.
///
/// **Owned, and returning a future**, because the handler runs as a task that
/// outlives the sample it was decoded from: a borrow of the received payload
/// could not be held across an await. The header travels with the payload so the
/// task can build the [`CallContext`](crate::types::CallContext) of the call it
/// serves.
pub type MethodHandler =
    Box<dyn Fn(RpcHeader, Vec<u8>, OwnedEmitter) -> BoxResponseFuture + Send + Sync>;

/// A handler's boxed future, re-exported where the codegen names it.
pub type BoxResponseFuture = crate::types::BoxResponseFuture;

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
        F: Fn(RpcHeader, Vec<u8>, OwnedEmitter) -> BoxResponseFuture + Send + Sync + 'static,
    {
        self.handlers.insert(name, Box::new(handler));
        self
    }

    /// Builds the task that serves one request — **without running it**.
    ///
    /// Returns `None` when no handler is registered for `method`, which the
    /// transport turns into an immediate [`crate::types::RpcError::UnknownMethod`]
    /// — a silent drop would only reach the caller as a timeout. One lookup: the
    /// transport needs no separate `has_method` probe.
    ///
    /// The future is handed back rather than awaited: the transport decides when
    /// it runs. It polls it once on the channel's thread — a handler that answers
    /// without yielding then costs no hop at all — and detaches it the moment it
    /// yields, so one slow call cannot hold back the next ones.
    pub fn dispatch(
        &self,
        method: &str,
        header: RpcHeader,
        payload: Vec<u8>,
        emitter: OwnedEmitter,
    ) -> Option<BoxResponseFuture> {
        self.handlers
            .get(method)
            .map(|handler| handler(header, payload, emitter))
    }
}

/// Encodes a transport-level technical error and emits it.
///
/// The payload is the archived [`RpcError`] **alone**, labelled
/// [`EventKind::RpcError`] — never a `WireEvent<T, E>`. A rejection says nothing
/// about the service types, and the provider cannot name them at all for a
/// method it does not have, so a generic framing could not answer those calls.
pub fn emit_rpc_error(err: RpcError, emitter: &mut dyn ResponseEmitter) -> bool {
    let scratch = AlignedVec::<{ PAYLOAD_ALIGNMENT }>::with_capacity(REQUEST_SCRATCH_CAPACITY);
    let encoded: Result<AlignedVec<{ PAYLOAD_ALIGNMENT }>, rkyv::rancor::Error> =
        to_bytes_in(&err, scratch);
    match encoded {
        Ok(bytes) => emitter.emit(EventKind::RpcError, &bytes),
        Err(e) => {
            log::error!("[bridge] rpc error serialization failed: {e:?}");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ObservableError;
    use std::time::Duration;

    /// Encodes a whole stream into its samples.
    ///
    /// An in-memory observable is ready on every poll, so this needs no runtime.
    fn encode(observable: Observable<i32, String>) -> Vec<(EventKind, Vec<u8>)> {
        let mut emitter = CollectEmitter::new();
        crate::rt::block_on(observable_to_responses(observable, &mut emitter));
        emitter.take()
    }

    /// Same, with every sample decoded back: a test then asserts on the wire
    /// framing a subscriber actually receives.
    fn fold(observable: Observable<i32, String>) -> Vec<(EventKind, WireEvent<i32, String>)> {
        encode(observable)
            .into_iter()
            .map(|(kind, payload)| {
                let event =
                    rkyv::from_bytes::<WireEvent<i32, String>, rkyv::rancor::Error>(&payload)
                        .expect("the payload of a service response is a WireEvent");
                (kind, event)
            })
            .collect()
    }

    #[test]
    fn a_single_value_response_travels_as_one_sample() {
        let samples = fold(Observable::from_events([Event::Next(42), Event::Complete]));
        assert_eq!(samples.len(), 1, "the pair must fold into one sample");
        assert_eq!(samples[0].0, EventKind::Complete);
        assert_eq!(samples[0].1, WireEvent::CompleteWith(42));
    }

    #[test]
    fn intermediate_values_keep_their_own_sample() {
        let samples = fold(Observable::from_events([
            Event::Next(1),
            Event::Next(2),
            Event::Complete,
        ]));
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0], (EventKind::Next, WireEvent::Next(1)));
        assert_eq!(
            samples[1],
            (EventKind::Complete, WireEvent::CompleteWith(2))
        );
    }

    /// The fold is the last step of a pipeline: an operator in front of the
    /// source must not cost the single-sample optimization.
    #[test]
    fn a_pipeline_single_value_keeps_one_wire_sample() {
        let samples = fold(
            Observable::<i32, String>::from_events([Event::Next(7), Event::Complete])
                .map(|v| v * 6),
        );
        assert_eq!(samples.len(), 1);
        assert_eq!(
            samples[0],
            (EventKind::Complete, WireEvent::CompleteWith(42))
        );
    }

    /// A technical error is framed as a bare `RpcError`, never as a
    /// `WireEvent<T, E>`: an observer must decode it without knowing the types.
    #[test]
    fn a_technical_error_is_framed_as_a_bare_rpc_error() {
        let samples = encode(Observable::from_events([Event::Error(
            ObservableError::Technical(RpcError::Timeout),
        )]));
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].0, EventKind::RpcError);

        // Reading it as the service's `WireEvent` must fail: that is exactly
        // what lets a provider answer a call for a method it does not have.
        assert!(
            rkyv::from_bytes::<WireEvent<i32, String>, rkyv::rancor::Error>(&samples[0].1).is_err()
        );
        let error = rkyv::from_bytes::<RpcError, rkyv::rancor::Error>(&samples[0].1)
            .expect("a technical error carries a bare RpcError");
        assert_eq!(error, RpcError::Timeout);
    }

    #[test]
    fn a_business_error_keeps_the_service_framing() {
        let samples = fold(Observable::from_events([Event::Error(
            ObservableError::Business("boom".into()),
        )]));
        assert_eq!(samples.len(), 1);
        assert_eq!(
            samples[0],
            (EventKind::Error, WireEvent::Error("boom".into()))
        );
    }

    /// The look-ahead must not hold a value back: a channel that emits a value
    /// and then stays open is published immediately, not after the next event.
    ///
    /// `test_block_on`, not `block_on`: `timeout` is built on the facade's
    /// `sleep`, which under the `tokio` feature needs an active runtime — exactly
    /// what the test helper supplies.
    #[test]
    fn a_value_followed_by_silence_is_published_at_once() {
        let (tx, rx) = crate::types::unbounded_channel::<i32, String>();
        tx.try_send_next(1).expect("the channel is unbounded");

        let mut stream = std::pin::pin!(WireFolding::new(rx));
        let wire = crate::rt::test_block_on(crate::rt::timeout(
            Duration::from_millis(500),
            stream.as_mut().next_wire(),
        ));
        assert_eq!(
            wire.expect("a value must not wait for the event that follows it"),
            Some(WireEvent::Next(1))
        );
    }

    /// `send_complete_with` enqueues two events, so the channel must have room
    /// for both: this is the documented contract of the bounded constructor.
    #[test]
    fn send_complete_with_folds_on_a_two_slot_channel() {
        let (tx, rx) = crate::types::channel::<i32, String>(2);
        crate::rt::block_on(tx.send_complete_with(42)).expect("two slots are enough");
        drop(tx);

        let mut stream = std::pin::pin!(WireFolding::new(rx));
        assert_eq!(
            crate::rt::block_on(stream.as_mut().next_wire()),
            Some(WireEvent::CompleteWith(42))
        );
        assert_eq!(crate::rt::block_on(stream.as_mut().next_wire()), None);
    }
}
