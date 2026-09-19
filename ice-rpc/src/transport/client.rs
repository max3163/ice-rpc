//! Consumer side: request publication and response routing.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use iceoryx2::prelude::*;
use iceoryx2_bb_posix::signal::SignalHandler;
use rkyv::api::high::to_bytes_in;
use rkyv::util::AlignedVec;

use super::notify::Coalescer;
use super::open::{open_event_service, open_service, OpenMode};
use super::{
    shared_node, transport_error, IoxEvent, IoxListener, IoxNotifier, IoxPubSub, IoxPublisher,
    IoxSubscriber, MAX_LOANED_SAMPLES, MAX_SLICE_LEN, PROVIDER_WAIT_DEFAULT, PUBLISH_RETRY_SLEEP,
    PUBLISH_SPIN_ATTEMPTS, REQUEST_NOTIFY_SUFFIX, REQUEST_SUFFIX, RESPONSE_NOTIFY_SUFFIX,
    RESPONSE_SUFFIX,
};
use crate::global::Locked;
use crate::sync::lock;
use crate::types::{
    fmt_correlation_id, normalize_wire_event, unbounded_channel, CallContext, Event, EventKind,
    Observable, ObservableError, RpcError, RpcHeader, ServiceRef, TraceContext, WireEvent,
    CORRELATION_ID_LEN,
};

/// Handler of one in-flight call: the sample's [`EventKind`] and its rkyv payload.
///
/// The kind is needed to pick the framing: a bare `RpcError` for a transport-level
/// rejection, the method's `WireEvent<T, E>` otherwise.
type ResponseHandler = Arc<dyn Fn(EventKind, &[u8]) + Send + Sync>;

/// Handlers of the calls in flight on one channel, keyed by correlation id.
type HandlerMap = HashMap<[u8; CORRELATION_ID_LEN], ResponseHandler>;

/// Removes the handler of one call, releasing its entry.
///
/// Returns `true` when the entry was still there — which is exactly what "the
/// call is still in flight" means, since the terminal event of a call removes it
/// too. That answer is what separates an abandoned call, which must be cancelled
/// remotely, from an answered one, which has nothing left to cancel.
///
/// Idempotent, because both the terminal event of the call and the drop of its
/// response stream call it.
fn release_handler(ports: &ConsumerPorts, cid: &[u8; CORRELATION_ID_LEN]) -> bool {
    lock(&ports.handlers).remove(cid).is_some()
}

/// Tells the provider to abandon the call of `request`, best-effort.
///
/// Published by the drop of a response stream whose call is **still in flight**,
/// and only then: a call already closed by a terminal event has nothing left to
/// cancel. This is what turns a local abandonment (`timeout`, `take_until`, a
/// dropped stream) into a remote one, instead of leaving a query, a report or a
/// scan running for a consumer that stopped listening.
///
/// Three properties, all deliberate:
///
/// - **one sample, no waiting**: the caller drops a stream on whatever thread it
///   happens to be on, so nothing here may block — not even the provider wait a
///   request is allowed;
/// - **the channel sequence is consumed**: a sample loss is detected in the gap
///   of [`RpcHeader::seq`], so a Cancel that skipped the counter would make the
///   observer report a loss that does not exist;
/// - **best-effort**: no subscriber connected, or a full buffer, and the Cancel
///   is simply lost. Cancelling a call nobody serves is not worth failing for.
fn publish_cancel(ports: &ConsumerPorts, request: &RpcHeader) {
    let header =
        RpcHeader::cancel_from(request).with_seq(ports.seq.fetch_add(1, Ordering::Relaxed));
    log::debug!(
        "[transport] cancelling call {}",
        fmt_correlation_id(&header.correlation_id)
    );

    publish_best_effort(ports, header);

    // Coalesced like a request's wake-up: at worst the provider polls the Cancel
    // on its next waitset expiry, one deadline (1 ms) later.
    if ports.last_request_notify.should_notify() {
        let _ = ports
            .request_notifier
            .notify_with_custom_event_id(EventId::new(0));
    }
}

/// Publishes one sample without ever waiting for anything.
///
/// Unlike [`publish_until_delivered`], this does **not** consult the global
/// shutdown tokens: a client asked to stop is precisely a client whose providers
/// should stop too, and one non-blocking write on shared memory is the right cost
/// for saying so. A failed attempt is logged and dropped.
fn publish_best_effort(ports: &ConsumerPorts, header: RpcHeader) {
    match try_publish(&ports.publisher, header, &[]) {
        // `Ok(false)` is the ordinary "nobody was there to take it" case: no
        // subscriber yet, or a subscriber buffer that stayed full.
        Ok(_) => {}
        Err(e) => log::debug!("[transport] best-effort sample not published: {e}"),
    }
}

/// Every port, handler table and counter of one consumed channel, kept alive for
/// the process lifetime.
///
/// Per channel rather than per service: the services of a channel share the same
/// request channel, routed by correlation id. The handler table is private to
/// the channel, so routing a response never contends with the other channels.
struct ConsumerPorts {
    /// Request service, read for its subscriber count before every publication.
    request_service: IoxPubSub,
    _response_service: IoxPubSub,
    _request_notify: IoxEvent,
    _response_notify: IoxEvent,
    /// Receives the responses of every call of the channel.
    subscriber: IoxSubscriber,
    /// Wakes the subscriber up when a response is published.
    listener: IoxListener,
    publisher: IoxPublisher,
    request_notifier: IoxNotifier,
    /// Handlers of the calls in flight, keyed by correlation id.
    handlers: Mutex<HandlerMap>,
    /// Coalescing window of the provider wake-ups.
    last_request_notify: Coalescer,
    /// Per-channel monotonic publication counter, stamped into
    /// [`RpcHeader::seq`](crate::types::RpcHeader::seq).
    ///
    /// Scoped to this channel on purpose: the correlation-id counter is
    /// process-wide, so reusing it would inject gaps whenever another channel
    /// publishes in between, defeating loss detection.
    seq: AtomicU64,
}

/// Ports of the channels this process consumes, keyed by channel name.
fn consumer_cache() -> &'static Locked<HashMap<String, Arc<ConsumerPorts>>> {
    static CACHE: Locked<HashMap<String, Arc<ConsumerPorts>>> = Locked::new();
    &CACHE
}

/// Drops the cached ports of every consumed channel, returning how many.
///
/// The cache lives in a `static`, and Rust never drops a `static`: without this,
/// a consumer that **created** a service — it opened it before any provider
/// existed — would keep that service alive on the bus after its own exit. Called
/// at shutdown, once the dispatch threads are joined.
pub(super) fn release_consumer_ports() -> usize {
    consumer_cache().with(|cache| {
        let count = cache.len();
        cache.clear();
        count
    })
}

/// Resolves [`PROVIDER_WAIT_DEFAULT`], allowing an environment override.
fn provider_wait_timeout() -> Duration {
    std::env::var("ICE_RPC_PROVIDER_WAIT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(PROVIDER_WAIT_DEFAULT)
}

/// Opens (once per process) the ports of `channel` on the consumer side and
/// starts the response dispatch thread.
///
/// The ports are per channel, not per service: every service of a channel sends
/// on the same request channel, routed by correlation id. The cache lock is held
/// across the creation so a single set of ports is created under a race.
fn consumer_ports(channel: &str) -> Result<Arc<ConsumerPorts>, RpcError> {
    // The lock is held across the creation on purpose: a race between two
    // threads must not open the same channel twice.
    consumer_cache().with(|cache| {
        if let Some(ports) = cache.get(channel) {
            return Ok(ports.clone());
        }
        let ports = open_consumer_ports(channel)?;
        cache.insert(channel.to_owned(), ports.clone());
        Ok(ports)
    })
}

/// Opens the ports of `channel` and starts its response dispatch thread.
///
/// Split from [`consumer_ports`] so the cache stays locked over the whole
/// creation while this function remains a straight-line setup.
fn open_consumer_ports(channel: &str) -> Result<Arc<ConsumerPorts>, RpcError> {
    let node = shared_node()?;
    let request_service = open_service(&node, channel, REQUEST_SUFFIX, OpenMode::CreateOrOpen)?;
    let response_service = open_service(&node, channel, RESPONSE_SUFFIX, OpenMode::CreateOrOpen)?;
    let request_notify = open_event_service(
        &node,
        channel,
        REQUEST_NOTIFY_SUFFIX,
        OpenMode::CreateOrOpen,
    )?;
    let response_notify = open_event_service(
        &node,
        channel,
        RESPONSE_NOTIFY_SUFFIX,
        OpenMode::CreateOrOpen,
    )?;

    let publisher = request_service
        .publisher_builder()
        .initial_max_slice_len(MAX_SLICE_LEN)
        .max_loaned_samples(MAX_LOANED_SAMPLES)
        .create()
        .map_err(|e| transport_error("request publisher", e))?;
    let request_notifier = request_notify
        .notifier_builder()
        .create()
        .map_err(|e| transport_error("request notifier", e))?;
    let subscriber = response_service
        .subscriber_builder()
        .create()
        .map_err(|e| transport_error("response subscriber", e))?;
    let listener = response_notify
        .listener_builder()
        .create()
        .map_err(|e| transport_error("response listener", e))?;

    let ports = Arc::new(ConsumerPorts {
        request_service,
        _response_service: response_service,
        _request_notify: request_notify,
        _response_notify: response_notify,
        subscriber,
        listener,
        publisher,
        request_notifier,
        handlers: Mutex::new(HashMap::new()),
        last_request_notify: Coalescer::new(),
        seq: AtomicU64::new(0),
    });

    // The dispatch thread owns a clone of the ports: its subscriber and listener
    // must outlive the loop that blocks on them.
    spawn_response_dispatcher(channel.to_owned(), Arc::clone(&ports));

    Ok(ports)
}

/// Blocks on the response event and routes every sample to its handler.
fn spawn_response_dispatcher(channel: String, ports: Arc<ConsumerPorts>) {
    let handle = crate::rt::spawn_blocking(move || {
        let cancel = crate::global_cancel_token().clone();
        super::pump::run_receive_loop(
            &channel,
            "response",
            &ports.subscriber,
            &ports.listener,
            || cancel.is_cancelled(),
            |header, payload| {
                // The handler is cloned out of the table so the lock is released
                // before the handler runs.
                let handler = lock(&ports.handlers).get(&header.correlation_id).cloned();
                if let Some(handler) = handler {
                    handler(header.event_kind(), payload);
                }
            },
        );
        log::debug!("[transport] response dispatcher for '{channel}' stopped");
    });
    crate::locator::ServiceLocator::global().register_shutdown_handle(handle);
}

/// Sends `payload` as the `method` call of `service` on `channel`, and returns
/// the streamed responses as an [`Observable`].
///
/// `service` carries both the id and the interface version, so the version
/// cannot be dropped between the caller and the frame: it reaches
/// [`RpcHeader::request`] from the same value the provider registered.
///
/// Dropping the returned stream while the call is in flight **cancels the call
/// remotely**: the provider abandons it, so the work it was doing for a caller
/// that stopped listening does not keep running.
pub fn native_call<T, E>(
    channel: &str,
    service: ServiceRef,
    method: &str,
    payload: &[u8],
) -> Result<Observable<T, E>, RpcError>
where
    T: Send + 'static,
    E: Send + 'static,
    WireEvent<T, E>: rkyv::Archive,
    <WireEvent<T, E> as rkyv::Archive>::Archived: rkyv::Deserialize<
        WireEvent<T, E>,
        rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
    >,
    for<'a> <WireEvent<T, E> as rkyv::Archive>::Archived:
        rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    let ports = consumer_ports(channel)?;

    // A call emitted from inside a provider handler continues that handler's
    // trace; a call emitted anywhere else starts a new one. The ambient read is
    // synchronous and the value is copied into the header right away — it is
    // never held across an await, which is what makes it safe here.
    let trace = match CallContext::current() {
        Some(ctx) => ctx.child_trace(),
        None => TraceContext::new_root(),
    };
    let header = RpcHeader::request(method, service.id, service.version)
        .with_trace(trace)
        .with_seq(ports.seq.fetch_add(1, Ordering::Relaxed));
    let cid = header.correlation_id;
    let (tx, rx) = unbounded_channel::<T, E>();

    let handler_ports = Arc::clone(&ports);
    let handler: ResponseHandler = Arc::new(move |kind: EventKind, bytes: &[u8]| {
        // A transport-level rejection is a bare `RpcError`, framed without the
        // service types: it can be answered for any method, known or not, so it
        // must be decoded without naming `(T, E)`.
        if kind == EventKind::RpcError {
            let error = match super::decode_aligned::<RpcError>(bytes) {
                Ok(err) => err,
                Err(e) => transport_error("decode rpc error", e),
            };
            let _ = tx.try_send_event(Event::Error(ObservableError::Technical(error)));
            release_handler(&handler_ports, &cid);
            return;
        }

        match super::decode_aligned::<WireEvent<T, E>>(bytes) {
            Ok(wire) => {
                // Terminality is read on the **wire** event, not on the normalized
                // one: a `CompleteWith` — the single-response case, by far the
                // most common — expands into `Next` followed by `Complete`, and
                // the call is over at the wire event. Reading the normalized
                // `Next` would leave the entry registered until the consumer
                // happens to drop the stream.
                let terminal = wire.is_terminal();
                let (event, follow_up) = normalize_wire_event(wire);
                if tx.try_send_event(event).is_err() {
                    release_handler(&handler_ports, &cid);
                    return;
                }
                if let Some(next) = follow_up {
                    let _ = tx.try_send_event(next);
                }
                if terminal {
                    release_handler(&handler_ports, &cid);
                }
            }
            Err(e) => {
                let _ = tx.try_send_event(Event::Error(ObservableError::Technical(
                    transport_error("decode response", e),
                )));
                release_handler(&handler_ports, &cid);
            }
        }
    });
    lock(&ports.handlers).insert(cid, handler);

    if let Err(e) = publish_until_delivered(
        &ports.publisher,
        &ports.request_service,
        header,
        payload,
        provider_wait_timeout(),
    ) {
        release_handler(&ports, &cid);
        return Err(e);
    }
    if ports.last_request_notify.should_notify() {
        let _ = ports
            .request_notifier
            .notify_with_custom_event_id(EventId::new(0));
    }

    // The call owns its handler: dropping the response stream releases it, so an
    // abandoned call (`timeout`, `take_until`, a dropped stream) cannot leave an
    // entry behind for the rest of the process lifetime.
    //
    // The same drop is the cancellation point. Two conditions must hold, and both
    // are needed: the stream must not have delivered a terminal event
    // (`terminated`), and the transport must not have answered the call yet
    // (`release_handler`). Together they mean "the caller stopped listening while
    // the provider was still working" — the only case where there is work to
    // abandon.
    let cleanup_ports = Arc::clone(&ports);
    Ok(rx.with_cleanup(move |terminated| {
        // Released whatever the outcome: this drop is the last owner of the call.
        let in_flight = release_handler(&cleanup_ports, &cid);
        if !terminated && in_flight {
            publish_cancel(&cleanup_ports, &header);
        }
    }))
}

thread_local! {
    /// Encoding buffer of the request path, one per thread.
    ///
    /// The generated client called `rkyv::to_bytes` per call, which allocates a
    /// buffer every time: `benches/hot_path.rs` measures 61.5 ns against 23.0 ns
    /// when the buffer is reused. Thread-local rather than shared, because a
    /// locked scratch measured 20 to 45 times slower than a local one in
    /// `benches/concurrency.rs` — the same reason the response path keeps its
    /// buffer local to one stream.
    static REQUEST_SCRATCH: RefCell<AlignedVec<16>> =
        RefCell::new(AlignedVec::<16>::with_capacity(256));
}

/// Serializes `request` into the thread's buffer and publishes it as a call.
///
/// This is the whole body of a generated client method: encoding the request and
/// starting the call are one step, so the error mapping — a serialization failure
/// becomes [`RpcError::SerializationError`] — is written here once instead of
/// being repeated in every method of every service.
///
/// The buffer travels through `to_bytes_in` and comes back, so the request path
/// allocates once per **thread** and not once per call.
pub fn serialize_and_call<T, E, V>(
    channel: &str,
    service: ServiceRef,
    method: &str,
    request: &V,
) -> Result<Observable<T, E>, RpcError>
where
    T: Send + 'static,
    E: Send + 'static,
    WireEvent<T, E>: rkyv::Archive,
    <WireEvent<T, E> as rkyv::Archive>::Archived: rkyv::Deserialize<
        WireEvent<T, E>,
        rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
    >,
    for<'a> <WireEvent<T, E> as rkyv::Archive>::Archived:
        rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    for<'a> V: rkyv::Serialize<
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
    REQUEST_SCRATCH.with(|cell| {
        // A re-entrant call — a `Serialize` implementation that calls back into
        // the framework — finds the buffer taken and allocates its own, rather
        // than panicking on a borrowed cell.
        let mut buffer = match cell.try_borrow_mut() {
            Ok(mut guard) => std::mem::replace(&mut *guard, AlignedVec::<16>::with_capacity(0)),
            Err(_) => AlignedVec::<16>::with_capacity(256),
        };
        buffer.clear();

        let encoded: Result<AlignedVec<16>, rkyv::rancor::Error> = to_bytes_in(request, buffer);
        let bytes = match encoded {
            Ok(bytes) => bytes,
            Err(e) => {
                log::error!("[ice-rpc] request serialization failed: {e:?}");
                return Err(RpcError::SerializationError);
            }
        };

        let call = native_call::<T, E>(channel, service, method, &bytes);

        // The allocation goes back to the thread whether the call started or not.
        if let Ok(mut guard) = cell.try_borrow_mut() {
            *guard = bytes;
        }
        call
    })
}

/// Publishes `header ++ payload` on `publisher`, retrying until at least one
/// subscriber receives it or `timeout` elapses.
///
/// `service` is the pub/sub service `publisher` belongs to, used only to read
/// its **subscriber count**. With no subscriber connected — typically a call
/// made before the provider process is up — nothing is loaned and the payload is
/// never copied: writing a sample nobody can receive would copy the whole
/// payload again on every attempt, for up to `timeout` (30 s by default). The
/// wait is spent on the subscriber count instead, and the payload is copied
/// once, when a receiver exists.
pub(super) fn publish_until_delivered(
    publisher: &IoxPublisher,
    service: &IoxPubSub,
    header: RpcHeader,
    payload: &[u8],
    timeout: Duration,
) -> Result<(), RpcError> {
    let deadline = Instant::now() + timeout;
    let mut attempts: u32 = 0;

    loop {
        if shutdown_requested() {
            return Err(RpcError::Cancelled);
        }

        if service.dynamic_config().number_of_subscribers() == 0 {
            if Instant::now() >= deadline {
                return Err(RpcError::TransportError(
                    "no subscriber connected (is the provider running?)".to_string(),
                ));
            }
            backoff(&mut attempts)?;
            continue;
        }

        if try_publish(publisher, header, payload)? {
            return Ok(());
        }

        // A subscriber is connected but did not take the sample: its buffer is
        // full, which is backpressure rather than a missing peer.
        if Instant::now() >= deadline {
            return Err(RpcError::TransportError(
                "delivery refused: the subscriber buffer stayed full".to_string(),
            ));
        }
        backoff(&mut attempts)?;
    }
}

/// Whether the process was asked to stop.
fn shutdown_requested() -> bool {
    crate::global_cancel_token().is_cancelled() || crate::registry_cancel_token().is_cancelled()
}

/// Waits between two delivery attempts: a burst of yields, then short sleeps.
///
/// This call path owns no `WaitSet`, so the OS termination flag is sampled here:
/// it is what makes Ctrl+C honourable when this wait is the only running code.
fn backoff(attempts: &mut u32) -> Result<(), RpcError> {
    if *attempts < PUBLISH_SPIN_ATTEMPTS {
        *attempts += 1;
        std::thread::yield_now();
        return Ok(());
    }

    if SignalHandler::termination_requested() {
        crate::request_shutdown();
        return Err(RpcError::Cancelled);
    }
    std::thread::sleep(PUBLISH_RETRY_SLEEP);
    Ok(())
}

/// Loans one sample, writes `header ++ payload` into it and sends it.
///
/// Returns `true` when at least one subscriber received the sample.
fn try_publish(
    publisher: &IoxPublisher,
    header: RpcHeader,
    payload: &[u8],
) -> Result<bool, RpcError> {
    // A zero-length payload still needs a sample, hence the `max(1)`.
    let len = payload.len().max(1);
    let sample = publisher
        .loan_slice_uninit(len)
        .map_err(|e| transport_error("loan sample", e))?;
    let mut sample = sample.write_from_fn(|i| payload.get(i).copied().unwrap_or(0));
    *sample.user_header_mut() = header;
    let delivered = sample
        .send()
        .map_err(|e| transport_error("send sample", e))?;
    Ok(delivered > 0)
}
