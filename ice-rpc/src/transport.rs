//! Publish/subscribe transport: one request channel and one response channel
//! per **channel** — a group of services — correlated by the request id carried
//! in the zero-copy header.
//!
//! A service belongs to a channel through the `group` parameter of
//! `#[service]`, which defaults to the service name (one channel per service,
//! the historical layout). Services sharing a channel also share its request
//! and response pub/sub services and its dispatch thread; the provider picks
//! the right dispatcher with the `service_id` of the header, a value derived
//! from the service name by [`ice_rpc::types::service_id_of`] and therefore
//! identical in every process without any discovery step.
//!
//! # Why pub/sub
//!
//! iceoryx2's request/response pattern allocates one channel (and one data
//! segment) per request in flight, which caps the throughput far below what the
//! shared-memory bus can do. The pub/sub pattern uses **one** publisher and
//! **one** subscriber per process and per service, so a call allocates nothing
//! but a loaned sample. Correlating the responses with the request id gives the
//! same call/response semantics for a fraction of the cost.
//!
//! # Wire format
//!
//! Every sample carries a [`RpcHeader`] in iceoryx2's `user_header`
//! (`ZeroCopySend`, no serialization): correlation id, method name, event kind
//! and protocol/service versions. The payload is the rkyv bytes alone, and the
//! sample is aligned so that rkyv can read it in place. The method name travels
//! in the header of a request only; a response is routed by its correlation id.
//!
//! # Wake-ups: Notifier + Listener + WaitSet
//!
//! A subscribe port cannot be attached to a `WaitSet` (only a `Listener` can),
//! so each side also owns a dedicated **event** service used purely as a wake-up
//! signal: the publisher notifies after each send, and the receiver blocks on a
//! `WaitSet` attached to its `Listener`. This keeps the latency in the
//! microsecond range for bursty traffic *and* leaves an idle process at 0% CPU,
//! which a polling loop cannot do on Windows (a sub-millisecond `sleep` is
//! floored at the system timer, ~1 ms).
//!
//! # Threads
//!
//! - the **provider** runs one dispatch thread per provided service;
//! - the **consumer** runs one dispatch thread per consumed service.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;

use iceoryx2::prelude::*;
use iceoryx2::service::ipc_threadsafe;
use iceoryx2::waitset::WaitSetRunResult;
use iceoryx2_bb_posix::signal::SignalHandler;

use crate::types::{
    normalize_wire_event, unbounded_channel, Event, EventKind, Observable, ObservableError,
    RpcError, RpcHeader, WireEvent, CORRELATION_ID_LEN, PROTOCOL_VERSION,
};
use crate::CancellationToken;

/// Concrete iceoryx2 service flavour used by the transport.
type Iox = ipc_threadsafe::Service;
type IoxNode = iceoryx2::node::Node<Iox>;
type IoxPubSub =
    iceoryx2::service::port_factory::publish_subscribe::PortFactory<Iox, [u8], RpcHeader>;
type IoxEvent = iceoryx2::service::port_factory::event::PortFactory<Iox>;
type IoxPublisher = iceoryx2::port::publisher::Publisher<Iox, [u8], RpcHeader>;
type IoxSubscriber = iceoryx2::port::subscriber::Subscriber<Iox, [u8], RpcHeader>;
type IoxListener = iceoryx2::port::listener::Listener<Iox>;
type IoxNotifier = iceoryx2::port::notifier::Notifier<Iox>;

/// Suffixes of the iceoryx2 services backing one logical service.
const REQUEST_SUFFIX: &str = "_req";
const RESPONSE_SUFFIX: &str = "_resp";
const REQUEST_NOTIFY_SUFFIX: &str = "_req_notify";
const RESPONSE_NOTIFY_SUFFIX: &str = "_resp_notify";

/// Samples a subscriber can buffer before backpressure is reported.
/// Kept small on purpose: since `enable_safe_overflow` is disabled a full
/// buffer is backpressure, not data loss, and the buffer is one of the two
/// terms of the shared-memory budget of a channel (see [`MAX_LOANED_SAMPLES`]).
const SUBSCRIBER_BUFFER: usize = 1024;

/// Publishers a channel accepts: one per process that sends on it.
const MAX_PUBLISHERS: usize = 16;

/// Subscribers a channel accepts: one per process and per channel.
const MAX_SUBSCRIBERS: usize = 16;

/// Processes that can open the same channel at once.
const MAX_NODES: usize = 32;

/// Samples a publisher can keep loaned at once.
///
/// iceoryx2 defaults it to 8, which caps the number of concurrently in-flight
/// sends; a high-throughput burst (hundreds of calls in flight) would fail with
/// `ExceedsMaxLoans` long before that.
///
/// It also sizes the data segment of the publisher
/// (`max_loaned_samples × sample`), i.e. most of the memory a channel reserves:
/// at 16 384 the ~400-byte sample of a request channel costs ~6.5 MB, which is
/// untenable with dozens of services. Backpressure makes 1024 (~400 KB) safe.
const MAX_LOANED_SAMPLES: usize = 1024;

/// Initial slice length of a sample. Large payloads grow the segment on demand.
///
/// Kept small: iceoryx2 pre-allocates `buffer × slice` bytes, so a large slice
/// multiplied by [`MAX_LOANED_SAMPLES`] would reserve tens of megabytes.
const MAX_SLICE_LEN: usize = 256;

/// Payload alignment requested from iceoryx2.
///
/// `rkyv::to_bytes` produces 16-byte aligned archives, so requesting the same
/// alignment from the bus makes the payload directly decodable in place.
const PAYLOAD_ALIGNMENT: usize = 16;

/// Default how long a call waits for the provider to be connected before failing.
///
/// A pub/sub send with no connected subscriber is silently lost, so a consumer
/// started *before* the provider would otherwise hang forever. Overridable with
/// `ICE_RPC_PROVIDER_WAIT_MS`.
const PROVIDER_WAIT_DEFAULT: Duration = Duration::from_secs(30);

/// Resolves [`PROVIDER_WAIT_DEFAULT`], allowing an environment override.
fn provider_wait_timeout() -> Duration {
    std::env::var("ICE_RPC_PROVIDER_WAIT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(PROVIDER_WAIT_DEFAULT)
}

/// How long a response waits for the consumer to be connected.
const CONSUMER_WAIT_TIMEOUT: Duration = Duration::from_millis(500);

/// Sleep between two delivery attempts, once the spin budget is exhausted.
const PUBLISH_RETRY_SLEEP: Duration = Duration::from_millis(1);

/// Consecutive delivery attempts spent yielding before the retry loop sleeps.
///
/// A full channel is the normal case of a burst: the receiver frees a slot in
/// microseconds, while sleeping costs at least the 1 ms system timer on Windows
/// and would throttle the sender far below what the channel can do. The spin
/// keeps the burst at full speed and only a genuinely absent subscriber reaches
/// the sleeping path.
const PUBLISH_SPIN_ATTEMPTS: u32 = 4_096;

/// Upper bound on how long a dispatch thread blocks before it drains again.
///
/// The wait itself is event-driven (the publisher notifies after each send), so
/// a burst is handled at microsecond latency. This deadline is the safety net
/// that makes a missed notification cost at most this much.
const WAITSET_DEADLINE: Duration = Duration::from_millis(1);

/// Coalescing window of the peer wake-up notifications.
///
/// The notification is a syscall, and a receiver that is currently *polling*
/// does not need one: the coalescing removes it from a saturated channel while
/// a sparse one (the receiver parked on its `WaitSet`) is always notified, since
/// two calls are then separated by far more than this window.
///
/// The window is deliberately far shorter than the time a receiver keeps
/// spinning before it parks ([`IDLE_SPINS`] yields, i.e. hundreds of
/// microseconds at worst), so a coalesced wake-up can only be skipped while the
/// receiver is still awake — the invariant that makes the coalescing safe.
const NOTIFY_COALESCE_WINDOW: Duration = Duration::from_micros(100);

/// Monotonic microsecond clock shared by the notification coalescers.
fn notify_clock_us() -> u64 {
    static BASE: OnceLock<std::time::Instant> = OnceLock::new();
    BASE.get_or_init(std::time::Instant::now)
        .elapsed()
        .as_micros() as u64
}

/// Returns `true` when the peer must be woken up (see
/// [`NOTIFY_COALESCE_WINDOW`]), and records the wake-up.
///
/// `last` holds the microsecond timestamp of the previous wake-up; two callers
/// racing on the same window is harmless (one notification is enough to wake a
/// parked receiver, and a spurious one only costs a poll).
fn should_notify(last: &AtomicU64) -> bool {
    let now = notify_clock_us();
    let previous = last.load(Ordering::Relaxed);
    if now.saturating_sub(previous) < NOTIFY_COALESCE_WINDOW.as_micros() as u64 {
        return false;
    }
    last.store(now, Ordering::Relaxed);
    true
}

/// Single-threaded variant of [`should_notify`], for the dispatch loops.
fn should_notify_local(last: &mut u64) -> bool {
    let now = notify_clock_us();
    if now.saturating_sub(*last) < NOTIFY_COALESCE_WINDOW.as_micros() as u64 {
        return false;
    }
    *last = now;
    true
}

/// Processed samples between two termination checks on the hot path.
///
/// `SignalHandler::termination_requested()` takes a process-wide mutex, so
/// calling it for every sample serializes the dispatch threads and divides the
/// throughput by ~10. Sampling every this many calls keeps the added cost
/// negligible while still bounding the Ctrl+C latency under load; an idle
/// service is covered by the `WaitSet` path, which detects the signal within
/// [`WAITSET_DEADLINE`].
const SIGNAL_CHECK_SAMPLES: u32 = 256;

/// Consecutive empty polls spent spinning before the thread blocks on its
/// `WaitSet`.
///
/// A saturated service finds a sample on (almost) every poll, so it never
/// reaches the blocking path and keeps the polling throughput. A bursty service
/// blocks instead of burning a core, and the notification wakes it immediately.
///
/// Once blocked the thread **stays** blocked: it only re-arms the spin burst
/// when a notification arrived, never on a plain deadline expiry. Re-spinning on
/// every expiry would run thousands of `yield_now` per millisecond and cost
/// ~30% of a core for a completely idle process (debug builds).
const IDLE_SPINS: u32 = 2_000;

fn transport_error(context: &str, err: impl std::fmt::Debug) -> RpcError {
    RpcError::TransportError(format!("{context}: {err:?}"))
}

/// Returns the **process-wide** iceoryx2 node, created on first use.
fn shared_node() -> Result<Arc<IoxNode>, RpcError> {
    static NODE: OnceLock<Result<Arc<IoxNode>, String>> = OnceLock::new();
    NODE.get_or_init(|| {
        NodeBuilder::new()
            .create::<Iox>()
            .map(Arc::new)
            .map_err(|e| format!("{e:?}"))
    })
    .clone()
    .map_err(|e| RpcError::TransportError(format!("node creation: {e}")))
}

/// Decodes a rkyv payload, copying it into an aligned buffer when needed.
///
/// The sample payload is aligned by construction ([`PAYLOAD_ALIGNMENT`]), but
/// the copy keeps the decoder correct even if that assumption is ever relaxed.
pub fn decode_aligned<T>(bytes: &[u8]) -> Result<T, rkyv::rancor::Error>
where
    T: rkyv::Archive,
    <T as rkyv::Archive>::Archived:
        rkyv::Deserialize<T, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
    for<'a> <T as rkyv::Archive>::Archived:
        rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
    aligned.extend_from_slice(bytes);
    rkyv::from_bytes::<T, rkyv::rancor::Error>(&aligned)
}

/// Lazy iterator of rkyv-encoded [`WireEvent`] samples produced by a service.
pub type ResponseIter = Box<dyn Iterator<Item = Vec<u8>> + Send>;

/// Wraps an [`Observable`] into a lazy [`ResponseIter`] of encoded [`WireEvent`].
///
/// The wire events are taken **raw** (`recv_wire`), which preserves the
/// [`WireEvent::CompleteWith`] single-sample optimization: a `Next(v)`
/// immediately followed by the terminal `Complete` travels as **one** iceoryx2
/// sample instead of two. The consumer expands it back transparently
/// (`recv` / `next` never expose the optimization).
pub fn observable_to_responses<T, E>(mut observable: Observable<T, E>) -> ResponseIter
where
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
    Box::new(std::iter::from_fn(move || {
        match crate::rt::block_on(observable.recv_wire()) {
            Ok(wire) => rkyv::to_bytes::<rkyv::rancor::Error>(&wire)
                .ok()
                .map(|bytes| bytes.to_vec()),
            Err(_) => None,
        }
    }))
}

/// Handler of one RPC method: decoded payload → lazy response samples.
pub type MethodHandler = Box<dyn Fn(&[u8]) -> ResponseIter + Send + Sync>;

/// Per-service table of method handlers, built by a generated provider.
#[derive(Default)]
pub struct ServiceDispatcher {
    handlers: HashMap<&'static str, MethodHandler>,
}

impl ServiceDispatcher {
    /// Creates an empty dispatcher.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers the handler of one RPC method.
    pub fn method<F>(&mut self, name: &'static str, handler: F) -> &mut Self
    where
        F: Fn(&[u8]) -> ResponseIter + Send + Sync + 'static,
    {
        self.handlers.insert(name, Box::new(handler));
        self
    }

    /// Routes a decoded request to its handler.
    pub fn dispatch(&self, method: &str, payload: &[u8]) -> ResponseIter {
        match self.handlers.get(method) {
            Some(handler) => handler(payload),
            None => Box::new(std::iter::empty()),
        }
    }
}

// ---------------------------------------------------------------------------
// Consumer side: response routing
// ---------------------------------------------------------------------------

/// Typed handler invoked with the rkyv response payload of one in-flight call.
type ResponseHandler = Arc<dyn Fn(&[u8]) + Send + Sync>;

type HandlerMap = HashMap<[u8; CORRELATION_ID_LEN], ResponseHandler>;

fn response_handlers() -> &'static std::sync::Mutex<HandlerMap> {
    static HANDLERS: OnceLock<std::sync::Mutex<HandlerMap>> = OnceLock::new();
    HANDLERS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Registers the handler of one in-flight call.
pub fn register_response_handler(cid: [u8; CORRELATION_ID_LEN], handler: ResponseHandler) {
    crate::sync::lock(response_handlers()).insert(cid, handler);
}

/// Removes the handler of one in-flight call.
pub fn unregister_response_handler(cid: &[u8; CORRELATION_ID_LEN]) {
    crate::sync::lock(response_handlers()).remove(cid);
}

/// Publishes port, wake-up notifier and response event service of a consumed
/// service, all kept alive for the process lifetime.
struct ConsumerPorts {
    _request_service: IoxPubSub,
    _response_service: IoxPubSub,
    _request_notify: IoxEvent,
    publisher: IoxPublisher,
    request_notifier: IoxNotifier,
    /// Timestamp of the last provider wake-up (see [`notify_clock_us`]).
    last_request_notify: AtomicU64,
}

fn consumer_cache() -> &'static std::sync::Mutex<HashMap<String, Arc<ConsumerPorts>>> {
    static CACHE: OnceLock<std::sync::Mutex<HashMap<String, Arc<ConsumerPorts>>>> = OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Opens (once per process) the ports of `channel` on the consumer side and
/// starts the response dispatch thread.
///
/// The ports are per channel, not per service: every service of a channel sends
/// on the same request channel and is served by the same response dispatcher
/// (the routing is by correlation id). The cache lock is held across the
/// creation so a single set of ports is created even when many workers race on
/// the first call.
fn consumer_ports(channel: &str) -> Result<Arc<ConsumerPorts>, RpcError> {
    let mut cache = crate::sync::lock(consumer_cache());
    if let Some(ports) = cache.get(channel) {
        return Ok(ports.clone());
    }

    let node = shared_node()?;
    let request_service = open_service(&node, channel, REQUEST_SUFFIX)?;
    let response_service = open_service(&node, channel, RESPONSE_SUFFIX)?;
    let request_notify = open_event_service(&node, channel, REQUEST_NOTIFY_SUFFIX)?;
    let response_notify = open_event_service(&node, channel, RESPONSE_NOTIFY_SUFFIX)?;

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

    // The listener must outlive the dispatch thread that blocks on it.
    spawn_response_dispatcher(channel.to_owned(), subscriber, listener, response_notify);

    let ports = Arc::new(ConsumerPorts {
        _request_service: request_service,
        _response_service: response_service,
        _request_notify: request_notify,
        last_request_notify: AtomicU64::new(0),
        publisher,
        request_notifier,
    });
    cache.insert(channel.to_owned(), ports.clone());
    Ok(ports)
}

/// Blocks on `waitset` until the notifier fires or [`WAITSET_DEADLINE`] expires.
///
/// Returns `true` when the wake-up came from the attached notification, which
/// lets the caller tell "data is probably available" from "still idle". Without
/// that distinction an idle thread restarts its spin burst on every deadline
/// expiry, i.e. thousands of `yield_now` calls per millisecond, and burns CPU
/// while doing nothing.
fn wait_for_wakeup(waitset: &WaitSet<Iox>, guard: &WaitSetGuard<'_, '_, Iox>) -> bool {
    let mut notified = false;
    let result = waitset.wait_and_process_once_with_timeout(
        |attachment_id| {
            if attachment_id.has_event_from(guard) {
                notified = true;
            }
            CallbackProgression::Continue
        },
        WAITSET_DEADLINE,
    );

    if matches!(
        result,
        Ok(WaitSetRunResult::TerminationRequest) | Ok(WaitSetRunResult::Interrupt)
    ) {
        // In `HandleTerminationRequests` mode iceoryx2 owns the SIGINT/SIGTERM
        // handler: the signal is reported here instead of killing the process,
        // so the framework cancels its tokens and the caller exits cleanly.
        crate::request_shutdown();
        return true;
    }

    notified
}

/// Blocks on the response event and routes every sample to its handler.
fn spawn_response_dispatcher(
    channel: String,
    subscriber: IoxSubscriber,
    listener: IoxListener,
    _response_notify: IoxEvent,
) {
    let handle = crate::rt::spawn_blocking(move || {
        let Ok(waitset) = WaitSetBuilder::new()
            .signal_handling_mode(crate::waitset_signal_handling_mode())
            .create::<Iox>()
        else {
            return;
        };
        let Ok(guard) = waitset.attach_deadline(&listener, WAITSET_DEADLINE) else {
            return;
        };

        let cancel = crate::global_cancel_token().clone();
        let mut idle_spins: u32 = 0;
        let mut signal_ticks: u32 = 0;
        loop {
            if cancel.is_cancelled() {
                break;
            }
            match subscriber.receive() {
                Ok(Some(sample)) => {
                    idle_spins = 0;
                    signal_ticks = signal_ticks.wrapping_add(1);
                    if signal_ticks & (SIGNAL_CHECK_SAMPLES - 1) == 0
                        && SignalHandler::termination_requested()
                    {
                        crate::request_shutdown();
                        break;
                    }
                    let cid = sample.user_header().correlation_id;
                    let payload: &[u8] = &sample;
                    let handler = crate::sync::lock(response_handlers()).get(&cid).cloned();
                    if let Some(handler) = handler {
                        handler(payload);
                    }
                }
                Ok(None) => {
                    if idle_spins < IDLE_SPINS {
                        idle_spins += 1;
                        std::thread::yield_now();
                    } else {
                        let notified = wait_for_wakeup(&waitset, &guard);
                        // Only a notification means there is something to poll
                        // for; a bare deadline expiry keeps the thread blocked.
                        idle_spins = if notified { 0 } else { IDLE_SPINS };
                    }
                }
                Err(e) => {
                    log::warn!("[transport] response receive error: {e:?}");
                    idle_spins = 0;
                    std::thread::yield_now();
                }
            }
        }
        log::debug!("[transport] response dispatcher for '{channel}' stopped");
    });
    crate::locator::ServiceLocator::global().register_shutdown_handle(handle);
}

/// Sends `payload` as the `method` call of the service `service_id` on
/// `channel`, and returns the streamed responses as an [`Observable`].
pub fn native_call<T, E>(
    channel: &str,
    service_id: u32,
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
    let header = RpcHeader::request(method, service_id, 1);
    let cid = header.correlation_id;
    let (tx, rx) = unbounded_channel::<T, E>();

    let handler: ResponseHandler =
        Arc::new(
            move |bytes: &[u8]| match decode_aligned::<WireEvent<T, E>>(bytes) {
                Ok(wire) => {
                    let (event, follow_up) = normalize_wire_event(wire);
                    let terminal = event.is_terminal();
                    if tx.try_send_event(event).is_err() {
                        unregister_response_handler(&cid);
                        return;
                    }
                    if let Some(next) = follow_up {
                        let _ = tx.try_send_event(next);
                    }
                    if terminal {
                        unregister_response_handler(&cid);
                    }
                }
                Err(e) => {
                    let _ = tx.try_send_event(Event::Error(ObservableError::Technical(
                        transport_error("decode response", e),
                    )));
                    unregister_response_handler(&cid);
                }
            },
        );
    register_response_handler(cid, handler);

    if let Err(e) =
        publish_until_delivered(&ports.publisher, header, payload, provider_wait_timeout())
    {
        unregister_response_handler(&cid);
        return Err(e);
    }
    // Wake the provider's dispatch thread: it runs in **another process**, so its
    // "parked on the WaitSet" state is not observable from here. Notifying on
    // every send would cost a syscall per call on a saturated channel, hence the
    // coalescing window (a parked provider is always notified, since two calls
    // are then milliseconds apart).
    if should_notify(&ports.last_request_notify) {
        let _ = ports
            .request_notifier
            .notify_with_custom_event_id(EventId::new(0));
    }

    Ok(rx)
}

// ---------------------------------------------------------------------------
// Provider side: request dispatch and response publication
// ---------------------------------------------------------------------------

/// Opens the pub/sub service of one direction of `channel`.
fn open_service(node: &IoxNode, channel: &str, suffix: &str) -> Result<IoxPubSub, RpcError> {
    let topic = format!("{channel}{suffix}");
    let name = ServiceName::new(&topic).map_err(|e| transport_error("service name", e))?;
    let alignment = Alignment::new(PAYLOAD_ALIGNMENT)
        .ok_or_else(|| RpcError::Internal("invalid payload alignment".to_string()))?;
    node.service_builder(&name)
        .publish_subscribe::<[u8]>()
        .user_header::<RpcHeader>()
        .payload_alignment(alignment)
        // A channel is shared by several processes: every consumer publishes its
        // requests and every provider publishes its responses on it.
        .max_publishers(MAX_PUBLISHERS)
        .max_subscribers(MAX_SUBSCRIBERS)
        .max_nodes(MAX_NODES)
        .subscriber_max_buffer_size(SUBSCRIBER_BUFFER)
        // Without it the receiver silently overwrites its oldest pending sample
        // when its buffer is full (iceoryx2's "safe overflow"), which loses a
        // request that will never be answered. Disabled, a full buffer makes
        // `send()` report 0 delivered and [`publish_until_delivered`] retries,
        // turning the overflow into backpressure instead of data loss.
        .enable_safe_overflow(false)
        .open_or_create()
        .map_err(|e| transport_error("open service", e))
}

/// Opens the event service used as a wake-up signal for `channel`.
fn open_event_service(node: &IoxNode, channel: &str, suffix: &str) -> Result<IoxEvent, RpcError> {
    let topic = format!("{channel}{suffix}");
    let name = ServiceName::new(&topic).map_err(|e| transport_error("service name", e))?;
    node.service_builder(&name)
        .event()
        .open_or_create()
        .map_err(|e| transport_error("open event service", e))
}

/// Spawns the provider side of one channel: a thread subscribing to the request
/// channel, routing every request to the dispatcher registered under its
/// `service_id`, and publishing the responses on the response channel (each
/// carrying the request's correlation id).
///
/// `services` is the `(service_id, dispatcher)` table of the services sharing
/// the channel; a request whose id is unknown is logged and dropped.
pub fn spawn_native_service(
    channel: &str,
    services: Vec<(u32, ServiceDispatcher)>,
    stop: CancellationToken,
) -> JoinHandle<()> {
    let table: HashMap<u32, ServiceDispatcher> = services.into_iter().collect();
    let channel = channel.to_owned();
    std::thread::spawn(move || {
        let node = match shared_node() {
            Ok(node) => node,
            Err(e) => {
                log::error!("[transport] node creation failed for '{channel}': {e:?}");
                return;
            }
        };
        let request_service = match open_service(&node, &channel, REQUEST_SUFFIX) {
            Ok(service) => service,
            Err(e) => {
                log::error!("[transport] open request channel '{channel}' failed: {e:?}");
                return;
            }
        };
        let response_service = match open_service(&node, &channel, RESPONSE_SUFFIX) {
            Ok(service) => service,
            Err(e) => {
                log::error!("[transport] open response channel '{channel}' failed: {e:?}");
                return;
            }
        };
        let request_notify = match open_event_service(&node, &channel, REQUEST_NOTIFY_SUFFIX) {
            Ok(service) => service,
            Err(e) => {
                log::error!("[transport] open request event '{channel}' failed: {e:?}");
                return;
            }
        };
        let response_notify = match open_event_service(&node, &channel, RESPONSE_NOTIFY_SUFFIX) {
            Ok(service) => service,
            Err(e) => {
                log::error!("[transport] open response event '{channel}' failed: {e:?}");
                return;
            }
        };

        let subscriber = match request_service.subscriber_builder().create() {
            Ok(subscriber) => subscriber,
            Err(e) => {
                log::error!("[transport] request subscriber failed: {e:?}");
                return;
            }
        };
        let listener = match request_notify.listener_builder().create() {
            Ok(listener) => listener,
            Err(e) => {
                log::error!("[transport] request listener failed: {e:?}");
                return;
            }
        };
        let publisher = match response_service
            .publisher_builder()
            .initial_max_slice_len(MAX_SLICE_LEN)
            .max_loaned_samples(MAX_LOANED_SAMPLES)
            .create()
        {
            Ok(publisher) => publisher,
            Err(e) => {
                log::error!("[transport] response publisher failed: {e:?}");
                return;
            }
        };
        let response_notifier = match response_notify.notifier_builder().create() {
            Ok(notifier) => notifier,
            Err(e) => {
                log::error!("[transport] response notifier failed: {e:?}");
                return;
            }
        };

        let Ok(waitset) = WaitSetBuilder::new()
            .signal_handling_mode(crate::waitset_signal_handling_mode())
            .create::<Iox>()
        else {
            log::error!("[transport] waitset creation failed");
            return;
        };
        let Ok(guard) = waitset.attach_deadline(&listener, WAITSET_DEADLINE) else {
            log::error!("[transport] waitset attach failed");
            return;
        };

        log::info!(
            "[transport] channel '{channel}' ready ({} service(s))",
            table.len()
        );
        let mut idle_spins: u32 = 0;
        let mut signal_ticks: u32 = 0;
        // Timestamp of the last consumer wake-up (see [`should_notify`]).
        let mut last_notify_us: u64 = 0;
        while !stop.is_cancelled() {
            match subscriber.receive() {
                Ok(Some(sample)) => {
                    idle_spins = 0;
                    // A saturated service never reaches the blocking path below,
                    // where iceoryx2 reports the termination request: sampling
                    // the flag here keeps Ctrl+C responsive under load.
                    signal_ticks = signal_ticks.wrapping_add(1);
                    if signal_ticks & (SIGNAL_CHECK_SAMPLES - 1) == 0
                        && SignalHandler::termination_requested()
                    {
                        crate::request_shutdown();
                        break;
                    }
                    let request_header = *sample.user_header();
                    let payload: &[u8] = &sample;

                    if request_header.event_kind() != EventKind::Request {
                        log::warn!(
                            "[transport] '{channel}': unexpected {:?} sample",
                            request_header.event_kind()
                        );
                        continue;
                    }
                    if request_header.protocol_version != PROTOCOL_VERSION {
                        log::warn!(
                            "[transport] '{channel}': protocol {} != {PROTOCOL_VERSION}",
                            request_header.protocol_version
                        );
                    }

                    // A channel hosts several services: the id in the header
                    // selects the dispatcher to run.
                    let Some(dispatcher) = table.get(&request_header.service_id) else {
                        log::warn!(
                            "[transport] channel '{channel}': unknown service_id {:#010x} for method '{}' (service not registered, or id collision)",
                            request_header.service_id,
                            request_header.method()
                        );
                        continue;
                    };

                    // Publish first, then wake the consumer's response thread once
                    // per coalescing window: it runs in another process, so its
                    // parked state cannot be observed here (see `native_call`).
                    let mut wake = false;
                    for response in dispatcher.dispatch(request_header.method(), payload) {
                        if publish_response(&publisher, &request_header, &response).is_err() {
                            break;
                        }
                        wake = true;
                    }
                    if wake && should_notify_local(&mut last_notify_us) {
                        let _ = response_notifier.notify_with_custom_event_id(EventId::new(0));
                    }
                }
                Ok(None) => {
                    if idle_spins < IDLE_SPINS {
                        idle_spins += 1;
                        std::thread::yield_now();
                    } else {
                        let notified = wait_for_wakeup(&waitset, &guard);
                        // Only a notification means there is something to poll
                        // for; a bare deadline expiry keeps the thread blocked.
                        idle_spins = if notified { 0 } else { IDLE_SPINS };
                    }
                }
                Err(e) => {
                    log::warn!("[transport] request receive error: {e:?}");
                    idle_spins = 0;
                    std::thread::yield_now();
                }
            }
        }
        log::info!("[transport] channel '{channel}' stopped");
    })
}

// ---------------------------------------------------------------------------
// Channel registration — deferred start
// ---------------------------------------------------------------------------

/// One service waiting for its channel to be started.
struct RegisteredChannelService {
    id: u32,
    name: &'static str,
    dispatcher: ServiceDispatcher,
}

/// A channel the process provides, filled while its services initialize.
struct PendingChannel {
    services: Vec<RegisteredChannelService>,
    stop: CancellationToken,
}

fn channel_registry() -> &'static std::sync::Mutex<HashMap<String, PendingChannel>> {
    static REGISTRY: OnceLock<std::sync::Mutex<HashMap<String, PendingChannel>>> = OnceLock::new();
    REGISTRY.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// `true` once [`start_registered_channels`] ran: later registrations start
/// their channel immediately instead of joining the registry.
static CHANNELS_SEALED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Returns the name already registered under `id`, if any.
fn conflicting_name(entries: &[RegisteredChannelService], id: u32) -> Option<&'static str> {
    entries.iter().find(|svc| svc.id == id).map(|svc| svc.name)
}

/// Registers one service on the channel it belongs to.
///
/// Called by a generated provider from its `on_init`. The channel thread is
/// **not** started here: it starts in [`start_registered_channels`], once every
/// provider of the process has registered. That ordering is what keeps the
/// guarantee [`publish_until_delivered`] relies on — "a subscriber exists"
/// means "the provider is ready" — since a request can never be received before
/// its dispatcher is installed.
///
/// # Errors
/// A second service registered on the same channel under the same `service_id`
/// (a 32-bit FNV-1a collision) fails the bootstrap instead of silently routing
/// requests to the wrong dispatcher.
pub fn register_native_service(
    channel: &str,
    service_id: u32,
    service_name: &'static str,
    dispatcher: ServiceDispatcher,
) -> Result<(), RpcError> {
    if CHANNELS_SEALED.load(std::sync::atomic::Ordering::Acquire) {
        // A provider created after the seal (a lazily initialized service) can
        // no longer join its channel: it starts its own thread.
        spawn_native_service(
            channel,
            vec![(service_id, dispatcher)],
            crate::global_cancel_token().clone(),
        );
        return Ok(());
    }

    let mut registry = crate::sync::lock(channel_registry());
    let pending = registry
        .entry(channel.to_owned())
        .or_insert_with(|| PendingChannel {
            services: Vec::new(),
            stop: crate::global_cancel_token().clone(),
        });

    if let Some(previous) = conflicting_name(&pending.services, service_id) {
        return Err(RpcError::TransportError(format!(
            "service_id collision on channel '{channel}': '{service_name}' and \
             '{previous}' both map to {service_id:#010x}"
        )));
    }

    pending.services.push(RegisteredChannelService {
        id: service_id,
        name: service_name,
        dispatcher,
    });
    log::debug!("[transport] '{service_name}' registered on channel '{channel}'");
    Ok(())
}

/// Starts one thread per registered channel.
///
/// Called once, after every provider `on_init`, by
/// [`ServiceLocator::initialize_all`](crate::locator::ServiceLocator::initialize_all).
pub fn start_registered_channels() {
    if CHANNELS_SEALED.swap(true, std::sync::atomic::Ordering::AcqRel) {
        return;
    }

    let channels: Vec<(String, PendingChannel)> =
        crate::sync::lock(channel_registry()).drain().collect();

    for (channel, pending) in channels {
        let services = pending
            .services
            .into_iter()
            .map(|svc| (svc.id, svc.dispatcher))
            .collect();
        spawn_native_service(&channel, services, pending.stop);
    }
}

/// Publishes `header ++ payload` on `publisher`, retrying until at least one
/// subscriber receives it or `timeout` elapses.
///
/// `send()` reports how many subscribers received the sample; `0` means it was
/// dropped because nobody was connected. Without this check, a consumer started
/// before its provider would send the request into the void and hang forever.
fn publish_until_delivered(
    publisher: &IoxPublisher,
    header: RpcHeader,
    payload: &[u8],
    timeout: Duration,
) -> Result<(), RpcError> {
    let deadline = std::time::Instant::now() + timeout;
    let mut attempts: u32 = 0;
    loop {
        // This is a **blocking** loop, and it can be the only thing running (a
        // consumer started before its provider, a provider whose consumer went
        // away). It must therefore observe the shutdown itself: otherwise Ctrl+C
        // is ignored for the whole deadline, and `main` cannot return — dropping
        // the runtime waits for this very thread.
        if crate::global_cancel_token().is_cancelled()
            || crate::registry_cancel_token().is_cancelled()
        {
            return Err(RpcError::Cancelled);
        }
        let len = payload.len().max(1);
        let sample = publisher
            .loan_slice_uninit(len)
            .map_err(|e| transport_error("loan sample", e))?;
        let mut sample = sample.write_from_fn(|i| payload.get(i).copied().unwrap_or(0));
        *sample.user_header_mut() = header;
        let delivered = sample
            .send()
            .map_err(|e| transport_error("send sample", e))?;
        if delivered > 0 {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(RpcError::TransportError(
                "no subscriber connected (is the provider running?)".to_string(),
            ));
        }
        if attempts < PUBLISH_SPIN_ATTEMPTS {
            // A full channel is the normal case of a burst: the receiver frees a
            // slot in microseconds, while sleeping costs at least the 1 ms
            // system timer on Windows.
            attempts += 1;
            std::thread::yield_now();
        } else {
            // Slow path: this call path owns no `WaitSet`, so if nothing else in
            // the process is parked on one nobody would report the termination
            // request. Sampling the flag here keeps Ctrl+C effective even when
            // this wait is the only running code.
            if SignalHandler::termination_requested() {
                crate::request_shutdown();
                return Err(RpcError::Cancelled);
            }
            std::thread::sleep(PUBLISH_RETRY_SLEEP);
        }
    }
}

/// Publishes one response sample, carrying the request's correlation id in its
/// zero-copy header.
fn publish_response(
    publisher: &IoxPublisher,
    request: &RpcHeader,
    response: &[u8],
) -> Result<(), RpcError> {
    let header = RpcHeader::response_from(request, EventKind::Next, request.service_version);
    publish_until_delivered(publisher, header, response, CONSUMER_WAIT_TIMEOUT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
    struct Sample {
        id: u32,
        count: u64,
    }

    #[test]
    fn decode_aligned_tolerates_an_unaligned_payload() {
        let value = Sample {
            id: 7,
            count: 0x0102_0304_0506_0708,
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&value).unwrap();

        // Simulate a payload starting at an odd offset.
        let mut shifted = vec![0u8];
        shifted.extend_from_slice(&bytes);

        let decoded = decode_aligned::<Sample>(&shifted[1..]).expect("aligned decode");
        assert_eq!(decoded, value);
    }

    #[test]
    fn dispatcher_routes_known_methods_and_closes_unknown_ones() {
        let mut dispatcher = ServiceDispatcher::new();
        dispatcher.method("echo", |payload| {
            Box::new(std::iter::once(payload.to_vec()))
        });

        let responses: Vec<_> = dispatcher.dispatch("echo", b"hi").collect();
        assert_eq!(responses, vec![b"hi".to_vec()]);

        assert_eq!(dispatcher.dispatch("unknown", b"hi").count(), 0);
    }

    #[test]
    fn a_duplicate_service_id_is_reported_with_the_previous_name() {
        let entries = vec![
            RegisteredChannelService {
                id: 7,
                name: "GetPerson",
                dispatcher: ServiceDispatcher::new(),
            },
            RegisteredChannelService {
                id: 9,
                name: "SetPerson",
                dispatcher: ServiceDispatcher::new(),
            },
        ];

        assert_eq!(conflicting_name(&entries, 9), Some("SetPerson"));
        assert_eq!(conflicting_name(&entries, 11), None);
    }

    #[test]
    fn a_channel_table_routes_by_service_id() {
        let mut first = ServiceDispatcher::new();
        first.method("echo", |payload| {
            Box::new(std::iter::once(payload.to_vec()))
        });
        let mut second = ServiceDispatcher::new();
        second.method("ping", |_payload| Box::new(std::iter::empty()));

        let table: HashMap<u32, ServiceDispatcher> =
            vec![(7, first), (9, second)].into_iter().collect();

        assert_eq!(table.get(&7).unwrap().dispatch("echo", b"x").count(), 1);
        assert_eq!(table.get(&9).unwrap().dispatch("echo", b"x").count(), 0);
        assert!(!table.contains_key(&11));
    }
}
