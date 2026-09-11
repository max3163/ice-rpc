//! Publish/subscribe transport: one request channel and one response channel
//! per service, correlated by a 16-byte request id.
//!
//! # Why pub/sub
//!
//! iceoryx2's request/response pattern allocates one channel (and one data
//! segment) per request in flight, which caps the throughput far below what the
//! shared-memory bus can do. The pub/sub pattern uses **one** publisher and
//! **one** subscriber per process and per service, so a call allocates nothing
//! but a loaned sample. Correlating the responses with a request id gives the
//! same call/response semantics for a fraction of the cost.
//!
//! # Wire format
//!
//! - **request sample**: `cid[16] ++ [method_len: u16 BE][method utf8][payload]`
//! - **response sample**: `cid[16] ++ rkyv(WireEvent<T, E>)`
//!
//! The method name is carried by the request only; a response is routed purely
//! by its correlation id.
//!
//! # Threads
//!
//! - the **provider** runs one dispatch thread per provided service (it
//!   subscribes to the request channel and publishes on the response channel);
//! - the **consumer** runs one dispatch thread per consumed service (it
//!   subscribes to the response channel and routes each sample to the handler
//!   registered for its correlation id).
//!
//! Both loops block on iceoryx2's `WaitSet`, so an idle process consumes no CPU.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;

use iceoryx2::prelude::*;
use iceoryx2::service::ipc_threadsafe;

use crate::types::{
    normalize_wire_event, unbounded_channel, Event, Observable, ObservableError, RpcError,
    WireEvent,
};
use crate::CancellationToken;

/// Concrete iceoryx2 service flavour used by the transport.
type Iox = ipc_threadsafe::Service;
type IoxNode = iceoryx2::node::Node<Iox>;
type IoxPubSub = iceoryx2::service::port_factory::publish_subscribe::PortFactory<Iox, [u8], ()>;
type IoxPublisher = iceoryx2::port::publisher::Publisher<Iox, [u8], ()>;
type IoxSubscriber = iceoryx2::port::subscriber::Subscriber<Iox, [u8], ()>;

/// Size of the correlation id prefixing every sample.
pub const CORRELATION_ID_LEN: usize = 16;

/// Suffixes of the two iceoryx2 services backing one logical service.
const REQUEST_SUFFIX: &str = "_req";
const RESPONSE_SUFFIX: &str = "_resp";

/// Samples a subscriber can buffer before backpressure is reported.
///
/// Sized to absorb a full benchmark burst without dropping a request.
const SUBSCRIBER_BUFFER: usize = 16_384;

/// Samples a publisher can keep loaned at once.
///
/// iceoryx2 defaults it to 8, which caps the number of concurrently in-flight
/// sends; a high-throughput burst (hundreds of calls in flight) would fail with
/// `ExceedsMaxLoans` long before that.
const MAX_LOANED_SAMPLES: usize = 16_384;

/// Initial slice length of a sample. Large payloads grow the segment on demand.
///
/// Kept small: iceoryx2 pre-allocates `buffer × slice` bytes, so a large slice
/// multiplied by [`MAX_LOANED_SAMPLES`] would reserve tens of megabytes.
const MAX_SLICE_LEN: usize = 256;

/// Consecutive idle polls spent spinning before parking the thread.
const IDLE_SPINS: u32 = 2_000;

/// Waits for the next sample without burning a core.
///
/// The first [`IDLE_SPINS`] iterations `yield`, which keeps the latency at the
/// microsecond level under load (a `sleep` would be floored at the Windows
/// timer granularity, ~1 ms). Beyond that the thread sleeps 1 ms, so an idle
/// service consumes no CPU.
fn idle_backoff(spins: &mut u32) {
    if *spins < IDLE_SPINS {
        *spins += 1;
        std::thread::yield_now();
    } else {
        std::thread::sleep(Duration::from_millis(1));
    }
}

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

/// Allocates a process-unique correlation id: `pid ++ counter`.
pub fn next_correlation_id() -> [u8; CORRELATION_ID_LEN] {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let pid = std::process::id() as u64;
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut out = [0u8; CORRELATION_ID_LEN];
    out[..8].copy_from_slice(&pid.to_be_bytes());
    out[8..].copy_from_slice(&counter.to_be_bytes());
    out
}

/// Encodes a request body as `[method_len: u16 BE][method utf8][payload]`.
pub fn encode_request(method: &str, payload: &[u8]) -> Vec<u8> {
    let method = method.as_bytes();
    let len = method.len().min(u16::MAX as usize);
    let mut out = Vec::with_capacity(2 + len + payload.len());
    out.extend_from_slice(&(len as u16).to_be_bytes());
    out.extend_from_slice(&method[..len]);
    out.extend_from_slice(payload);
    out
}

/// Decodes a request body produced by [`encode_request`].
pub fn decode_request(bytes: &[u8]) -> Option<(String, &[u8])> {
    if bytes.len() < 2 {
        return None;
    }
    let len = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    if bytes.len() < 2 + len {
        return None;
    }
    let method = std::str::from_utf8(&bytes[2..2 + len]).ok()?;
    Some((method.to_owned(), &bytes[2 + len..]))
}

/// Decodes a rkyv payload whose backing buffer may be **unaligned**.
///
/// The sample framing makes the payload start at an arbitrary offset, and
/// `rkyv::from_bytes` requires the archived type's alignment, so the bytes are
/// first copied into a 16-byte-aligned `AlignedVec` — the alignment
/// `rkyv::to_bytes` produces.
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
        match crate::rt::block_on(observable.recv()) {
            Ok(event) => {
                let wire: WireEvent<T, E> = event.into();
                rkyv::to_bytes::<rkyv::rancor::Error>(&wire)
                    .ok()
                    .map(|bytes| bytes.to_vec())
            }
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

/// Typed handler invoked with the rkyv response body (already stripped of the
/// correlation id) of one in-flight call.
type ResponseHandler = Arc<dyn Fn(&[u8]) + Send + Sync>;

fn response_handlers(
) -> &'static std::sync::Mutex<HashMap<[u8; CORRELATION_ID_LEN], ResponseHandler>> {
    static HANDLERS: OnceLock<
        std::sync::Mutex<HashMap<[u8; CORRELATION_ID_LEN], ResponseHandler>>,
    > = OnceLock::new();
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

/// Publishes and subscribes ports of one consumed service, kept alive.
struct ConsumerPorts {
    _request_service: IoxPubSub,
    _response_service: IoxPubSub,
    publisher: IoxPublisher,
}

fn consumer_cache() -> &'static std::sync::Mutex<HashMap<String, Arc<ConsumerPorts>>> {
    static CACHE: OnceLock<std::sync::Mutex<HashMap<String, Arc<ConsumerPorts>>>> = OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Opens (once per process) the ports of `service_name` on the consumer side and
/// starts the response dispatch thread.
///
/// The cache lock is held across the creation so a single set of ports is
/// created even when many workers race on the first call.
fn consumer_ports(service_name: &str) -> Result<Arc<ConsumerPorts>, RpcError> {
    let mut cache = crate::sync::lock(consumer_cache());
    if let Some(ports) = cache.get(service_name) {
        return Ok(ports.clone());
    }

    let node = shared_node()?;
    let request_service = open_service(&node, service_name, REQUEST_SUFFIX)?;
    let response_service = open_service(&node, service_name, RESPONSE_SUFFIX)?;
    let publisher = request_service
        .publisher_builder()
        .initial_max_slice_len(MAX_SLICE_LEN)
        .max_loaned_samples(MAX_LOANED_SAMPLES)
        .create()
        .map_err(|e| transport_error("request publisher", e))?;
    let subscriber = response_service
        .subscriber_builder()
        .create()
        .map_err(|e| transport_error("response subscriber", e))?;

    spawn_response_dispatcher(service_name.to_owned(), subscriber);

    let ports = Arc::new(ConsumerPorts {
        _request_service: request_service,
        _response_service: response_service,
        publisher,
    });
    cache.insert(service_name.to_owned(), ports.clone());
    Ok(ports)
}

/// Blocks on the response channel and routes every sample to its handler.
fn spawn_response_dispatcher(service_name: String, subscriber: IoxSubscriber) {
    let handle = crate::rt::spawn_blocking(move || {
        let cancel = crate::global_cancel_token().clone();
        let mut idle_spins: u32 = 0;
        loop {
            if cancel.is_cancelled() {
                break;
            }
            match subscriber.receive() {
                Ok(Some(sample)) => {
                    idle_spins = 0;
                    let bytes: &[u8] = &sample;
                    if bytes.len() <= CORRELATION_ID_LEN {
                        continue;
                    }
                    let mut cid = [0u8; CORRELATION_ID_LEN];
                    cid.copy_from_slice(&bytes[..CORRELATION_ID_LEN]);
                    let handler = crate::sync::lock(response_handlers()).get(&cid).cloned();
                    if let Some(handler) = handler {
                        handler(&bytes[CORRELATION_ID_LEN..]);
                    }
                }
                Ok(None) => idle_backoff(&mut idle_spins),
                Err(e) => {
                    log::warn!("[transport] response receive error: {e:?}");
                    idle_backoff(&mut idle_spins);
                }
            }
        }
        log::debug!("[transport] response dispatcher for '{service_name}' stopped");
    });
    crate::locator::ServiceLocator::global().register_shutdown_handle(handle);
}

/// Sends `payload` as the `method` call of `service_name` and returns the
/// streamed responses as an [`Observable`].
pub fn native_call<T, E>(
    service_name: &str,
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
    let ports = consumer_ports(service_name)?;
    let cid = next_correlation_id();
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

    // `cid ++ [method_len][method][payload]`
    let body = encode_request(method, payload);
    let mut frame = Vec::with_capacity(CORRELATION_ID_LEN + body.len());
    frame.extend_from_slice(&cid);
    frame.extend_from_slice(&body);

    let len = frame.len().max(1);
    let sample = ports
        .publisher
        .loan_slice_uninit(len)
        .map_err(|e| transport_error("loan request", e))?;
    sample
        .write_from_fn(|i| frame.get(i).copied().unwrap_or(0))
        .send()
        .map_err(|e| {
            unregister_response_handler(&cid);
            transport_error("send request", e)
        })?;

    Ok(rx)
}

// ---------------------------------------------------------------------------
// Provider side: request dispatch and response publication
// ---------------------------------------------------------------------------

/// Opens one iceoryx2 pub/sub service dedicated to `service_name`.
fn open_service(node: &IoxNode, service_name: &str, suffix: &str) -> Result<IoxPubSub, RpcError> {
    let topic = format!("{service_name}{suffix}");
    let name = ServiceName::new(&topic).map_err(|e| transport_error("service name", e))?;
    node.service_builder(&name)
        .publish_subscribe::<[u8]>()
        .max_publishers(1)
        .max_subscribers(8)
        .subscriber_max_buffer_size(SUBSCRIBER_BUFFER)
        .open_or_create()
        .map_err(|e| transport_error("open service", e))
}

/// Spawns the provider side of one service: a thread subscribing to the request
/// channel, dispatching each request, and publishing the responses on the
/// response channel (each prefixed with the request's correlation id).
pub fn spawn_native_service<F>(
    service_name: &str,
    dispatcher: F,
    stop: CancellationToken,
) -> JoinHandle<()>
where
    F: Fn(&str, &[u8]) -> ResponseIter + Send + Sync + 'static,
{
    let service_name = service_name.to_owned();
    std::thread::spawn(move || {
        let node = match shared_node() {
            Ok(node) => node,
            Err(e) => {
                log::error!("[transport] node creation failed for '{service_name}': {e:?}");
                return;
            }
        };
        let request_service = match open_service(&node, &service_name, REQUEST_SUFFIX) {
            Ok(service) => service,
            Err(e) => {
                log::error!("[transport] open request service '{service_name}' failed: {e:?}");
                return;
            }
        };
        let response_service = match open_service(&node, &service_name, RESPONSE_SUFFIX) {
            Ok(service) => service,
            Err(e) => {
                log::error!("[transport] open response service '{service_name}' failed: {e:?}");
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

        log::info!("[transport] service '{service_name}' ready");
        let mut idle_spins: u32 = 0;
        while !stop.is_cancelled() {
            match subscriber.receive() {
                Ok(Some(sample)) => {
                    idle_spins = 0;
                    let bytes: &[u8] = &sample;
                    if bytes.len() <= CORRELATION_ID_LEN {
                        continue;
                    }
                    let cid = &bytes[..CORRELATION_ID_LEN];
                    let Some((method, payload)) = decode_request(&bytes[CORRELATION_ID_LEN..])
                    else {
                        log::warn!("[transport] '{service_name}': malformed request");
                        continue;
                    };
                    for response in dispatcher(&method, payload) {
                        if publish_response(&publisher, cid, &response).is_err() {
                            break;
                        }
                    }
                }
                Ok(None) => idle_backoff(&mut idle_spins),
                Err(e) => {
                    log::warn!("[transport] request receive error: {e:?}");
                    idle_backoff(&mut idle_spins);
                }
            }
        }
        log::info!("[transport] service '{service_name}' stopped");
    })
}

/// Publishes one response sample as `cid ++ rkyv(WireEvent)`.
fn publish_response(publisher: &IoxPublisher, cid: &[u8], response: &[u8]) -> Result<(), RpcError> {
    let len = (CORRELATION_ID_LEN + response.len()).max(1);
    let sample = publisher
        .loan_slice_uninit(len)
        .map_err(|e| transport_error("loan response", e))?;
    sample
        .write_from_fn(|i| {
            if i < CORRELATION_ID_LEN {
                cid.get(i).copied().unwrap_or(0)
            } else {
                response.get(i - CORRELATION_ID_LEN).copied().unwrap_or(0)
            }
        })
        .send()
        .map_err(|e| transport_error("send response", e))?;
    Ok(())
}
