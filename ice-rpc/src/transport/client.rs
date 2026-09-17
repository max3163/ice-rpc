//! Consumer side: request publication and response routing.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use iceoryx2::prelude::*;
use iceoryx2_bb_posix::signal::SignalHandler;

use super::notify::Coalescer;
use super::server::{open_event_service, open_service, OpenMode};
use super::{
    shared_node, transport_error, IoxEvent, IoxListener, IoxNotifier, IoxPubSub, IoxPublisher,
    IoxSubscriber, MAX_LOANED_SAMPLES, MAX_SLICE_LEN, PROVIDER_WAIT_DEFAULT, PUBLISH_RETRY_SLEEP,
    PUBLISH_SPIN_ATTEMPTS, REQUEST_NOTIFY_SUFFIX, REQUEST_SUFFIX, RESPONSE_NOTIFY_SUFFIX,
    RESPONSE_SUFFIX,
};
use crate::global::Registry;
use crate::types::{
    normalize_wire_event, unbounded_channel, Event, Observable, ObservableError, RpcError,
    RpcHeader, WireEvent, CORRELATION_ID_LEN,
};

/// Typed handler invoked with the rkyv response payload of one in-flight call.
type ResponseHandler = Arc<dyn Fn(&[u8]) + Send + Sync>;

/// Handlers of the calls in flight, keyed by correlation id.
fn response_handlers() -> &'static Registry<[u8; CORRELATION_ID_LEN], ResponseHandler> {
    static HANDLERS: Registry<[u8; CORRELATION_ID_LEN], ResponseHandler> = Registry::new();
    &HANDLERS
}

/// Registers the handler of one in-flight call.
fn register_response_handler(cid: [u8; CORRELATION_ID_LEN], handler: ResponseHandler) {
    response_handlers().insert(cid, handler);
}

/// Removes the handler of one in-flight call.
fn unregister_response_handler(cid: &[u8; CORRELATION_ID_LEN]) {
    response_handlers().remove(cid);
}

/// Publishes port, wake-up notifier and response event service of a consumed
/// service, all kept alive for the process lifetime.
struct ConsumerPorts {
    _request_service: IoxPubSub,
    _response_service: IoxPubSub,
    _request_notify: IoxEvent,
    publisher: IoxPublisher,
    request_notifier: IoxNotifier,
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
fn consumer_cache() -> &'static Registry<String, Arc<ConsumerPorts>> {
    static CACHE: Registry<String, Arc<ConsumerPorts>> = Registry::new();
    &CACHE
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

    // The listener must outlive the dispatch thread that blocks on it.
    spawn_response_dispatcher(channel.to_owned(), subscriber, listener, response_notify);

    let ports = Arc::new(ConsumerPorts {
        _request_service: request_service,
        _response_service: response_service,
        _request_notify: request_notify,
        last_request_notify: Coalescer::new(),
        seq: AtomicU64::new(0),
        publisher,
        request_notifier,
    });
    Ok(ports)
}

/// Blocks on the response event and routes every sample to its handler.
fn spawn_response_dispatcher(
    channel: String,
    subscriber: IoxSubscriber,
    listener: IoxListener,
    _response_notify: IoxEvent,
) {
    let handle = crate::rt::spawn_blocking(move || {
        let cancel = crate::global_cancel_token().clone();
        super::pump::run_receive_loop(
            &channel,
            "response",
            &subscriber,
            &listener,
            || cancel.is_cancelled(),
            |header, payload| {
                // The handler is cloned out of the registry so the lock is
                // released before the handler runs.
                if let Some(handler) = response_handlers().get_cloned(&header.correlation_id) {
                    handler(payload);
                }
            },
        );
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
    let header = RpcHeader::request(method, service_id, 1)
        .with_seq(ports.seq.fetch_add(1, Ordering::Relaxed));
    let cid = header.correlation_id;
    let (tx, rx) = unbounded_channel::<T, E>();

    let handler: ResponseHandler =
        Arc::new(
            move |bytes: &[u8]| match super::decode_aligned::<WireEvent<T, E>>(bytes) {
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
    if ports.last_request_notify.should_notify() {
        let _ = ports
            .request_notifier
            .notify_with_custom_event_id(EventId::new(0));
    }

    Ok(rx)
}

/// Publishes `header ++ payload` on `publisher`, retrying until at least one
/// subscriber receives it or `timeout` elapses.
pub(super) fn publish_until_delivered(
    publisher: &IoxPublisher,
    header: RpcHeader,
    payload: &[u8],
    timeout: Duration,
) -> Result<(), RpcError> {
    let deadline = Instant::now() + timeout;
    let mut attempts: u32 = 0;
    loop {
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
        if Instant::now() >= deadline {
            return Err(RpcError::TransportError(
                "no subscriber connected (is the provider running?)".to_string(),
            ));
        }
        if attempts < PUBLISH_SPIN_ATTEMPTS {
            attempts += 1;
            std::thread::yield_now();
        } else {
            // This call path owns no `WaitSet`: sample the OS termination flag so
            // Ctrl+C is honoured even when this wait is the only running code.
            if SignalHandler::termination_requested() {
                crate::request_shutdown();
                return Err(RpcError::Cancelled);
            }
            std::thread::sleep(PUBLISH_RETRY_SLEEP);
        }
    }
}
