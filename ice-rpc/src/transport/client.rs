//! Consumer side: request publication and response routing.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use iceoryx2::prelude::*;
use iceoryx2_bb_posix::signal::SignalHandler;

use super::notify::should_notify;
use super::server::{open_event_service, open_service};
use super::waitset::wait_for_wakeup;
use super::{
    shared_node, transport_error, Iox, IoxEvent, IoxListener, IoxNotifier, IoxPubSub, IoxPublisher,
    IoxSubscriber, IDLE_SPINS, MAX_LOANED_SAMPLES, MAX_SLICE_LEN, PROVIDER_WAIT_DEFAULT,
    PUBLISH_RETRY_SLEEP, PUBLISH_SPIN_ATTEMPTS, REQUEST_NOTIFY_SUFFIX, REQUEST_SUFFIX,
    RESPONSE_NOTIFY_SUFFIX, RESPONSE_SUFFIX, SIGNAL_CHECK_SAMPLES, WAITSET_DEADLINE,
};
use crate::types::{
    normalize_wire_event, unbounded_channel, Event, Observable, ObservableError, RpcError,
    RpcHeader, WireEvent, CORRELATION_ID_LEN,
};

/// Typed handler invoked with the rkyv response payload of one in-flight call.
type ResponseHandler = Arc<dyn Fn(&[u8]) + Send + Sync>;

type HandlerMap = HashMap<[u8; CORRELATION_ID_LEN], ResponseHandler>;

fn response_handlers() -> &'static std::sync::Mutex<HandlerMap> {
    static HANDLERS: OnceLock<std::sync::Mutex<HandlerMap>> = OnceLock::new();
    HANDLERS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Registers the handler of one in-flight call.
fn register_response_handler(cid: [u8; CORRELATION_ID_LEN], handler: ResponseHandler) {
    crate::sync::lock(response_handlers()).insert(cid, handler);
}

/// Removes the handler of one in-flight call.
fn unregister_response_handler(cid: &[u8; CORRELATION_ID_LEN]) {
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
    /// Timestamp of the last provider wake-up.
    last_request_notify: AtomicU64,
}

fn consumer_cache() -> &'static std::sync::Mutex<HashMap<String, Arc<ConsumerPorts>>> {
    static CACHE: OnceLock<std::sync::Mutex<HashMap<String, Arc<ConsumerPorts>>>> = OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
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
                    // A saturated channel never reaches the blocking path below,
                    // where iceoryx2 reports the termination request.
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
    if should_notify(&ports.last_request_notify) {
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
