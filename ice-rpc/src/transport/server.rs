//! Provider side: channel creation, request dispatch, response publication and
//! the deferred channel start used during service initialization.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::thread::JoinHandle;

use iceoryx2::prelude::*;
use iceoryx2_bb_posix::signal::SignalHandler;

use super::bridge::ServiceDispatcher;
use super::client::publish_until_delivered;
use super::notify::should_notify_local;
use super::waitset::wait_for_wakeup;
use super::{
    shared_node, transport_error, Iox, IoxEvent, IoxNode, IoxPubSub, IoxPublisher,
    CONSUMER_WAIT_TIMEOUT, IDLE_SPINS, MAX_LOANED_SAMPLES, MAX_NODES, MAX_PUBLISHERS,
    MAX_SLICE_LEN, MAX_SUBSCRIBERS, PAYLOAD_ALIGNMENT, REQUEST_NOTIFY_SUFFIX, REQUEST_SUFFIX,
    RESPONSE_NOTIFY_SUFFIX, RESPONSE_SUFFIX, SIGNAL_CHECK_SAMPLES, SUBSCRIBER_BUFFER,
    WAITSET_DEADLINE,
};
use crate::types::{EventKind, RpcError, RpcHeader, PROTOCOL_VERSION};
use crate::CancellationToken;

/// Opens the pub/sub service of one direction of `channel`.
pub(super) fn open_service(
    node: &IoxNode,
    channel: &str,
    suffix: &str,
) -> Result<IoxPubSub, RpcError> {
    let topic = format!("{channel}{suffix}");
    let name = ServiceName::new(&topic).map_err(|e| transport_error("service name", e))?;
    let alignment = Alignment::new(PAYLOAD_ALIGNMENT)
        .ok_or_else(|| RpcError::Internal("invalid payload alignment".to_string()))?;
    node.service_builder(&name)
        .publish_subscribe::<[u8]>()
        .user_header::<RpcHeader>()
        .payload_alignment(alignment)
        // Every consumer publishes its requests and every provider its responses.
        .max_publishers(MAX_PUBLISHERS)
        .max_subscribers(MAX_SUBSCRIBERS)
        .max_nodes(MAX_NODES)
        .subscriber_max_buffer_size(SUBSCRIBER_BUFFER)
        // Enabled, the receiver silently overwrites its oldest pending sample: a
        // full buffer must report 0 delivered so the sender retries.
        .enable_safe_overflow(false)
        .open_or_create()
        .map_err(|e| transport_error("open service", e))
}

/// Opens the event service used as a wake-up signal for `channel`.
pub(super) fn open_event_service(
    node: &IoxNode,
    channel: &str,
    suffix: &str,
) -> Result<IoxEvent, RpcError> {
    let topic = format!("{channel}{suffix}");
    let name = ServiceName::new(&topic).map_err(|e| transport_error("service name", e))?;
    node.service_builder(&name)
        .event()
        .open_or_create()
        .map_err(|e| transport_error("open event service", e))
}

/// Spawns the provider side of one channel: a thread that routes every request to
/// the dispatcher registered under its `service_id` and publishes the responses.
///
/// `services` is the `(service_id, dispatcher)` table of the services sharing the
/// channel; a request whose id is unknown is logged and dropped.
pub fn spawn_native_service(
    channel: &str,
    services: Vec<(u32, ServiceDispatcher)>,
    stop: CancellationToken,
) -> JoinHandle<()> {
    let table: HashMap<u32, ServiceDispatcher> = services.into_iter().collect();
    let channel = channel.to_owned();
    std::thread::spawn(move || {
        // Any port that cannot be opened makes the whole channel useless: log it
        // and stop the thread.
        macro_rules! or_stop {
            ($what:literal, $result:expr) => {
                match $result {
                    Ok(port) => port,
                    Err(e) => {
                        log::error!("[transport] {} on '{channel}': {e:?}", $what);
                        return;
                    }
                }
            };
        }

        let node = or_stop!("node creation", shared_node());
        let request_service = or_stop!(
            "request channel",
            open_service(&node, &channel, REQUEST_SUFFIX)
        );
        let response_service = or_stop!(
            "response channel",
            open_service(&node, &channel, RESPONSE_SUFFIX)
        );
        let request_notify = or_stop!(
            "request event",
            open_event_service(&node, &channel, REQUEST_NOTIFY_SUFFIX)
        );
        let response_notify = or_stop!(
            "response event",
            open_event_service(&node, &channel, RESPONSE_NOTIFY_SUFFIX)
        );

        let subscriber = or_stop!(
            "request subscriber",
            request_service.subscriber_builder().create()
        );
        let listener = or_stop!(
            "request listener",
            request_notify.listener_builder().create()
        );
        let publisher = or_stop!(
            "response publisher",
            response_service
                .publisher_builder()
                .initial_max_slice_len(MAX_SLICE_LEN)
                .max_loaned_samples(MAX_LOANED_SAMPLES)
                .create()
        );
        let response_notifier = or_stop!(
            "response notifier",
            response_notify.notifier_builder().create()
        );

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
        let mut last_notify_us: u64 = 0;
        while !stop.is_cancelled() {
            match subscriber.receive() {
                Ok(Some(sample)) => {
                    idle_spins = 0;
                    // A saturated service never reaches the blocking path below,
                    // where iceoryx2 reports the termination request.
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

                    // Publish the responses, then wake the consumer's response
                    // thread once per coalescing window.
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

/// `true` once [`start_registered_channels`] ran: later registrations start their
/// channel immediately instead of joining the registry.
static CHANNELS_SEALED: AtomicBool = AtomicBool::new(false);

/// Returns the name already registered under `id`, if any.
fn conflicting_name(entries: &[RegisteredChannelService], id: u32) -> Option<&'static str> {
    entries.iter().find(|svc| svc.id == id).map(|svc| svc.name)
}

/// Registers one service on the channel it belongs to.
///
/// Called by a generated provider from its `on_init`. The channel thread starts
/// later, in [`start_registered_channels`], once the dispatcher table is complete.
pub fn register_native_service(
    channel: &str,
    service_id: u32,
    service_name: &'static str,
    dispatcher: ServiceDispatcher,
) -> Result<(), RpcError> {
    if CHANNELS_SEALED.load(Ordering::Acquire) {
        // A provider created after the seal (a lazily initialized service) can no
        // longer join its channel: it starts its own thread.
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
/// Called once, after every provider `on_init`.
pub fn start_registered_channels() {
    if CHANNELS_SEALED.swap(true, Ordering::AcqRel) {
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

#[cfg(test)]
mod tests {
    use super::*;

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
