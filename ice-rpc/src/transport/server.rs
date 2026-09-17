//! Provider side: channel creation, request dispatch, response publication and
//! the deferred channel start used during service initialization.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

use iceoryx2::prelude::*;

use super::bridge::{ResponseEmitter, ServiceDispatcher};
use super::client::publish_until_delivered;
use super::notify::Coalescer;
use super::{
    shared_node, transport_error, IoxEvent, IoxListener, IoxNode, IoxNotifier, IoxPubSub,
    IoxPublisher, IoxSubscriber, CONSUMER_WAIT_TIMEOUT, MAX_LOANED_SAMPLES, MAX_NODES,
    MAX_PUBLISHERS, MAX_SLICE_LEN, MAX_SUBSCRIBERS, PAYLOAD_ALIGNMENT, REQUEST_NOTIFY_SUFFIX,
    REQUEST_SUFFIX, RESPONSE_NOTIFY_SUFFIX, RESPONSE_SUFFIX, SUBSCRIBER_BUFFER,
};
use crate::global::Locked;
use crate::types::{EventKind, RpcError, RpcHeader, PROTOCOL_VERSION};
use crate::CancellationToken;

/// Whether opening a service may create it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OpenMode {
    /// Transport side: create the service when no peer owns it yet.
    CreateOrOpen,
    /// Observer side: attach to an existing service, never create one.
    ReadOnly,
}

/// Opens the pub/sub service of one direction of `channel`.
///
/// The definition is shared by the transport and the observer, so an
/// out-of-band observer is guaranteed to attach to the very service the
/// transport created.
pub(super) fn open_service(
    node: &IoxNode,
    channel: &str,
    suffix: &str,
    mode: OpenMode,
) -> Result<IoxPubSub, RpcError> {
    let topic = format!("{channel}{suffix}");
    let name = ServiceName::new(&topic).map_err(|e| transport_error("service name", e))?;
    let alignment = Alignment::new(PAYLOAD_ALIGNMENT)
        .ok_or_else(|| RpcError::Internal("invalid payload alignment".to_string()))?;
    let builder = node
        .service_builder(&name)
        .publish_subscribe::<[u8]>()
        .user_header::<RpcHeader>()
        .payload_alignment(alignment)
        // Every consumer publishes its requests and every provider its responses.
        .max_publishers(MAX_PUBLISHERS)
        .max_subscribers(MAX_SUBSCRIBERS)
        .max_nodes(MAX_NODES)
        .subscriber_max_buffer_size(SUBSCRIBER_BUFFER)
        // Must stay false: enabled, the receiver overwrites its oldest sample.
        .enable_safe_overflow(false);

    match mode {
        OpenMode::CreateOrOpen => builder
            .open_or_create()
            .map_err(|e| transport_error("open service", e)),
        OpenMode::ReadOnly => builder
            .open()
            .map_err(|e| transport_error("open service (read-only)", e)),
    }
}

/// Opens the event service used as a wake-up signal for `channel`.
pub(super) fn open_event_service(
    node: &IoxNode,
    channel: &str,
    suffix: &str,
    mode: OpenMode,
) -> Result<IoxEvent, RpcError> {
    let topic = format!("{channel}{suffix}");
    let name = ServiceName::new(&topic).map_err(|e| transport_error("service name", e))?;
    let builder = node.service_builder(&name).event();

    match mode {
        OpenMode::CreateOrOpen => builder
            .open_or_create()
            .map_err(|e| transport_error("open event service", e)),
        OpenMode::ReadOnly => builder
            .open()
            .map_err(|e| transport_error("open event service (read-only)", e)),
    }
}

/// The ports of one channel, provider side.
///
/// The service and event handles are kept even when no code reads them: dropping
/// one would close the iceoryx2 service the ports are attached to.
pub(super) struct ChannelPorts {
    _request_service: IoxPubSub,
    _response_service: IoxPubSub,
    _request_notify: IoxEvent,
    _response_notify: IoxEvent,
    /// Receives the requests of every service of the channel.
    pub(super) subscriber: IoxSubscriber,
    /// Wakes the subscriber up when a request is published.
    pub(super) listener: IoxListener,
    /// Publishes the responses of every service of the channel.
    pub(super) publisher: IoxPublisher,
    /// Wakes the consumers' dispatch threads up.
    pub(super) response_notifier: IoxNotifier,
}

/// Opens every port of one channel, provider side.
pub(super) fn open_channel_ports(channel: &str) -> Result<ChannelPorts, RpcError> {
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

    let subscriber = request_service
        .subscriber_builder()
        .create()
        .map_err(|e| transport_error("request subscriber", e))?;
    let listener = request_notify
        .listener_builder()
        .create()
        .map_err(|e| transport_error("request listener", e))?;
    let publisher = response_service
        .publisher_builder()
        .initial_max_slice_len(MAX_SLICE_LEN)
        .max_loaned_samples(MAX_LOANED_SAMPLES)
        .create()
        .map_err(|e| transport_error("response publisher", e))?;
    let response_notifier = response_notify
        .notifier_builder()
        .create()
        .map_err(|e| transport_error("response notifier", e))?;

    Ok(ChannelPorts {
        _request_service: request_service,
        _response_service: response_service,
        _request_notify: request_notify,
        _response_notify: response_notify,
        subscriber,
        listener,
        publisher,
        response_notifier,
    })
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
        let ports = match open_channel_ports(&channel) {
            Ok(ports) => ports,
            Err(e) => {
                log::error!("[transport] '{channel}': {e}");
                return;
            }
        };

        log::info!(
            "[transport] channel '{channel}' ready ({} service(s))",
            table.len()
        );

        let mut sink = ResponseSink::new(
            &channel,
            &ports.publisher,
            &ports._response_service,
            &ports.response_notifier,
        );
        super::pump::run_receive_loop(
            &channel,
            "request",
            &ports.subscriber,
            &ports.listener,
            || stop.is_cancelled(),
            |header, payload| handle_request(&channel, &table, &mut sink, header, payload),
        );
        log::info!("[transport] channel '{channel}' stopped");
    })
}

/// Publishes the responses of one channel and wakes the consumers up.
///
/// This is the transport end of [`ResponseEmitter`]: the generated handlers push
/// their samples into it, so a response goes from the serializer's scratch
/// buffer to the shared-memory sample without an intermediate allocation.
struct ResponseSink<'a> {
    channel: &'a str,
    publisher: &'a IoxPublisher,
    /// Response service, read for its subscriber count before every publication.
    service: &'a IoxPubSub,
    notifier: &'a IoxNotifier,
    /// Header of the request being answered, copied by [`ResponseSink::begin`].
    request: Option<RpcHeader>,
    /// Coalescing window of the consumer wake-ups.
    coalescer: Coalescer,
    /// Per-channel monotonic sample counter, stamped into `RpcHeader::seq`.
    ///
    /// One publisher per channel, drained by this single thread: a plain counter
    /// is enough and stays monotonic.
    seq: u64,
    /// Whether a response was published since the last [`ResponseSink::begin`].
    published: bool,
}

impl<'a> ResponseSink<'a> {
    fn new(
        channel: &'a str,
        publisher: &'a IoxPublisher,
        service: &'a IoxPubSub,
        notifier: &'a IoxNotifier,
    ) -> Self {
        Self {
            channel,
            publisher,
            service,
            notifier,
            request: None,
            coalescer: Coalescer::new(),
            seq: 0,
            published: false,
        }
    }

    /// Points the sink at the request whose responses are about to be emitted.
    fn begin(&mut self, request: &RpcHeader) {
        self.request = Some(*request);
    }

    /// Wakes the consumers' response threads up, if anything was published.
    fn finish(&mut self) {
        if std::mem::take(&mut self.published) && self.coalescer.should_notify() {
            let _ = self.notifier.notify_with_custom_event_id(EventId::new(0));
        }
    }
}

impl ResponseEmitter for ResponseSink<'_> {
    fn emit(&mut self, kind: EventKind, payload: &[u8]) -> bool {
        let Some(request) = self.request else {
            log::error!(
                "[transport] '{}': response emitted outside of a request",
                self.channel
            );
            return false;
        };

        let header =
            RpcHeader::response_from(&request, kind, request.service_version).with_seq(self.seq);
        self.seq = self.seq.wrapping_add(1);

        match publish_until_delivered(
            self.publisher,
            self.service,
            header,
            payload,
            CONSUMER_WAIT_TIMEOUT,
        ) {
            Ok(()) => {
                self.published = true;
                true
            }
            Err(e) => {
                log::warn!(
                    "[transport] '{}': response publication failed: {e}",
                    self.channel
                );
                false
            }
        }
    }
}

/// Routes one request to the dispatcher of its service and publishes the
/// responses.
fn handle_request(
    channel: &str,
    table: &HashMap<u32, ServiceDispatcher>,
    sink: &mut ResponseSink<'_>,
    header: &RpcHeader,
    payload: &[u8],
) {
    if header.event_kind() != EventKind::Request {
        log::warn!(
            "[transport] '{channel}': unexpected {:?} sample",
            header.event_kind()
        );
        return;
    }
    if header.protocol_version != PROTOCOL_VERSION {
        log::warn!(
            "[transport] '{channel}': protocol {} != {PROTOCOL_VERSION}",
            header.protocol_version
        );
    }

    // A channel hosts several services: the header id selects the dispatcher.
    let Some(dispatcher) = table.get(&header.service_id) else {
        log::warn!(
            "[transport] channel '{channel}': unknown service_id {:#010x} for method '{}' (service not registered, or id collision)",
            header.service_id,
            header.method()
        );
        return;
    };

    // The handler pushes its responses into the sink; the consumers are woken
    // once per coalescing window, and only if something was published.
    sink.begin(header);
    dispatcher.dispatch(header.method(), payload, sink);
    sink.finish();
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

/// Channels this process provides, keyed by channel name.
fn channel_registry() -> &'static Locked<HashMap<String, PendingChannel>> {
    static REGISTRY: Locked<HashMap<String, PendingChannel>> = Locked::new();
    &REGISTRY
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
        // A provider created after the seal (a lazily initialized service)
        // cannot join its channel: it starts its own thread.
        spawn_native_service(
            channel,
            vec![(service_id, dispatcher)],
            crate::global_cancel_token().clone(),
        );
        return Ok(());
    }

    // The registration is atomic: two services of one channel must never race
    // into a duplicate `service_id`.
    let registered = channel_registry().with(|registry| {
        let pending = registry
            .entry(channel.to_owned())
            .or_insert_with(|| PendingChannel {
                services: Vec::new(),
                stop: crate::global_cancel_token().clone(),
            });

        if let Some(previous) = conflicting_name(&pending.services, service_id) {
            return Err(format!(
                "service_id collision on channel '{channel}': '{service_name}' and \
                 '{previous}' both map to {service_id:#010x}"
            ));
        }

        pending.services.push(RegisteredChannelService {
            id: service_id,
            name: service_name,
            dispatcher,
        });
        Ok(())
    });
    registered.map_err(RpcError::TransportError)?;

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
        channel_registry().with(|registry| registry.drain().collect());

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
    use crate::transport::CollectEmitter;

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
        first.method("echo", |payload, emitter| {
            emitter.emit(EventKind::Next, payload);
        });
        let mut second = ServiceDispatcher::new();
        second.method("ping", |_payload, _emitter| {});

        let table: HashMap<u32, ServiceDispatcher> =
            vec![(7, first), (9, second)].into_iter().collect();

        let mut emitter = CollectEmitter::new();
        table.get(&7).unwrap().dispatch("echo", b"x", &mut emitter);
        assert_eq!(emitter.take().len(), 1);

        // The second dispatcher has no `echo` method: nothing is emitted.
        table.get(&9).unwrap().dispatch("echo", b"x", &mut emitter);
        assert!(emitter.take().is_empty());

        assert!(!table.contains_key(&11));
    }
}
