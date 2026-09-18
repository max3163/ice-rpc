//! Provider side: channel creation, request dispatch, response publication and
//! the deferred channel start used during service initialization.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

use iceoryx2::prelude::*;

use super::bridge::{ResponseEmitter, ServiceDispatcher};
use super::client::publish_until_delivered;
use super::notify::Coalescer;
use super::open::{open_event_service, open_service, OpenMode};
use super::{
    shared_node, transport_error, IoxEvent, IoxListener, IoxNotifier, IoxPubSub, IoxPublisher,
    IoxSubscriber, CONSUMER_WAIT_TIMEOUT, MAX_LOANED_SAMPLES, MAX_SLICE_LEN, OPEN_RETRY_ATTEMPTS,
    OPEN_RETRY_SLEEP, REQUEST_NOTIFY_SUFFIX, REQUEST_SUFFIX, RESPONSE_NOTIFY_SUFFIX,
    RESPONSE_SUFFIX,
};
use crate::global::Locked;
use crate::types::{EventKind, RpcError, RpcHeader, PROTOCOL_VERSION};
use crate::CancellationToken;

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
/// `services` is the set of dispatchers sharing the channel; each one carries its
/// own identity, so a request whose id is unknown is logged and dropped.
///
/// The ports are opened with a bounded retry on the failures that are transient:
/// a channel is not worth losing to a race with another process.
pub fn spawn_native_service(
    channel: &str,
    services: Vec<ServiceDispatcher>,
    stop: CancellationToken,
) -> JoinHandle<()> {
    // The table key is read from the dispatcher: it is the same value the client
    // stamps in the frame, so it can never disagree with the version answered for.
    let table: HashMap<u32, ServiceDispatcher> = services
        .into_iter()
        .map(|dispatcher| (dispatcher.service().id, dispatcher))
        .collect();
    let channel = channel.to_owned();
    let handle = std::thread::spawn(move || {
        // Any port that cannot be opened makes the whole channel useless: log it
        // and stop the thread. A transient failure is retried first — a channel
        // must not be lost to a race, see `open_with_retries`.
        let ports = match open_with_retries(&channel, &stop) {
            Ok(ports) => ports,
            Err(e) => {
                if stop.is_cancelled() {
                    // Shutdown asked for while opening: not a failure to report.
                    log::debug!("[transport] '{channel}': open abandoned on shutdown: {e}");
                } else {
                    log::error!("[transport] '{channel}': {e}");
                }
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
    });

    handle
}

/// Opens the ports of one channel, retrying the failures that are worth retrying.
fn open_with_retries(channel: &str, stop: &CancellationToken) -> Result<ChannelPorts, RpcError> {
    retry_open(channel, OPEN_RETRY_ATTEMPTS, stop, || {
        open_channel_ports(channel)
    })
}

/// Attempts `attempt_open` until it succeeds, fails for good, the budget runs
/// out, or shutdown is requested.
///
/// iceoryx2 answers `SystemInFlux` when another process is creating or removing
/// that very service at this instant — a genuine race, which the next attempt
/// wins. Every such failure is already classified retryable on both sides, so
/// this loop is what makes that classification true: before it, a single blip
/// left a whole channel dead for the rest of the process lifetime.
///
/// Two asymmetries, and both matter:
///
/// - a **non**-retryable failure — a service left behind by another build —
///   returns at once, because no attempt can fix it and the operator already has
///   the remedy in the message;
/// - a cancelled `stop` also returns at once, so the worst case of a Ctrl+C
///   during the budget is the sleep in progress (50 ms), not the whole second.
///
/// The budget itself never delays anything the caller waits for: the open happens
/// on the channel's own thread, not in the registration path or in a call.
fn retry_open<T>(
    channel: &str,
    attempts: u32,
    stop: &CancellationToken,
    mut attempt_open: impl FnMut() -> Result<T, RpcError>,
) -> Result<T, RpcError> {
    let mut attempt = 0;
    loop {
        match attempt_open() {
            Ok(opened) => return Ok(opened),
            Err(e) if !e.is_retryable() || attempt >= attempts || stop.is_cancelled() => {
                return Err(e)
            }
            Err(e) => {
                attempt += 1;
                log::warn!(
                    "[transport] '{channel}': {e} — attempt {attempt}/{attempts}, retrying in {:?}",
                    OPEN_RETRY_SLEEP
                );
                std::thread::sleep(OPEN_RETRY_SLEEP);
            }
        }
    }
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

/// Reason a request must not reach its handler.
///
/// Classified from the **stable header alone**, so it stays readable whatever the
/// payload layout of the sender is.
#[derive(Debug, PartialEq, Eq)]
enum RequestRejection {
    /// The framing protocol of the sender differs from this build's.
    Protocol { actual: u16 },
    /// The service interface version of the sender differs from the provider's.
    ServiceVersion { expected: u16, actual: u16 },
}

impl RequestRejection {
    /// Returns why `header` cannot be served by `dispatcher`, if it cannot.
    ///
    /// The protocol is checked first: a peer whose framing differs cannot be
    /// trusted to have filled the rest of the header at all.
    fn classify(dispatcher: &ServiceDispatcher, header: &RpcHeader) -> Option<Self> {
        if header.protocol_version != PROTOCOL_VERSION {
            return Some(Self::Protocol {
                actual: header.protocol_version,
            });
        }
        let expected = dispatcher.service().version;
        if header.service_version != expected {
            return Some(Self::ServiceVersion {
                expected,
                actual: header.service_version,
            });
        }
        None
    }

    /// Technical error reported to the caller.
    fn rpc_error(&self) -> RpcError {
        match *self {
            Self::Protocol { actual } => RpcError::ProtocolMismatch(format!(
                "protocol_version {actual} != {PROTOCOL_VERSION}"
            )),
            Self::ServiceVersion { expected, actual } => {
                RpcError::IncompatibleVersion { expected, actual }
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

    // An incompatible protocol or interface version is rejected **before** the
    // payload is decoded, on the stable header alone: the payload layout is
    // exactly what changes when either version moves, so a guard that had to
    // decode first would be doing the very operation it protects against.
    if let Some(rejection) = RequestRejection::classify(dispatcher, header) {
        let err = rejection.rpc_error();
        log::warn!(
            "[transport] '{channel}': rejecting '{}': {err}",
            header.method()
        );
        if !dispatcher.reject_with(header.method(), err, sink) {
            log::warn!(
                "[transport] '{channel}': no handler for method '{}', rejection not reported",
                header.method()
            );
        }
        sink.finish();
        return;
    }

    dispatcher.dispatch(header.method(), payload, sink);
    sink.finish();
}

// ---------------------------------------------------------------------------
// Channel registration — deferred start
// ---------------------------------------------------------------------------

/// One service waiting for its channel to be started.
///
/// The id is not duplicated here: it is read back from the dispatcher, which
/// carries the interface version too.
struct RegisteredChannelService {
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
    entries
        .iter()
        .find(|svc| svc.dispatcher.service().id == id)
        .map(|svc| svc.name)
}

/// Registers one service on the channel it belongs to.
///
/// Called by a generated provider from its `on_init`. The channel thread starts
/// later, in [`start_registered_channels`], once the dispatcher table is complete.
/// The id and the interface version both come from `dispatcher`, so neither can
/// be passed — or lost — separately.
pub fn register_native_service(
    channel: &str,
    service_name: &'static str,
    dispatcher: ServiceDispatcher,
) -> Result<(), RpcError> {
    let service_id = dispatcher.service().id;

    if CHANNELS_SEALED.load(Ordering::Acquire) {
        // A provider created after the seal (a lazily initialized service)
        // cannot join its channel: it starts its own thread.
        let handle = spawn_native_service(
            channel,
            vec![dispatcher],
            crate::global_cancel_token().clone(),
        );
        crate::locator::ServiceLocator::global().register_shutdown_thread(handle);
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
            .map(|svc| svc.dispatcher)
            .collect();
        // The handle is registered, never discarded: the thread owns the channel's
        // ports, so the clean shutdown must wait for it to return — otherwise
        // nothing unlinks the services it created.
        let handle = spawn_native_service(&channel, services, pending.stop);
        crate::locator::ServiceLocator::global().register_shutdown_thread(handle);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::CollectEmitter;
    use crate::types::ServiceRef;

    #[test]
    fn a_duplicate_service_id_is_reported_with_the_previous_name() {
        let entries = vec![
            RegisteredChannelService {
                name: "GetPerson",
                dispatcher: ServiceDispatcher::new(ServiceRef::new(7, 1)),
            },
            RegisteredChannelService {
                name: "SetPerson",
                dispatcher: ServiceDispatcher::new(ServiceRef::new(9, 1)),
            },
        ];

        assert_eq!(conflicting_name(&entries, 9), Some("SetPerson"));
        assert_eq!(conflicting_name(&entries, 11), None);
    }

    /// A token nobody cancelled: the retries run their course.
    fn running() -> CancellationToken {
        CancellationToken::new()
    }

    /// The race `SystemInFlux` describes is transient: it must be retried, and
    /// the loop must return what the successful attempt produced.
    #[test]
    fn a_retryable_open_failure_is_retried_until_it_succeeds() {
        let mut attempts = 0;
        let opened = retry_open("chan", 5, &running(), || {
            attempts += 1;
            if attempts < 3 {
                Err(RpcError::TransportError("SystemInFlux".to_string()))
            } else {
                Ok(attempts)
            }
        });

        assert_eq!(opened.unwrap(), 3);
        assert_eq!(attempts, 3, "two failures then one success");
    }

    /// A service left behind by another build cannot be fixed by retrying: the
    /// caller must get the error — and the remedy it carries — immediately.
    #[test]
    fn a_stale_open_failure_is_not_retried() {
        let mut attempts = 0;
        let opened = retry_open::<()>("chan", 5, &running(), || {
            attempts += 1;
            Err(RpcError::ProtocolMismatch("stale service".to_string()))
        });

        assert!(opened.is_err());
        assert_eq!(attempts, 1, "not one wasted attempt on a stale service");
    }

    /// The budget is a budget: it is spent, then the error comes out.
    #[test]
    fn the_retry_budget_is_finite() {
        let mut attempts = 0;
        let opened = retry_open::<()>("chan", 2, &running(), || {
            attempts += 1;
            Err(RpcError::TransportError("SystemInFlux".to_string()))
        });

        assert!(matches!(opened, Err(RpcError::TransportError(_))));
        assert_eq!(attempts, 3, "the first attempt plus the two retries");
    }

    /// A shutdown request ends the wait at once: the worst case of a Ctrl+C is
    /// the sleep in progress, not the whole budget.
    #[test]
    fn a_cancelled_shutdown_stops_the_retries() {
        let stop = CancellationToken::new();
        stop.cancel();

        let mut attempts = 0;
        let opened = retry_open::<()>("chan", 5, &stop, || {
            attempts += 1;
            Err(RpcError::TransportError("SystemInFlux".to_string()))
        });

        assert!(opened.is_err());
        assert_eq!(attempts, 1, "a cancelled token stops before any retry");
    }

    #[test]
    fn a_channel_table_routes_by_service_id() {
        let mut first = ServiceDispatcher::new(ServiceRef::new(7, 1));
        first.method("echo", |payload, emitter| {
            emitter.emit(EventKind::Next, payload);
        });
        let mut second = ServiceDispatcher::new(ServiceRef::new(9, 1));
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

    /// The type-erased transport borrows the generated emitter to answer a
    /// version mismatch with the typed `RpcError` the caller decodes.
    #[test]
    fn a_version_mismatch_emits_the_typed_incompatible_error() {
        let mut dispatcher = ServiceDispatcher::new(ServiceRef::new(7, 2));
        dispatcher.method("echo", |_payload, _emitter| {});
        dispatcher.on_error("echo", |err, emitter| {
            crate::transport::emit_rpc_error::<i32, String>(err, emitter);
        });

        let mut emitter = CollectEmitter::new();
        assert!(dispatcher.reject_with(
            "echo",
            RpcError::IncompatibleVersion {
                expected: 2,
                actual: 1,
            },
            &mut emitter
        ));

        let samples = emitter.take();
        assert_eq!(
            samples.len(),
            1,
            "the rejection is a single terminal sample"
        );
        assert_eq!(samples[0].0, EventKind::Error);

        let wire: crate::types::WireEvent<i32, String> =
            crate::transport::decode_aligned(&samples[0].1).expect("decode the error sample");
        assert_eq!(
            wire,
            crate::types::WireEvent::RpcError(RpcError::IncompatibleVersion {
                expected: 2,
                actual: 1,
            })
        );

        // An unknown method has no emitter: nothing is emitted, nothing panics.
        let mut emitter = CollectEmitter::new();
        assert!(!dispatcher.reject_with(
            "missing",
            RpcError::IncompatibleVersion {
                expected: 2,
                actual: 1,
            },
            &mut emitter
        ));
        assert!(emitter.take().is_empty());
    }

    /// A peer whose framing differs must be rejected, not dispatched: the rest of
    /// its header cannot be trusted.
    #[test]
    fn an_incompatible_protocol_is_rejected_before_dispatch() {
        let dispatcher = ServiceDispatcher::new(ServiceRef::new(7, 1));
        let mut header = RpcHeader::request("echo", 7, 1);
        header.protocol_version = PROTOCOL_VERSION.wrapping_add(1);

        let rejection = RequestRejection::classify(&dispatcher, &header).expect("rejected");
        assert_eq!(
            rejection,
            RequestRejection::Protocol {
                actual: PROTOCOL_VERSION.wrapping_add(1)
            }
        );
        assert!(matches!(
            rejection.rpc_error(),
            RpcError::ProtocolMismatch(_)
        ));
    }

    /// The interface version is classified with both values, for the caller.
    #[test]
    fn an_incompatible_service_version_is_classified() {
        let dispatcher = ServiceDispatcher::new(ServiceRef::new(7, 2));
        let header = RpcHeader::request("echo", 7, 1);

        let rejection = RequestRejection::classify(&dispatcher, &header).expect("rejected");
        assert_eq!(
            rejection,
            RequestRejection::ServiceVersion {
                expected: 2,
                actual: 1
            }
        );
        assert_eq!(
            rejection.rpc_error(),
            RpcError::IncompatibleVersion {
                expected: 2,
                actual: 1
            }
        );
    }

    /// A header that agrees on both versions is admitted.
    #[test]
    fn a_matching_header_is_never_rejected() {
        let dispatcher = ServiceDispatcher::new(ServiceRef::new(7, 2));
        let header = RpcHeader::request("echo", 7, 2);
        assert_eq!(RequestRejection::classify(&dispatcher, &header), None);
    }
}
