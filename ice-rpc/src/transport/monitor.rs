//! Read-only observation surface for out-of-band monitoring.
//!
//! An external process attaches to the very same iceoryx2 services the transport
//! creates (`{channel}_req`, `{channel}_resp` and their `_notify` event services)
//! **without touching the hot path**: it only reads the zero-copy [`RpcHeader`]
//! and the payload length, never the rkyv payload.
//!
//! The ports are built from the exact same service definition as the transport
//! ([`open_service_with`] / [`open_event_service_with`]), so the definition stays
//! single-sourced and cannot drift. Iceoryx2 natively supports several
//! subscribers per pub/sub service, and — because the transport disables safe
//! overflow — a slow observer is simply skipped by the publisher instead of
//! blocking it.
//!
//! The same surface answers whether a channel is **served right now**:
//! [`is_provider_connected`] reads the publisher count of the response
//! direction, which is exactly the provider's presence on the bus.

use std::collections::BTreeSet;
use std::time::Duration;

use iceoryx2::prelude::*;
use iceoryx2::service::header::publish_subscribe::Header as SampleHeader;
use iceoryx2::service::static_config::messaging_pattern::MessagingPattern;
use iceoryx2::service::{Service, ServiceDetails};

use super::open::{OpenMode, open_event_service, open_existing_service, open_service};
use super::{
    Direction, Iox, IoxEvent, IoxListener, IoxPubSub, IoxSubscriber, REQUEST_SUFFIX,
    RESPONSE_SUFFIX, shared_node, transport_error,
};
use crate::types::{RpcError, RpcHeader};

/// Identity of the process that emitted an observed sample.
///
/// Read from the **native** iceoryx2 sample header, so it cannot diverge from
/// the bus: the wire header carries no emitter field of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Emitter {
    /// PID of the emitting process (`node_id().pid()`).
    pub pid: u32,
    /// Unique iceoryx2 node id: stable, and survives a PID reuse.
    pub node_id: u128,
    /// Unique publisher port id.
    ///
    /// A port publishes on exactly one `(channel, direction)` pair, and the id is
    /// unique per port, so it is the right scope for the per-publisher `seq`: an
    /// observer tracking `seq` per id counts the samples **it** missed without
    /// aliasing two publishers of one channel (two consumers) nor a restarted one.
    pub publisher_id: u128,
}

impl Emitter {
    /// Builds the identity from the native sample header.
    fn from_native(header: &SampleHeader) -> Self {
        Self {
            pid: crate::types::node_pid_to_u32(header.node_id().pid::<Iox>()),
            node_id: header.node_id().value(),
            publisher_id: header.publisher_id().value(),
        }
    }
}

/// A read-only view of one direction of a channel.
///
/// Holds the subscriber and the wake-up listener, both created from the same
/// definition as the transport. Dropping it detaches the observer.
pub struct DirectionView {
    subscriber: IoxSubscriber,
    listener: IoxListener,
    // Kept alive for as long as the ports they own are used.
    _pub_sub: IoxPubSub,
    _event: IoxEvent,
}

impl DirectionView {
    /// Opens a read-only view of `channel` in the given `direction`.
    ///
    /// Does **not** create the service: when no provider or consumer owns it yet,
    /// the call fails and a later retry succeeds once the service exists.
    ///
    /// # Errors
    /// Returns a [`RpcError`] when the service is missing or cannot be opened.
    pub fn open(channel: &str, direction: Direction) -> Result<Self, RpcError> {
        let node = shared_node()?;
        let pub_sub = open_service(
            &node,
            channel,
            direction.pub_sub_suffix(),
            OpenMode::ReadOnly,
        )?;
        let event = open_event_service(
            &node,
            channel,
            direction.notify_suffix(),
            OpenMode::ReadOnly,
        )?;

        let subscriber = pub_sub
            .subscriber_builder()
            .create()
            .map_err(|e| transport_error("monitor subscriber", e))?;
        let listener = event
            .listener_builder()
            .create()
            .map_err(|e| transport_error("monitor listener", e))?;

        Ok(Self {
            subscriber,
            listener,
            _pub_sub: pub_sub,
            _event: event,
        })
    }

    /// Non-blocking read of the next sample: `(header, emitter, payload length)`.
    ///
    /// The payload itself is deliberately not returned: a monitor never needs to
    /// decode the rkyv body, which keeps it ignorant of the service types.
    ///
    /// # Errors
    /// Returns a [`RpcError`] when the underlying port reports a failure.
    pub fn try_receive(&self) -> Result<Option<(RpcHeader, Emitter, usize)>, RpcError> {
        match self.subscriber.receive() {
            Ok(Some(sample)) => Ok(Some((
                *sample.user_header(),
                Emitter::from_native(sample.header()),
                sample.len(),
            ))),
            Ok(None) => Ok(None),
            Err(e) => Err(transport_error("monitor receive", e)),
        }
    }

    /// Like [`try_receive`](Self::try_receive), but also copies the payload.
    ///
    /// Reading the payload defeats the zero-copy property and costs a copy per
    /// sample, so it is meant for the *detail* mode only, at moderate
    /// throughput.
    ///
    /// # Errors
    /// Returns a [`RpcError`] when the underlying port reports a failure.
    pub fn try_receive_payload(&self) -> Result<Option<(RpcHeader, Emitter, Vec<u8>)>, RpcError> {
        match self.subscriber.receive() {
            Ok(Some(sample)) => {
                let header = *sample.user_header();
                let emitter = Emitter::from_native(sample.header());
                let payload = sample.to_vec();
                Ok(Some((header, emitter, payload)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(transport_error("monitor receive", e)),
        }
    }

    /// Blocks until the wake-up notifier fires or `timeout` elapses.
    ///
    /// Returns `true` when a notification was observed, `false` on a bare
    /// timeout. A notification means "there is probably something to drain".
    ///
    /// # Errors
    /// Returns a [`RpcError`] when the listener reports a failure.
    pub fn wait(&self, timeout: Duration) -> Result<bool, RpcError> {
        match self.listener.timed_wait(|_activation| {}, timeout) {
            Ok(activations) => Ok(activations > 0),
            Err(e) => Err(transport_error("monitor wait", e)),
        }
    }

    /// Number of samples the observer can buffer before the publisher skips it.
    pub fn buffer_size(&self) -> usize {
        self.subscriber.buffer_size()
    }

    /// Name of the underlying iceoryx2 service.
    pub fn service_name(&self) -> String {
        self._pub_sub.name().as_str().to_owned()
    }

    /// Number of active publishers on this service.
    pub fn publisher_count(&self) -> usize {
        self._pub_sub.dynamic_config().number_of_publishers()
    }

    /// Number of active subscribers, **excluding** this observer's own subscriber.
    pub fn subscriber_count(&self) -> usize {
        self._pub_sub
            .dynamic_config()
            .number_of_subscribers()
            .saturating_sub(1)
    }

    /// Maximum number of publishers the service was created with.
    pub fn max_publishers(&self) -> usize {
        self._pub_sub.static_config().max_publishers()
    }

    /// Maximum number of subscribers the service was created with.
    pub fn max_subscribers(&self) -> usize {
        self._pub_sub.static_config().max_subscribers()
    }

    /// Largest buffer a subscriber of this service may request.
    pub fn subscriber_max_buffer_size(&self) -> usize {
        self._pub_sub.static_config().subscriber_max_buffer_size()
    }

    /// Publisher history size.
    pub fn history_size(&self) -> usize {
        self._pub_sub.static_config().history_size()
    }

    /// Whether a saturated publisher overwrites old samples instead of refusing
    /// to publish (the transport disables it).
    pub fn has_safe_overflow(&self) -> bool {
        self._pub_sub.static_config().has_safe_overflow()
    }

    /// Size in bytes of one payload, as declared by the service definition.
    pub fn payload_size(&self) -> usize {
        self._pub_sub
            .static_config()
            .message_type_details()
            .payload
            .size()
    }
}

/// Strips the direction suffix of an ice-rpc service name.
fn strip_direction_suffix(name: &str) -> Option<&str> {
    name.strip_suffix(REQUEST_SUFFIX)
        .or_else(|| name.strip_suffix(RESPONSE_SUFFIX))
}

/// Lists the channels that currently expose at least one ice-rpc service.
///
/// Scans the registered `pub/sub` services and keeps the names ending with
/// `_req` or `_resp`; each channel is returned once. The notification services
/// (`_req_notify` / `_resp_notify`) use the `Event` pattern and are ignored.
///
/// # Errors
/// Returns a [`RpcError`] when the service listing itself fails.
pub fn discover_channels() -> Result<Vec<String>, RpcError> {
    let config = crate::config::build_iceoryx2_config();
    let mut channels: BTreeSet<String> = BTreeSet::new();

    <Iox as Service>::list(&config, |details: ServiceDetails<Iox>| {
        if matches!(
            details.static_details.messaging_pattern(),
            MessagingPattern::PublishSubscribe(_)
        ) && let Some(channel) = strip_direction_suffix(details.static_details.name().as_str())
        {
            channels.insert(channel.to_owned());
        }
        CallbackProgression::Continue
    })
    .map_err(|e| transport_error("service discovery", e))?;

    Ok(channels.into_iter().collect())
}

/// Whether a provider is currently serving `channel`.
///
/// A provider owns the **publisher** of the response direction
/// (`{channel}_resp`): its channel thread creates that publisher when it opens
/// its ports and drops it when it stops, so the publisher count of the service
/// *is* the answer. A consumer only subscribes to that direction, which is why
/// the caller's own process can never make this return `true`.
///
/// The probe is **read-only**: it opens the response service without creating
/// it, so asking the question never makes a channel appear on the bus. A channel
/// no provider ever created answers `Ok(false)`, exactly like a channel whose
/// provider has stopped.
///
/// It is synchronous and bounded — no port, no wait, no retry — which makes it
/// usable as a readiness check or as a guard before an optional call. Its cost
/// has two very different regimes:
///
/// - a channel this process already **consumed** answers from the ports the
///   consumer keeps: 40 ns measured in release (the publisher count alone is a
///   7 ns atomic load, the rest is the port lookup);
/// - a channel never consumed here has to be opened first (name lookup, static
///   config open, deserialization): 131 µs measured in release, 1.06 ms in a
///   debug build. That is cheap for a readiness poll, not for a per-call guard —
///   make one call first, and the cheap path takes over.
///
/// It is **not** meant for the hot path, and it does not retry: a transient bus
/// state is reported, not waited out.
///
/// The answer is per **channel**: every service sharing a `group` shares it, so
/// the probe says whether *some* provider serves the group, not which service of
/// it is answering.
///
/// # Errors
/// Returns a [`RpcError`] when the channel exists but cannot be opened — a
/// configuration mismatch with its creator, or state left behind by a process
/// killed while it held it. Those are reported rather than folded into `false`,
/// because a "not connected" answer would hide a deployment problem that no
/// amount of retrying resolves.
pub fn is_provider_connected(channel: &str) -> Result<bool, RpcError> {
    // Fast path: the channel is already consumed by this process, so its
    // response service is open and the answer is an atomic read of its publisher
    // count — the same signal, without opening anything.
    if let Some(connected) = super::client::cached_provider_connected(channel) {
        return Ok(connected);
    }

    let node = shared_node()?;
    Ok(open_existing_service(&node, channel, RESPONSE_SUFFIX)?
        .map(|service| service.dynamic_config().number_of_publishers() > 0)
        .unwrap_or(false))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_strips_only_the_direction_suffix() {
        assert_eq!(
            strip_direction_suffix("DatabaseService_req"),
            Some("DatabaseService")
        );
        assert_eq!(
            strip_direction_suffix("DatabaseService_resp"),
            Some("DatabaseService")
        );
        // The notification services must not be mistaken for a channel.
        assert_eq!(strip_direction_suffix("DatabaseService_req_notify"), None);
        assert_eq!(strip_direction_suffix("DatabaseService_resp_notify"), None);
        assert_eq!(strip_direction_suffix("DatabaseService"), None);
    }

    /// A channel nobody created is not an error: it is simply not connected. This
    /// is what lets a readiness check run before the provider is started.
    #[test]
    fn a_channel_that_does_not_exist_is_not_connected() {
        let channel = format!("UnitLivenessMissing{}", std::process::id());
        assert!(
            !is_provider_connected(&channel).expect("the probe must answer"),
            "an unknown channel owns no provider"
        );
    }

    /// The probe reads the provider's *publisher*, not the consumer's
    /// subscriber: attaching a subscriber must leave the answer at `false`, and
    /// the publisher appearing is what flips it — then its drop flips it back,
    /// which is how a stopped provider is observed.
    #[test]
    fn the_response_publisher_is_what_makes_a_channel_connected() {
        let node = shared_node().expect("the process node");
        // Unique to this test process, so the service it creates cannot be another
        // test's channel.
        let channel = format!("UnitLiveness{}", std::process::id());
        let service = open_service(&node, &channel, RESPONSE_SUFFIX, OpenMode::CreateOrOpen)
            .expect("the channel is created");

        let _subscriber = service
            .subscriber_builder()
            .create()
            .expect("a consumer subscriber");
        assert!(
            !is_provider_connected(&channel).expect("the probe must answer"),
            "a consumer subscriber is not a provider"
        );

        let publisher = service
            .publisher_builder()
            .create()
            .expect("a provider publisher");
        assert!(
            is_provider_connected(&channel).expect("the probe must answer"),
            "the response publisher is the provider"
        );

        drop(publisher);
        assert!(
            !is_provider_connected(&channel).expect("the probe must answer"),
            "a provider that stopped is no longer connected"
        );
    }
}
