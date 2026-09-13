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

use std::collections::BTreeSet;
use std::time::Duration;

use iceoryx2::prelude::*;
use iceoryx2::service::static_config::messaging_pattern::MessagingPattern;
use iceoryx2::service::{Service, ServiceDetails};

use super::server::{open_event_service_with, open_service_with};
use super::{
    shared_node, transport_error, Iox, IoxEvent, IoxListener, IoxPubSub, IoxSubscriber,
    REQUEST_NOTIFY_SUFFIX, REQUEST_SUFFIX, RESPONSE_NOTIFY_SUFFIX, RESPONSE_SUFFIX,
};
use crate::types::{RpcError, RpcHeader};

/// Direction of the traffic on a channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// `consumer → provider` requests.
    Request,
    /// `provider → consumer` responses.
    Response,
}

impl Direction {
    /// Suffix of the pub/sub service backing this direction.
    const fn pub_sub_suffix(self) -> &'static str {
        match self {
            Direction::Request => REQUEST_SUFFIX,
            Direction::Response => RESPONSE_SUFFIX,
        }
    }

    /// Suffix of the event service used as a wake-up signal.
    const fn notify_suffix(self) -> &'static str {
        match self {
            Direction::Request => REQUEST_NOTIFY_SUFFIX,
            Direction::Response => RESPONSE_NOTIFY_SUFFIX,
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
        let pub_sub = open_service_with(&node, channel, direction.pub_sub_suffix(), false)?;
        let event = open_event_service_with(&node, channel, direction.notify_suffix(), false)?;

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

    /// Non-blocking read of the next sample: `(header, payload length)`.
    ///
    /// The payload itself is deliberately not returned: a monitor never needs to
    /// decode the rkyv body, which keeps it ignorant of the service types.
    ///
    /// # Errors
    /// Returns a [`RpcError`] when the underlying port reports a failure.
    pub fn try_receive(&self) -> Result<Option<(RpcHeader, usize)>, RpcError> {
        match self.subscriber.receive() {
            Ok(Some(sample)) => Ok(Some((*sample.user_header(), sample.len()))),
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
        match self.listener.timed_wait_one(timeout) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(transport_error("monitor wait", e)),
        }
    }

    /// Number of samples the observer can buffer before the publisher skips it.
    pub fn buffer_size(&self) -> usize {
        self.subscriber.buffer_size()
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
        ) {
            if let Some(channel) = strip_direction_suffix(details.static_details.name().as_str()) {
                channels.insert(channel.to_owned());
            }
        }
        CallbackProgression::Continue
    })
    .map_err(|e| transport_error("service discovery", e))?;

    Ok(channels.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direction_suffixes_match_the_transport() {
        assert_eq!(Direction::Request.pub_sub_suffix(), "_req");
        assert_eq!(Direction::Response.pub_sub_suffix(), "_resp");
        assert_eq!(Direction::Request.notify_suffix(), "_req_notify");
        assert_eq!(Direction::Response.notify_suffix(), "_resp_notify");
    }

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
}
