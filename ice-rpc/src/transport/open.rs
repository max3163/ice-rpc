//! Opening an iceoryx2 service, and what its failure means.
//!
//! iceoryx2 records the **static configuration** of a service when it is created
//! — wire format, payload alignment, buffer sizes, port limits — and refuses to
//! open it later if a process asks for a different one. A process killed while it
//! held a service leaves the same kind of state behind. Both cases look like a
//! transport failure from the call site, and neither is one: retrying changes
//! nothing, and the raw `Debug` of the iceoryx2 error tells the reader nothing
//! about what to do.
//!
//! This module is therefore also the place where the **remedy** is written, once.

use iceoryx2::prelude::*;
use iceoryx2::service::builder::event::{EventCreateError, EventOpenError, EventOpenOrCreateError};
use iceoryx2::service::builder::publish_subscribe::{
    PublishSubscribeCreateError, PublishSubscribeOpenError, PublishSubscribeOpenOrCreateError,
};

use super::{
    transport_error, IoxEvent, IoxNode, IoxPubSub, MAX_NODES, MAX_PUBLISHERS, MAX_SUBSCRIBERS,
    PAYLOAD_ALIGNMENT, SUBSCRIBER_BUFFER,
};
use crate::types::{RpcError, RpcHeader};

/// Whether opening a service may create it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OpenMode {
    /// Transport side: create the service when no peer owns it yet.
    CreateOrOpen,
    /// Observer side: attach to an existing service, never create one.
    ReadOnly,
}

/// What to do about a service the bus refuses to open.
///
/// Single-sourced on purpose: every failure that ends here has the same fix, so
/// the instruction is written once instead of being reworded per call site.
const REMEDY: &str = "the iceoryx2 state on this machine was created by another build of this \
                      service (wire format, buffer sizes or port limits changed), or a process was \
                      killed while it held it. Once no process still runs the previous build, \
                      remove the iceoryx2 root path (the `root-path` of the effective \
                      `iceoryx2.toml`, or iceoryx2's default: C:\\Temp\\iceoryx2 on Windows, \
                      /tmp/iceoryx2 elsewhere)";

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
        OpenMode::CreateOrOpen => builder.open_or_create().map_err(|e| match e {
            PublishSubscribeOpenOrCreateError::PublishSubscribeOpenError(inner) => {
                pub_sub_open_error("open service", inner)
            }
            PublishSubscribeOpenOrCreateError::PublishSubscribeCreateError(inner) => {
                pub_sub_create_error("create service", inner)
            }
            PublishSubscribeOpenOrCreateError::SystemInFlux => system_in_flux("open service"),
        }),
        OpenMode::ReadOnly => builder
            .open()
            .map_err(|e| pub_sub_open_error("open service (read-only)", e)),
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
        OpenMode::CreateOrOpen => builder.open_or_create().map_err(event_error),
        OpenMode::ReadOnly => builder
            .open()
            .map_err(|e| event_open_error("open event service (read-only)", e)),
    }
}

/// Whether the failure means "this machine holds another build of the service".
///
/// The listed variants are the ones iceoryx2 reports when the recorded
/// configuration differs from the requested one, plus the two that describe
/// left-over state: resources missing or corrupted, and a creation that never
/// finished because its process died.
fn is_stale_service(error: &PublishSubscribeOpenError) -> bool {
    use PublishSubscribeOpenError as E;
    matches!(
        error,
        E::IncompatibleTypes
            | E::IncompatibleMessagingPattern
            | E::IncompatibleAttributes
            | E::IncompatibleOverflowBehavior
            | E::DoesNotSupportRequestedMinBufferSize
            | E::DoesNotSupportRequestedMinHistorySize
            | E::DoesNotSupportRequestedMinSubscriberBorrowedSamples
            | E::DoesNotSupportRequestedAmountOfPublishers
            | E::DoesNotSupportRequestedAmountOfSubscribers
            | E::DoesNotSupportRequestedAmountOfNodes
            | E::ServiceInCorruptedState
            | E::HangsInCreation
    )
}

/// Same question as [`is_stale_service`], for the creation side.
fn is_stale_creation(error: &PublishSubscribeCreateError) -> bool {
    matches!(
        error,
        PublishSubscribeCreateError::ServiceInCorruptedState
            | PublishSubscribeCreateError::HangsInCreation
    )
}

/// Reports a pub/sub open failure: a stale service is a protocol mismatch, the
/// rest is a transport failure.
fn pub_sub_open_error(context: &str, error: PublishSubscribeOpenError) -> RpcError {
    if is_stale_service(&error) {
        RpcError::ProtocolMismatch(format!("{context}: {error:?}. {REMEDY}"))
    } else {
        transport_error(context, error)
    }
}

/// Reports a pub/sub creation failure, with the same split.
fn pub_sub_create_error(context: &str, error: PublishSubscribeCreateError) -> RpcError {
    if is_stale_creation(&error) {
        RpcError::ProtocolMismatch(format!("{context}: {error:?}. {REMEDY}"))
    } else {
        transport_error(context, error)
    }
}

/// Reports an event-service failure, with the same split.
fn event_error(error: EventOpenOrCreateError) -> RpcError {
    match error {
        EventOpenOrCreateError::EventOpenError(inner) => event_open_error("open event", inner),
        EventOpenOrCreateError::EventCreateError(inner) => {
            event_create_error("create event", inner)
        }
        EventOpenOrCreateError::SystemInFlux => system_in_flux("open event service"),
    }
}

/// Message for iceoryx2's `SystemInFlux`, which has two very different causes.
///
/// It stays a [`RpcError::TransportError`], hence retryable, because one of the
/// causes is genuinely transient: another process is creating or removing that
/// exact service at this instant, and the next attempt succeeds.
///
/// The other cause is not: a process that died while it held the service leaves
/// an entry iceoryx2 can no longer reconcile — the same shape as
/// `ServiceInCorruptedState` and `HangsInCreation`, which are already reported as
/// a [`RpcError::ProtocolMismatch`] with the remedy. A bare `SystemInFlux` in a
/// log leaves the reader to guess which of the two they have, and the case it
/// hides is the one that never resolves itself, so the message names both and
/// carries the remedy.
fn system_in_flux(context: &str) -> RpcError {
    RpcError::TransportError(format!(
        "{context}: SystemInFlux. Either another process is creating or removing this service \
         right now, in which case retrying works, or the state is left over from a process that \
         died while it held it, in which case retrying cannot help and this persists: {REMEDY}"
    ))
}

/// Reports an event-service creation failure, with the same split.
fn event_create_error(context: &str, error: EventCreateError) -> RpcError {
    if matches!(error, EventCreateError::ServiceInCorruptedState) {
        RpcError::ProtocolMismatch(format!("{context}: {error:?}. {REMEDY}"))
    } else {
        transport_error(context, error)
    }
}

/// Reports an event-service open failure, with the same split.
fn event_open_error(context: &str, error: EventOpenError) -> RpcError {
    use EventOpenError as E;
    let stale = matches!(
        error,
        E::IncompatibleMessagingPattern
            | E::IncompatibleAttributes
            | E::IncompatibleDeadline
            | E::IncompatibleNotifierCreatedEvent
            | E::IncompatibleNotifierDroppedEvent
            | E::IncompatibleNotifierDeadEvent
            | E::DoesNotSupportRequestedAmountOfNotifiers
            | E::DoesNotSupportRequestedAmountOfListeners
            | E::DoesNotSupportRequestedMaxEventId
            | E::DoesNotSupportRequestedAmountOfNodes
            | E::ServiceInCorruptedState
            | E::HangsInCreation
    );

    if stale {
        RpcError::ProtocolMismatch(format!("{context}: {error:?}. {REMEDY}"))
    } else {
        transport_error(context, error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceoryx2::service::builder::event::EventOpenError;
    use iceoryx2::service::builder::publish_subscribe::PublishSubscribeOpenError;

    /// A service whose recorded configuration is not the requested one, or whose
    /// state was left behind by a killed process, is a protocol mismatch: no
    /// retry fixes it, and the message says what does.
    #[test]
    fn a_stale_service_is_reported_as_a_protocol_mismatch() {
        for error in [
            PublishSubscribeOpenError::IncompatibleTypes,
            PublishSubscribeOpenError::IncompatibleOverflowBehavior,
            PublishSubscribeOpenError::DoesNotSupportRequestedMinBufferSize,
            PublishSubscribeOpenError::ServiceInCorruptedState,
            PublishSubscribeOpenError::HangsInCreation,
        ] {
            let mapped = pub_sub_open_error("open service", error);
            assert!(
                matches!(&mapped, RpcError::ProtocolMismatch(_)),
                "{mapped:?}"
            );
            assert!(!mapped.is_retryable(), "no retry fixes a stale service");
            assert!(mapped.to_string().contains("root path"), "{mapped}");
        }
    }

    /// Everything else stays a transport failure, so the retry policy of the
    /// caller does not change for a genuinely transient error.
    #[test]
    fn another_open_failure_stays_a_transport_error() {
        let mapped = pub_sub_open_error("open service", PublishSubscribeOpenError::DoesNotExist);
        assert!(matches!(&mapped, RpcError::TransportError(_)), "{mapped:?}");
        assert!(mapped.is_retryable());
    }

    /// `SystemInFlux` covers both a concurrent create and left-over state: it
    /// must stay retryable for the first, and say what to do about the second.
    #[test]
    fn system_in_flux_names_both_of_its_causes() {
        let mapped = system_in_flux("open service");

        assert!(matches!(&mapped, RpcError::TransportError(_)), "{mapped:?}");
        assert!(
            mapped.is_retryable(),
            "a concurrent create is transient, so the retry policy must not change"
        );

        let text = mapped.to_string();
        assert!(text.contains("retrying works"), "{text}");
        assert!(
            text.contains("root path"),
            "the persistent case must carry the remedy: {text}"
        );
    }

    /// The event services (the wake-up channels) are classified the same way.
    #[test]
    fn the_event_side_is_classified_too() {
        let stale = event_open_error(
            "open event",
            EventOpenError::IncompatibleNotifierCreatedEvent,
        );
        assert!(matches!(&stale, RpcError::ProtocolMismatch(_)), "{stale:?}");

        let transient = event_open_error("open event", EventOpenError::DoesNotExist);
        assert!(
            matches!(&transient, RpcError::TransportError(_)),
            "{transient:?}"
        );
    }
}
