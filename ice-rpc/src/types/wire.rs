//! The wire representation of a stream event.
//!
//! [`WireEvent`] is what the transport serializes: [`Event`] plus the
//! single-sample `CompleteWith` optimization, which lets a one-value response
//! travel as a single iceoryx2 sample. The conversion to and from the
//! user-facing [`Event`] lives here too, because this is the only place where
//! the two vocabularies meet — the stream layer knows nothing about the wire.

use ice_rpc_rx::{Event, ObservableError, RpcError};
use rkyv::{Archive, Deserialize, Serialize};

use super::header::EventKind;

/// Transport-level event carried over the wire and through a producer channel.
///
/// Internal counterpart of [`Event`]: it adds the [`WireEvent::CompleteWith`]
/// single-sample optimization used by producers. Consumers never observe it —
/// the transport folds and normalizes it back into [`Event`] on both sides.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub enum WireEvent<T, E> {
    /// Intermediate business value.
    Next(T),
    /// Normal end of the stream.
    Complete,
    /// Single terminal value carried by the Complete.
    CompleteWith(T),
    /// Business error.
    Error(E),
    /// Technical RPC error (e.g. incompatible version), terminal.
    RpcError(RpcError),
}

impl<T, E> WireEvent<T, E> {
    /// Returns `true` if this event terminates the stream.
    #[inline]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            WireEvent::Complete
                | WireEvent::CompleteWith(_)
                | WireEvent::Error(_)
                | WireEvent::RpcError(_)
        )
    }

    /// Maps the wire variant to the [`EventKind`] stamped in the zero-copy header.
    ///
    /// This lets the provider label each response sample with its real kind, and
    /// an out-of-band observer count completion and errors **without decoding**
    /// the rkyv payload.
    #[inline]
    pub fn kind(&self) -> EventKind {
        match self {
            WireEvent::Next(_) => EventKind::Next,
            WireEvent::Complete | WireEvent::CompleteWith(_) => EventKind::Complete,
            WireEvent::Error(_) => EventKind::Error,
            // A technical error has its own kind: its framed payload is a bare
            // `RpcError`, not a `WireEvent<T, E>`.
            WireEvent::RpcError(_) => EventKind::RpcError,
        }
    }
}

/// Converts a user-facing [`Event`] into its transport representation: a
/// business error becomes [`WireEvent::Error`], a technical one
/// [`WireEvent::RpcError`].
impl<T, E> From<Event<T, E>> for WireEvent<T, E> {
    fn from(event: Event<T, E>) -> Self {
        match event {
            Event::Next(v) => WireEvent::Next(v),
            Event::Complete => WireEvent::Complete,
            Event::Error(ObservableError::Business(e)) => WireEvent::Error(e),
            Event::Error(ObservableError::Technical(e)) => WireEvent::RpcError(e),
            // `Empty` is a pull-side artefact: on the wire the stream just ends.
            Event::Error(ObservableError::Empty) => WireEvent::Complete,
        }
    }
}

/// Normalizes a transport [`WireEvent`] into the user-facing form.
///
/// Returns the event to yield **now** plus an optional **follow-up**: the
/// [`WireEvent::CompleteWith`] optimization expands into `Next(v)` then
/// `Complete`.
///
/// Used by the consumer side, which decodes an incoming sample and relays it to
/// the caller. The producer side applies the mirror-image fold
/// (`transport::bridge`), where a local `Next` followed by a `Complete` becomes
/// one [`WireEvent::CompleteWith`] sample.
pub(crate) fn normalize_wire_event<T, E>(
    event: WireEvent<T, E>,
) -> (Event<T, E>, Option<Event<T, E>>) {
    match event {
        WireEvent::Next(v) => (Event::Next(v), None),
        WireEvent::Complete => (Event::Complete, None),
        WireEvent::CompleteWith(v) => (Event::Next(v), Some(Event::Complete)),
        WireEvent::Error(e) => (Event::Error(ObservableError::Business(e)), None),
        WireEvent::RpcError(e) => (Event::Error(ObservableError::Technical(e)), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_event_is_terminal_flags() {
        assert!(!WireEvent::<i32, String>::Next(1).is_terminal());
        assert!(WireEvent::<i32, String>::Complete.is_terminal());
        assert!(WireEvent::<i32, String>::CompleteWith(1).is_terminal());
        assert!(WireEvent::<i32, String>::Error("boom".to_string()).is_terminal());
        assert!(WireEvent::<i32, String>::RpcError(RpcError::Timeout).is_terminal());
    }

    #[test]
    fn wire_event_rkyv_roundtrip_complete_with() {
        let event: WireEvent<i32, String> = WireEvent::CompleteWith(42);
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&event).expect("the event is encodable");
        let decoded = rkyv::from_bytes::<WireEvent<i32, String>, rkyv::rancor::Error>(&bytes)
            .expect("what was just encoded must decode");
        match decoded {
            WireEvent::CompleteWith(v) => assert_eq!(v, 42),
            other => panic!("expected CompleteWith, got {other:?}"),
        }
    }

    #[test]
    fn normalize_complete_with_expands_into_next_then_complete() {
        let (event, follow_up) = normalize_wire_event(WireEvent::<i32, String>::CompleteWith(7));
        assert_eq!(event, Event::Next(7));
        assert_eq!(follow_up, Some(Event::Complete));
    }

    /// Every wire kind is stamped with its own label: an observer then counts
    /// completion and errors without decoding the payload.
    #[test]
    fn every_wire_variant_maps_to_its_own_kind() {
        let kinds = [
            (WireEvent::<i32, String>::Next(1), EventKind::Next),
            (WireEvent::<i32, String>::Complete, EventKind::Complete),
            (
                WireEvent::<i32, String>::CompleteWith(1),
                EventKind::Complete,
            ),
            (
                WireEvent::<i32, String>::Error(String::new()),
                EventKind::Error,
            ),
            (
                WireEvent::<i32, String>::RpcError(RpcError::Timeout),
                EventKind::RpcError,
            ),
        ];
        for (event, expected) in kinds {
            assert_eq!(event.kind(), expected, "wrong kind for {event:?}");
        }
    }
}
