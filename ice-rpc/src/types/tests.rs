//! Tests for the fundamental types: events, wire events, the `Observable`
//! stream and the technical error predicates.

use super::*;
use crate::types::wire::normalize_wire_event;

// ── Event ───────────────────────────────────────────────────────────

#[test]
fn event_is_terminal_flags() {
    assert!(!Event::<i32, String>::Next(1).is_terminal());
    assert!(Event::<i32, String>::Complete.is_terminal());
    assert!(Event::<i32, String>::Error(ObservableError::Business("e".into())).is_terminal());
    assert!(
        Event::<i32, String>::Error(ObservableError::Technical(RpcError::Timeout)).is_terminal()
    );
}

#[test]
fn observable_error_predicates() {
    let business: ObservableError<String> = ObservableError::Business("boom".into());
    assert_eq!(business.as_business(), Some(&"boom".to_string()));
    assert!(business.as_technical().is_none());

    let technical: ObservableError<String> = ObservableError::Technical(RpcError::Timeout);
    assert!(technical.as_business().is_none());
    assert!(technical.as_technical().is_some());
}

#[test]
fn observable_error_display() {
    let business: ObservableError<String> = ObservableError::Business("boom".into());
    assert_eq!(business.to_string(), "boom");

    let technical: ObservableError<String> = ObservableError::Technical(RpcError::Timeout);
    assert!(technical.to_string().contains("timeout"));

    let empty: ObservableError<String> = ObservableError::Empty;
    assert_eq!(empty.to_string(), "stream ended without a value");
    assert!(empty.as_business().is_none());
    assert!(empty.as_technical().is_none());
    assert!(!empty.is_business());
    assert!(!empty.is_technical());
}

// ── RpcError ────────────────────────────────────────────────────────

#[test]
fn rpc_error_is_retryable_classification() {
    assert!(RpcError::TransportError("boom".into()).is_retryable());
    assert!(RpcError::Timeout.is_retryable());
    assert!(!RpcError::SerializationError.is_retryable());
    assert!(!RpcError::Cancelled.is_retryable());
    assert!(!RpcError::Internal("boom".into()).is_retryable());
}

#[test]
fn rpc_error_display_variants() {
    assert!(RpcError::SerializationError
        .to_string()
        .contains("serialization"));
    assert!(RpcError::Timeout.to_string().contains("timeout"));
    assert!(RpcError::Cancelled.to_string().contains("cancelled"));
    assert!(RpcError::TransportError("boom".into())
        .to_string()
        .contains("boom"));
}

// ── WireEvent ───────────────────────────────────────────────────────

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
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&event).unwrap();
    let decoded = rkyv::from_bytes::<WireEvent<i32, String>, rkyv::rancor::Error>(&bytes).unwrap();
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

// ── Observable ──────────────────────────────────────────────────────

#[test]
fn stream_poll_next_normalizes_complete_with() {
    let (tx, mut rx) = channel::<i32, String>(4);
    tx.try_send_wire(WireEvent::CompleteWith(42)).unwrap();
    drop(tx);

    assert_eq!(pollster::block_on(rx.recv()).unwrap(), Event::Next(42));
    assert_eq!(pollster::block_on(rx.recv()).unwrap(), Event::Complete);
}

#[test]
fn from_events_is_channel_free_and_replays_in_order() {
    let mut stream =
        Observable::<i32, String>::from_events([Event::Next(1), Event::Next(2), Event::Complete]);
    assert_eq!(pollster::block_on(stream.recv()).unwrap(), Event::Next(1));
    assert_eq!(pollster::block_on(stream.recv()).unwrap(), Event::Next(2));
    assert_eq!(pollster::block_on(stream.recv()).unwrap(), Event::Complete);
}

#[test]
fn from_technical_error_emits_single_terminal_error() {
    let mut stream = Observable::<i32, String>::from_technical_error(RpcError::Timeout);
    match pollster::block_on(stream.recv()).unwrap() {
        Event::Error(ObservableError::Technical(RpcError::Timeout)) => {}
        other => panic!("expected a technical error, got {other:?}"),
    }
}

#[test]
fn recv_wire_coalesces_next_complete_into_complete_with() {
    let mut stream = Observable::<i32, String>::from_events([Event::Next(3), Event::Complete]);
    assert_eq!(
        pollster::block_on(stream.recv_wire()).unwrap(),
        WireEvent::CompleteWith(3)
    );
}

#[test]
fn recv_wire_keeps_intermediate_values_then_coalesces_last() {
    let mut stream =
        Observable::<i32, String>::from_events([Event::Next(1), Event::Next(2), Event::Complete]);
    assert_eq!(
        pollster::block_on(stream.recv_wire()).unwrap(),
        WireEvent::Next(1)
    );
    assert_eq!(
        pollster::block_on(stream.recv_wire()).unwrap(),
        WireEvent::CompleteWith(2)
    );
}

#[test]
fn channel_close_without_terminal_is_reported_as_closed() {
    let mut stream = Observable::<i32, String>::from_events([Event::Next(1)]);
    assert_eq!(pollster::block_on(stream.recv()).unwrap(), Event::Next(1));
    assert!(pollster::block_on(stream.recv()).is_err());
}

#[test]
fn next_maps_the_event_vocabulary_and_keeps_none_for_a_clean_end() {
    let mut stream = Observable::<i32, String>::from_events([Event::Next(1), Event::Complete]);
    assert_eq!(pollster::block_on(stream.next()), Some(Ok(1)));
    // `Complete` was delivered, so `None` means "the producer finished".
    assert_eq!(pollster::block_on(stream.next()), None);

    let mut failing = Observable::<i32, String>::from_events([Event::Error(
        ObservableError::Business("boom".into()),
    )]);
    assert!(matches!(
        pollster::block_on(failing.next()),
        Some(Err(ObservableError::Business(_)))
    ));
    assert_eq!(pollster::block_on(failing.next()), None);
}

#[test]
fn next_turns_an_abrupt_close_into_a_technical_error() {
    // The producer disappears without completing: `next` reports it instead of
    // pretending the stream completed.
    let (tx, mut stream) = channel::<i32, String>(4);
    tx.try_send_next(1).unwrap();
    drop(tx);

    assert_eq!(pollster::block_on(stream.next()), Some(Ok(1)));
    match pollster::block_on(stream.next()) {
        Some(Err(ObservableError::Technical(RpcError::TransportError(message)))) => {
            assert!(
                message.contains("before completion"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected an abrupt-close technical error, got {other:?}"),
    }
    // The failure is reported once, then the stream reads as finished.
    assert_eq!(pollster::block_on(stream.next()), None);
}

#[test]
fn wire_relay_forwards_terminal_errors_unchanged() {
    let (tx, mut stream) = channel::<i32, String>(4);
    tx.try_send_event(Event::Error(ObservableError::Business("boom".into())))
        .unwrap();
    drop(tx);
    assert_eq!(
        pollster::block_on(stream.recv_wire()).unwrap(),
        WireEvent::Error("boom".to_string())
    );
}

// ── terminal combinators ────────────────────────────────────────────

#[test]
fn first_event_returns_first_value() {
    let stream = Observable::<i32, String>::from_events([Event::Next(5), Event::Complete]);
    assert_eq!(pollster::block_on(first_event(stream)).unwrap(), 5);
}

#[test]
fn first_event_reports_business_error() {
    let stream = Observable::<i32, String>::from_events([Event::Error(ObservableError::Business(
        "boom".into(),
    ))]);
    assert!(matches!(
        pollster::block_on(first_event(stream)),
        Err(ObservableError::Business(_))
    ));
}

#[test]
fn collect_values_gathers_values_and_reports_business_error() {
    let stream =
        Observable::<i32, String>::from_events([Event::Next(1), Event::Next(2), Event::Complete]);
    assert_eq!(
        pollster::block_on(collect_values(stream)).unwrap(),
        vec![1, 2]
    );

    let failing = Observable::<i32, String>::from_events([Event::Error(
        ObservableError::Business("boom".into()),
    )]);
    assert!(pollster::block_on(collect_values(failing)).is_err());
}
