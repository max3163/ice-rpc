//! Tests for the fundamental types: events, streams, header, correlation ids,
//! errors. These exercise the items re-exported by [`super`].

use super::*;
use std::pin::Pin;
use std::task::{Context, Poll};

#[test]
fn event_kind_request_is_not_terminal() {
    assert!(!EventKind::Request.is_terminal());
}

#[test]
fn event_kind_next_is_not_terminal() {
    assert!(!EventKind::Next.is_terminal());
}

#[test]
fn event_kind_complete_is_terminal() {
    assert!(EventKind::Complete.is_terminal());
}

#[test]
fn event_kind_error_is_terminal() {
    assert!(EventKind::Error.is_terminal());
}

#[test]
fn event_kind_default_is_request() {
    assert_eq!(EventKind::default(), EventKind::Request);
}

#[test]
fn event_kind_discriminants() {
    assert_eq!(EventKind::Request as u8, 0);
    assert_eq!(EventKind::Next as u8, 1);
    assert_eq!(EventKind::Complete as u8, 2);
    assert_eq!(EventKind::Error as u8, 3);
}

#[test]
fn event_is_terminal_flags() {
    assert!(!Event::<i32, String>::Next(1).is_terminal());
    assert!(Event::<i32, String>::Complete.is_terminal());
    assert!(
        Event::<i32, String>::Error(ObservableError::Business("boom".to_string())).is_terminal()
    );
    assert!(
        Event::<i32, String>::Error(ObservableError::Technical(RpcError::Timeout)).is_terminal()
    );
}

#[test]
fn observable_error_predicates() {
    let business: ObservableError<String> = ObservableError::Business("nope".into());
    assert!(business.is_business());
    assert!(!business.is_technical());
    assert!(business.as_technical().is_none());

    let technical: ObservableError<String> = ObservableError::Technical(RpcError::Timeout);
    assert!(technical.is_technical());
    assert!(!technical.is_business());
    assert!(technical.as_business().is_none());
    assert!(technical.as_technical().is_some());
}

#[test]
fn stream_error_from_observable_error() {
    let business: StreamError<String> = ObservableError::Business("nope".to_string()).into();
    assert!(matches!(business, StreamError::Business(e) if e == "nope"));

    let technical: StreamError<String> = ObservableError::Technical(RpcError::Timeout).into();
    assert!(matches!(
        technical,
        StreamError::Technical(RpcError::Timeout)
    ));
}

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
    let decoded: WireEvent<i32, String> =
        rkyv::from_bytes::<_, rkyv::rancor::Error>(&bytes).unwrap();
    match decoded {
        WireEvent::CompleteWith(v) => assert_eq!(v, 42),
        other => panic!("expected CompleteWith, got {:?}", other),
    }
}

#[test]
fn stream_poll_next_normalizes_complete_with() {
    let (tx, rx) = channel::<i32, String>(2);
    pollster::block_on(tx.send_complete_with(5)).unwrap();
    drop(tx);

    let mut stream = Box::pin(rx);

    let first = pollster::block_on(futures_lite::future::poll_fn(|cx| {
        futures_lite::Stream::poll_next(stream.as_mut(), cx)
    }));
    assert!(matches!(first, Some(Event::Next(5))));

    let second = pollster::block_on(futures_lite::future::poll_fn(|cx| {
        futures_lite::Stream::poll_next(stream.as_mut(), cx)
    }));
    assert!(matches!(second, Some(Event::Complete)));

    let third = pollster::block_on(futures_lite::future::poll_fn(|cx| {
        futures_lite::Stream::poll_next(stream.as_mut(), cx)
    }));
    assert!(third.is_none());
}

#[test]
fn from_events_is_channel_free_and_replays_in_order() {
    let mut stream =
        Observable::<i32, String>::from_events([Event::Next(1), Event::Next(2), Event::Complete]);

    assert!(!stream.is_channel_backed());

    assert!(matches!(
        pollster::block_on(stream.recv()),
        Ok(Event::Next(1))
    ));
    assert!(matches!(
        pollster::block_on(stream.recv()),
        Ok(Event::Next(2))
    ));
    assert!(matches!(
        pollster::block_on(stream.recv()),
        Ok(Event::Complete)
    ));
    // An exhausted buffered stream behaves like a closed channel.
    assert!(pollster::block_on(stream.recv()).is_err());
}

#[test]
fn buffered_recv_wire_coalesces_next_complete_into_complete_with() {
    let mut stream = Observable::<i32, String>::from_events([Event::Next(42), Event::Complete]);

    // A single-response `of(42)` travels as ONE wire sample.
    assert!(matches!(
        pollster::block_on(stream.recv_wire()),
        Ok(WireEvent::CompleteWith(42))
    ));
    assert!(pollster::block_on(stream.recv_wire()).is_err());
}

#[test]
fn buffered_recv_wire_keeps_intermediate_values_then_coalesces_last() {
    let mut stream = Observable::<i32, String>::from_events([
        Event::Next(1),
        Event::Next(2),
        Event::Next(3),
        Event::Complete,
    ]);

    assert!(matches!(
        pollster::block_on(stream.recv_wire()),
        Ok(WireEvent::Next(1))
    ));
    assert!(matches!(
        pollster::block_on(stream.recv_wire()),
        Ok(WireEvent::Next(2))
    ));
    assert!(matches!(
        pollster::block_on(stream.recv_wire()),
        Ok(WireEvent::CompleteWith(3))
    ));
    assert!(pollster::block_on(stream.recv_wire()).is_err());
}

#[test]
fn buffered_recv_wire_maps_terminal_errors() {
    let mut business = Observable::<i32, String>::from_events([Event::Error(
        ObservableError::Business("boom".to_string()),
    )]);
    assert!(matches!(
        pollster::block_on(business.recv_wire()),
        Ok(WireEvent::Error(e)) if e == "boom"
    ));

    let mut technical = Observable::<i32, String>::from_technical_error(RpcError::Timeout);
    assert!(matches!(
        pollster::block_on(technical.recv_wire()),
        Ok(WireEvent::RpcError(RpcError::Timeout))
    ));
}

#[test]
fn wire_relay_preserves_the_single_sample_optimization() {
    // Shape produced by the generated client relay: the raw IPC sample is
    // forwarded verbatim to the consumer channel. A `CompleteWith` must stay
    // a single channel message, expanded only when the consumer reads it.
    let (tx, mut stream) = channel::<i32, String>(4);
    tx.try_send_wire(WireEvent::CompleteWith(7)).unwrap();
    drop(tx);

    match pollster::block_on(stream.recv()).unwrap() {
        Event::Next(v) => assert_eq!(v, 7),
        other => panic!("expected Next(7), got {:?}", other),
    }
    assert!(matches!(
        pollster::block_on(stream.recv()).unwrap(),
        Event::Complete
    ));
}

#[test]
fn wire_relay_forwards_terminal_errors_unchanged() {
    let (tx, mut stream) = channel::<i32, String>(4);
    tx.try_send_wire(WireEvent::RpcError(RpcError::Timeout))
        .unwrap();
    drop(tx);

    match pollster::block_on(stream.recv()).unwrap() {
        Event::Error(ObservableError::Technical(RpcError::Timeout)) => {}
        other => panic!("expected a technical Timeout, got {:?}", other),
    }
}

/// Collects every wire sample a source produces (server-relay viewpoint).
fn wire_samples<T, E>(mut stream: Observable<T, E>) -> Vec<WireEvent<T, E>> {
    let mut out = Vec::new();
    while let Ok(event) = pollster::block_on(stream.recv_wire()) {
        out.push(event);
    }
    out
}

/// A stream that yields a value, then stays `Pending` until the test flips
/// the gate, then completes. Used to prove the coalescing peek never blocks.
struct GatedStream {
    gate: std::sync::Arc<std::sync::atomic::AtomicBool>,
    step: usize,
}

impl Unpin for GatedStream {}

impl futures_lite::Stream for GatedStream {
    type Item = Event<i32, String>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.step == 0 {
            self.step = 1;
            return Poll::Ready(Some(Event::Next(1)));
        }
        if !self.gate.load(std::sync::atomic::Ordering::SeqCst) {
            // The terminal event is not available yet.
            return Poll::Pending;
        }
        if self.step == 1 {
            self.step = 2;
            return Poll::Ready(Some(Event::Complete));
        }
        Poll::Ready(None)
    }
}

#[test]
fn boxed_stream_delegates_poll_next_and_coalesces() {
    let inner = Observable::<i32, String>::from_events([Event::Next(42), Event::Complete]);
    let stream = Observable::<i32, String>::from_stream(inner);

    // The inner stream is drained through the boxed variant, and the
    // trailing `Next + Complete` is still folded into one wire sample.
    assert_eq!(wire_samples(stream).len(), 1);
}

#[test]
fn boxed_stream_does_not_wait_for_the_terminal_event() {
    let gate = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut stream = Observable::<i32, String>::from_stream(GatedStream {
        gate: gate.clone(),
        step: 0,
    });

    // A single poll must already resolve with the value: the best-effort
    // coalescing peek returns `Pending` and the value is emitted as is, so a
    // long-lived stream is never stalled waiting for its `Complete`.
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    {
        let mut fut = Box::pin(stream.recv_wire());
        match std::future::Future::poll(fut.as_mut(), &mut cx) {
            Poll::Ready(Ok(WireEvent::Next(1))) => {}
            other => panic!("expected an immediate Next(1), got {:?}", other),
        }
    }

    // Once the gate opens, the terminal event is delivered.
    gate.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(matches!(
        pollster::block_on(stream.recv_wire()),
        Ok(WireEvent::Complete)
    ));
}

#[test]
fn single_value_source_travels_as_exactly_one_wire_sample() {
    // Shape produced by `ice_rpc_rx::of(value)`: Next + Complete.
    let samples = wire_samples(Observable::<i32, String>::from_events([
        Event::Next(42),
        Event::Complete,
    ]));

    assert_eq!(samples.len(), 1, "expected exactly one sample: {samples:?}");
    assert!(matches!(samples[0], WireEvent::CompleteWith(42)));
}

#[test]
fn business_error_source_travels_as_exactly_one_wire_sample() {
    // Shape produced by `ice_rpc_rx::throw_error(err)`.
    let samples = wire_samples(Observable::<i32, String>::from_events([Event::Error(
        ObservableError::Business("boom".to_string()),
    )]));

    assert_eq!(samples.len(), 1, "expected exactly one sample: {samples:?}");
    assert!(matches!(&samples[0], WireEvent::Error(e) if e == "boom"));
}

#[test]
fn technical_error_source_travels_as_exactly_one_wire_sample() {
    // Shape produced by `Observable::from_technical_error` and by the generated
    // client when a call fails before the transport stream exists.
    let samples = wire_samples(Observable::<i32, String>::from_technical_error(
        RpcError::Timeout,
    ));

    assert_eq!(samples.len(), 1, "expected exactly one sample: {samples:?}");
    assert!(matches!(samples[0], WireEvent::RpcError(RpcError::Timeout)));
}

#[test]
fn multi_value_source_coalesces_only_its_last_value() {
    // `ice_rpc_rx::from([1, 2, 3])`: three values in, three samples out —
    // the last value rides the terminal `CompleteWith`.
    let samples = wire_samples(Observable::<i32, String>::from_events([
        Event::Next(1),
        Event::Next(2),
        Event::Next(3),
        Event::Complete,
    ]));

    assert_eq!(samples.len(), 3, "expected three samples: {samples:?}");
    assert!(matches!(samples[0], WireEvent::Next(1)));
    assert!(matches!(samples[1], WireEvent::Next(2)));
    assert!(matches!(samples[2], WireEvent::CompleteWith(3)));
}

#[test]
fn from_technical_error_emits_single_terminal_error() {
    let mut stream = Observable::<i32, String>::from_technical_error(RpcError::Cancelled);

    match pollster::block_on(stream.recv()) {
        Ok(Event::Error(ObservableError::Technical(RpcError::Cancelled))) => {}
        other => panic!("expected terminal technical error, got {:?}", other),
    }
    assert!(pollster::block_on(stream.recv()).is_err());
}

#[test]
fn first_value_reports_technical_error() {
    let stream = Observable::<i32, String>::from_technical_error(RpcError::Timeout);
    let err = pollster::block_on(stream.first_value()).unwrap_err();
    assert!(matches!(err, StreamError::Technical(RpcError::Timeout)));
}

#[test]
fn collect_returns_business_error() {
    let stream = Observable::<i32, String>::from_events([
        Event::Next(1),
        Event::Error(ObservableError::Business("boom".to_string())),
    ]);
    let err = pollster::block_on(stream.collect()).unwrap_err();
    assert!(matches!(err, ObservableError::Business(e) if e == "boom"));
}

#[test]
fn buffered_stream_poll_next_yields_none_when_drained() {
    let stream = Observable::<i32, String>::from_events([Event::Next(7)]);
    let mut stream = Box::pin(stream);

    let first = pollster::block_on(futures_lite::future::poll_fn(|cx| {
        futures_lite::Stream::poll_next(stream.as_mut(), cx)
    }));
    assert!(matches!(first, Some(Event::Next(7))));

    let second = pollster::block_on(futures_lite::future::poll_fn(|cx| {
        futures_lite::Stream::poll_next(stream.as_mut(), cx)
    }));
    assert!(second.is_none());
}

#[test]
fn rpc_header_new_populates_correlation_id() {
    let header = RpcHeader::new("test_svc", "test_method");
    assert_ne!(header.correlation_id, [0u8; 16]);
}

#[test]
fn rpc_header_new_sets_service_and_method() {
    let header = RpcHeader::new("MyService", "hello_world");
    assert_eq!(header.service(), "MyService");
    assert_eq!(header.method(), "hello_world");
}

#[test]
fn rpc_header_new_event_kind_is_request() {
    let header = RpcHeader::new("any_svc", "any");
    assert_eq!(header.event_kind, EventKind::Request);
}

#[test]
fn rpc_header_new_sets_protocol_version() {
    let header = RpcHeader::new("svc", "m");
    assert_eq!(header.protocol_version, PROTOCOL_VERSION);
    assert_eq!(header.service_version, 0);
}

#[test]
fn rpc_header_with_service_version() {
    let header = RpcHeader::new("svc", "m").with_service_version(7);
    assert_eq!(header.service_version, 7);
}

#[test]
fn rpc_header_new_sent_at_ns_is_nonzero() {
    let header = RpcHeader::new("svc", "test");
    assert!(header.sent_at_ns > 0);
}

#[test]
fn next_correlation_id_is_unique() {
    let id1 = RpcHeader::next_correlation_id();
    let id2 = RpcHeader::next_correlation_id();
    assert_ne!(id1, id2);
}

#[test]
fn next_correlation_id_pid_field() {
    let id = RpcHeader::next_correlation_id();
    let pid = u32::from_le_bytes([id[0], id[1], id[2], id[3]]);
    assert_eq!(pid, std::process::id());
}

#[test]
fn rpc_header_method_empty_on_default() {
    let header = RpcHeader::default();
    assert!(header.method().is_empty());
}

#[test]
fn rpc_header_method_truncated_on_long_name() {
    let long_name = "a".repeat(METHOD_NAME_LEN + 50);
    let header = RpcHeader::new("svc", &long_name);
    assert!(header.method().len() <= METHOD_NAME_LEN);
}

#[test]
fn now_ns_returns_reasonable_value() {
    let ns = RpcHeader::now_ns();
    assert!(ns > 1_500_000_000_000_000_000);
}

#[test]
fn now_ns_is_monotonic() {
    let t1 = RpcHeader::now_ns();
    let t2 = RpcHeader::now_ns();
    assert!(t2 >= t1);
}

#[test]
fn fmt_correlation_id_format_length() {
    let cid = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
        0x10,
    ];
    let formatted = fmt_correlation_id(&cid);
    assert_eq!(formatted.len(), 36);
}

#[test]
fn fmt_correlation_id_contains_dashes() {
    let cid = [0xAA; 16];
    let formatted = fmt_correlation_id(&cid);
    assert!(formatted.contains('-'));
    assert_eq!(formatted.matches('-').count(), 4);
}

#[test]
fn fmt_correlation_id_stable_output() {
    let cid = [
        0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
        0x77,
    ];
    let expected = "deadbeef-cafe-babe-0011-223344556677";
    assert_eq!(fmt_correlation_id(&cid), expected);
}

#[test]
fn fmt_correlation_id_short_length() {
    let cid = [
        0x01, 0x02, 0x03, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00,
    ];
    let formatted = fmt_correlation_id_short(&cid);
    assert_eq!(formatted.len(), 8);
}

#[test]
fn fmt_correlation_id_short_first_four_bytes() {
    let cid = [
        0x12, 0x34, 0x56, 0x78, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF,
    ];
    let formatted = fmt_correlation_id_short(&cid);
    assert_eq!(formatted, "12345678");
}

#[test]
fn rpc_error_display_serialization_error() {
    let err = RpcError::SerializationError;
    assert!(err.to_string().contains("serialization"));
}

#[test]
fn rpc_error_display_transport_error() {
    let err = RpcError::TransportError("connection refused".into());
    assert!(err.to_string().contains("connection refused"));
}

#[test]
fn rpc_error_display_timeout() {
    let err = RpcError::Timeout;
    assert!(err.to_string().contains("timeout"));
}

#[test]
fn rpc_error_protocol_mismatch_display() {
    let err = RpcError::ProtocolMismatch {
        expected_protocol: 1,
        received_protocol: 2,
        expected_service: 3,
        received_service: 4,
    };
    assert!(err.to_string().contains("protocol mismatch"));
}

#[test]
fn rpc_error_is_retryable_classification() {
    assert!(RpcError::TransportError("boom".into()).is_retryable());
    assert!(RpcError::DiscoveryError("boom".into()).is_retryable());
    assert!(RpcError::ProviderUnavailable { node: 1 }.is_retryable());
    assert!(RpcError::Timeout.is_retryable());

    assert!(!RpcError::SerializationError.is_retryable());
    assert!(!RpcError::ServiceNotFound {
        service: "svc".into()
    }
    .is_retryable());
    assert!(!RpcError::Cancelled.is_retryable());
    assert!(!RpcError::PayloadTooLarge { size: 1, limit: 1 }.is_retryable());
    assert!(!RpcError::Internal("boom".into()).is_retryable());
}

#[test]
fn try_clone_duplicates_a_buffered_observable() {
    let mut original = Observable::<i32, String>::from_events([Event::Next(1), Event::Complete]);
    let mut copy = original
        .try_clone()
        .expect("a buffered observable is clonable");

    // Each handle drains its own copy of the queue.
    assert!(matches!(
        pollster::block_on(original.recv()),
        Ok(Event::Next(1))
    ));
    assert!(matches!(
        pollster::block_on(copy.recv()),
        Ok(Event::Next(1))
    ));
}

#[test]
fn try_clone_shares_a_channel_backed_observable() {
    let (tx, mut rx) = channel::<i32, String>(4);
    let mut clone = rx
        .try_clone()
        .expect("a channel-backed observable is clonable");

    // Competing consumers: the message is delivered to exactly one handle.
    pollster::block_on(tx.send_next(7)).unwrap();
    drop(tx);

    let values: Vec<i32> = [rx.recv(), clone.recv()]
        .into_iter()
        .filter_map(|fut| match pollster::block_on(fut) {
            Ok(Event::Next(v)) => Some(v),
            _ => None,
        })
        .collect();
    assert_eq!(
        values,
        vec![7],
        "the message must be delivered exactly once"
    );
}

#[test]
fn try_clone_returns_none_for_a_boxed_pipeline() {
    let inner = Observable::<i32, String>::from_events([Event::Next(1), Event::Complete]);
    let boxed = Observable::<i32, String>::from_stream(inner);
    assert!(boxed.try_clone().is_none());
}
