//! The read-only observation surface, exercised against a real provider.
//!
//! [`DirectionView`] is what an out-of-band observer attaches with, so every one
//! of its accessors is part of the monitoring contract. This file drives a real
//! channel and reads it back through the view, in both directions, and covers the
//! two framing failures a caller must survive: a payload that is not the
//! service's `WireEvent`, and a transport-level rejection that is not a valid
//! `RpcError`.
//!
//! These tests run in their own test binary (a separate process), so the global
//! iceoryx2 node and the consumer port cache cannot disturb the unit tests.

#![allow(clippy::unwrap_used)]

use std::time::Duration;

use ice_rpc::gen::{
    rkyv, serialize_and_call, service_id_of, EventKind, RpcHeader, ServiceDispatcher, ServiceRef,
    WireEvent,
};
use ice_rpc::transport::{
    discover_channels, native_call, spawn_native_service, Direction, DirectionView, Emitter,
};
use ice_rpc::CancellationToken;

/// Encodes one `Next` sample the way a provider does.
fn encode_next(value: i32) -> Vec<u8> {
    rkyv::to_bytes::<rkyv::rancor::Error>(&WireEvent::<i32, String>::Next(value))
        .expect("encode a Next event")
        .to_vec()
}

/// Encodes the terminal `Complete` sample.
fn encode_complete() -> Vec<u8> {
    rkyv::to_bytes::<rkyv::rancor::Error>(&WireEvent::<i32, String>::Complete)
        .expect("encode a Complete event")
        .to_vec()
}

/// Starts a provider whose `echo` streams one value then completes.
fn start_echo(channel: &str) -> (u32, CancellationToken, std::thread::JoinHandle<()>) {
    let service_id = service_id_of(channel);
    let mut dispatcher = ServiceDispatcher::new(ServiceRef::new(service_id, 1));
    dispatcher.method("echo", |_header, _payload, mut emitter| {
        Box::pin(async move {
            if emitter.emit(EventKind::Next, &encode_next(1)) {
                let _ = emitter.emit(EventKind::Complete, &encode_complete());
            }
        })
    });
    let stop = CancellationToken::new();
    let server = spawn_native_service(channel, vec![dispatcher], stop.clone());
    // Let the service thread create the node and the channel ports.
    std::thread::sleep(Duration::from_millis(300));
    (service_id, stop, server)
}

/// Polls a view until one metadata sample arrives, or gives up.
fn drain(view: &DirectionView) -> Option<(RpcHeader, Emitter, usize)> {
    for _ in 0..200 {
        if let Some(sample) = view.try_receive().expect("receive must not fail") {
            return Some(sample);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    None
}

/// Polls a view until one copied sample arrives, or gives up.
fn drain_payload(view: &DirectionView) -> Option<(RpcHeader, Emitter, Vec<u8>)> {
    for _ in 0..200 {
        if let Some(sample) = view.try_receive_payload().expect("receive must not fail") {
            return Some(sample);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    None
}

#[test]
fn the_view_describes_and_reads_a_real_channel() {
    let channel = format!("IceRpcObserverView{}", std::process::id());
    let (service_id, stop, server) = start_echo(&channel);

    let requests = DirectionView::open(&channel, Direction::Request).expect("request view");
    let responses = DirectionView::open(&channel, Direction::Response).expect("response view");

    // ── The description of the service the view attached to ─────────────
    assert_eq!(requests.service_name(), format!("{channel}_req"));
    assert_eq!(responses.service_name(), format!("{channel}_resp"));
    assert!(
        requests.max_publishers() >= 1,
        "the consumer publishes here"
    );
    assert!(
        requests.max_subscribers() >= 1,
        "the provider subscribes here"
    );
    assert!(requests.subscriber_max_buffer_size() >= 1);
    assert!(
        requests.buffer_size() >= 1,
        "the observer can buffer at least one sample"
    );
    assert!(
        !requests.has_safe_overflow(),
        "the transport disables safe overflow, which is what lets a slow observer be skipped"
    );
    // The two remaining static accessors are part of the surface an observer
    // renders; their values are iceoryx2's.
    let _ = requests.payload_size();
    let _ = requests.history_size();

    // ── Traffic ─────────────────────────────────────────────────────────
    let stream =
        native_call::<i32, String>(&channel, ServiceRef::new(service_id, 1), "echo", b"go")
            .expect("native_call opens the service");
    let values = pollster::block_on(stream.collect()).expect("collect");
    assert_eq!(values, vec![1]);

    // A peer is connected now, so the dynamic counts see it. The subscriber count
    // excludes the observer itself.
    assert!(
        requests.publisher_count() >= 1,
        "the consumer publishes the request"
    );
    assert!(
        requests.subscriber_count() >= 1,
        "the provider subscribes to the request, and the observer does not count itself"
    );

    // ── The request direction: metadata only ────────────────────────────
    let (header, emitter, payload_len) = drain(&requests).expect("the observer saw the request");
    assert_eq!(header.service_id, service_id);
    assert_eq!(header.event_kind(), EventKind::Request);
    assert_eq!(payload_len, 2, "'go' is two bytes");
    assert_eq!(
        emitter.pid,
        std::process::id(),
        "the emitter is read from the native sample header"
    );

    // ── The response direction: the payload can be copied out ───────────
    let (header, emitter, payload) =
        drain_payload(&responses).expect("the observer saw the response");
    assert_eq!(header.service_id, service_id);
    assert_eq!(emitter.pid, std::process::id());
    assert!(!payload.is_empty(), "the response payload was copied out");

    // A bare deadline is not a failure.
    let _ = requests
        .wait(Duration::from_millis(50))
        .expect("wait must not fail");

    // The channel is discoverable from the bus alone.
    let channels = discover_channels().expect("discovery must not fail");
    assert!(channels.contains(&channel), "{channels:?}");

    stop.cancel();
    let _ = server.join();
}

/// The observer never creates what it watches: a channel with no provider on the
/// bus must fail to open rather than appear out of nowhere.
#[test]
fn an_unknown_channel_cannot_be_opened_read_only() {
    let channel = format!("IceRpcObserverMissing{}", std::process::id());
    assert!(DirectionView::open(&channel, Direction::Request).is_err());
    assert!(DirectionView::open(&channel, Direction::Response).is_err());
}

/// A provider that emits bytes no `WireEvent<T, E>` can decode must fail the call
/// with a technical error instead of hanging it forever.
#[test]
fn a_malformed_response_reaches_the_caller_as_a_technical_error() {
    let channel = format!("IceRpcObserverMalformed{}", std::process::id());
    let service_id = service_id_of(&channel);
    let mut dispatcher = ServiceDispatcher::new(ServiceRef::new(service_id, 1));
    dispatcher.method("garbage", |_header, _payload, mut emitter| {
        Box::pin(async move {
            // Not a valid archive of the service's `WireEvent<T, E>`.
            let _ = emitter.emit(EventKind::Next, &[0xff, 0x00, 0xff]);
        })
    });
    dispatcher.method("broken_rejection", |_header, _payload, mut emitter| {
        Box::pin(async move {
            // A rejection whose payload is not a valid bare `RpcError` either.
            let _ = emitter.emit(EventKind::RpcError, &[0xff, 0x00, 0xff]);
        })
    });
    let stop = CancellationToken::new();
    let server = spawn_native_service(&channel, vec![dispatcher], stop.clone());
    std::thread::sleep(Duration::from_millis(300));

    let stream =
        native_call::<i32, String>(&channel, ServiceRef::new(service_id, 1), "garbage", b"")
            .expect("native_call opens the service");
    let error =
        pollster::block_on(stream.collect()).expect_err("a malformed payload fails the call");
    assert!(error.is_technical(), "{error:?}");
    assert!(error.to_string().contains("decode response"), "{error}");

    let stream = native_call::<i32, String>(
        &channel,
        ServiceRef::new(service_id, 1),
        "broken_rejection",
        b"",
    )
    .expect("native_call opens the service");
    let error =
        pollster::block_on(stream.collect()).expect_err("a malformed rejection fails the call");
    assert!(error.is_technical(), "{error:?}");
    assert!(error.to_string().contains("decode rpc error"), "{error}");

    stop.cancel();
    let _ = server.join();
}

/// The generated clients go through `serialize_and_call`: the request is encoded
/// into the thread's reusable buffer, then published like an explicit call.
#[test]
fn a_serialized_request_is_published_and_streamed() {
    let channel = format!("IceRpcObserverSerialized{}", std::process::id());
    let (service_id, stop, server) = start_echo(&channel);

    // The provider ignores the request body, so any rkyv-serializable value stands
    // in for the generated `{Service}Request`.
    let stream = serialize_and_call::<i32, String, i32>(
        &channel,
        ServiceRef::new(service_id, 1),
        "echo",
        &7i32,
    )
    .expect("the request is serialized and published");
    let values = pollster::block_on(stream.collect()).expect("collect");
    assert_eq!(values, vec![1]);

    stop.cancel();
    let _ = server.join();
}
