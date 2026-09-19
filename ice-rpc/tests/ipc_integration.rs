//! Integration tests exercising the iceoryx2-backed IPC paths.
//!
//! These tests run in their own test binary (separate process), so the global
//! iceoryx2 node and the global lock do not conflict with the unit tests.

#![allow(clippy::unwrap_used)]
use ice_rpc::gen::{rkyv, service_id_of, EventKind, ServiceDispatcher, ServiceRef, WireEvent};
use ice_rpc::transport::{native_call, spawn_native_service};
use ice_rpc::{CancellationToken, Event, Observable};

/// Encodes a wire event together with the [`EventKind`] the transport stamps in
/// the zero-copy header.
fn encode_i32(event: WireEvent<i32, String>) -> (EventKind, Vec<u8>) {
    let kind = event.kind();
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&event)
        .expect("encode event")
        .to_vec();
    (kind, bytes)
}

/// A native request must carry N streamed responses, then complete when the
/// service closes the connection.
#[test]
fn native_request_response_streams_then_completes() {
    let channel = format!("IceRpcIntegration/Roundtrip{}", std::process::id());
    let service_id = service_id_of(&channel);
    let stop = CancellationToken::new();

    let mut dispatcher = ServiceDispatcher::new(ServiceRef::new(service_id, 1));
    dispatcher.method("echo", |_header, _payload, mut emitter| {
        Box::pin(async move {
            // A response stream must end with a terminal event: the transport has
            // no per-call connection to signal the end of the stream.
            let samples: Vec<(EventKind, Vec<u8>)> = (0..3i32)
                .map(|value| encode_i32(WireEvent::Next(value)))
                .chain([encode_i32(WireEvent::Complete)])
                .collect();
            for (kind, payload) in samples {
                if !emitter.emit(kind, &payload) {
                    break;
                }
            }
        })
    });
    let server = spawn_native_service(&channel, vec![dispatcher], stop.clone());

    // Give the service thread time to create the shared node and the channel.
    std::thread::sleep(std::time::Duration::from_millis(300));

    let stream =
        native_call::<i32, String>(&channel, ServiceRef::new(service_id, 1), "echo", b"go")
            .expect("native_call must open the native service");
    let values = pollster::block_on(stream.collect()).expect("collect must succeed");
    assert_eq!(values, vec![0, 1, 2], "all streamed responses must arrive");

    stop.cancel();
    let _ = server.join();
}

/// The consumer-side `Observable` normalizes a `CompleteWith` produced by a
/// channel into `Next` followed by `Complete`.
#[test]
fn stream_recv_normalizes_complete_with_as_next_then_complete() {
    let (tx, mut rx) = ice_rpc::gen::channel::<i32, String>(4);
    pollster::block_on(tx.send_complete_with(42)).unwrap();
    drop(tx);

    match pollster::block_on(rx.recv()) {
        Ok(Event::Next(v)) => assert_eq!(v, 42),
        other => panic!("expected Next, got {:?}", other),
    }
    assert!(matches!(pollster::block_on(rx.recv()), Ok(Event::Complete)));
}

/// A real `Observable` (the shape a generated provider returns) must stream
/// through the native transport.
#[test]
fn native_request_response_streams_a_real_observable() {
    let channel = format!("IceRpcIntegration/Obs{}", std::process::id());
    let service_id = service_id_of(&channel);
    let stop = CancellationToken::new();

    let mut dispatcher = ServiceDispatcher::new(ServiceRef::new(service_id, 1));
    dispatcher.method("watch", |_header, _payload, mut emitter| {
        Box::pin(async move {
            let observable = Observable::<i32, String>::from_events([
                Event::Next(10),
                Event::Next(20),
                Event::Complete,
            ]);
            ice_rpc::transport::observable_to_responses(observable, &mut *emitter).await;
        })
    });
    let server = spawn_native_service(&channel, vec![dispatcher], stop.clone());

    std::thread::sleep(std::time::Duration::from_millis(300));

    let stream = native_call::<i32, String>(&channel, ServiceRef::new(service_id, 1), "watch", b"")
        .expect("native_call must open the native service");
    let values = pollster::block_on(stream.collect()).expect("collect");
    assert_eq!(values, vec![10, 20]);

    stop.cancel();
    let _ = server.join();
}

/// A version mismatch must reach the caller as `RpcError::IncompatibleVersion`,
/// not as a timeout: the provider answers before dispatching.
#[test]
fn a_version_mismatch_is_reported_to_the_caller() {
    let channel = format!("IceRpcIntegration/Version{}", std::process::id());
    let service_id = service_id_of(&channel);
    let stop = CancellationToken::new();

    // The provider answers v2; the caller below asks for v1.
    let mut dispatcher = ServiceDispatcher::new(ServiceRef::new(service_id, 2));
    dispatcher.method("echo", |_header, _payload, mut emitter| {
        Box::pin(async move {
            let _ = emitter.emit(EventKind::Complete, &[]);
        })
    });
    let server = spawn_native_service(&channel, vec![dispatcher], stop.clone());

    std::thread::sleep(std::time::Duration::from_millis(300));

    let stream =
        native_call::<i32, String>(&channel, ServiceRef::new(service_id, 1), "echo", b"go")
            .expect("native_call must open the native service");
    let outcome = pollster::block_on(stream.collect());

    assert!(
        matches!(
            outcome,
            Err(ice_rpc::ObservableError::Technical(
                ice_rpc::RpcError::IncompatibleVersion {
                    expected: 2,
                    actual: 1
                }
            ))
        ),
        "expected IncompatibleVersion, got {outcome:?}"
    );

    stop.cancel();
    let _ = server.join();
}

/// A call to a method the provider does not expose is answered immediately,
/// instead of leaving the caller waiting for the transport timeout.
#[test]
fn an_unknown_method_is_reported_to_the_caller() {
    let channel = format!("IceRpcIntegration/UnknownMethod{}", std::process::id());
    let service_id = service_id_of(&channel);
    let stop = CancellationToken::new();

    let mut dispatcher = ServiceDispatcher::new(ServiceRef::new(service_id, 1));
    dispatcher.method("echo", |_header, _payload, mut emitter| {
        Box::pin(async move {
            let _ = emitter.emit(EventKind::Complete, &[]);
        })
    });
    let server = spawn_native_service(&channel, vec![dispatcher], stop.clone());

    std::thread::sleep(std::time::Duration::from_millis(300));

    let started = std::time::Instant::now();
    let stream = native_call::<i32, String>(
        &channel,
        ServiceRef::new(service_id, 1),
        "does_not_exist",
        b"go",
    )
    .expect("native_call must open the native service");
    let outcome = pollster::block_on(stream.collect());

    assert!(
        matches!(
            outcome,
            Err(ice_rpc::ObservableError::Technical(
                ice_rpc::RpcError::UnknownMethod(_)
            ))
        ),
        "expected UnknownMethod, got {outcome:?}"
    );
    // The point of the change: an answer, not a timeout.
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "the rejection waited for the transport timeout instead of being answered"
    );

    stop.cancel();
    let _ = server.join();
}

/// A call whose `service_id` nobody registered is answered immediately too.
#[test]
fn an_unknown_service_is_reported_to_the_caller() {
    let channel = format!("IceRpcIntegration/UnknownService{}", std::process::id());
    let registered = service_id_of(&format!("{channel}-registered"));
    let missing = service_id_of(&format!("{channel}-missing"));
    let stop = CancellationToken::new();

    let mut dispatcher = ServiceDispatcher::new(ServiceRef::new(registered, 1));
    dispatcher.method("echo", |_header, _payload, mut emitter| {
        Box::pin(async move {
            let _ = emitter.emit(EventKind::Complete, &[]);
        })
    });
    let server = spawn_native_service(&channel, vec![dispatcher], stop.clone());

    std::thread::sleep(std::time::Duration::from_millis(300));

    let stream = native_call::<i32, String>(&channel, ServiceRef::new(missing, 1), "echo", b"go")
        .expect("native_call must open the native service");
    let outcome = pollster::block_on(stream.collect());

    assert!(
        matches!(
            outcome,
            Err(ice_rpc::ObservableError::Technical(
                ice_rpc::RpcError::UnknownService(_)
            ))
        ),
        "expected UnknownService, got {outcome:?}"
    );

    stop.cancel();
    let _ = server.join();
}
