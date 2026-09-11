//! Integration tests exercising the iceoryx2-backed IPC paths.
//!
//! These tests run in their own test binary (separate process), so the global
//! iceoryx2 node and the global lock do not conflict with the unit tests.

#![allow(clippy::unwrap_used)]
use ice_rpc::gen::{rkyv, WireEvent};
use ice_rpc::reqres::{native_call, spawn_native_service};
use ice_rpc::{CancellationToken, Event, Observable};

fn encode_i32(event: WireEvent<i32, String>) -> Vec<u8> {
    rkyv::to_bytes::<rkyv::rancor::Error>(&event)
        .expect("encode event")
        .to_vec()
}

/// A native request must carry N streamed responses, then complete when the
/// service closes the connection.
#[test]
fn native_request_response_streams_then_completes() {
    let service = format!("IceRpcIntegration/Roundtrip{}", std::process::id());
    let stop = CancellationToken::new();
    let server = spawn_native_service(
        &service,
        |_method, _payload| {
            let samples: Vec<Vec<u8>> = (0..3i32)
                .map(|value| encode_i32(WireEvent::Next(value)))
                .collect();
            Box::new(samples.into_iter())
        },
        stop.clone(),
    );

    // Give the service thread time to create the shared node and the service.
    std::thread::sleep(std::time::Duration::from_millis(300));

    let stream = native_call::<i32, String>(&service, "echo", b"go")
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
    let service = format!("IceRpcIntegration/Obs{}", std::process::id());
    let stop = CancellationToken::new();
    let server = spawn_native_service(
        &service,
        |_method, _payload| {
            let observable = Observable::<i32, String>::from_events([
                Event::Next(10),
                Event::Next(20),
                Event::Complete,
            ]);
            ice_rpc::reqres::observable_to_responses(observable)
        },
        stop.clone(),
    );

    std::thread::sleep(std::time::Duration::from_millis(300));

    let stream = native_call::<i32, String>(&service, "watch", b"")
        .expect("native_call must open the native service");
    let values = pollster::block_on(stream.collect()).expect("collect");
    assert_eq!(values, vec![10, 20]);

    stop.cancel();
    let _ = server.join();
}
