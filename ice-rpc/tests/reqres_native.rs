//! Integration test for the native iceoryx2 request/response transport.
//!
//! Only compiled with the `native-transport` feature.

#![cfg(feature = "native-transport")]

use ice_rpc::gen::{rkyv, WireEvent};
use ice_rpc::reqres::{native_call, observable_to_responses, spawn_native_service};
use ice_rpc::{CancellationToken, Event, Observable};

fn encode_i32(event: WireEvent<i32, String>) -> Vec<u8> {
    rkyv::to_bytes::<rkyv::rancor::Error>(&event)
        .expect("encode event")
        .to_vec()
}

/// One request must carry N streamed responses (rkyv `WireEvent`), then complete
/// when the service closes the connection.
#[test]
fn native_transport_streams_n_responses_then_completes() {
    let service = format!("IceRpcNative/Test{}", std::process::id());
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

/// A service must be able to stream a real `Observable` (the shape a generated
/// provider returns) through the native transport.
#[test]
fn native_transport_streams_a_real_observable() {
    let service = format!("IceRpcNative/Obs{}", std::process::id());
    let stop = CancellationToken::new();
    let server = spawn_native_service(
        &service,
        |_method, _payload| {
            let observable = Observable::<i32, String>::from_events([
                Event::Next(10),
                Event::Next(20),
                Event::Complete,
            ]);
            observable_to_responses(observable)
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
