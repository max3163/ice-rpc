//! Non-regression test: an undecodable request payload is answered **at once**
//! with a technical error, instead of being dropped.
//!
//! The generated native dispatcher used to swallow any payload it could not
//! decode (`_ => {}`): the provider emitted nothing, so the call reached the
//! caller as a transport timeout that named nothing. Every other rejection —
//! unknown service, unknown method, protocol and interface version mismatch —
//! was already answered immediately, which made this arm the last silent case of
//! the error strategy. It is now fail-fast too:
//!
//! ```text
//! invalid payload -> RpcError::SerializationError -> EventKind::RpcError -> immediate answer
//! ```
//!
//! Two distinct situations reach that arm, and both are asserted here because
//! both used to be silent:
//!
//! 1. the payload does not decode at all (`Err` of the rkyv decode);
//! 2. the payload decodes, but as the request variant of **another** method of
//!    the same service. The decode succeeds, so this is a caller contract
//!    violation rather than a serialization failure in the strict sense; no
//!    `RpcError` variant names it more precisely, so it is answered as a
//!    `SerializationError` like the first case, and told apart by the log line.
//!
//! Run with:
//! `cargo test -p ice-rpc-macros-tests --test invalid_payload -- --nocapture`

#![allow(missing_docs)]
#![allow(clippy::unwrap_used)]

use ice_rpc::{service, Event, Observable, ObservableError, RpcError};

/// Boots ice-rpc once for the whole test binary.
///
/// A per-test `ShutdownGuard` would cancel the global token when the first test
/// ends, stopping the transport threads of every following test.
fn init_global() {
    static GUARD: std::sync::OnceLock<ice_rpc::gen::ShutdownGuard> = std::sync::OnceLock::new();
    GUARD.get_or_init(ice_rpc::gen::init_without_ctrl_c);
}

/// Channel of this service: `#[service]` defaults the group to the logical name.
const CHANNEL: &str = "InvalidPayloadService";

#[service("InvalidPayloadService")]
#[async_trait::async_trait]
pub trait InvalidPayloadService: Send + Sync + 'static {
    /// The method the malformed frame claims to invoke.
    async fn echo(&self, value: i32) -> Observable<i32, String>;
    /// The method whose request variant the mismatched frame carries.
    async fn other(&self, value: i32) -> Observable<i32, String>;
}

struct Impl;

#[async_trait::async_trait]
impl InvalidPayloadService for Impl {
    async fn echo(&self, value: i32) -> Observable<i32, String> {
        Observable::from_events([Event::Next(value), Event::Complete])
    }

    async fn other(&self, value: i32) -> Observable<i32, String> {
        Observable::from_events([Event::Next(value), Event::Complete])
    }
}

/// Starts the provider and waits until its channel thread created the services.
fn start_provider() {
    init_global();

    let locator = ice_rpc::locator();
    let provider = InvalidPayloadServiceProxy::provide(Impl);
    ice_rpc::rt::block_on(async {
        locator.register(provider).await;
        locator.initialize_all().await.expect("initialize_all");
    });

    // Let the native service thread create the iceoryx2 services.
    std::thread::sleep(std::time::Duration::from_millis(500));
}

/// Sends `payload` as the call of `method` and returns its terminal event.
///
/// `native_call` is the low-level entry point of the generated client: it takes
/// the **already framed** payload, which is exactly what a malformed peer would
/// put on the wire.
fn call_with_raw_payload(method: &str, payload: &[u8]) -> ObservableError<String> {
    let stream = ice_rpc::gen::native_call::<i32, String>(
        CHANNEL,
        <InvalidPayloadServiceProxy>::SERVICE,
        method,
        payload,
    )
    .expect("the call must start");

    ice_rpc::rt::block_on(stream.collect()).expect_err("a rejected call must not produce a value")
}

/// Both silent cases are now answered, and with the same technical error.
#[test]
fn an_undecodable_or_mismatched_payload_is_answered_immediately() {
    start_provider();

    // ── Case 1: the payload does not decode ─────────────────────────────────
    // `0xff` is not the discriminant of any request variant, so the rkyv
    // validation rejects the frame outright.
    let undecodable = call_with_raw_payload("echo", &[0xffu8; 64]);
    assert_eq!(
        undecodable,
        ObservableError::Technical(RpcError::SerializationError),
        "the provider must answer an undecodable payload with a technical error"
    );

    // ── Case 2: the payload decodes as another method's variant ─────────────
    // A perfectly valid request — of the *other* method. The decode succeeds, so
    // only the routing knows it cannot serve this call; it must still be
    // answered rather than dropped.
    let request = InvalidPayloadServiceRequest::Other { value: 7 };
    let payload = ice_rpc::gen::rkyv::to_bytes::<ice_rpc::gen::rkyv::rancor::Error>(&request)
        .expect("the request is encodable");

    let mismatched = call_with_raw_payload("echo", &payload);
    assert_eq!(
        mismatched,
        ObservableError::Technical(RpcError::SerializationError),
        "a request of another method must be answered, not left to time out"
    );
}
