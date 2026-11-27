//! End-to-end coverage of the **one-way** provider path.
//!
//! `native_oneway_handler` and `reject_undecodable_oneway` were only
//! type-checked: `tests/service_macro.rs` compiles a notification call but never
//! runs one, and `tests/invalid_payload.rs` covers the request/response side
//! only. Two silent regressions could therefore slip through:
//!
//! 1. a notification that is received but never executed — the transport would
//!    have to treat it as a request and wait for a response that never comes;
//! 2. an undecodable notification that panics or is answered, instead of being
//!    logged and dropped.
//!
//! Both are asserted here against a real provider. The `no_tracing` twin of the
//! handler shares the same body, and which twin the codegen picks is pinned by
//! `the_no_tracing_parameter_selects_the_untraced_helpers` in the golden tests,
//! so it needs no second process here.
//!
//! Run with:
//! `cargo test -p ice-rpc-macros-tests --test oneway_notification -- --nocapture`

#![allow(missing_docs)]
#![allow(clippy::unwrap_used)]

use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use ice_rpc::codegen::ShutdownGuard;
use ice_rpc::service;

/// Channel of the service under test: `#[service]` defaults the group to its
/// logical name.
const CHANNEL: &str = "NotifyProbe";

/// A notification has no return type: the provider must execute it and answer
/// nothing.
#[service("NotifyProbe")]
#[async_trait::async_trait]
pub trait NotifyProbe: Send + Sync + 'static {
    /// Executes the notification; nothing comes back.
    async fn push(&self, message: String);
}

/// Where the handler writes what it received.
///
/// A `std::sync::mpsc` sender rather than a shared `Vec`: the receiver blocks
/// with a timeout, so a missing execution fails the test instead of racing it.
static SINK: Mutex<Option<mpsc::Sender<String>>> = Mutex::new(None);

/// Serializes the two scenarios: they share one provider process and one sink.
static SCENARIO: Mutex<()> = Mutex::new(());

/// The implementation whose execution the tests observe.
struct Probe;

#[async_trait::async_trait]
impl NotifyProbe for Probe {
    async fn push(&self, message: String) {
        // Cloned out of the lock so no guard is held across the body, and a
        // poisoned lock from a failed scenario cannot break the next one.
        let sender = SINK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(sender) = sender {
            let _ = sender.send(message);
        }
    }
}

/// Boots ice-rpc once for the whole test binary.
///
/// A per-test `ShutdownGuard` would cancel the global token when the first test
/// ends, stopping the transport threads of the next one.
fn init_global() {
    static GUARD: OnceLock<ShutdownGuard> = OnceLock::new();
    GUARD.get_or_init(ice_rpc::codegen::init_without_ctrl_c);
}

/// Registers the provider once, then waits until its channel thread created the
/// services.
fn start_provider() {
    static STARTED: OnceLock<()> = OnceLock::new();
    STARTED.get_or_init(|| {
        init_global();

        let locator = ice_rpc::locator();
        let provider = NotifyProbeProxy::provide(Probe);
        ice_rpc::rt::block_on(async {
            locator.register(provider).await;
            locator.initialize_all().await.expect("initialize_all");
        });

        // Let the native service thread create the iceoryx2 services.
        std::thread::sleep(Duration::from_millis(500));
    });
}

/// Takes the scenario lock and installs a fresh sink.
fn begin_scenario() -> (MutexGuard<'static, ()>, mpsc::Receiver<String>) {
    let guard = SCENARIO
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    start_provider();

    let (sender, receiver) = mpsc::channel();
    *SINK.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(sender);
    (guard, receiver)
}

/// Removes the sink so a late sample cannot reach the next scenario, then
/// releases the lock.
fn end_scenario(guard: MutexGuard<'static, ()>) {
    *SINK.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    drop(guard);
}

/// A notification is **executed** on the provider, and its caller learns from the
/// emitter whether the sample was taken.
#[test]
fn a_notification_is_executed_by_the_provider() {
    let (guard, receiver) = begin_scenario();

    let consumer = NotifyProbeProxy::consume();
    let delivered = consumer
        .push("hello".to_string())
        .expect("the notification must be published");
    assert!(
        delivered,
        "the provider must take the sample: a one-way call reports delivery"
    );

    let received = receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("the provider must execute the notification");
    assert_eq!(received, "hello");

    end_scenario(guard);
}

/// A notification whose payload cannot be decoded is **dropped**: the handler is
/// never executed, and the channel keeps serving valid notifications afterwards.
#[test]
fn an_undecodable_notification_is_dropped_without_running_the_handler() {
    let (guard, receiver) = begin_scenario();

    // `send_one_way` is the low-level entry point of the generated caller: it
    // takes an already framed payload, which is exactly what a malformed peer
    // would put on the wire. `0xff` is not the discriminant of any request
    // variant, so the rkyv validation rejects the frame outright.
    let delivered = ice_rpc::transport::send_one_way(
        CHANNEL,
        <NotifyProbeProxy>::SERVICE,
        "push",
        &[0xffu8; 64],
    )
    .expect("the sample must be published");
    assert!(
        delivered,
        "the provider must take the sample before rejecting it"
    );

    match receiver.recv_timeout(Duration::from_millis(500)) {
        Err(RecvTimeoutError::Timeout) => {}
        other => panic!("an undecodable notification must never reach the handler: {other:?}"),
    }

    // The rejection left the channel healthy: the next valid notification still
    // runs through the same handler.
    let consumer = NotifyProbeProxy::consume();
    let delivered = consumer
        .push("still alive".to_string())
        .expect("the channel must still accept notifications");
    assert!(delivered);

    let received = receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("a valid notification after a rejection must still run");
    assert_eq!(received, "still alive");

    end_scenario(guard);
}
