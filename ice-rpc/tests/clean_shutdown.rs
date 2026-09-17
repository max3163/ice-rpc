//! A clean shutdown must release what the process created on the bus.
//!
//! The dispatch thread of a channel owns its iceoryx2 ports, and dropping those
//! ports is what unlinks the services — including the `*.shm_state` marker files
//! `iceoryx2-pal-posix` keeps for them on Windows, in `C:\Temp`. A process that
//! stops *cleanly* therefore still leaks if it does not wait for its threads:
//! the services survive it, and the next process to open one of them can be told
//! `SystemInFlux` for state that a clean exit should have removed.
//!
//! Waiting for them is what [`ice_rpc::gen::shutdown_and_release`] does, through
//! the shutdown registry, and this test is the guard of that behaviour.
//!
//! It shuts down **programmatically** rather than by signal on purpose:
//! `signal_shutdown.rs` needs `kill -INT` and skips on Windows, which is the very
//! platform where the leftover markers are the visible symptom.
#![allow(clippy::unwrap_used)] // tests/examples/benches may panic

use std::time::{Duration, Instant};

use ice_rpc::gen::{service_id_of, spawn_native_service, ServiceDispatcher};

/// Channel — and service name — the test creates on the bus.
const SERVICE: &str = "CleanShutdownProbeService";

/// How long a bus state change is given to appear: the registry is written by
/// another thread, and a loaded CI machine is slow.
const SETTLE: Duration = Duration::from_secs(5);

/// Number of iceoryx2 services whose name mentions [`SERVICE`].
///
/// One channel is four of them: requests, responses, and the two notification
/// services used as wake-up signals.
fn probe_services() -> usize {
    ice_rpc::monitor::list_services()
        .map(|services| {
            services
                .iter()
                .filter(|service| service.name.contains(SERVICE))
                .count()
        })
        .unwrap_or(0)
}

/// Waits for `condition`, or fails with what it was waiting for.
fn wait_until(condition: impl Fn() -> bool, expectation: &str) {
    let deadline = Instant::now() + SETTLE;
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for: {expectation}");
}

/// The process creates a channel, shuts down cleanly, and the bus must be back to
/// what it was: nothing left behind.
#[test]
fn a_clean_shutdown_releases_the_services_this_process_created() {
    let _guard = ice_rpc::gen::init();

    assert_eq!(
        probe_services(),
        0,
        "the probe service must not exist before the test"
    );

    // Registered exactly as the generated provider does: without it, a clean
    // shutdown has nothing to wait for and the services survive the process.
    let handle = spawn_native_service(
        SERVICE,
        vec![(service_id_of(SERVICE), ServiceDispatcher::new())],
        ice_rpc::global_cancel_token().clone(),
    );
    ice_rpc::locator().register_shutdown_thread(handle);

    wait_until(
        || probe_services() > 0,
        "the dispatch thread must create the services of its channel",
    );

    // The path `#[ice_rpc::main]` takes at the end of `main`, and the one Ctrl+C
    // takes: cancel, join the dispatch threads, release the cached ports.
    pollster::block_on(ice_rpc::gen::shutdown_and_release());

    wait_until(
        || probe_services() == 0,
        "a clean shutdown must release every service the process created \
         (the dispatch thread was not joined, so its ports were never dropped)",
    );
}
