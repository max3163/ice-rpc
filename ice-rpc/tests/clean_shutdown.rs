//! A clean shutdown must release what the process created on the bus.
//!
//! The dispatch thread of a channel owns its iceoryx2 ports, and dropping those
//! ports is what unlinks the services — including the `*.shm_state` marker files
//! `iceoryx2-pal-posix` keeps for them on Windows, in `C:\Temp`. A process that
//! stops *cleanly* therefore still leaks if it does not wait for its threads:
//! the services survive it, and the next process to open one of them can be told
//! `SystemInFlux` for state that a clean exit should have removed.
//!
//! The **node** is the other half, and the piece that outlives its ports: iceoryx2
//! removes `nodes/<id>/` and its `node_monitor*` files when the node is dropped,
//! and the transport keeps its node in a process-wide cell. `shutdown_and_release`
//! empties that cell once the ports are gone; without it, every clean exit left
//! one node per run for the *next* process to reap.
//!
//! Waiting for the threads and releasing both caches is what
//! [`ice_rpc::gen::shutdown_and_release`] does, and this test is the guard of
//! that behaviour — it asserts the services *and* the node are gone.
//!
//! It shuts down **programmatically** rather than by signal on purpose:
//! `signal_shutdown.rs` needs `kill -INT` and skips on Windows, which is the very
//! platform where the leftover markers are the visible symptom.
#![allow(clippy::unwrap_used)] // tests/examples/benches may panic

use std::time::{Duration, Instant};

use ice_rpc::gen::iceoryx2::prelude::SemanticString;
use ice_rpc::gen::{service_id_of, spawn_native_service, ServiceDispatcher, ServiceRef};

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

/// Number of entries in iceoryx2's node directory.
///
/// This is the state a **node** owns — its `nodes/<id>/` directory and the
/// `node_monitor*` files beside it — and that iceoryx2 removes when the node is
/// dropped. A clean shutdown must give it back: the node the transport creates
/// lives in a process-wide cell, and a cell nothing empties keeps the node, and
/// its files, alive until the process dies.
///
/// The directory is shared by every process on the machine, so the test compares
/// the count before and after rather than asserting it is empty: an observer or
/// another provider may own entries of its own.
fn node_entries() -> usize {
    let config = ice_rpc::gen::iceoryx2::config::Config::global_config();
    // Bound first: `node_dir` returns the path by value, and `as_bytes` borrows it.
    let node_dir = config.global.node_dir();
    let dir = std::str::from_utf8(node_dir.as_bytes()).unwrap_or_default();
    std::fs::read_dir(dir)
        .map(|entries| entries.count())
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
/// what it was: nothing left behind — neither its services nor its node.
#[test]
fn a_clean_shutdown_releases_what_this_process_created() {
    let _guard = ice_rpc::gen::init();

    assert_eq!(
        probe_services(),
        0,
        "the probe service must not exist before the test"
    );

    let nodes_before = node_entries();

    // Registered exactly as the generated provider does: without it, a clean
    // shutdown has nothing to wait for and the services survive the process.
    let handle = spawn_native_service(
        SERVICE,
        vec![ServiceDispatcher::new(ServiceRef::new(
            service_id_of(SERVICE),
            1,
        ))],
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

    // The node is the other half: dropping it is what removes `nodes/<id>/` and
    // its `node_monitor*` files. Without it, a clean exit leaves one node per run
    // for the *next* process to reap.
    wait_until(
        || node_entries() <= nodes_before,
        "a clean shutdown must release the node this process created \
         (`nodes/<id>/` and its `node_monitor*` files, removed when the node is dropped)",
    );
}
