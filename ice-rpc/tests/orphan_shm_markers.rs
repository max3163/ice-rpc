//! A process that is **killed** leaves the `*.shm_state` markers of the event
//! services it created behind, and a provider start reaps them.
//!
//! On Windows, `iceoryx2-pal-posix` emulates `shm_open` with memory-mapped files
//! and keeps one `<segment>.shm_state` marker per segment in a directory of its
//! own — `C:\Temp`, taken from the PAL constant `TEMP_DIRECTORY`, **not** under
//! the iceoryx2 root-path. The marker is deleted by `shm_unlink` when a process
//! releases its last reference, so a process that is killed rather than exiting
//! leaves it behind: the segment is gone, the marker stays.
//!
//! `cleanup_dead_nodes` cannot reach those markers — it removes the *service and
//! port tags* of a dead node, never the dynamic storage of an event service — so
//! [`ice_rpc::gen::sweep_orphan_shm_markers`] is the other half, and this test is
//! its guard. It is the leak the `*.event_mgmt.shm_state` files of `C:\Temp`
//! accumulate: the `_mgmt` storage of an event service, and nothing else.
//!
//! The test re-executes its own binary as a child provider, the same way
//! `crash_reconnect.rs` obtains a real second process from `cargo test`.
#![allow(clippy::unwrap_used)] // tests/examples/benches may panic

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use ice_rpc::gen::iceoryx2::prelude::ServiceName;
use ice_rpc::gen::iceoryx2::service::ipc_threadsafe::Service;

/// Set in the child process to make it create the event service and wait.
const CHILD_ENV: &str = "ICE_RPC_ORPHAN_CHILD";

/// Test re-run in the child, so only the child role executes.
const TEST_NAME: &str = "a_killed_process_leaves_shm_markers_that_a_provider_start_reaps";

/// Event service the child creates. Its `_mgmt` segment is the marker.
const EVENT: &str = "OrphanMarkerProbeService_req_notify";

/// Every `*.shm_state` marker iceoryx2 currently owns on this machine.
fn markers() -> Vec<String> {
    iceoryx2_bb_posix::shared_memory::SharedMemory::list()
        .iter()
        .map(|name| name.to_string())
        .collect()
}

/// Markers present now that were absent from `before`, or an empty vector once
/// `timeout` expires.
fn wait_for_new_markers(before: &[String], timeout: Duration) -> Vec<String> {
    let deadline = Instant::now() + timeout;
    loop {
        let new: Vec<String> = markers()
            .into_iter()
            .filter(|marker| !before.contains(marker))
            .collect();
        if !new.is_empty() {
            return new;
        }
        if Instant::now() >= deadline {
            return Vec::new();
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Sweeps until every marker of `orphans` is gone, or `timeout` expires.
///
/// Retried on purpose: the operating system releases the mapping of a killed
/// process asynchronously, and the sweep only removes a marker whose segment is
/// already unmapped.
fn wait_until_swept(orphans: &[String], timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        ice_rpc::gen::sweep_orphan_shm_markers();
        let present = markers();
        if orphans.iter().all(|orphan| !present.contains(orphan)) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Child role: create an event service, then hold it until the parent kills the
/// process — no destructor ever runs.
fn child_creates_event_and_waits() -> ! {
    ice_rpc::gen::setup_iceoryx2_global_config();
    let node = ice_rpc::gen::iceoryx2::node::NodeBuilder::new()
        .create::<Service>()
        .expect("create node");
    let name = ServiceName::new(EVENT).expect("valid service name");
    let event = node
        .service_builder(&name)
        .event()
        .open_or_create()
        .expect("open or create the event service");
    // A port makes the service's `_mgmt` storage real, not merely declared.
    let _notifier = event.notifier_builder().create().expect("create notifier");

    println!("READY {}", std::process::id());
    std::io::stdout().flush().expect("flush stdout");

    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

/// Spawns this binary again in the child role.
fn spawn_child() -> Child {
    let exe = std::env::current_exe().expect("current_exe");
    Command::new(exe)
        .args(["--exact", TEST_NAME, "--nocapture"])
        .env(CHILD_ENV, "1")
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn child provider")
}

/// Reads the `READY <pid>` line printed by the child.
///
/// The reader is returned too: closing the child's stdout pipe makes its test
/// harness abort on `BrokenPipe`, which would kill the process under test.
fn read_ready_pid(child: &mut Child) -> u32 {
    let stdout = child.stdout.take().expect("child stdout");
    let mut reader = BufReader::new(stdout);
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut line = String::new();
    while Instant::now() < deadline {
        line.clear();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            break;
        }
        if let Some(rest) = line.trim().strip_prefix("READY ") {
            return rest.parse().expect("child pid");
        }
    }
    panic!("child provider never became ready (last line: {line:?})");
}

/// The child creates an event segment, is killed, and the sweep reaps the marker
/// the kill left behind.
#[test]
fn a_killed_process_leaves_shm_markers_that_a_provider_start_reaps() {
    if std::env::var(CHILD_ENV).is_ok() {
        child_creates_event_and_waits();
    }

    ice_rpc::gen::setup_iceoryx2_global_config();

    let before = markers();
    let mut child = spawn_child();
    let pid = read_ready_pid(&mut child);

    // The child's event service shows up as a new marker on the machine.
    let created = wait_for_new_markers(&before, Duration::from_secs(10));
    assert!(
        !created.is_empty(),
        "the child (pid {pid}) created no new `*.shm_state` marker"
    );

    child.kill().expect("kill child");
    let _ = child.wait();

    // The leak under test: the segment went away with the process, the marker
    // stayed — this is what nothing but the sweep removes.
    assert!(
        created.iter().all(|marker| markers().contains(marker)),
        "a killed process must leave its markers behind, got: {created:?}"
    );

    assert!(
        wait_until_swept(&created, Duration::from_secs(10)),
        "the sweep must remove every orphan marker: {created:?}"
    );
}
