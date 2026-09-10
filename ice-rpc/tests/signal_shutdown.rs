//! Integration test: a process must stop cleanly when it receives SIGINT
//! (Ctrl+C), through iceoryx2's **native** `WaitSet` signal handling — with no
//! `ctrlc` handler involved.
//!
//! The test re-executes its own binary as a child process (same pattern as
//! `crash_reconnect.rs`): the child starts the dispatch `WaitSet`, prints
//! `READY` once the native handler is armed, and the parent sends `SIGINT`.
//! The child must then exit by itself with a success code and report
//! `SHUTDOWN OK`.
#![allow(clippy::unwrap_used)] // tests/examples/benches may panic; production libs keep the deny, see [workspace.lints]
#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Set in the child process to make it act as the provider.
const CHILD_ENV: &str = "ICE_RPC_SIGNAL_CHILD";

#[test]
fn sigint_triggers_clean_shutdown() {
    if std::env::var(CHILD_ENV).is_ok() {
        child_provider();
        return;
    }

    let mut child = Command::new(std::env::current_exe().expect("current_exe"))
        .env(CHILD_ENV, "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn child");

    let stdout = child.stdout.take().expect("child stdout");
    let mut reader = BufReader::new(stdout);

    // Wait for `READY`: the dispatch loop has entered its first
    // `wait_and_process`, so iceoryx2 has registered its native signal handler.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let mut line = String::new();
        let read = reader.read_line(&mut line).expect("read child stdout");
        if read == 0 {
            panic!("child exited before printing READY");
        }
        if line.contains("READY") {
            break;
        }
        assert!(Instant::now() < deadline, "child never became ready");
    }

    // Send SIGINT (Ctrl+C) to the child process.
    let pid = child.id();
    let status = Command::new("kill")
        .arg("-INT")
        .arg(pid.to_string())
        .status()
        .expect("send SIGINT with kill");
    assert!(status.success(), "kill -INT failed");

    // The child must exit on its own (no SIGKILL) within the deadline.
    let deadline = Instant::now() + Duration::from_secs(15);
    let exit = loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!("child did not shut down after SIGINT");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(exit.success(), "child exited with {exit:?}");

    let mut rest = String::new();
    reader
        .read_to_string(&mut rest)
        .expect("drain child stdout");
    assert!(
        rest.contains("SHUTDOWN OK"),
        "child did not report the clean shutdown path; output was: {rest:?}"
    );
}

/// Child role: enable the native signal handling, arm the dispatch `WaitSet`,
/// then wait for the shutdown triggered by the signal.
fn child_provider() {
    // `init()` enables iceoryx2's native SIGINT/SIGTERM handling for the
    // `WaitSet` loops. The returned guard must stay alive for the process.
    let _guard = ice_rpc::gen::init();

    // Create the node and start the dispatch loop: its `WaitSet` is what arms
    // iceoryx2's signal handler on first wait.
    let _node = ice_rpc::ServiceLocator::global()
        .get_node_sync()
        .expect("get_node_sync");
    ice_rpc::ServiceLocator::global().start_dispatch_if_needed();

    // Give the dispatch loop time to enter its first `wait_and_process` so the
    // native SIGINT handler is registered before the parent sends the signal.
    std::thread::sleep(Duration::from_millis(700));

    println!("READY {}", std::process::id());
    let _ = std::io::stdout().flush();

    pollster::block_on(ice_rpc::gen::wait_for_shutdown());

    println!("SHUTDOWN OK");
    let _ = std::io::stdout().flush();
}
