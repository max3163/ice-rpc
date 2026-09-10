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

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

/// Set in the child process to make it act as the provider.
const CHILD_ENV: &str = "ICE_RPC_SIGNAL_CHILD";

/// Test name re-run in the child, so only the provider role executes.
const TEST_NAME: &str = "sigint_triggers_clean_shutdown";

/// Maximum time allowed for the child to arm its signal handling and print
/// `READY`. Generous: instrumented (`llvm-cov`) builds are much slower than a
/// release build, and the parent now fails fast instead of hanging.
const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum time allowed for the child to shut down cleanly after `SIGINT`.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Delay letting the child's dispatch loop reach its first
/// `wait_and_process`, which is what installs iceoryx2's process-wide
/// SIGINT/SIGTERM handler. Without it an early `SIGINT` would take the default
/// disposition (immediate, ungraceful termination) and the test would report a
/// false regression.
const ARMING_SETTLE: Duration = Duration::from_millis(1500);

#[test]
fn sigint_triggers_clean_shutdown() {
    if std::env::var(CHILD_ENV).is_ok() {
        child_provider();
        return;
    }

    let mut child = spawn_child();
    let stdout = child.stdout.take().expect("child stdout");

    // `READY` is read on a dedicated thread so that the deadline below is
    // actually enforced: a blocking `read_line` would wait forever on a silent
    // child and hang the whole test binary until the CI job times out.
    let (line_tx, line_rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if line_tx.send(line.unwrap_or_default()).is_err() {
                break;
            }
        }
    });

    // Wait for `READY`: the dispatch loop has entered its first
    // `wait_and_process`, so iceoryx2 has registered its native signal handler.
    wait_for_ready(&mut child, &line_rx);

    // Send SIGINT (Ctrl+C) to the child process.
    let pid = child.id();
    let status = Command::new("kill")
        .arg("-INT")
        .arg(pid.to_string())
        .status()
        .expect("send SIGINT with kill");
    assert!(status.success(), "kill -INT failed");

    // The child must exit on its own (no SIGKILL) within the deadline.
    wait_for_clean_exit(&mut child);

    // The reader thread sees EOF once the child has exited and the pipe is
    // closed, so draining the channel yields the whole remaining output.
    let mut rest = String::new();
    while let Ok(line) = line_rx.recv() {
        rest.push_str(&line);
        rest.push('\n');
    }
    let _ = reader.join();
    assert!(
        rest.contains("SHUTDOWN OK"),
        "child did not report the clean shutdown path; output was: {rest:?}"
    );
}

/// Spawns this test binary as the child provider.
///
/// `--exact` makes the child run only the provider role, and `--nocapture` is
/// **mandatory**: libtest buffers the stdout *and* stderr of a test unless it is
/// disabled, so without it the child's `READY` line never reaches this process
/// and the handshake deadlocks. `crash_reconnect.rs` relies on the same flags.
fn spawn_child() -> Child {
    Command::new(std::env::current_exe().expect("current_exe"))
        .args(["--exact", TEST_NAME, "--nocapture"])
        .env(CHILD_ENV, "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn child")
}

/// Blocks until the child announces `READY`, enforcing [`READY_TIMEOUT`].
///
/// The child is killed and reaped on every failure path: a lingering provider
/// would keep its iceoryx2 node (and the root-path lock) alive and make the
/// following tests flaky.
fn wait_for_ready(child: &mut Child, line_rx: &mpsc::Receiver<String>) {
    let deadline = Instant::now() + READY_TIMEOUT;
    let mut observed: Vec<String> = Vec::new();

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match line_rx.recv_timeout(remaining.min(Duration::from_millis(200))) {
            Ok(line) if line.contains("READY") => return,
            Ok(line) => observed.push(line),
            Err(RecvTimeoutError::Disconnected) => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("child stdout closed before printing READY; output was: {observed:?}");
            }
            Err(RecvTimeoutError::Timeout) => {
                if let Some(status) = child.try_wait().expect("try_wait") {
                    panic!(
                        "child exited ({status:?}) before printing READY; output was: {observed:?}"
                    );
                }
            }
        }

        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("child never became ready within {READY_TIMEOUT:?}; output was: {observed:?}");
        }
    }
}

/// Waits for the child to exit by itself after `SIGINT` (no `SIGKILL`).
fn wait_for_clean_exit(child: &mut Child) {
    let deadline = Instant::now() + SHUTDOWN_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            assert!(status.success(), "child exited with {status:?}");
            return;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("child did not shut down within {SHUTDOWN_TIMEOUT:?} after SIGINT");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
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
    std::thread::sleep(ARMING_SETTLE);

    println!("READY {}", std::process::id());
    let _ = std::io::stdout().flush();

    pollster::block_on(ice_rpc::gen::wait_for_shutdown());

    println!("SHUTDOWN OK");
    let _ = std::io::stdout().flush();
}
