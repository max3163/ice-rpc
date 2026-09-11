//! Integration test: a process must stop cleanly when it receives SIGINT
//! (Ctrl+C), through iceoryx2's **native** `WaitSet` signal handling — with no
//! `ctrlc` handler involved.
//!
//! The test re-executes its own binary as a child process (same pattern as
//! `crash_reconnect.rs`): the child starts a dispatch `WaitSet`, prints `READY`
//! once the native handler is armed, and the parent sends `SIGINT`. The child
//! must then exit by itself with a success code and report `SHUTDOWN OK`.
//!
//! This is the regression test for the `HandleTerminationRequests` contract:
//! iceoryx2 **owns** the SIGINT/SIGTERM handler in that mode, so the process
//! does not die on its own. Without the transport reacting to
//! `WaitSetRunResult::TerminationRequest` (or to
//! `SignalHandler::termination_requested()` on the busy path), Ctrl+C would be
//! swallowed and the child would hang until the timeout.
#![allow(clippy::unwrap_used)] // tests/examples/benches may panic; production libs keep the deny, see [workspace.lints]

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

/// Set in the child process to make it act as the provider.
const CHILD_ENV: &str = "ICE_RPC_SIGNAL_CHILD";

/// Test name re-run in the child, so only the provider role executes.
const TEST_NAME: &str = "sigint_triggers_clean_shutdown";

/// Logical service the child provides: it only has to exist so a dispatch
/// thread — and therefore a `WaitSet` — is created.
const CHILD_SERVICE: &str = "SignalProbeService";

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
    if !send_sigint(pid) {
        let _ = child.kill();
        let _ = child.wait();
        eprintln!("skipping: no `kill` available on this platform (pid {pid})");
        return;
    }

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

/// Delivers `SIGINT` to `pid` through the `kill` command.
///
/// Returns `false` when no signal could be sent, so the caller skips the test
/// instead of failing it.
///
/// Unix only: `kill` resolves a *native* Windows pid only when the process was
/// registered by an MSYS shell, so a child spawned by `std::process` cannot be
/// signalled that way — and Rust's std exposes no `GenerateConsoleCtrlEvent`.
/// The Ctrl+C path was verified manually on Windows when the regression was
/// fixed (the provider logs `Termination signal received, shutting down...` and
/// stops); this test keeps the coverage on the platforms that can send it.
#[cfg(unix)]
fn send_sigint(pid: u32) -> bool {
    match Command::new("kill")
        .arg("-INT")
        .arg(pid.to_string())
        .status()
    {
        Ok(status) => {
            assert!(status.success(), "kill -INT failed for pid {pid}");
            true
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => panic!("could not run kill: {e}"),
    }
}

/// `SendConsoleCtrlEvent` is unavailable to Rust's std, see [`send_sigint`].
#[cfg(not(unix))]
fn send_sigint(_pid: u32) -> bool {
    false
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

/// Child role: enable the native signal handling, arm a dispatch `WaitSet`,
/// then wait for the shutdown triggered by the signal.
fn child_provider() {
    // `init()` selects iceoryx2's native SIGINT/SIGTERM handling for the
    // transport `WaitSet`s. The returned guard must stay alive for the process.
    let _guard = ice_rpc::gen::init();

    // Start one real service: spawning its dispatch thread is what creates the
    // `WaitSet` that arms iceoryx2's signal handler.
    let _handle = ice_rpc::gen::spawn_native_service(
        CHILD_SERVICE,
        |_method, _payload| -> ice_rpc::gen::ResponseIter { Box::new(std::iter::empty()) },
        ice_rpc::global_cancel_token().clone(),
    );

    // Give the dispatch loop time to enter its first `wait_and_process` so the
    // native SIGINT handler is registered before the parent sends the signal.
    std::thread::sleep(ARMING_SETTLE);

    println!("READY {}", std::process::id());
    let _ = std::io::stdout().flush();

    pollster::block_on(ice_rpc::gen::wait_for_shutdown());

    println!("SHUTDOWN OK");
    let _ = std::io::stdout().flush();
}
