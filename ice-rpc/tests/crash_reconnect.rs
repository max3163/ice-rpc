//! Integration test: iceoryx2's native node monitoring detects a provider that
//! disappeared — for both an abnormal end (`SIGKILL`) and a clean shutdown.
//!
//! The test re-executes its own binary as a child provider, using environment
//! variables to switch roles — the standard way to obtain a real second process
//! from `cargo test`.

#![allow(clippy::unwrap_used)] // tests/examples/benches may panic
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use ice_rpc::gen::iceoryx2::prelude::{
    CallbackProgression, Config, Node, NodeBuilder, NodeState, SemanticString,
};
use ice_rpc::gen::iceoryx2::service::ipc_threadsafe::Service;
use ice_rpc::gen::raw_pid_to_u32;

/// Set in the child process to make it act as the provider.
const CHILD_ENV: &str = "ICE_RPC_C7_CHILD";
/// Set, together with [`CHILD_ENV`], to make the child shut down cleanly.
const CHILD_CLEAN_ENV: &str = "ICE_RPC_C7_CHILD_CLEAN";
const TEST_NAME: &str = "node_death_is_detected";

#[test]
fn node_death_is_detected() {
    if std::env::var(CHILD_ENV).is_ok() {
        if std::env::var(CHILD_CLEAN_ENV).is_ok() {
            child_provider_clean_shutdown();
        } else {
            child_provider_until_killed();
        }
        return;
    }

    let crash = run_scenario(false);
    let clean = run_scenario(true);
    eprintln!("[c7] SIGKILL detected in {crash:?}; clean shutdown detected in {clean:?}");
}

/// Child role: create an iceoryx2 Node like a provider does, then hold it
/// until the parent kills the process.
fn child_provider_until_killed() {
    ice_rpc::gen::setup_iceoryx2_global_config();
    let _node = NodeBuilder::new().create::<Service>().expect("create node");
    announce_ready();
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

/// Child role: create an iceoryx2 Node, then drop it (the clean shutdown the
/// watcher must detect).
fn child_provider_clean_shutdown() {
    ice_rpc::gen::setup_iceoryx2_global_config();
    let node = NodeBuilder::new().create::<Service>().expect("create node");
    announce_ready();
    // Give the parent time to register its watcher and observe the node alive.
    std::thread::sleep(Duration::from_millis(2000));
    drop(node);
}

fn announce_ready() {
    eprintln!(
        "[c7][child] pid={} global_root={} nodes={:?}",
        std::process::id(),
        root_string(),
        dump_nodes()
    );
    println!("READY {}", std::process::id());
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

/// Root path of the global iceoryx2 configuration (diagnostics).
fn root_string() -> String {
    let path = Config::global_config().global.root_path();
    String::from_utf8_lossy(path.as_bytes()).to_string()
}

/// Runs one scenario and returns the detection latency.
///
/// `clean == true` lets the child exit by itself; otherwise it is killed with
/// `SIGKILL` (`Child::kill`).
fn run_scenario(clean: bool) -> Duration {
    ice_rpc::gen::setup_iceoryx2_global_config();

    let exe = std::env::current_exe().expect("current_exe");
    let mut cmd = Command::new(exe);
    cmd.args(["--exact", TEST_NAME, "--nocapture"])
        .env(CHILD_ENV, "1")
        .stdout(Stdio::piped());
    if clean {
        cmd.env(CHILD_CLEAN_ENV, "1");
    }
    let mut child = cmd.spawn().expect("spawn child provider");

    // The reader must stay alive: closing the child's stdout pipe makes its
    // test harness abort on `BrokenPipe`, which would kill the provider.
    let (pid, _child_stdout) = read_ready_pid(&mut child);

    // Register the native liveness watcher and wait until the node is observed.
    ice_rpc::gen::register_node_liveness_watcher(ice_rpc::gen::NodeId(pid));
    let alive_deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < alive_deadline && !ice_rpc::gen::is_pid_alive(pid) {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        ice_rpc::gen::is_pid_alive(pid),
        "child node (pid {pid}) was never observed alive; nodes={:?} (parent global_root={})",
        dump_nodes(),
        root_string()
    );

    let gone_at = Instant::now();
    if clean {
        wait_for_exit(&mut child, Duration::from_secs(10));
    } else {
        child.kill().expect("kill child");
        let _ = child.wait();
    }

    // The native monitoring must report the node as gone.
    let deadline = Instant::now() + Duration::from_secs(10);
    while ice_rpc::gen::is_pid_alive(pid) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }

    let kind = if clean { "clean shutdown" } else { "SIGKILL" };
    assert!(
        !ice_rpc::gen::is_pid_alive(pid),
        "the node is still reported alive after {kind}"
    );

    gone_at.elapsed()
}

/// Diagnostics: every node visible through the global config.
fn dump_nodes() -> Vec<(u32, &'static str)> {
    let mut rows = Vec::new();
    let _ = Node::<Service>::list(Config::global_config(), |state| {
        let label = match state {
            NodeState::Alive(_) => "Alive",
            NodeState::Dead(_) => "Dead",
            NodeState::Inaccessible(_) => "Inaccessible",
            NodeState::Undefined(_) => "Undefined",
        };
        rows.push((raw_pid_to_u32(state.node_id().pid().value()), label));
        CallbackProgression::Continue
    });
    rows
}

/// Reads the `READY <pid>` line printed by the child.
///
/// Returns the reader too, so the caller can keep the pipe open (see above).
fn read_ready_pid(child: &mut Child) -> (u32, BufReader<std::process::ChildStdout>) {
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
            let pid = rest.parse().expect("child pid");
            return (pid, reader);
        }
    }
    panic!("child provider never became ready (last line: {line:?})");
}

/// Waits for the child to exit on its own, killing it if it does not.
fn wait_for_exit(child: &mut Child, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(20));
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return;
            }
        }
    }
}
