//! Diagnostic harness for audit **C7**: validate that iceoryx2's *native* node
//! monitoring detects a provider killed with `SIGKILL`, before removing the
//! hand-written kernel lock (`node_lock.rs`).
//!
//! It is deliberately standalone: it uses the same iceoryx2 service as ice-rpc
//! (`ipc_threadsafe::Service`) and `Node::list` / `NodeState`.
//!
//! # Usage
//! ```bash
//! cargo build -p ice-rpc --example node_liveness_probe
//! node_liveness_probe provider [hold_secs] [direct]
//!     # create a Node; `direct` uses NodeBuilder (local Drop is decisive),
//!     # otherwise it goes through ServiceLocator::get_node_sync (realistic
//!     # provider path, whose Node is also kept by the global singleton)
//! node_liveness_probe list                          # print "<pid> <state>" for every node
//! node_liveness_probe watch <pid>                   # poll until <pid> is Dead, print latency
//! node_liveness_probe bench <iters>                 # cost of Node::list
//! ```
//!
//! See `scripts/validate-node-liveness.sh` for the orchestrated T1/T2/T3 runs.

use std::io::Write;
use std::time::{Duration, Instant};

use iceoryx2::prelude::*;

fn setup() {
    // Installs the same global iceoryx2 config (root path) that ice-rpc uses,
    // so `Node::list(Config::global_config(), ..)` sees the same nodes.
    ice_rpc::gen::setup_iceoryx2_global_config();
}

fn label<S: Service>(state: &NodeState<S>) -> &'static str {
    match state {
        NodeState::Alive(_) => "Alive",
        NodeState::Dead(_) => "Dead",
        NodeState::Inaccessible(_) => "Inaccessible",
        NodeState::Undefined(_) => "Undefined",
    }
}

/// One `Node::list` pass: `(pid, state label)` for every iceoryx2 node.
fn scan() -> Vec<(u32, &'static str)> {
    let mut out = Vec::new();
    Node::<ipc_threadsafe::Service>::list(Config::global_config(), |state| {
        out.push((state.node_id().pid().value() as u32, label(&state)));
        CallbackProgression::Continue
    })
    .expect("Node::list failed");
    out
}

fn list_cmd() {
    setup();
    for (pid, state) in scan() {
        println!("{pid} {state}");
    }
}

fn watch_cmd(target: u32) {
    setup();
    let start = Instant::now();
    loop {
        let snapshot = scan();
        if snapshot.iter().any(|(p, s)| *p == target && *s == "Dead") {
            let ms = start.elapsed().as_secs_f64() * 1000.0;
            println!("DEAD {target} after {ms:.1} ms");
            return;
        }
        if start.elapsed() > Duration::from_secs(10) {
            println!("TIMEOUT {target} snapshot={snapshot:?}");
            std::process::exit(2);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn bench_cmd(iters: usize) {
    setup();

    let t0 = Instant::now();
    for _ in 0..iters {
        let _ = scan();
    }
    let list_us = t0.elapsed().as_secs_f64() * 1e6 / iters as f64;

    println!("Node::list: {list_us:.1} us/iter");
}

fn print_pid_and_hold(hold: Option<u64>) {
    println!("PID {}", std::process::id());
    let _ = std::io::stdout().flush();
    match hold {
        Some(secs) if secs > 0 => std::thread::sleep(Duration::from_secs(secs)),
        _ => loop {
            std::thread::sleep(Duration::from_secs(3600));
        },
    }
}

fn provider_cmd(hold: Option<u64>, direct: bool) {
    setup();

    if direct {
        // A locally owned Node: dropping it *is* the clean shutdown, so this
        // isolates the monitoring semantics (clean removal vs crash).
        let node = NodeBuilder::new()
            .create::<ipc_threadsafe::Service>()
            .expect("NodeBuilder::create");
        print_pid_and_hold(hold);
        drop(node);
    } else {
        // Realistic provider path: the Node is also held by the global
        // singleton, so only `ShutdownGuard`/`release_node` removes it.
        let node = ice_rpc::ServiceLocator::global()
            .get_node_sync()
            .expect("get_node_sync");
        print_pid_and_hold(hold);
        drop(node);
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str).unwrap_or("") {
        "provider" => {
            let hold = args.get(2).and_then(|s| s.parse().ok());
            let direct = args.iter().any(|a| a == "direct");
            provider_cmd(hold, direct);
        }
        "list" => list_cmd(),
        "watch" => {
            let pid: u32 = args
                .get(2)
                .expect("usage: watch <pid>")
                .parse()
                .expect("pid");
            watch_cmd(pid);
        }
        "bench" => {
            let iters: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(100);
            bench_cmd(iters);
        }
        _ => {
            eprintln!(
                "usage: node_liveness_probe provider [hold_secs] [direct] | list | watch <pid> | bench <iters>"
            );
            std::process::exit(1);
        }
    }
}
