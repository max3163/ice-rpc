//! Command-line entry point of the ice-rpc out-of-band observer.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use iceoryx2_bb_posix::signal::SignalHandler;

use ice_rpc_monitor::config::{Config, HELP};
use ice_rpc_monitor::console::{self, LiveConsole};
use ice_rpc_monitor::metrics::Metrics;
use ice_rpc_monitor::{prometheus, Monitor};

/// Refresh interval of the live console view.
const LIVE_REFRESH: Duration = Duration::from_millis(1000);

/// Sleeps up to `total`, waking early when `cancel` is set.
fn sleep_interruptible(total: Duration, cancel: &AtomicBool) {
    let step = Duration::from_millis(100);
    let mut slept = Duration::ZERO;
    while slept < total && !cancel.load(Ordering::Relaxed) {
        let chunk = step.min(total - slept);
        std::thread::sleep(chunk);
        slept += chunk;
    }
}

fn main() {
    env_logger::init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let config = match Config::from_args(&args) {
        Ok(config) => config,
        Err(message) => {
            if message == HELP {
                println!("{message}");
                return;
            }
            eprintln!("{message}");
            std::process::exit(2);
        }
    };

    // Share the iceoryx2 root path with the observed processes, so the observer
    // and the services live in the same shared-memory domain.
    ice_rpc::gen::setup_iceoryx2_global_config();

    let live_requested = config.console_live;
    let channels = config.channels.len();

    let metrics = Arc::new(Metrics::new());
    if let Some(addr) = config.prometheus_addr {
        if let Err(e) = prometheus::serve(addr, metrics.clone()) {
            eprintln!("failed to start the Prometheus endpoint: {e}");
        }
    }

    // Bridge the process termination signal to the acquisition loop.
    let cancel = Arc::new(AtomicBool::new(false));
    let signal_flag = cancel.clone();
    std::thread::spawn(move || loop {
        if SignalHandler::termination_requested() {
            signal_flag.store(true, Ordering::Relaxed);
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    });

    let monitor = match Monitor::new(config, metrics.clone()) {
        Ok(monitor) => monitor,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };

    // `top`-like view: only when explicitly requested *and* a terminal is
    // attached; otherwise the observer stays quiet (metrics + traces only).
    let mut console = LiveConsole::new(live_requested);
    if !console.is_active() {
        if let Err(e) = monitor.run(&cancel) {
            eprintln!("monitor error: {e}");
            std::process::exit(1);
        }
        return;
    }

    eprintln!(
        "ice-rpc-monitor: live view, observing {channels} requested channel(s), Ctrl+C to stop"
    );
    let recent = monitor.recent_messages();
    let monitor_cancel = cancel.clone();
    let observer = std::thread::spawn(move || {
        if let Err(e) = monitor.run(&monitor_cancel) {
            eprintln!("monitor error: {e}");
        }
    });

    while !cancel.load(Ordering::Relaxed) {
        sleep_interruptible(LIVE_REFRESH, &cancel);
        if cancel.load(Ordering::Relaxed) {
            break;
        }
        console.draw(&console::frame(&metrics, recent.as_ref()));
    }
    console.leave();
    let _ = observer.join();
}
