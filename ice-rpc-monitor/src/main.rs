//! Command-line entry point of the ice-rpc out-of-band observer.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ice_rpc_monitor::cleanup;
use ice_rpc_monitor::config::{Config, HELP};
use ice_rpc_monitor::console::{self, LiveConsole};
use ice_rpc_monitor::metrics::Metrics;
use ice_rpc_monitor::shutdown::{self, Termination};
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

    let live_requested = config.console_live;
    let channels = config.channels.len();

    // Opt-in: heal a bus polluted by killed runs before observing it. Without
    // the flag the observer stays strictly read-only.
    if config.cleanup {
        let (dead_nodes, orphan_markers) = cleanup::reap_dead_state();
        log::info!(
            "[monitor] cleanup: reaped {dead_nodes} dead node(s), {orphan_markers} orphan shm marker(s)"
        );
        eprintln!(
            "ice-rpc-monitor: cleanup reaped {dead_nodes} dead node(s), {orphan_markers} orphan shm marker(s)"
        );
    }

    let metrics = Arc::new(Metrics::new());
    if let Some(addr) = config.prometheus_addr {
        if let Err(e) = prometheus::serve(addr, metrics.clone()) {
            eprintln!("failed to start the Prometheus endpoint: {e}");
        }
    }

    // A persistent signal callback sets the shutdown flag once, so the loops no
    // longer depend on a bridge competing for the (consumed) pending signal. Keep
    // the guard alive for the whole process; it falls back to polling internally.
    let _termination = Termination::install();
    let cancel: &'static AtomicBool = shutdown::flag();

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
        if let Err(e) = monitor.run(cancel) {
            eprintln!("monitor error: {e}");
            std::process::exit(1);
        }
        return;
    }

    eprintln!(
        "ice-rpc-monitor: live view, observing {channels} requested channel(s), Ctrl+C to stop"
    );
    let recent = monitor.recent_messages();
    let monitor_cancel = cancel;
    let observer = std::thread::spawn(move || {
        if let Err(e) = monitor.run(monitor_cancel) {
            eprintln!("monitor error: {e}");
        }
    });

    while !cancel.load(Ordering::Relaxed) {
        sleep_interruptible(LIVE_REFRESH, cancel);
        if cancel.load(Ordering::Relaxed) {
            break;
        }
        console.draw(&console::frame(&metrics, recent.as_ref()));
    }
    console.leave();
    let _ = observer.join();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sleep_returns_early_when_already_cancelled() {
        let cancel = AtomicBool::new(true);
        let started = std::time::Instant::now();
        sleep_interruptible(Duration::from_secs(30), &cancel);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "a cancelled sleep must not wait out its whole duration"
        );
    }

    #[test]
    fn a_sleep_waits_its_duration_when_not_cancelled() {
        let cancel = AtomicBool::new(false);
        let started = std::time::Instant::now();
        sleep_interruptible(Duration::from_millis(250), &cancel);
        assert!(
            started.elapsed() >= Duration::from_millis(200),
            "an uncancelled sleep waits for its duration"
        );
    }
}
