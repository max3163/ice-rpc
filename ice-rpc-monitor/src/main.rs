//! Command-line entry point of the ice-rpc out-of-band observer.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use iceoryx2_bb_posix::signal::SignalHandler;

use ice_rpc_monitor::config::{Config, HELP};
use ice_rpc_monitor::metrics::Metrics;
use ice_rpc_monitor::{prometheus, Monitor};

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

    let monitor = match Monitor::new(config, metrics) {
        Ok(monitor) => monitor,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };

    if let Err(e) = monitor.run(&cancel) {
        eprintln!("monitor error: {e}");
        std::process::exit(1);
    }
}
