//! Complete console example of the ice-rpc out-of-band observer.
//!
//! `console-monitor` attaches **in read-only mode** to the ice-rpc channels of
//! the machine and prints a live stats block on the console. By default it only
//! shows the stats; with `--detail` it also decodes and prints the request and
//! the response of every completed call — the *messages*.
//!
//! Decoding works because the observer is linked against the **same service
//! contract** as the providers and consumers (`common`, here): each `#[service]`
//! generates a `{Service}Decoder`, `common::decoders()` registers them all, and
//! the monitor renders every payload with the `Display` implementation of the
//! service types, falling back to their `Debug` implementation when they have
//! none.
//!
//! ```text
//! # 1. Standalone demonstration: an in-process DatabaseService provider is
//! #    started and called, so the console shows decoded messages.
//! cargo run -p ice-rpc-monitor --example console-monitor -- --demo
//! cargo run -p ice-rpc-monitor --example console-monitor -- --demo --detail
//!
//! # 2. Against your own provider/consumer (e.g. `provider-app`), in another
//! #    terminal.
//! cargo run -p ice-rpc-monitor --example console-monitor
//! cargo run -p ice-rpc-monitor --example console-monitor -- --detail --interval-ms 500
//! ```
//!
//! Everything is printed on stdout: the stats as `===== ... =====` blocks, the
//! messages (`--detail`) as one `[msg] ...` line each. Run it from the workspace
//! root so it shares the generated `config/iceoryx2.toml` with the observed
//! processes.

#![allow(clippy::unwrap_used)] // an example is allowed to panic

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::{decoders as common_decoders, DatabaseError, DatabaseServiceRequest};
use ice_rpc::gen::{decode_aligned, rkyv, service_id_of, ServiceDispatcher, ServiceRef};
use ice_rpc::transport::{native_call, observable_to_responses, spawn_native_service};
use ice_rpc::{CancellationToken, Event, Observable};

use ice_rpc_monitor::config::{Config, Mode};
use ice_rpc_monitor::console::{self, LiveConsole};
use ice_rpc_monitor::metrics::Metrics;
use ice_rpc_monitor::traces::TraceFormat;
use ice_rpc_monitor::Monitor;

/// Logical name of the service the standalone demo hosts.
const DEMO_SERVICE: &str = "DatabaseService";

/// Usage text for the flags this example owns (the rest are `Config`'s).
const HELP: &str = "\
ice-rpc-monitor console example — observe ice-rpc traffic from a terminal

USAGE:
    cargo run -p ice-rpc-monitor --example console-monitor -- [OPTIONS]

EXAMPLE OPTIONS:
    --detail                       Full mode: also decode and print the messages
                                   (request and response) of every completed call
    --demo                         Start and call an in-process DatabaseService so
                                   the example is self-contained (default: observe
                                   the existing channels only)
    --live                         Redraw the stats in place, like `top`, and show
                                   the last messages inside the frame (needs a
                                   terminal; ignored when piped)
    --interval-ms <ms>             Stats refresh interval (default 1000)
    -h, --help                     Show this help and the observer options";

/// Options owned by the example; every other argument is forwarded to [`Config`].
struct Options {
    detail: bool,
    demo: bool,
    live: bool,
    interval: Duration,
    config: Config,
}

impl Options {
    /// Splits the command line between the example flags and the observer config.
    ///
    /// # Errors
    /// Returns a human-readable message on a missing value, a bad number, or an
    /// unknown observer flag.
    fn parse(args: &[String]) -> Result<Self, String> {
        let mut detail = false;
        let mut demo = false;
        let mut live = false;
        let mut interval = Duration::from_millis(1000);
        let mut forwarded: Vec<String> = Vec::new();

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--detail" => detail = true,
                "--demo" => demo = true,
                "--live" => live = true,
                "--interval-ms" => {
                    i += 1;
                    let raw = args.get(i).ok_or("missing value for --interval-ms")?;
                    let ms: u64 = raw
                        .parse()
                        .map_err(|e| format!("invalid --interval-ms '{raw}': {e}"))?;
                    interval = Duration::from_millis(ms.max(1));
                }
                // A bare `--` is a separator (cargo-make forwards one): ignore it.
                "--" => {}
                other => forwarded.push(other.to_owned()),
            }
            i += 1;
        }

        let mut config = Config::from_args(&forwarded)?;

        // The observer is linked against `common`: register every generated
        // decoder so the payloads are rendered in clear text.
        config.decoders = Arc::new(common_decoders());

        config.console_live = live;
        if detail {
            // "Full" mode: read the payload of every sample, decode it, and emit
            // one readable trace per completed call.
            config.mode = Mode::Detail;
            if config.trace_sample_rate == 0 {
                config.trace_sample_rate = 1;
            }
            config.trace_format = TraceFormat::Human;
        }
        Ok(Self {
            detail,
            demo,
            live,
            interval,
            config,
        })
    }
}

/// A self-contained demo: an in-process `DatabaseService` provider plus a thread
/// calling it every 200 ms until `cancel` is set.
struct Demo {
    channel: String,
    server_stop: CancellationToken,
    server: std::thread::JoinHandle<()>,
    generator: std::thread::JoinHandle<()>,
}

impl Demo {
    /// Starts the provider and the traffic generator on a dedicated channel.
    ///
    /// The channel is unique per process, but the **service id** stays that of
    /// `DatabaseService`, so the registered decoders apply.
    fn start(cancel: &Arc<AtomicBool>) -> Self {
        let channel = format!("ConsoleMonitorDemo{}", std::process::id());
        let service_id = service_id_of(DEMO_SERVICE);

        let mut dispatcher = ServiceDispatcher::new(ServiceRef::new(service_id, 1));
        dispatcher.method("get_user_age", |payload, emitter| {
            let name = match decode_aligned::<DatabaseServiceRequest>(payload) {
                Ok(DatabaseServiceRequest::GetUserAge { name }) => name,
                _ => return,
            };
            let age: i32 = match name.as_str() {
                "Alice" => 30,
                "Bob" => 42,
                "Charlie" => 25,
                _ => 99,
            };
            observable_to_responses(
                Observable::<i32, DatabaseError>::from_events([Event::Next(age), Event::Complete]),
                emitter,
            );
        });

        let server_stop = CancellationToken::new();
        let server = spawn_native_service(&channel, vec![dispatcher], server_stop.clone());
        // Give the provider time to open every port before the first call.
        std::thread::sleep(Duration::from_millis(300));

        let generator_cancel = cancel.clone();
        let generator_channel = channel.clone();
        let generator = std::thread::spawn(move || {
            const NAMES: [&str; 3] = ["Alice", "Bob", "Max"];
            let mut call = 0u64;
            while !generator_cancel.load(Ordering::Relaxed) {
                let name = NAMES[(call as usize) % NAMES.len()];
                let request = DatabaseServiceRequest::GetUserAge {
                    name: name.to_owned(),
                };
                match rkyv::to_bytes::<rkyv::rancor::Error>(&request) {
                    Ok(bytes) => {
                        if let Ok(stream) = native_call::<i32, DatabaseError>(
                            &generator_channel,
                            ServiceRef::new(service_id, 1),
                            "get_user_age",
                            &bytes,
                        ) {
                            let _ = pollster::block_on(stream.collect());
                        }
                    }
                    // Expected while shutting down: the transport is cancelled a
                    // moment before the stop flag flips, so keep it at debug level.
                    Err(e) => log::debug!("[demo] encode failed: {e}"),
                }
                call += 1;
                std::thread::sleep(Duration::from_millis(200));
            }
        });

        Self {
            channel,
            server_stop,
            server,
            generator,
        }
    }

    /// Stops the generator first (the provider is still up, so its in-flight
    /// call cannot block), then the provider.
    fn stop(self) {
        let _ = self.generator.join();
        self.server_stop.cancel();
        let _ = self.server.join();
    }
}

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

/// Prints the startup banner describing what will be observed.
fn print_banner(
    detail: bool,
    interval: Duration,
    channels: &str,
    demo: Option<&str>,
    prometheus: Option<SocketAddr>,
) {
    println!("ice-rpc-monitor console example");
    println!("  channels : {channels}");
    println!(
        "  mode     : {}",
        if detail {
            "detail (stats + decoded messages)"
        } else {
            "stats (metadata only, pass --detail for the messages)"
        }
    );
    println!("  interval : {} ms", interval.as_millis());
    match prometheus {
        Some(addr) => println!("  metrics  : http://{addr}/metrics"),
        None => println!("  metrics  : disabled (--prometheus off)"),
    }
    match demo {
        Some(channel) => println!("  demo     : DatabaseService hosted on '{channel}'"),
        None => println!("  hint     : add --demo to host and call a service on its own"),
    }
    println!("  stop     : Ctrl+C");
    println!();
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "-h" || arg == "--help") {
        println!("{HELP}\n");
        println!("{}", ice_rpc_monitor::config::HELP);
        return;
    }

    let mut options = match Options::parse(&args) {
        Ok(options) => options,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };

    // The observer and the observed processes must share the same iceoryx2 domain.
    ice_rpc::gen::setup_iceoryx2_global_config();

    // Bridge the process termination signal (Ctrl+C) to the shutdown flag.
    let cancel = Arc::new(AtomicBool::new(false));
    let signal_flag = cancel.clone();
    std::thread::spawn(move || loop {
        if iceoryx2_bb_posix::signal::SignalHandler::termination_requested() {
            signal_flag.store(true, Ordering::Relaxed);
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    });

    let demo = options.demo.then(|| Demo::start(&cancel));
    if let Some(demo) = &demo {
        // Pin the observer to the demo channel so it attaches deterministically.
        options.config.channels.push(demo.channel.clone());
    }

    let channels_text = if options.config.channels.is_empty() {
        "(auto-discovery)".to_owned()
    } else {
        options.config.channels.join(", ")
    };
    let detail = options.detail;
    let live = options.live;
    let interval = options.interval;
    let demo_channel = demo.as_ref().map(|demo| demo.channel.as_str());
    let prometheus_addr = options.config.prometheus_addr;

    let metrics = Arc::new(Metrics::new());
    // Expose the same registry over HTTP when an address is configured; the
    // serving thread is detached (dropping the handle does not stop it).
    if let Some(addr) = prometheus_addr {
        if let Err(e) = ice_rpc_monitor::prometheus::serve(addr, metrics.clone()) {
            eprintln!("failed to start the Prometheus endpoint: {e}");
        }
    }

    let monitor = match Monitor::new(options.config, metrics.clone()) {
        Ok(monitor) => monitor,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    let recent = monitor.recent_messages();
    let monitor_cancel = cancel.clone();
    let observer = std::thread::spawn(move || {
        if let Err(e) = monitor.run(&monitor_cancel) {
            eprintln!("monitor error: {e}");
        }
    });

    // `top`-like view: only when requested *and* a terminal is attached. Without
    // a TTY the same frames are simply appended, so a redirected run stays
    // readable and pipeable.
    let mut console = LiveConsole::new(live);
    if !console.is_active() {
        print_banner(
            detail,
            interval,
            &channels_text,
            demo_channel,
            prometheus_addr,
        );
    }

    while !cancel.load(Ordering::Relaxed) {
        sleep_interruptible(interval, &cancel);
        if cancel.load(Ordering::Relaxed) {
            break;
        }
        console.draw(&console::frame(&metrics, recent.as_ref()));
    }
    console.leave();

    let _ = observer.join();
    if let Some(demo) = demo {
        demo.stop();
    }

    println!("\n--- final ---");
    print!("{}", console::frame(&metrics, recent.as_ref()));
}
