//! End-to-end validation: a real provider and consumer, observed in read-only
//! mode by [`Monitor`].
//!
//! The tests drive genuine iceoryx2 traffic through the ice-rpc transport and
//! assert that the observer reconstructed it from the zero-copy headers alone:
//! request/response counts, the real response kinds, the exact latency, and — in
//! detail mode — the raw payloads.

#![allow(clippy::unwrap_used)] // tests may panic

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ice_rpc::gen::{service_id_of, ServiceDispatcher};
use ice_rpc::transport::{native_call, observable_to_responses, spawn_native_service};
use ice_rpc::{CancellationToken, Event, Observable};
use ice_rpc_monitor::config::{Config, Mode};
use ice_rpc_monitor::metrics::Metrics;
use ice_rpc_monitor::{ClosureDecoder, Decoders, Monitor};

/// Returns the value of the first exposition line starting with `prefix`.
fn counter_value(metrics: &Metrics, prefix: &str) -> Option<u64> {
    metrics
        .render_prometheus()
        .lines()
        .filter(|line| line.starts_with(prefix))
        .filter_map(|line| line.rsplit(' ').next())
        .filter_map(|value| value.parse::<u64>().ok())
        .next()
}

/// Waits until both the requests and the completions reach `expected`.
fn traffic_settled(metrics: &Metrics, channel: &str, service_id: u32, expected: u64) -> bool {
    let requests = counter_value(
        metrics,
        &format!("ice_rpc_requests_total{{channel=\"{channel}\""),
    );
    let completes = counter_value(
        metrics,
        &format!(
            "ice_rpc_responses_total{{channel=\"{channel}\",service=\"{service_id}\",kind=\"complete\"}}"
        ),
    );
    requests == Some(expected) && completes == Some(expected)
}

/// Starts a provider that streams two values then completes.
fn start_provider(channel: &str) -> (u32, CancellationToken, std::thread::JoinHandle<()>) {
    let service_id = service_id_of(channel);
    let mut dispatcher = ServiceDispatcher::new();
    dispatcher.method("echo", |_payload| {
        let observable = Observable::<i32, String>::from_events([
            Event::Next(1),
            Event::Next(2),
            Event::Complete,
        ]);
        observable_to_responses(observable)
    });
    let stop = CancellationToken::new();
    let server = spawn_native_service(channel, vec![(service_id, dispatcher)], stop.clone());
    std::thread::sleep(Duration::from_millis(300));
    (service_id, stop, server)
}

#[test]
fn the_observer_reconstructs_the_traffic_from_the_headers() {
    let _ = env_logger::builder().is_test(false).try_init();
    ice_rpc::gen::setup_iceoryx2_global_config();

    let channel = format!("MonitorE2E{}", std::process::id());
    let (service_id, stop, server) = start_provider(&channel);

    // ── Observer: attached to the channel only, stats mode ──────────────
    let metrics = Arc::new(Metrics::new());
    let config = Config {
        channels: vec![channel.clone()],
        prometheus_addr: None,
        ..Config::default()
    };
    let monitor = Monitor::new(config, metrics.clone()).expect("monitor builds");
    let cancel = Arc::new(AtomicBool::new(false));
    let monitor_cancel = cancel.clone();
    let observer = std::thread::spawn(move || monitor.run(&monitor_cancel).expect("monitor runs"));

    // Let the observer discover and attach to the channel.
    std::thread::sleep(Duration::from_millis(500));

    // ── Traffic ─────────────────────────────────────────────────────────
    const CALLS: u64 = 10;
    for _ in 0..CALLS {
        let stream = native_call::<i32, String>(&channel, service_id, "echo", b"go")
            .expect("native_call opens the service");
        let values = pollster::block_on(stream.collect()).expect("collect");
        assert_eq!(values, vec![1, 2]);
    }

    // ── Requests *and* completions must reach exactly the number of calls ─
    let mut observed = false;
    for _ in 0..60 {
        if traffic_settled(&metrics, &channel, service_id, CALLS) {
            observed = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    // Let the terminal processing settle before stopping the observer.
    std::thread::sleep(Duration::from_millis(100));

    let before_stop = metrics.render_prometheus();

    cancel.store(true, Ordering::Relaxed);
    let _ = observer.join();
    stop.cancel();
    let _ = server.join();

    assert!(
        observed,
        "the observer never saw {CALLS} requests:\n{before_stop}"
    );

    let text = metrics.render_prometheus();
    // Every call streamed two Next then one Complete: the header carried the
    // real kind, so completion is countable without decoding the payload.
    assert!(
        text.contains(&format!(
            "ice_rpc_responses_total{{channel=\"{channel}\",service=\"{service_id}\",kind=\"complete\"}} {CALLS}"
        )),
        "missing complete count:\n{text}"
    );
    // One latency per call, measured on the first response.
    assert!(
        text.contains(&format!(
            "ice_rpc_latency_seconds_count{{channel=\"{channel}\",service=\"{service_id}\",method=\"echo\"}} {CALLS}"
        )),
        "missing latency count:\n{text}"
    );
    // In-flight must come back to zero once every call completed.
    assert!(
        text.contains(&format!(
            "ice_rpc_inflight{{channel=\"{channel}\",service=\"{service_id}\"}} 0"
        )),
        "in-flight gauge did not return to zero:\n{text}"
    );
    // No sample was dropped by the observer.
    assert!(
        !text.contains(&format!("ice_rpc_sample_gaps_total{{channel=\"{channel}\"")),
        "unexpected sample loss:\n{text}"
    );
}

/// Detail mode must capture the payload, correlated through the same id.
#[test]
fn the_detail_mode_captures_the_payloads() {
    ice_rpc::gen::setup_iceoryx2_global_config();

    let channel = format!("MonitorDetail{}", std::process::id());
    let (service_id, stop, server) = start_provider(&channel);

    let trace_path =
        std::env::temp_dir().join(format!("ice-rpc-trace-{}.ndjson", std::process::id()));
    let _ = std::fs::remove_file(&trace_path);

    // Detail mode decodes the payloads: register a decoder for the channel.
    let mut decoders = Decoders::new();
    decoders.register(
        service_id,
        Arc::new(ClosureDecoder::new(
            |_method, payload| Some(format!("request:{}", payload.len())),
            |_method, payload| Some(format!("response:{}", payload.len())),
        )),
    );

    let metrics = Arc::new(Metrics::new());
    let config = Config {
        channels: vec![channel.clone()],
        prometheus_addr: None,
        mode: Mode::Detail,
        trace_sample_rate: 1,
        trace_file: Some(trace_path.clone()),
        decoders: Arc::new(decoders),
        ..Config::default()
    };
    let monitor = Monitor::new(config, metrics.clone()).expect("monitor builds");
    let cancel = Arc::new(AtomicBool::new(false));
    let monitor_cancel = cancel.clone();
    let observer = std::thread::spawn(move || monitor.run(&monitor_cancel).expect("monitor runs"));

    std::thread::sleep(Duration::from_millis(500));

    let stream = native_call::<i32, String>(&channel, service_id, "echo", b"go")
        .expect("native_call opens the service");
    let _ = pollster::block_on(stream.collect()).expect("collect");

    // Wait until the completed call has been traced.
    let mut recorded = String::new();
    for _ in 0..40 {
        if let Ok(content) = std::fs::read_to_string(&trace_path) {
            if content.contains("\"trace_id\"") {
                recorded = content;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    cancel.store(true, Ordering::Relaxed);
    let _ = observer.join();
    stop.cancel();
    let _ = server.join();

    // The file may have been flushed further while stopping: re-read it.
    if let Ok(content) = std::fs::read_to_string(&trace_path) {
        recorded = content;
    }
    let _ = std::fs::remove_file(&trace_path);

    assert!(
        recorded.contains("\"trace_id\""),
        "no trace record was written to {trace_path:?}"
    );
    assert!(recorded.contains("\"method\":\"echo\""));
    assert!(recorded.contains("\"event_kind\":\"complete\""));
    // `b"go"` is two bytes: detail mode decoded the request through the registry.
    assert!(
        recorded.contains("\"request\":\"request:2\""),
        "detail mode did not decode the request:\n{recorded}"
    );
    assert!(
        recorded.contains("\"response\":\"response:"),
        "detail mode did not decode the response:\n{recorded}"
    );
}

/// Stats mode must never read the payload: no payload field is emitted.
#[test]
fn the_stats_mode_never_emits_payloads() {
    ice_rpc::gen::setup_iceoryx2_global_config();

    let channel = format!("MonitorStats{}", std::process::id());
    let (service_id, stop, server) = start_provider(&channel);

    let trace_path =
        std::env::temp_dir().join(format!("ice-rpc-stats-{}.ndjson", std::process::id()));
    let _ = std::fs::remove_file(&trace_path);

    let metrics = Arc::new(Metrics::new());
    let config = Config {
        channels: vec![channel.clone()],
        prometheus_addr: None,
        mode: Mode::Stats,
        trace_sample_rate: 1,
        trace_file: Some(trace_path.clone()),
        ..Config::default()
    };
    let monitor = Monitor::new(config, metrics.clone()).expect("monitor builds");
    let cancel = Arc::new(AtomicBool::new(false));
    let monitor_cancel = cancel.clone();
    let observer = std::thread::spawn(move || monitor.run(&monitor_cancel).expect("monitor runs"));

    std::thread::sleep(Duration::from_millis(500));

    let stream = native_call::<i32, String>(&channel, service_id, "echo", b"go")
        .expect("native_call opens the service");
    let _ = pollster::block_on(stream.collect()).expect("collect");

    let mut recorded = String::new();
    for _ in 0..40 {
        if let Ok(content) = std::fs::read_to_string(&trace_path) {
            if content.contains("\"trace_id\"") {
                recorded = content;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    cancel.store(true, Ordering::Relaxed);
    let _ = observer.join();
    stop.cancel();
    let _ = server.join();

    if let Ok(content) = std::fs::read_to_string(&trace_path) {
        recorded = content;
    }
    let _ = std::fs::remove_file(&trace_path);

    assert!(recorded.contains("\"trace_id\""), "no trace record written");
    assert!(
        !recorded.contains("\"request\"") && !recorded.contains("\"response\""),
        "stats mode must not read the payload:\n{recorded}"
    );
}

/// An observer started **after** the provider and a first burst of traffic must
/// still catch the calls that follow its attachment — and only those.
///
/// This is the normal deployment: the provider/consumer are already running, the
/// observer is attached later. The services already exist, so it attaches at the
/// first attempt; `iceoryx2` subscribers never receive past samples, so the
/// earlier calls are simply out of scope.
#[test]
fn a_late_observer_catches_the_following_traffic() {
    ice_rpc::gen::setup_iceoryx2_global_config();

    let channel = format!("MonitorLate{}", std::process::id());
    let (service_id, stop, server) = start_provider(&channel);

    // Traffic emitted *before* the observer exists.
    for _ in 0..3 {
        let stream = native_call::<i32, String>(&channel, service_id, "echo", b"early")
            .expect("native_call opens the service");
        let _ = pollster::block_on(stream.collect()).expect("collect");
    }

    // The observer attaches only now; the services already exist.
    let metrics = Arc::new(Metrics::new());
    let config = Config {
        channels: vec![channel.clone()],
        prometheus_addr: None,
        ..Config::default()
    };
    let monitor = Monitor::new(config, metrics.clone()).expect("monitor builds");
    let cancel = Arc::new(AtomicBool::new(false));
    let monitor_cancel = cancel.clone();
    let observer = std::thread::spawn(move || monitor.run(&monitor_cancel).expect("monitor runs"));

    // Let it discover and attach to the already-running channel.
    std::thread::sleep(Duration::from_millis(500));

    // Traffic emitted *after* the attachment.
    const AFTER: u64 = 5;
    for _ in 0..AFTER {
        let stream = native_call::<i32, String>(&channel, service_id, "echo", b"late")
            .expect("native_call opens the service");
        let _ = pollster::block_on(stream.collect()).expect("collect");
    }

    let mut observed = false;
    for _ in 0..60 {
        if traffic_settled(&metrics, &channel, service_id, AFTER) {
            observed = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    cancel.store(true, Ordering::Relaxed);
    let _ = observer.join();
    stop.cancel();
    let _ = server.join();

    assert!(
        observed,
        "the late observer never saw the following {AFTER} calls"
    );
    // Exactly the post-attachment calls: the pre-attachment burst is out of scope.
    let text = metrics.render_prometheus();
    assert!(
        text.contains(&format!(
            "ice_rpc_requests_total{{channel=\"{channel}\",service=\"{service_id}\",method=\"echo\"}} {AFTER}"
        )),
        "unexpected request count (pre-attachment traffic observed?):\n{text}"
    );
}

/// The opposite order: the observer starts **before** the provider. It must
/// retry until the service appears, then observe the traffic that follows.
#[test]
fn an_observer_started_before_the_provider_attaches_once_it_appears() {
    ice_rpc::gen::setup_iceoryx2_global_config();

    let channel = format!("MonitorEarly{}", std::process::id());

    // The observer starts first: nothing to attach to yet.
    let metrics = Arc::new(Metrics::new());
    let config = Config {
        channels: vec![channel.clone()],
        prometheus_addr: None,
        // Speed the discovery retry up so the test stays quick.
        discover_interval: Duration::from_millis(200),
        ..Config::default()
    };
    let monitor = Monitor::new(config, metrics.clone()).expect("monitor builds");
    let cancel = Arc::new(AtomicBool::new(false));
    let monitor_cancel = cancel.clone();
    let observer = std::thread::spawn(move || monitor.run(&monitor_cancel).expect("monitor runs"));

    // The provider appears afterwards; the next discovery tick attaches.
    let (service_id, stop, server) = start_provider(&channel);
    std::thread::sleep(Duration::from_millis(800));

    const CALLS: u64 = 4;
    for _ in 0..CALLS {
        let stream = native_call::<i32, String>(&channel, service_id, "echo", b"go")
            .expect("native_call opens the service");
        let _ = pollster::block_on(stream.collect()).expect("collect");
    }

    let mut observed = false;
    for _ in 0..60 {
        if traffic_settled(&metrics, &channel, service_id, CALLS) {
            observed = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    cancel.store(true, Ordering::Relaxed);
    let _ = observer.join();
    stop.cancel();
    let _ = server.join();

    assert!(
        observed,
        "the observer never attached after the provider appeared"
    );
}

/// The observer must publish the generic network inventory: nodes, services and
/// the per-channel capacity, without ever reporting a live node as dead.
#[test]
fn the_observer_publishes_the_network_inventory() {
    ice_rpc::gen::setup_iceoryx2_global_config();

    let channel = format!("MonitorHealth{}", std::process::id());
    let (_service_id, stop, server) = start_provider(&channel);

    let metrics = Arc::new(Metrics::new());
    let config = Config {
        channels: vec![channel.clone()],
        prometheus_addr: None,
        health_interval: Duration::from_millis(100),
        ..Config::default()
    };
    let monitor = Monitor::new(config, metrics.clone()).expect("monitor builds");
    let cancel = Arc::new(AtomicBool::new(false));
    let monitor_cancel = cancel.clone();
    let observer = std::thread::spawn(move || monitor.run(&monitor_cancel).expect("monitor runs"));

    let pid = std::process::id();
    let mut text = String::new();
    let mut found = false;
    for _ in 0..60 {
        text = metrics.render_prometheus();
        if text.contains(&format!("ice_rpc_services{{service=\"{channel}_req\""))
            && text.contains("ice_rpc_health_scans_total ")
            && text.contains(&format!(
                "ice_rpc_channel{{channel=\"{channel}\",direction=\"req\"}} 1"
            ))
        {
            found = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    cancel.store(true, Ordering::Relaxed);
    let _ = observer.join();
    stop.cancel();
    let _ = server.join();

    assert!(found, "the inventory was never published:\n{text}");
    // Our own process hosts the provider: its node is alive, never dead.
    assert!(
        text.contains(&format!("ice_rpc_node_info{{pid=\"{pid}\",state=\"alive\"")),
        "our own node was not reported alive:\n{text}"
    );
    assert_eq!(
        metrics
            .render_prometheus()
            .lines()
            .find(|line| line.starts_with("ice_rpc_health_errors_total "))
            .and_then(|line| line.rsplit(' ').next()),
        Some("0"),
        "the inventory scan failed:\n{text}"
    );
}
