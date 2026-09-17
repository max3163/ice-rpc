//! Aggregated metrics produced by the observer.
//!
//! All the series are keyed by a **statically bounded** set (`channel`, numeric
//! `service_id`, `method` come from the compiled service definitions), so the
//! cardinality cannot grow with the traffic.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Mutex;

use ice_rpc::monitor::Direction;

use crate::health::{ChannelHealth, HealthSnapshot};

/// Latency histogram bounds, in seconds.
const LATENCY_BOUNDS: &[f64] = &[
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
    5.0, 10.0,
];

/// Payload-size histogram bounds, in bytes.
const PAYLOAD_BOUNDS: &[f64] = &[
    16.0, 64.0, 256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0,
];

/// A cumulative fixed-bucket histogram.
#[derive(Debug, Clone)]
struct Histogram {
    /// Cumulative counts, one per bound: `buckets[i]` counts values `<= bounds[i]`.
    buckets: Vec<u64>,
    sum: f64,
    count: u64,
}

impl Histogram {
    fn empty(bounds: &[f64]) -> Self {
        Self {
            buckets: vec![0; bounds.len()],
            sum: 0.0,
            count: 0,
        }
    }

    fn observe(&mut self, value: f64, bounds: &[f64]) {
        for (slot, bound) in self.buckets.iter_mut().zip(bounds) {
            if value <= *bound {
                *slot += 1;
            }
        }
        self.sum += value;
        self.count += 1;
    }
}

/// Maps a direction to its Prometheus label.
fn direction_label(direction: Direction) -> &'static str {
    match direction {
        Direction::Request => "req",
        Direction::Response => "resp",
    }
}

/// Label of the event kind of a response sample.
fn kind_label(kind: &'static str) -> &'static str {
    kind
}

/// Health of one observed channel direction.
#[derive(Debug, Default, Clone, Copy)]
struct ChannelSample {
    attached: i64,
    publishers: i64,
    subscribers: i64,
    max_publishers: i64,
    max_subscribers: i64,
    subscriber_buffer: i64,
}

/// Resources of one observed process.
#[derive(Debug, Clone)]
struct ProcessSample {
    name: String,
    cpu_percent: f64,
    cpu_percent_total: f64,
    rss_bytes: i64,
    virtual_bytes: i64,
    run_time_secs: i64,
}

#[derive(Default)]
struct Inner {
    requests: BTreeMap<(String, u32, String), u64>,
    responses: BTreeMap<(String, u32, &'static str), u64>,
    latency: BTreeMap<(String, u32, String), Histogram>,
    payload: BTreeMap<(String, &'static str), Histogram>,
    inflight: BTreeMap<(String, u32), i64>,
    unmatched: BTreeMap<String, u64>,
    orphan: BTreeMap<String, u64>,
    /// Samples the observer itself missed (see [`LossTracker`]).
    ///
    /// [`LossTracker`]: crate::loss::LossTracker
    sample_gaps: u64,
    clock_skew: u64,
    nodes_alive: i64,
    node_crashes: u64,
    // ── Health inventory, replaced wholesale on every scan ──
    node_states: BTreeMap<&'static str, i64>,
    node_info: BTreeMap<(u32, &'static str, String), i64>,
    services: BTreeMap<(String, &'static str, &'static str), i64>,
    service_participants: BTreeMap<String, i64>,
    channels: BTreeMap<(String, &'static str), ChannelSample>,
    processes: BTreeMap<u32, ProcessSample>,
    shm_enabled: bool,
    shm_bytes: i64,
    shm_segments: i64,
    shm_files: i64,
    health_scans: u64,
    health_errors: u64,
    observer_dropped: u64,
    discovery_errors: u64,
    host_cpu_count: i64,
}

/// Thread-safe metric registry.
#[derive(Default)]
pub struct Metrics {
    inner: Mutex<Inner>,
}

impl Metrics {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records an observed request.
    pub fn on_request(&self, channel: &str, service_id: u32, method: &str) {
        if let Ok(mut inner) = self.inner.lock() {
            *inner
                .requests
                .entry((channel.to_owned(), service_id, method.to_owned()))
                .or_default() += 1;
        }
    }

    /// Records an observed response of the given kind.
    pub fn on_response(&self, channel: &str, service_id: u32, kind: &'static str) {
        if let Ok(mut inner) = self.inner.lock() {
            *inner
                .responses
                .entry((channel.to_owned(), service_id, kind_label(kind)))
                .or_default() += 1;
        }
    }

    /// Records the payload length of one sample.
    pub fn on_payload(&self, channel: &str, direction: Direction, len: usize) {
        if let Ok(mut inner) = self.inner.lock() {
            inner
                .payload
                .entry((channel.to_owned(), direction_label(direction)))
                .or_insert_with(|| Histogram::empty(PAYLOAD_BOUNDS))
                .observe(len as f64, PAYLOAD_BOUNDS);
        }
    }

    /// Records the exact latency of one call, in seconds.
    pub fn on_latency(&self, channel: &str, service_id: u32, method: &str, seconds: f64) {
        if let Ok(mut inner) = self.inner.lock() {
            inner
                .latency
                .entry((channel.to_owned(), service_id, method.to_owned()))
                .or_insert_with(|| Histogram::empty(LATENCY_BOUNDS))
                .observe(seconds, LATENCY_BOUNDS);
        }
    }

    /// Adjusts the in-flight gauge of one service.
    pub fn on_inflight(&self, channel: &str, service_id: u32, delta: i64) {
        if let Ok(mut inner) = self.inner.lock() {
            *inner
                .inflight
                .entry((channel.to_owned(), service_id))
                .or_default() += delta;
        }
    }

    /// Counts a request that was never answered before its TTL expired.
    pub fn on_unmatched_request(&self, channel: &str) {
        if let Ok(mut inner) = self.inner.lock() {
            *inner.unmatched.entry(channel.to_owned()).or_default() += 1;
        }
    }

    /// Counts a response whose request was not tracked.
    pub fn on_orphan_response(&self, channel: &str) {
        if let Ok(mut inner) = self.inner.lock() {
            *inner.orphan.entry(channel.to_owned()).or_default() += 1;
        }
    }

    /// Adds samples the observer **itself** missed, detected through a `seq` hole.
    ///
    /// One counter, deliberately not one per channel: the observer is the only
    /// subscriber the publisher may skip, so what it measures is the completeness
    /// of its own view — the first thing to check before trusting a count or a
    /// latency histogram (see [`LossTracker`](crate::loss::LossTracker)).
    pub fn on_sample_gap(&self, missed: u64) {
        if missed == 0 {
            return;
        }
        if let Ok(mut inner) = self.inner.lock() {
            inner.sample_gaps += missed;
        }
    }

    /// Counts a response whose timestamp precedes its request (clock adjustment).
    pub fn on_clock_skew(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.clock_skew += 1;
        }
    }

    /// Sets the number of emitting processes currently alive.
    pub fn set_nodes_alive(&self, alive: i64) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.nodes_alive = alive;
        }
    }

    /// Counts one emitting process that disappeared.
    pub fn add_node_crash(&self, count: u64) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.node_crashes += count;
        }
    }

    /// Replaces the whole health inventory with a fresh scan.
    ///
    /// The inventory is *replaced*, never accumulated, so a service or a node
    /// that disappears stops being exported on the very next scan.
    pub fn set_health(&self, snapshot: &HealthSnapshot, channels: &[ChannelHealth]) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };

        inner.node_states.clear();
        inner.node_info.clear();
        for node in &snapshot.nodes {
            let state = node.health.label();
            *inner.node_states.entry(state).or_default() += 1;
            let executable = node.executable.clone().unwrap_or_else(|| "?".to_owned());
            inner.node_info.insert((node.pid, state, executable), 1);
        }

        inner.services.clear();
        inner.service_participants.clear();
        for service in &snapshot.services {
            inner.services.insert(
                (service.name.clone(), service.pattern, service.role.label()),
                1,
            );
            inner
                .service_participants
                .insert(service.name.clone(), service.participants as i64);
        }

        inner.channels.clear();
        for channel in channels {
            inner.channels.insert(
                (channel.channel.clone(), channel.direction),
                ChannelSample {
                    attached: i64::from(channel.attached),
                    publishers: channel.publishers as i64,
                    subscribers: channel.subscribers as i64,
                    max_publishers: channel.max_publishers as i64,
                    max_subscribers: channel.max_subscribers as i64,
                    subscriber_buffer: channel.subscriber_buffer as i64,
                },
            );
        }

        inner.processes.clear();
        for process in &snapshot.processes {
            inner.processes.insert(
                process.pid,
                ProcessSample {
                    name: process.name.clone(),
                    cpu_percent: process.cpu_percent as f64,
                    cpu_percent_total: process.cpu_percent_total as f64,
                    rss_bytes: process.rss_bytes as i64,
                    virtual_bytes: process.virtual_bytes as i64,
                    run_time_secs: process.run_time_secs as i64,
                },
            );
        }

        inner.shm_enabled = snapshot.shm.is_some();
        inner.shm_bytes = snapshot.shm.map(|shm| shm.bytes as i64).unwrap_or(0);
        inner.shm_segments = snapshot.shm.map(|shm| shm.segments as i64).unwrap_or(0);
        inner.shm_files = snapshot.shm.map(|shm| shm.files as i64).unwrap_or(0);
        inner.health_scans = snapshot.scans;
        inner.health_errors = snapshot.errors;
        inner.host_cpu_count = snapshot.cpu_count as i64;
    }

    /// Publishes the observer's own health counters.
    pub fn set_observer(&self, dropped_traces: u64, discovery_errors: u64) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.observer_dropped = dropped_traces;
            inner.discovery_errors = discovery_errors;
        }
    }

    /// Renders a compact, human-readable summary of the registry.
    ///
    /// Meant for a console observer: global counters, exact-latency quantiles
    /// read from the fixed-bucket histograms (so they are **upper bounds**),
    /// payload averages, the in-flight gauge and the loss/orphan counters,
    /// followed by a per-service breakdown.
    pub fn render_console(&self) -> String {
        let Ok(inner) = self.inner.lock() else {
            return String::new();
        };
        let rule = "-".repeat(64);
        let mut out = String::with_capacity(2048);
        let _ = writeln!(out, "===== ice-rpc-monitor (console stats) =====");

        let requests: u64 = inner.requests.values().sum();
        let responses: u64 = inner.responses.values().sum();

        let mut kinds: BTreeMap<&'static str, u64> = BTreeMap::new();
        for ((_, _, kind), value) in &inner.responses {
            *kinds.entry(*kind).or_default() += value;
        }
        let kind_text = if kinds.is_empty() {
            "none".to_owned()
        } else {
            kinds
                .iter()
                .map(|(kind, value)| format!("{kind}={value}"))
                .collect::<Vec<_>>()
                .join(" ")
        };

        let latency = merge(inner.latency.values(), LATENCY_BOUNDS);
        let req_payload = merge_direction(&inner.payload, Direction::Request, PAYLOAD_BOUNDS);
        let resp_payload = merge_direction(&inner.payload, Direction::Response, PAYLOAD_BOUNDS);

        let _ = writeln!(out, " requests        : {requests}");
        let _ = writeln!(out, " responses       : {responses}  [{kind_text}]");
        let _ = writeln!(
            out,
            " latency (exact) : {}",
            describe_latency(&latency, LATENCY_BOUNDS)
        );
        let _ = writeln!(
            out,
            " payload         : req avg={}  resp avg={}",
            byte_avg(&req_payload),
            byte_avg(&resp_payload)
        );
        let in_flight: i64 = inner.inflight.values().copied().sum();
        let gaps = inner.sample_gaps;
        let unmatched: u64 = inner.unmatched.values().copied().sum();
        let orphan: u64 = inner.orphan.values().copied().sum();
        let _ = writeln!(out, " in-flight       : {in_flight}");
        let _ = writeln!(out, " observer gaps   : {gaps}");
        let _ = writeln!(out, " unmatched req.  : {unmatched}");
        let _ = writeln!(out, " orphan responses: {orphan}");
        let _ = writeln!(out, " clock skew      : {}", inner.clock_skew);
        let _ = writeln!(
            out,
            " nodes           : alive={} crashes={}",
            inner.nodes_alive, inner.node_crashes
        );

        // Per-service breakdown; bounded by the static set of compiled services.
        // Kept short: the console frame must stay on one screen (see `console`).
        if !inner.requests.is_empty() {
            let _ = writeln!(out, "{rule}");
            const MAX_LINES: usize = 8;
            for (index, ((channel, service, method), count)) in inner.requests.iter().enumerate() {
                if index >= MAX_LINES {
                    let _ = writeln!(out, "   ... and {} more", inner.requests.len() - index);
                    break;
                }
                let series = inner
                    .latency
                    .get(&(channel.clone(), *service, method.clone()));
                let latency_text = match series {
                    Some(histogram) => describe_latency(histogram, LATENCY_BOUNDS),
                    None => "no response yet".to_owned(),
                };
                let _ = writeln!(
                    out,
                    " {channel} service={service} method={method}: requests={count} {latency_text}"
                );
            }
        }

        // Network and resource health; empty until the first inventory scan.
        if !inner.node_states.is_empty() || !inner.services.is_empty() {
            let _ = writeln!(out, "{rule}");
            let alive = inner.node_states.get("alive").copied().unwrap_or(0);
            let dead = inner.node_states.get("dead").copied().unwrap_or(0);
            let total: i64 = inner.node_states.values().copied().sum();
            let _ = writeln!(
                out,
                " network : nodes={total} (alive {alive}, dead {dead})  services={}  channels={}",
                inner.services.len(),
                inner.channels.len()
            );
            if inner.shm_enabled {
                if inner.shm_segments == 0 {
                    // Metadata files exist, but no `.data` segment: the segments
                    // are not file-backed on this platform (e.g. Windows).
                    let _ = writeln!(out, " shm     : n/a (segments are not file-backed here)");
                } else {
                    let _ = writeln!(
                        out,
                        " shm     : {} segment(s), {}",
                        inner.shm_segments,
                        fmt_bytes(inner.shm_bytes as f64)
                    );
                }
            }
            // One line per emitting process (iceoryx2 node), bounded so a busy
            // machine cannot push the frame past the terminal height.
            const MAX_NODES: usize = 5;
            for (index, (pid, sample)) in inner.processes.iter().enumerate() {
                if index >= MAX_NODES {
                    let _ = writeln!(
                        out,
                        " node    : ... and {} more",
                        inner.processes.len() - index
                    );
                    break;
                }
                let _ = writeln!(
                    out,
                    " node    : pid={pid} {}  cpu {:.1}% core / {:.1}% host  rss {}  up {}s",
                    sample.name,
                    sample.cpu_percent,
                    sample.cpu_percent_total,
                    fmt_bytes(sample.rss_bytes as f64),
                    sample.run_time_secs
                );
            }
        }
        let _ = writeln!(out, "===========================================");
        out
    }

    /// Renders the registry in the Prometheus text exposition format.
    pub fn render_prometheus(&self) -> String {
        let Ok(inner) = self.inner.lock() else {
            return String::new();
        };
        let mut out = String::with_capacity(4096);

        out.push_str("# HELP ice_rpc_requests_total Requests observed on the bus\n");
        out.push_str("# TYPE ice_rpc_requests_total counter\n");
        for ((channel, service, method), value) in &inner.requests {
            let _ = writeln!(
                out,
                "ice_rpc_requests_total{{channel=\"{channel}\",service=\"{service}\",method=\"{method}\"}} {value}"
            );
        }

        out.push_str("# HELP ice_rpc_responses_total Response samples observed on the bus\n");
        out.push_str("# TYPE ice_rpc_responses_total counter\n");
        for ((channel, service, kind), value) in &inner.responses {
            let _ = writeln!(
                out,
                "ice_rpc_responses_total{{channel=\"{channel}\",service=\"{service}\",kind=\"{kind}\"}} {value}"
            );
        }

        out.push_str("# HELP ice_rpc_latency_seconds Exact request-to-first-response latency\n");
        out.push_str("# TYPE ice_rpc_latency_seconds histogram\n");
        for ((channel, service, method), histogram) in &inner.latency {
            let labels = format!("channel=\"{channel}\",service=\"{service}\",method=\"{method}\"");
            render_histogram(
                &mut out,
                "ice_rpc_latency_seconds",
                &labels,
                histogram,
                LATENCY_BOUNDS,
            );
        }

        out.push_str("# HELP ice_rpc_payload_bytes Payload length of the observed samples\n");
        out.push_str("# TYPE ice_rpc_payload_bytes histogram\n");
        for ((channel, direction), histogram) in &inner.payload {
            let labels = format!("channel=\"{channel}\",direction=\"{direction}\"");
            render_histogram(
                &mut out,
                "ice_rpc_payload_bytes",
                &labels,
                histogram,
                PAYLOAD_BOUNDS,
            );
        }

        out.push_str("# HELP ice_rpc_inflight Calls currently awaiting their response\n");
        out.push_str("# TYPE ice_rpc_inflight gauge\n");
        for ((channel, service), value) in &inner.inflight {
            let _ = writeln!(
                out,
                "ice_rpc_inflight{{channel=\"{channel}\",service=\"{service}\"}} {value}"
            );
        }

        out.push_str(
            "# HELP ice_rpc_unmatched_requests_total Requests never answered before their TTL\n",
        );
        out.push_str("# TYPE ice_rpc_unmatched_requests_total counter\n");
        for (channel, value) in &inner.unmatched {
            let _ = writeln!(
                out,
                "ice_rpc_unmatched_requests_total{{channel=\"{channel}\"}} {value}"
            );
        }

        out.push_str("# HELP ice_rpc_orphan_responses_total Responses without a tracked request\n");
        out.push_str("# TYPE ice_rpc_orphan_responses_total counter\n");
        for (channel, value) in &inner.orphan {
            let _ = writeln!(
                out,
                "ice_rpc_orphan_responses_total{{channel=\"{channel}\"}} {value}"
            );
        }

        out.push_str("# HELP ice_rpc_clock_skew_total Negative latencies (clock adjustment)\n");
        out.push_str("# TYPE ice_rpc_clock_skew_total counter\n");
        let _ = writeln!(out, "ice_rpc_clock_skew_total {}", inner.clock_skew);

        out.push_str("# HELP ice_rpc_nodes_alive Emitting processes currently alive\n");
        out.push_str("# TYPE ice_rpc_nodes_alive gauge\n");
        let _ = writeln!(out, "ice_rpc_nodes_alive {}", inner.nodes_alive);

        out.push_str("# HELP ice_rpc_node_crashes_total Emitting processes that disappeared\n");
        out.push_str("# TYPE ice_rpc_node_crashes_total counter\n");
        let _ = writeln!(out, "ice_rpc_node_crashes_total {}", inner.node_crashes);

        out.push_str("# HELP ice_rpc_nodes Nodes by native liveness state\n");
        out.push_str("# TYPE ice_rpc_nodes gauge\n");
        for (state, value) in &inner.node_states {
            let _ = writeln!(out, "ice_rpc_nodes{{state=\"{state}\"}} {value}");
        }

        out.push_str("# HELP ice_rpc_node_info One series per node, value is always 1\n");
        out.push_str("# TYPE ice_rpc_node_info gauge\n");
        for ((pid, state, executable), value) in &inner.node_info {
            let _ = writeln!(
                out,
                "ice_rpc_node_info{{pid=\"{pid}\",state=\"{state}\",executable=\"{executable}\"}} {value}"
            );
        }

        out.push_str("# HELP ice_rpc_services One series per iceoryx2 service\n");
        out.push_str("# TYPE ice_rpc_services gauge\n");
        for ((service, pattern, role), value) in &inner.services {
            let _ = writeln!(
                out,
                "ice_rpc_services{{service=\"{service}\",pattern=\"{pattern}\",role=\"{role}\"}} {value}"
            );
        }

        out.push_str("# HELP ice_rpc_service_participants Nodes registered on a service\n");
        out.push_str("# TYPE ice_rpc_service_participants gauge\n");
        for (service, value) in &inner.service_participants {
            let _ = writeln!(
                out,
                "ice_rpc_service_participants{{service=\"{service}\"}} {value}"
            );
        }

        out.push_str("# HELP ice_rpc_channel Whether the observer is attached to a direction\n");
        out.push_str("# TYPE ice_rpc_channel gauge\n");
        for ((channel, direction), sample) in &inner.channels {
            let _ = writeln!(
                out,
                "ice_rpc_channel{{channel=\"{channel}\",direction=\"{direction}\"}} {}",
                sample.attached
            );
        }

        out.push_str(
            "# HELP ice_rpc_channel_publishers Active publishers of a channel direction\n",
        );
        out.push_str("# TYPE ice_rpc_channel_publishers gauge\n");
        for ((channel, direction), sample) in &inner.channels {
            let _ = writeln!(
                out,
                "ice_rpc_channel_publishers{{channel=\"{channel}\",direction=\"{direction}\"}} {}",
                sample.publishers
            );
        }

        out.push_str("# HELP ice_rpc_channel_subscribers Active subscribers of a channel direction, observer excluded\n");
        out.push_str("# TYPE ice_rpc_channel_subscribers gauge\n");
        for ((channel, direction), sample) in &inner.channels {
            let _ = writeln!(
                out,
                "ice_rpc_channel_subscribers{{channel=\"{channel}\",direction=\"{direction}\"}} {}",
                sample.subscribers
            );
        }

        out.push_str("# HELP ice_rpc_channel_capacity Declared capacity of a channel direction\n");
        out.push_str("# TYPE ice_rpc_channel_capacity gauge\n");
        for ((channel, direction), sample) in &inner.channels {
            let labels = format!("channel=\"{channel}\",direction=\"{direction}\"");
            let _ = writeln!(
                out,
                "ice_rpc_channel_capacity{{{labels},kind=\"max_publishers\"}} {}",
                sample.max_publishers
            );
            let _ = writeln!(
                out,
                "ice_rpc_channel_capacity{{{labels},kind=\"max_subscribers\"}} {}",
                sample.max_subscribers
            );
            let _ = writeln!(
                out,
                "ice_rpc_channel_capacity{{{labels},kind=\"subscriber_buffer_samples\"}} {}",
                sample.subscriber_buffer
            );
        }

        if !inner.processes.is_empty() {
            out.push_str("# HELP ice_rpc_process_cpu_percent CPU usage of an observed process, as a percentage of ONE core (can exceed 100)\n");
            out.push_str("# TYPE ice_rpc_process_cpu_percent gauge\n");
            for (pid, sample) in &inner.processes {
                let _ = writeln!(
                    out,
                    "ice_rpc_process_cpu_percent{{pid=\"{pid}\",name=\"{}\"}} {}",
                    sample.name, sample.cpu_percent
                );
            }

            out.push_str("# HELP ice_rpc_process_cpu_percent_total CPU usage of an observed process, normalised to 0..100 over all cores\n");
            out.push_str("# TYPE ice_rpc_process_cpu_percent_total gauge\n");
            for (pid, sample) in &inner.processes {
                let _ = writeln!(
                    out,
                    "ice_rpc_process_cpu_percent_total{{pid=\"{pid}\",name=\"{}\"}} {}",
                    sample.name, sample.cpu_percent_total
                );
            }

            out.push_str(
                "# HELP ice_rpc_process_rss_bytes Resident memory of an observed process\n",
            );
            out.push_str("# TYPE ice_rpc_process_rss_bytes gauge\n");
            for (pid, sample) in &inner.processes {
                let _ = writeln!(
                    out,
                    "ice_rpc_process_rss_bytes{{pid=\"{pid}\",name=\"{}\"}} {}",
                    sample.name, sample.rss_bytes
                );
            }

            out.push_str(
                "# HELP ice_rpc_process_virtual_bytes Virtual memory of an observed process\n",
            );
            out.push_str("# TYPE ice_rpc_process_virtual_bytes gauge\n");
            for (pid, sample) in &inner.processes {
                let _ = writeln!(
                    out,
                    "ice_rpc_process_virtual_bytes{{pid=\"{pid}\",name=\"{}\"}} {}",
                    sample.name, sample.virtual_bytes
                );
            }

            out.push_str("# HELP ice_rpc_process_uptime_seconds Uptime of an observed process\n");
            out.push_str("# TYPE ice_rpc_process_uptime_seconds gauge\n");
            for (pid, sample) in &inner.processes {
                let _ = writeln!(
                    out,
                    "ice_rpc_process_uptime_seconds{{pid=\"{pid}\",name=\"{}\"}} {}",
                    sample.name, sample.run_time_secs
                );
            }
        }

        out.push_str(
            "# HELP ice_rpc_shm_scan_enabled Whether the shared-memory footprint scan is enabled\n",
        );
        out.push_str("# TYPE ice_rpc_shm_scan_enabled gauge\n");
        let _ = writeln!(
            out,
            "ice_rpc_shm_scan_enabled {}",
            i64::from(inner.shm_enabled)
        );

        out.push_str(
            "# HELP ice_rpc_shm_bytes Measured size of the iceoryx2 shared-memory segments\n",
        );
        out.push_str("# TYPE ice_rpc_shm_bytes gauge\n");
        let _ = writeln!(out, "ice_rpc_shm_bytes {}", inner.shm_bytes);

        out.push_str("# HELP ice_rpc_shm_segments Measured number of iceoryx2 segment files\n");
        out.push_str("# TYPE ice_rpc_shm_segments gauge\n");
        let _ = writeln!(out, "ice_rpc_shm_segments {}", inner.shm_segments);

        out.push_str("# HELP ice_rpc_shm_files iceoryx2 files seen by the footprint walk\n");
        out.push_str("# TYPE ice_rpc_shm_files gauge\n");
        let _ = writeln!(out, "ice_rpc_shm_files {}", inner.shm_files);

        out.push_str("# HELP ice_rpc_host_cpu_count Number of logical CPUs of the host\n");
        out.push_str("# TYPE ice_rpc_host_cpu_count gauge\n");
        let _ = writeln!(out, "ice_rpc_host_cpu_count {}", inner.host_cpu_count);

        out.push_str("# HELP ice_rpc_health_scans_total Inventory scans performed\n");
        out.push_str("# TYPE ice_rpc_health_scans_total counter\n");
        let _ = writeln!(out, "ice_rpc_health_scans_total {}", inner.health_scans);

        out.push_str("# HELP ice_rpc_health_errors_total Failed partial inventory scans\n");
        out.push_str("# TYPE ice_rpc_health_errors_total counter\n");
        let _ = writeln!(out, "ice_rpc_health_errors_total {}", inner.health_errors);

        out.push_str(
            "# HELP ice_rpc_observer_dropped_traces_total Trace records dropped by a saturated sink\n",
        );
        out.push_str("# TYPE ice_rpc_observer_dropped_traces_total counter\n");
        let _ = writeln!(
            out,
            "ice_rpc_observer_dropped_traces_total {}",
            inner.observer_dropped
        );

        out.push_str(
            "# HELP ice_rpc_observer_gaps_total Samples the observer itself missed, from seq holes\n",
        );
        out.push_str("# TYPE ice_rpc_observer_gaps_total counter\n");
        let _ = writeln!(out, "ice_rpc_observer_gaps_total {}", inner.sample_gaps);

        out.push_str("# HELP ice_rpc_discovery_errors_total Failed channel discovery attempts\n");
        out.push_str("# TYPE ice_rpc_discovery_errors_total counter\n");
        let _ = writeln!(
            out,
            "ice_rpc_discovery_errors_total {}",
            inner.discovery_errors
        );

        out
    }
}

/// Sums several histograms into one, sharing the same bounds.
fn merge<'a>(histograms: impl Iterator<Item = &'a Histogram>, bounds: &[f64]) -> Histogram {
    let mut merged = Histogram::empty(bounds);
    for histogram in histograms {
        for (slot, value) in merged.buckets.iter_mut().zip(&histogram.buckets) {
            *slot += value;
        }
        merged.sum += histogram.sum;
        merged.count += histogram.count;
    }
    merged
}

/// Merges the payload histograms of one direction across every channel.
fn merge_direction(
    payload: &BTreeMap<(String, &'static str), Histogram>,
    direction: Direction,
    bounds: &[f64],
) -> Histogram {
    merge(
        payload
            .iter()
            .filter(|((_, label), _)| *label == direction_label(direction))
            .map(|(_, histogram)| histogram),
        bounds,
    )
}

/// Upper bound of the `q` quantile, read from a cumulative histogram.
fn quantile(histogram: &Histogram, bounds: &[f64], q: f64) -> Option<f64> {
    if histogram.count == 0 {
        return None;
    }
    let target = ((q * histogram.count as f64).ceil() as u64).max(1);
    for (slot, bound) in histogram.buckets.iter().zip(bounds) {
        if *slot >= target {
            return Some(*bound);
        }
    }
    Some(f64::INFINITY)
}

/// Human-readable latency summary of one histogram.
fn describe_latency(histogram: &Histogram, bounds: &[f64]) -> String {
    if histogram.count == 0 {
        return "none".to_owned();
    }
    let average = histogram.sum / histogram.count as f64;
    let p50 = quantile(histogram, bounds, 0.50).unwrap_or(f64::INFINITY);
    let p90 = quantile(histogram, bounds, 0.90).unwrap_or(f64::INFINITY);
    let p99 = quantile(histogram, bounds, 0.99).unwrap_or(f64::INFINITY);
    format!(
        "calls={} avg={} p50={} p90={} p99={}",
        histogram.count,
        fmt_seconds(average),
        fmt_seconds(p50),
        fmt_seconds(p90),
        fmt_seconds(p99)
    )
}

/// Formats a duration in seconds with a readable unit.
fn fmt_seconds(seconds: f64) -> String {
    if !seconds.is_finite() {
        return ">10s".to_owned();
    }
    if seconds < 1e-3 {
        format!("{:.0}us", seconds * 1e6)
    } else if seconds < 1.0 {
        format!("{:.2}ms", seconds * 1e3)
    } else {
        format!("{:.2}s", seconds)
    }
}

/// Average payload size of one histogram, or `n/a` when it saw nothing.
fn byte_avg(histogram: &Histogram) -> String {
    if histogram.count == 0 {
        return "n/a".to_owned();
    }
    fmt_bytes(histogram.sum / histogram.count as f64)
}

/// Formats a byte count with a readable unit.
fn fmt_bytes(bytes: f64) -> String {
    if bytes < 1024.0 {
        format!("{bytes:.0}B")
    } else if bytes < 1024.0 * 1024.0 {
        format!("{:.1}KiB", bytes / 1024.0)
    } else {
        format!("{:.1}MiB", bytes / (1024.0 * 1024.0))
    }
}

/// Renders one histogram in the Prometheus format.
fn render_histogram(
    out: &mut String,
    name: &str,
    labels: &str,
    histogram: &Histogram,
    bounds: &[f64],
) {
    for (slot, bound) in histogram.buckets.iter().zip(bounds) {
        let _ = writeln!(out, "{name}_bucket{{{labels},le=\"{bound}\"}} {slot}");
    }
    let _ = writeln!(
        out,
        "{name}_bucket{{{labels},le=\"+Inf\"}} {}",
        histogram.count
    );
    let _ = writeln!(out, "{name}_sum{{{labels}}} {}", histogram.sum);
    let _ = writeln!(out, "{name}_count{{{labels}}} {}", histogram.count);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_and_labels_are_rendered() {
        let metrics = Metrics::new();
        metrics.on_request("DatabaseService", 42, "get_user_age");
        metrics.on_request("DatabaseService", 42, "get_user_age");
        metrics.on_response("DatabaseService", 42, "complete");
        metrics.on_sample_gap(3);

        let text = metrics.render_prometheus();
        assert!(text.contains(
            "ice_rpc_requests_total{channel=\"DatabaseService\",service=\"42\",method=\"get_user_age\"} 2"
        ));
        assert!(text.contains(
            "ice_rpc_responses_total{channel=\"DatabaseService\",service=\"42\",kind=\"complete\"} 1"
        ));
        assert!(text.contains("ice_rpc_observer_gaps_total 3"));
    }

    #[test]
    fn latency_histogram_reports_a_sum_and_a_count() {
        let metrics = Metrics::new();
        metrics.on_latency("c", 1, "m", 0.001);
        metrics.on_latency("c", 1, "m", 0.002);

        let text = metrics.render_prometheus();
        assert!(text
            .contains("ice_rpc_latency_seconds_count{channel=\"c\",service=\"1\",method=\"m\"} 2"));
        assert!(text.contains("le=\"+Inf\""));
    }

    #[test]
    fn inflight_is_signed() {
        let metrics = Metrics::new();
        metrics.on_inflight("c", 1, 1);
        metrics.on_inflight("c", 1, 1);
        metrics.on_inflight("c", 1, -1);
        let text = metrics.render_prometheus();
        assert!(text.contains("ice_rpc_inflight{channel=\"c\",service=\"1\"} 1"));
    }

    #[test]
    fn console_summary_reports_counters_and_latency() {
        let metrics = Metrics::new();
        metrics.on_request("DatabaseService", 42, "get_user_age");
        metrics.on_response("DatabaseService", 42, "complete");
        metrics.on_latency("DatabaseService", 42, "get_user_age", 0.001);

        let text = metrics.render_console();
        assert!(
            text.contains("requests        : 1"),
            "missing requests:\n{text}"
        );
        assert!(text.contains("responses       : 1"));
        assert!(text.contains("complete=1"), "missing kind:\n{text}");
        assert!(text.contains("p50=1.00ms"), "missing latency:\n{text}");
        assert!(text.contains("DatabaseService service=42 method=get_user_age"));
    }

    #[test]
    fn console_summary_on_an_empty_registry_is_safe() {
        let text = Metrics::new().render_console();
        assert!(text.contains("requests        : 0"));
        assert!(text.contains("latency (exact) : none"));
    }

    #[test]
    fn the_health_inventory_is_exported() {
        use ice_rpc::monitor::{NodeHealth, NodeInfo, ServiceInfo, ServiceRole};

        let metrics = Metrics::new();
        let snapshot = HealthSnapshot {
            nodes: vec![NodeInfo {
                pid: 42,
                health: NodeHealth::Alive,
                executable: Some("provider-app".to_owned()),
                name: Some(String::new()),
            }],
            services: vec![ServiceInfo {
                name: "DatabaseService_req".to_owned(),
                pattern: "PublishSubscribe",
                role: ServiceRole::Request,
                participants: 2,
            }],
            scans: 1,
            errors: 0,
            ..HealthSnapshot::default()
        };
        let channels = vec![ChannelHealth {
            channel: "DatabaseService".to_owned(),
            direction: "req",
            attached: true,
            publishers: 1,
            subscribers: 1,
            max_publishers: 16,
            max_subscribers: 16,
            subscriber_buffer: 8,
        }];
        metrics.set_health(&snapshot, &channels);

        let text = metrics.render_prometheus();
        assert!(text.contains("ice_rpc_nodes{state=\"alive\"} 1"), "{text}");
        assert!(text.contains(
            "ice_rpc_node_info{pid=\"42\",state=\"alive\",executable=\"provider-app\"} 1"
        ));
        assert!(text.contains(
            "ice_rpc_services{service=\"DatabaseService_req\",pattern=\"PublishSubscribe\",role=\"req\"} 1"
        ));
        assert!(text.contains("ice_rpc_service_participants{service=\"DatabaseService_req\"} 2"));
        assert!(text.contains(
            "ice_rpc_channel_capacity{channel=\"DatabaseService\",direction=\"req\",kind=\"max_publishers\"} 16"
        ));
        assert!(text.contains(
            "ice_rpc_channel_capacity{channel=\"DatabaseService\",direction=\"req\",kind=\"subscriber_buffer_samples\"} 8"
        ));
        assert!(text.contains("ice_rpc_shm_scan_enabled 0"));
        assert!(text.contains("ice_rpc_health_scans_total 1"));

        let console = metrics.render_console();
        assert!(
            console.contains("network : nodes=1 (alive 1, dead 0)  services=1  channels=1"),
            "{console}"
        );
    }
}
