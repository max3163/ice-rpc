//! Aggregated metrics produced by the observer.
//!
//! All the series are keyed by a **statically bounded** set (`channel`, numeric
//! `service_id`, `method` come from the compiled service definitions), so the
//! cardinality cannot grow with the traffic.
//!
//! # One state, two renderings
//!
//! This module owns the state and its lock: [`Metrics`] records, and nothing else
//! does. The two ways of *showing* that state live apart —
//! [`Metrics::render_console`] for a human summary,
//! [`Metrics::render_prometheus`] for the text exposition format — and share
//! their primitives through the private `render` module. A rendering therefore
//! cannot take the lock itself, and the recording path is not buried under
//! several hundred lines of formatting.

use std::collections::BTreeMap;
use std::sync::Mutex;

use ice_rpc::monitor::Direction;

use crate::health::{ChannelHealth, HealthSnapshot};

mod console;
mod prometheus;
mod render;

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
    /// latency histogram (the `LossTracker` of `loss.rs` is what calls this).
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

    /// Replaces the per-channel attachment block.
    ///
    /// Built from the observer's own views, so it costs no scan and is published
    /// on every acquisition pass: a channel must never be reported as unattached
    /// — nor the reverse — one inventory interval late.
    pub fn set_channels(&self, channels: &[ChannelHealth]) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };

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
    }

    /// Replaces the whole health inventory with a fresh scan.
    ///
    /// The inventory is *replaced*, never accumulated, so a service or a node
    /// that disappears stops being exported on the very next scan. The
    /// per-channel block is not part of it, see [`Metrics::set_channels`].
    pub fn set_health(&self, snapshot: &HealthSnapshot) {
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
        console::render(&inner)
    }

    /// Renders the registry in the Prometheus text exposition format.
    pub fn render_prometheus(&self) -> String {
        let Ok(inner) = self.inner.lock() else {
            return String::new();
        };
        prometheus::render(&inner)
    }
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
        metrics.set_channels(&channels);
        metrics.set_health(&snapshot);

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
