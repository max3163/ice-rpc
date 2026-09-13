//! Aggregated metrics produced by the observer.
//!
//! All the series are keyed by a **statically bounded** set (`channel`, numeric
//! `service_id`, `method` come from the compiled service definitions), so the
//! cardinality cannot grow with the traffic.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Mutex;

use ice_rpc::monitor::Direction;

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

#[derive(Default)]
struct Inner {
    requests: BTreeMap<(String, u32, String), u64>,
    responses: BTreeMap<(String, u32, &'static str), u64>,
    latency: BTreeMap<(String, u32, String), Histogram>,
    payload: BTreeMap<(String, &'static str), Histogram>,
    inflight: BTreeMap<(String, u32), i64>,
    unmatched: BTreeMap<String, u64>,
    orphan: BTreeMap<String, u64>,
    gaps: BTreeMap<(String, &'static str, u32), u64>,
    clock_skew: u64,
    nodes_alive: i64,
    node_crashes: u64,
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

    /// Adds missed samples detected through a `seq` hole.
    pub fn on_sample_gap(
        &self,
        channel: &str,
        direction: Direction,
        emitter_pid: u32,
        missed: u64,
    ) {
        if missed == 0 {
            return;
        }
        if let Ok(mut inner) = self.inner.lock() {
            *inner
                .gaps
                .entry((channel.to_owned(), direction_label(direction), emitter_pid))
                .or_default() += missed;
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

        out.push_str("# HELP ice_rpc_sample_gaps_total Samples missed, from seq holes\n");
        out.push_str("# TYPE ice_rpc_sample_gaps_total counter\n");
        for ((channel, direction, pid), value) in &inner.gaps {
            let _ = writeln!(
                out,
                "ice_rpc_sample_gaps_total{{channel=\"{channel}\",direction=\"{direction}\",emitter=\"{pid}\"}} {value}"
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

        out
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
        metrics.on_sample_gap("DatabaseService", Direction::Response, 1234, 3);

        let text = metrics.render_prometheus();
        assert!(text.contains(
            "ice_rpc_requests_total{channel=\"DatabaseService\",service=\"42\",method=\"get_user_age\"} 2"
        ));
        assert!(text.contains(
            "ice_rpc_responses_total{channel=\"DatabaseService\",service=\"42\",kind=\"complete\"} 1"
        ));
        assert!(text.contains(
            "ice_rpc_sample_gaps_total{channel=\"DatabaseService\",direction=\"resp\",emitter=\"1234\"} 3"
        ));
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
}
