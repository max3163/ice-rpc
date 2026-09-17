//! Prometheus exposition: the registry as text-format series.
//!
//! One series per channel/service/method for the counters, the payload and
//! latency histograms, the gauges and the monitor's own health counters.

use std::fmt::Write as _;

use super::render::*;
use super::{Inner, LATENCY_BOUNDS, PAYLOAD_BOUNDS};

/// Renders the registry in the Prometheus text exposition format.
pub(super) fn render(inner: &Inner) -> String {
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

    out.push_str("# HELP ice_rpc_channel_publishers Active publishers of a channel direction\n");
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

        out.push_str("# HELP ice_rpc_process_rss_bytes Resident memory of an observed process\n");
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

    out.push_str("# HELP ice_rpc_shm_bytes Measured size of the iceoryx2 shared-memory segments\n");
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
