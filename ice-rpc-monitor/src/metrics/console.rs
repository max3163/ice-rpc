//! Console rendering: a human-readable summary of the registry.
//!
//! Meant for a console observer: global counters, exact-latency quantiles read
//! from the fixed-bucket histograms (so they are **upper bounds**), payload
//! averages, the in-flight gauge and the loss/orphan counters, followed by a
//! per-service breakdown.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use ice_rpc::monitor::Direction;

use super::render::*;
use super::{Inner, LATENCY_BOUNDS, PAYLOAD_BOUNDS};

/// Renders the registry for a console observer.
pub(super) fn render(inner: &Inner) -> String {
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
