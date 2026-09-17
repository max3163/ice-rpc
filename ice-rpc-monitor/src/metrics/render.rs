//! Rendering primitives shared by the console and the Prometheus renderers.
//!
//! Two very different outputs read the same counters: the console prints a
//! human-readable summary with quantiles, Prometheus dumps the series. What they
//! share — summing histograms, reading a quantile, summarising a latency — lives
//! here, so neither renderer owns a piece of the other's vocabulary.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use ice_rpc::monitor::Direction;

use super::{direction_label, Histogram};

/// Sums several histograms into one, sharing the same bounds.
pub(super) fn merge<'a>(
    histograms: impl Iterator<Item = &'a Histogram>,
    bounds: &[f64],
) -> Histogram {
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
pub(super) fn merge_direction(
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
pub(super) fn quantile(histogram: &Histogram, bounds: &[f64], q: f64) -> Option<f64> {
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
pub(super) fn describe_latency(histogram: &Histogram, bounds: &[f64]) -> String {
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
pub(super) fn fmt_seconds(seconds: f64) -> String {
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
pub(super) fn byte_avg(histogram: &Histogram) -> String {
    if histogram.count == 0 {
        return "n/a".to_owned();
    }
    fmt_bytes(histogram.sum / histogram.count as f64)
}

/// Formats a byte count with a readable unit.
pub(super) fn fmt_bytes(bytes: f64) -> String {
    if bytes < 1024.0 {
        format!("{bytes:.0}B")
    } else if bytes < 1024.0 * 1024.0 {
        format!("{:.1}KiB", bytes / 1024.0)
    } else {
        format!("{:.1}MiB", bytes / (1024.0 * 1024.0))
    }
}

/// Renders one histogram in the Prometheus format.
pub(super) fn render_histogram(
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
