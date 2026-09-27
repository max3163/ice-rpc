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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{LATENCY_BOUNDS, PAYLOAD_BOUNDS};

    fn histogram(values: &[f64], bounds: &[f64]) -> Histogram {
        let mut histogram = Histogram::empty(bounds);
        for value in values {
            histogram.observe(*value, bounds);
        }
        histogram
    }

    #[test]
    fn merging_sums_the_buckets_sums_and_counts() {
        let merged = merge(
            [
                histogram(&[0.001], LATENCY_BOUNDS),
                histogram(&[0.002, 0.003], LATENCY_BOUNDS),
            ]
            .iter(),
            LATENCY_BOUNDS,
        );
        assert_eq!(merged.count, 3);
        assert!((merged.sum - 0.006).abs() < 1e-12);
        assert_eq!(merged.buckets.len(), LATENCY_BOUNDS.len());
    }

    #[test]
    fn merging_a_direction_ignores_the_other_one() {
        let mut payload = BTreeMap::new();
        payload.insert(
            ("c".to_owned(), "req"),
            histogram(&[10.0, 20.0], PAYLOAD_BOUNDS),
        );
        payload.insert(("c".to_owned(), "resp"), histogram(&[30.0], PAYLOAD_BOUNDS));

        assert_eq!(
            merge_direction(&payload, Direction::Request, PAYLOAD_BOUNDS).count,
            2
        );
        assert_eq!(
            merge_direction(&payload, Direction::Response, PAYLOAD_BOUNDS).count,
            1
        );
        // A direction with no series merges to an empty histogram, not a panic.
        assert_eq!(
            merge_direction(&BTreeMap::new(), Direction::Request, PAYLOAD_BOUNDS).count,
            0
        );
    }

    #[test]
    fn a_quantile_is_the_first_bucket_bound_reaching_the_rank() {
        let single = histogram(&[0.001], LATENCY_BOUNDS);
        assert_eq!(quantile(&single, LATENCY_BOUNDS, 0.5), Some(0.001));
        assert_eq!(quantile(&single, LATENCY_BOUNDS, 0.99), Some(0.001));
    }

    #[test]
    fn a_quantile_of_an_empty_histogram_is_unknown() {
        let empty = Histogram::empty(LATENCY_BOUNDS);
        assert_eq!(quantile(&empty, LATENCY_BOUNDS, 0.5), None);
    }

    #[test]
    fn a_quantile_beyond_the_last_bound_is_infinite() {
        // A value above every bound is counted, but falls in no bucket.
        let last = LATENCY_BOUNDS[LATENCY_BOUNDS.len() - 1];
        let huge = histogram(&[last * 10.0], LATENCY_BOUNDS);
        assert_eq!(quantile(&huge, LATENCY_BOUNDS, 0.5), Some(f64::INFINITY));
    }

    #[test]
    fn describing_an_empty_latency_says_none() {
        let empty = Histogram::empty(LATENCY_BOUNDS);
        assert_eq!(describe_latency(&empty, LATENCY_BOUNDS), "none");
    }

    #[test]
    fn describing_a_latency_reports_the_count_and_the_quantiles() {
        let latency = histogram(&[0.001, 0.002, 0.003], LATENCY_BOUNDS);
        let text = describe_latency(&latency, LATENCY_BOUNDS);
        assert!(text.starts_with("calls=3 "), "{text}");
        assert!(text.contains("p50="), "{text}");
        assert!(text.contains("p99="), "{text}");
    }

    #[test]
    fn seconds_pick_a_readable_unit() {
        assert_eq!(fmt_seconds(0.000_001), "1us");
        assert_eq!(fmt_seconds(0.0015), "1.50ms");
        assert_eq!(fmt_seconds(1.5), "1.50s");
    }

    #[test]
    fn an_unrepresentable_duration_is_rendered_as_a_floor() {
        assert_eq!(fmt_seconds(f64::INFINITY), ">10s");
        assert_eq!(fmt_seconds(f64::NAN), ">10s");
    }

    #[test]
    fn bytes_pick_a_readable_unit() {
        assert_eq!(fmt_bytes(512.0), "512B");
        assert_eq!(fmt_bytes(2048.0), "2.0KiB");
        assert_eq!(fmt_bytes(3.0 * 1024.0 * 1024.0), "3.0MiB");
    }

    #[test]
    fn an_empty_payload_histogram_has_no_average() {
        assert_eq!(byte_avg(&Histogram::empty(PAYLOAD_BOUNDS)), "n/a");
        assert_eq!(
            byte_avg(&histogram(&[100.0, 200.0], PAYLOAD_BOUNDS)),
            "150B"
        );
    }

    #[test]
    fn a_rendered_histogram_carries_every_bucket_plus_infinity() {
        let mut out = String::new();
        render_histogram(
            &mut out,
            "m",
            "channel=\"c\"",
            &histogram(&[0.001], LATENCY_BOUNDS),
            LATENCY_BOUNDS,
        );
        assert_eq!(out.lines().count(), LATENCY_BOUNDS.len() + 3);
        assert!(out.contains("m_bucket{channel=\"c\",le=\"+Inf\"} 1"));
        assert!(out.contains("m_count{channel=\"c\"} 1"));
    }
}
