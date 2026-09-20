//! Micro-benchmarks for the operator pipeline: what does the boxed
//! `Box<dyn Stream>` chain cost?
//!
//! The operators of `ice-rpc` are **inherent methods** returning `Observable`:
//! each step wraps the previous one in a boxed, poll-based combinator
//! (`Observable::from_stream`). This benchmark measures its price on 100 000
//! events, by comparing:
//!
//! - `direct_loop` — a hand-written iterator chain over a range (lower bound);
//! - `source_observable` — draining a bare `from(..)` observable;
//! - `pipeline_boxed` — `from(..).filter(..).map(..).take(..)`, i.e. three
//!   boxed combinators in series over the same source.
//!
//! The three variants are checked to agree before the measurement, so the
//! numbers cannot come from a pipeline doing the wrong work.
//!
//! Run with: `cargo bench -p ice-rpc --bench pipeline`.

#![allow(clippy::unwrap_used)] // tests/examples/benches may panic
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::hint::black_box;

use ice_rpc::rt::block_on;
use ice_rpc::Observable;

/// Events pushed through every variant.
const EVENTS: u32 = 100_000;

/// Values kept by the pipeline (half of the even values).
const KEPT: usize = (EVENTS / 4) as usize;

/// Sum of the source values: `1 + 2 + … + EVENTS`.
const SOURCE_SUM: u64 = EVENTS as u64 * (EVENTS as u64 + 1) / 2;

/// Sum of the even values of the source.
const EVEN_SUM: u64 = {
    let pairs = (EVENTS / 2) as u64;
    pairs * (pairs + 1)
};

/// Sum of the `KEPT` first values.
const HEAD_SUM: u64 = KEPT as u64 * (KEPT as u64 + 1) / 2;

/// Sum of the running accumulator of `scan`: `Σ k(k+1)/2`.
const SCAN_SUM: u64 = {
    let n = EVENTS as u64;
    n * (n + 1) * (n + 2) / 6
};

/// Sum produced by the `filter` + `map` + `take` chain of the two stream
/// variants.
fn pipeline_sum() -> u64 {
    (1..=EVENTS)
        .filter(|v| v % 2 == 0)
        .take(KEPT)
        .map(|v| u64::from(v) * 2)
        .sum()
}

/// Drains `stream`, summing every value.
fn drain_sum(stream: Observable<u32, String>) -> u64 {
    let mut sum = 0u64;
    block_on(stream.for_each(|v| sum = sum.wrapping_add(u64::from(v)))).unwrap();
    sum
}

/// Fails loudly if a variant does not compute what the others do.
fn variants_agree() {
    let expected = pipeline_sum();

    let direct: u64 = (1..=EVENTS)
        .filter(|v| v % 2 == 0)
        .take(KEPT)
        .map(|v| u64::from(v) * 2)
        .sum();
    assert_eq!(direct, expected, "direct_loop");

    let source = drain_sum(ice_rpc::from(1..=EVENTS));
    assert_eq!(
        source,
        (1..=EVENTS).map(u64::from).sum::<u64>(),
        "source_observable"
    );

    let piped = drain_sum(
        ice_rpc::from(1..=EVENTS)
            .filter(|v| v % 2 == 0)
            .map(|v| v * 2)
            .take(KEPT),
    );
    assert_eq!(piped, expected, "pipeline_boxed");
}

/// Verifies that one operator, applied alone, produces the expected sum.
fn operator_check<F>(name: &str, build: &F, expected: u64)
where
    F: Fn(Observable<u32, String>) -> Observable<u32, String>,
{
    let produced = drain_sum(build(ice_rpc::from(1..=EVENTS)));
    assert_eq!(produced, expected, "operator '{name}'");
}

/// Verifies an operator, then measures it alone on the shared source.
///
/// The verification is what makes the number readable: an operator that silently
/// dropped half the values would otherwise look like a fast one.
///
/// The macro rebuilds the closure with an **annotated** parameter rather than
/// passing the caller's closure through. `from` infers both its parameters from
/// its use, so a closure whose parameter type came only from that call would
/// leave two inferences depending on each other — the compiler then asks for an
/// annotation at every call site, which is the caller's problem only by
/// accident.
macro_rules! bench_operator {
    ($group:ident, $name:literal, |$source:ident| $body:expr, $expected:expr) => {{
        let build = |$source: Observable<u32, String>| $body;
        operator_check($name, &build, $expected);
        $group.bench_function($name, |b| {
            b.iter(|| {
                black_box(drain_sum(build(ice_rpc::from::<u32, String, _>(
                    1..=black_box(EVENTS),
                ))))
            })
        });
    }};
}

/// Each operator of `ice-rpc-rx`, alone, over the same source.
fn bench_operators(c: &mut Criterion) {
    let mut group = c.benchmark_group("operators");
    group.throughput(Throughput::Elements(EVENTS as u64));

    bench_operator!(group, "map", |s| s.map(|v| v), SOURCE_SUM);
    bench_operator!(group, "filter", |s| s.filter(|v| v % 2 == 0), EVEN_SUM);
    bench_operator!(group, "take", |s| s.take(KEPT), HEAD_SUM);
    bench_operator!(group, "skip", |s| s.skip(1), SOURCE_SUM - 1);
    bench_operator!(group, "scan", |s| s.scan(0, |acc, v| acc + v), SCAN_SUM);
    bench_operator!(group, "tap", |s| s.tap(|_| {}), SOURCE_SUM);
    bench_operator!(group, "start_with", |s| s.start_with(0), SOURCE_SUM);
    bench_operator!(group, "finalize", |s| s.finalize(|| {}), SOURCE_SUM);
    bench_operator!(group, "map_err", |s| s.map_err(|e| e), SOURCE_SUM);
    bench_operator!(group, "catch_error", |s| s.catch_error(|_| 0), SOURCE_SUM);

    // A token that is never cancelled: the operator is measured on its watch,
    // not on its reaction.
    let token = ice_rpc::CancellationToken::new();
    bench_operator!(group, "take_until", |s| s.take_until(&token), SOURCE_SUM);

    group.finish();
}

fn bench_pipeline(c: &mut Criterion) {
    variants_agree();

    let mut group = c.benchmark_group("pipeline");
    group.throughput(Throughput::Elements(EVENTS as u64));

    group.bench_function("direct_loop", |b| {
        b.iter(|| {
            let sum: u64 = (1..=black_box(EVENTS))
                .filter(|v| v % 2 == 0)
                .take(KEPT)
                .map(|v| u64::from(v) * 2)
                .sum();
            black_box(sum)
        });
    });

    group.bench_function("source_observable", |b| {
        b.iter(|| {
            let stream = ice_rpc::from(1..=black_box(EVENTS));
            black_box(drain_sum(stream))
        });
    });

    group.bench_function("pipeline_boxed", |b| {
        b.iter(|| {
            black_box(drain_sum(
                ice_rpc::from(1..=black_box(EVENTS))
                    .filter(|v| v % 2 == 0)
                    .map(|v| v * 2)
                    .take(KEPT),
            ))
        });
    });

    group.finish();
}

criterion_group!(benches, bench_pipeline, bench_operators);
criterion_main!(benches);
