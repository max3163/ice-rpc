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

criterion_group!(benches, bench_pipeline);
criterion_main!(benches);
