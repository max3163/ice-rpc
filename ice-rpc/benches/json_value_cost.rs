//! What the `serde_json::Value` intermediary costs in the Node.js bridge.
//!
//! The bridge is often described as "no serialization", which is true for JSON
//! **text**: N-API converts a `Value` to V8 values directly, with no
//! `JSON.stringify` / `JSON.parse`. It still builds a `Value` tree in Rust. That
//! tree is the only part a `#[napi(object)]` fast path could remove, so this
//! bench prices it against the rkyv decode and encode it accompanies.
//!
//! The numbers are an **upper bound** of the conversion saving: a napi-object
//! path still creates the V8 objects, and it cannot represent every payload that
//! `serde` can (enums with data, custom `Serialize`, `flatten`, …). What the
//! bench measures is therefore the tree, not an end-to-end speedup.
//!
//! The shapes are the ones the example services carry: scalars, one text field
//! at several sizes, a record of four strings, a list of entries, a nested query
//! and an HTTP response with a binary body. `from_value` runs through
//! `iter_with_setup`, so the clone of the input `Value` is outside the timed
//! section; `value_clone` prices that clone on its own, because the bridge's
//! envelope normalization does it once per event.
//!
//! Run with: `cargo bench -p ice-rpc --bench json_value_cost`.

#![allow(clippy::unwrap_used)] // benchmarks may panic

use std::hint::black_box;

use common::{
    ContextEntry, HttpResponseParams, PersonneInfo, WorkloadFilter, WorkloadProfile, WorkloadQuery,
    WorkloadScalars,
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rkyv::rancor::Error;

/// One shape, measured under the five costs that decide the bridge's share.
///
/// `to_value` is the provider-side tree build (rkyv request → `Value`),
/// `from_value` is the response-side tree read (JS → `Value` → payload),
/// `value_clone` is the envelope's `data` clone, and `rkyv_encode` /
/// `rkyv_decode` are the baseline the tree is added to.
macro_rules! bench_shape {
    ($group:expr, $label:expr, $ty:ty, $payload:expr) => {{
        let payload: $ty = $payload;
        let value = serde_json::to_value(&payload).unwrap();
        let encoded = rkyv::to_bytes::<Error>(&payload).unwrap();

        $group.bench_with_input(
            BenchmarkId::new("to_value", $label),
            &payload,
            |b, payload| {
                b.iter(|| black_box(serde_json::to_value(black_box(payload)).unwrap()));
            },
        );

        $group.bench_with_input(
            BenchmarkId::new("from_value", $label),
            &value,
            |b, value| {
                b.iter_with_setup(
                    || value.clone(),
                    |value| black_box(serde_json::from_value::<$ty>(value).unwrap()),
                );
            },
        );

        $group.bench_with_input(
            BenchmarkId::new("value_clone", $label),
            &value,
            |b, value| b.iter(|| black_box(value.clone())),
        );

        $group.bench_with_input(
            BenchmarkId::new("rkyv_encode", $label),
            &payload,
            |b, payload| {
                b.iter(|| black_box(rkyv::to_bytes::<Error>(black_box(payload)).unwrap()));
            },
        );

        $group.bench_with_input(
            BenchmarkId::new("rkyv_decode", $label),
            &encoded,
            |b, encoded| {
                b.iter(|| {
                    black_box(rkyv::from_bytes::<$ty, Error>(black_box(&encoded[..])).unwrap());
                });
            },
        );
    }};
}

fn text(bytes: usize) -> String {
    "x".repeat(bytes)
}

fn personne() -> PersonneInfo {
    PersonneInfo {
        nom: "Dupont".to_string(),
        prenom: "Jean".to_string(),
        age: 42,
        ville: "Paris".to_string(),
        profession: "Ingénieur".to_string(),
    }
}

fn entries(count: usize) -> Vec<ContextEntry> {
    (0..count)
        .map(|index| ContextEntry {
            key: format!("key.{index:04}"),
            value: format!("value.{index:04}"),
        })
        .collect()
}

fn workload_query(tags: usize, filters: usize) -> WorkloadQuery {
    WorkloadQuery {
        key: "bench.key".to_string(),
        tags: (0..tags).map(|index| format!("tag-{index:03}")).collect(),
        filters: (0..filters)
            .map(|index| WorkloadFilter {
                field: format!("field-{index:03}"),
                value: format!("value-{index:03}"),
            })
            .collect(),
    }
}

fn http_response(body_bytes: usize) -> HttpResponseParams {
    HttpResponseParams {
        status_code: 200,
        status_text: "OK".to_string(),
        headers: vec![
            (
                "content-type".to_string(),
                "application/octet-stream".to_string(),
            ),
            ("server".to_string(), "ice-rpc-bench".to_string()),
        ],
        body: vec![0xAB; body_bytes],
    }
}

fn scalars() -> WorkloadScalars {
    WorkloadScalars {
        flag: true,
        count: 42,
        total: 1_000_000,
        ratio: 0.5,
        profile: WorkloadProfile::Balanced,
        optional: Some(7),
    }
}

fn scalar(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_value_scalar");
    group.sample_size(50);
    bench_shape!(&mut group, "u32", u32, 42u32);
    bench_shape!(&mut group, "workload_scalars", WorkloadScalars, scalars());
    group.finish();
}

fn text_shapes(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_value_text");
    group.sample_size(20);
    for bytes in [64usize, 1 << 10, 1 << 20] {
        let label = format!("{bytes}B");
        bench_shape!(&mut group, label.as_str(), String, text(bytes));
    }
    group.finish();
}

fn record(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_value_record");
    group.sample_size(50);
    bench_shape!(&mut group, "personne_info", PersonneInfo, personne());
    group.finish();
}

fn list(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_value_list");
    group.sample_size(20);
    for count in [4usize, 64, 1024] {
        let label = format!("{count}x_entry");
        bench_shape!(
            &mut group,
            label.as_str(),
            Vec<ContextEntry>,
            entries(count)
        );
    }
    group.finish();
}

fn nested(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_value_nested");
    group.sample_size(20);
    for (tags, filters) in [(8usize, 8usize), (64, 64)] {
        let label = format!("{tags}tags_{filters}filters");
        bench_shape!(
            &mut group,
            label.as_str(),
            WorkloadQuery,
            workload_query(tags, filters)
        );
    }
    group.finish();
}

fn binary(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_value_binary");
    group.sample_size(10);
    for body_bytes in [1usize << 10, 1 << 20] {
        let label = format!("{body_bytes}B_body");
        bench_shape!(
            &mut group,
            label.as_str(),
            HttpResponseParams,
            http_response(body_bytes)
        );
    }
    group.finish();
}

criterion_group!(benches, scalar, text_shapes, record, list, nested, binary);
criterion_main!(benches);
