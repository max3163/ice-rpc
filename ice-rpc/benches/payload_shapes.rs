//! Realistic payload shapes: what the request path costs per shape and size.
//!
//! The existing [`handler_handoff`](handler_handoff.rs) bench prices a single
//! shape — one `String` — at three sizes. That misses the two dimensions that
//! actually decide whether the zero-copy design is worth its cost:
//!
//! - **the number of allocations**, which grows with the number of variable
//!   fields (`String`/`Vec`), not with the byte count — a request with 4 096
//!   short strings costs 4 096 allocations to `from_bytes`, and **zero** to
//!   `access`;
//! - **the byte count**, where the copies dominate — one 1 MiB blob.
//!
//! Each shape is measured under the three strategies of the design plan
//! ([`plans/zero-copie-requetes-provider.md`](../plans/zero-copie-requetes-provider.md)):
//!
//! - `current_copy_then_decode` — what the transport does today: `to_vec()`
//!   then `from_bytes` on the copy (2 copies, one allocation-free, one not);
//! - `in_place_decode` — Tier A2: `from_bytes` straight from the (aligned)
//!   sample, no extra copy, same allocations;
//! - `access_zero_alloc` — Tier B: `rkyv::access` and a traversal of the
//!   archived fields, **zero copy and zero allocation**.
//!
//! Every payload is a real rkyv-encoded request, built from the borrowed form
//! (the one the client would send), so the bytes are exactly what the transport
//! would publish.
//!
//! Run with: `cargo bench -p ice-rpc --bench payload_shapes`.

#![allow(clippy::unwrap_used)] // benches may panic

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rkyv::rancor::Error;
use rkyv::util::AlignedVec;
use rkyv::with::{AsString, AsVec};
use rkyv::{Archive, Deserialize, Serialize};

/// One shape, one size: the three strategies, priced on the same bytes.
///
/// `$bytes` is an [`AlignedVec`] (what `to_bytes` returns and what an iceoryx2
/// sample payload is), `$read` touches every variable field of the archived
/// form so the `access` variant cannot be optimized away.
macro_rules! bench_shape {
    ($g:expr, $label:expr, $owned:ty, $archived:ty, $bytes:expr, $read:expr) => {{
        let bytes: AlignedVec<16> = $bytes;
        let read = $read;
        $g.throughput(Throughput::Bytes(bytes.len() as u64));

        $g.bench_with_input(
            BenchmarkId::new("current_copy_then_decode", $label),
            &bytes,
            |b, bytes| {
                b.iter(|| {
                    let owned = bytes.to_vec();
                    black_box(rkyv::from_bytes::<$owned, Error>(&owned).unwrap());
                });
            },
        );

        $g.bench_with_input(
            BenchmarkId::new("in_place_decode", $label),
            &bytes,
            |b, bytes| {
                b.iter(|| black_box(rkyv::from_bytes::<$owned, Error>(bytes).unwrap()));
            },
        );

        $g.bench_with_input(
            BenchmarkId::new("access_zero_alloc", $label),
            &bytes,
            |b, bytes| {
                b.iter(|| {
                    // Black-box the archived view itself: without it, a reader
                    // that only takes a length lets the compiler elide the whole
                    // access.
                    let archived = black_box(rkyv::access::<$archived, Error>(bytes).unwrap());
                    black_box(read(archived));
                });
            },
        );
    }};
}

// ── scalar: the floor, no variable data ────────────────────────────────────

#[derive(Archive, Serialize, Deserialize)]
struct Scalar {
    id: u32,
}

fn scalar_bytes() -> AlignedVec<16> {
    rkyv::to_bytes::<Error>(&Scalar { id: 7 }).unwrap()
}

// ── text: one string of N bytes ────────────────────────────────────────────

#[derive(Archive, Serialize, Deserialize)]
struct Text {
    s: String,
}

#[derive(Archive, Serialize)]
struct BorrowedText<'a> {
    #[rkyv(with = AsString)]
    s: &'a str,
}

fn text_bytes(n: usize) -> AlignedVec<16> {
    let s = "x".repeat(n);
    rkyv::to_bytes::<Error>(&BorrowedText { s: &s }).unwrap()
}

// ── record: the demo shape, several short strings + a scalar ───────────────

#[derive(Archive, Serialize, Deserialize)]
struct Record {
    nom: String,
    prenom: String,
    ville: String,
    profession: String,
    age: u32,
}

#[derive(Archive, Serialize)]
struct BorrowedRecord<'a> {
    #[rkyv(with = AsString)]
    nom: &'a str,
    #[rkyv(with = AsString)]
    prenom: &'a str,
    #[rkyv(with = AsString)]
    ville: &'a str,
    #[rkyv(with = AsString)]
    profession: &'a str,
    age: u32,
}

fn record_bytes(pad: usize) -> AlignedVec<16> {
    let nom = "Dupont".to_string();
    let prenom = "Jean".to_string();
    let ville = "Paris".to_string();
    let profession = "x".repeat(pad);
    let r = BorrowedRecord {
        nom: &nom,
        prenom: &prenom,
        ville: &ville,
        profession: &profession,
        age: 42,
    };
    rkyv::to_bytes::<Error>(&r).unwrap()
}

// ── blob: one large byte vector (the HTTP body case) ───────────────────────

#[derive(Archive, Serialize, Deserialize)]
struct Blob {
    data: Vec<u8>,
}

#[derive(Archive, Serialize)]
struct BorrowedBlob<'a> {
    #[rkyv(with = AsVec)]
    data: &'a [u8],
}

fn blob_bytes(n: usize) -> AlignedVec<16> {
    let data = vec![0xABu8; n];
    rkyv::to_bytes::<Error>(&BorrowedBlob { data: &data }).unwrap()
}

// ── many_text: K strings — allocates K times under from_bytes ──────────────

#[derive(Archive, Serialize, Deserialize)]
struct ManyText {
    fields: Vec<String>,
}

#[derive(Archive, Serialize)]
struct BorrowedManyText<'a> {
    #[rkyv(with = AsVec)]
    fields: &'a [String],
}

fn many_text_bytes(count: usize, per: usize) -> AlignedVec<16> {
    let fields: Vec<String> = (0..count)
        .map(|i| format!("{i:0>width$}", width = per))
        .collect();
    rkyv::to_bytes::<Error>(&BorrowedManyText { fields: &fields }).unwrap()
}

// ── table: K rows, each with a string — K allocations + structure ──────────

#[derive(Archive, Serialize, Deserialize)]
struct Row {
    name: String,
    value: u64,
}

#[derive(Archive, Serialize, Deserialize)]
struct Table {
    rows: Vec<Row>,
}

#[derive(Archive, Serialize)]
struct BorrowedTable<'a> {
    #[rkyv(with = AsVec)]
    rows: &'a [Row],
}

fn table_bytes(count: usize, per: usize) -> AlignedVec<16> {
    let rows: Vec<Row> = (0..count)
        .map(|i| Row {
            name: format!("{i:0>width$}", width = per),
            value: i as u64,
        })
        .collect();
    rkyv::to_bytes::<Error>(&BorrowedTable { rows: &rows }).unwrap()
}

// ── groups ─────────────────────────────────────────────────────────────────

fn scalar(c: &mut Criterion) {
    let mut g = c.benchmark_group("payload_scalar");
    bench_shape!(
        g,
        "1",
        Scalar,
        ArchivedScalar,
        scalar_bytes(),
        |a: &ArchivedScalar| u32::from(a.id) as usize
    );
    g.finish();
}

fn text(c: &mut Criterion) {
    let mut g = c.benchmark_group("payload_text");
    for n in [64usize, 1 << 10, 1 << 16, 1 << 20] {
        let label = format!("{n}B");
        bench_shape!(
            g,
            &label,
            Text,
            ArchivedBorrowedText,
            text_bytes(n),
            |a: &ArchivedBorrowedText| a.s.as_str().len()
        );
    }
    g.finish();
}

fn record(c: &mut Criterion) {
    let mut g = c.benchmark_group("payload_record");
    for pad in [8usize, 1 << 10] {
        let label = format!("{pad}B");
        bench_shape!(
            g,
            &label,
            Record,
            ArchivedBorrowedRecord,
            record_bytes(pad),
            |a: &ArchivedBorrowedRecord| {
                a.nom.as_str().len()
                    + a.prenom.as_str().len()
                    + a.ville.as_str().len()
                    + a.profession.as_str().len()
                    + u32::from(a.age) as usize
            }
        );
    }
    g.finish();
}

fn blob(c: &mut Criterion) {
    let mut g = c.benchmark_group("payload_blob");
    for n in [64usize, 1 << 10, 1 << 16, 1 << 20] {
        let label = format!("{n}B");
        bench_shape!(
            g,
            &label,
            Blob,
            ArchivedBorrowedBlob,
            blob_bytes(n),
            |a: &ArchivedBorrowedBlob| a.data.len()
        );
    }
    g.finish();
}

fn many_text(c: &mut Criterion) {
    let mut g = c.benchmark_group("payload_many_text");
    for count in [4usize, 64, 1024] {
        let label = format!("{count}x32B");
        bench_shape!(
            g,
            &label,
            ManyText,
            ArchivedBorrowedManyText,
            many_text_bytes(count, 32),
            |a: &ArchivedBorrowedManyText| a.fields.iter().map(|s| s.as_str().len()).sum::<usize>()
        );
    }
    g.finish();
}

fn table(c: &mut Criterion) {
    let mut g = c.benchmark_group("payload_table");
    for count in [4usize, 64, 1024] {
        let label = format!("{count}x32B");
        bench_shape!(
            g,
            &label,
            Table,
            ArchivedBorrowedTable,
            table_bytes(count, 32),
            |a: &ArchivedBorrowedTable| a
                .rows
                .iter()
                .map(|r| r.name.as_str().len() + u64::from(r.value) as usize)
                .sum::<usize>()
        );
    }
    g.finish();
}

criterion_group!(benches, scalar, text, record, blob, many_text, table);
criterion_main!(benches);
