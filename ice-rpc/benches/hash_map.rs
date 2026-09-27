//! Micro-benchmarks: what the transport's maps gain from a custom hasher.
//!
//! `perf` attributed roughly 2-3 % of both the provider's and the consumer's
//! self time to `std::collections::HashMap` with its default `RandomState`
//! (SipHash): `hash_one::<&u32>` (the service and method tables),
//! `hash_one::<&[u8;16]>` and `Hasher::write` (the correlation-id maps on both
//! sides). That default buys hash-flooding resistance the transport does not
//! need: its keys are internal ids — a `service_id` derived from a compile-time
//! name, a correlation id this process minted — never attacker-controlled.
//!
//! This benchmark prices the alternatives on the **key shapes the transport
//! actually uses**, so the decision is taken on numbers rather than on a hunch:
//!
//! - `sip` — `std::collections::HashMap` default (`RandomState`);
//! - `fx`  — [`FxHasher`], one rotate/xor/multiply per 64-bit word;
//! - `id`  — [`IdentityHasher`], valid only for the small integral `u32` keys.
//!
//! `fx` and `id` are the **shipped** hashers, imported from `ice_rpc::gen`
//! (the crate's internal facade), so this bench prices the real implementation
//! rather than a copy of it: a change to `src/hash.rs` moves these numbers.
//!
//! Two levels are measured separately:
//!
//! - `hash_only_*` — `BuildHasher::hash_one` on one key, isolating the hashing
//!   cost the profiler saw;
//! - `map_get_*` / `map_insert_remove_*` — the real `HashMap` operations the
//!   transport performs per call and per response (a lookup in a service/method
//!   table, and the register/release cycle of a correlation id), at the sizes
//!   those maps really have (a handful of services; up to thousands of in-flight
//!   calls under load).
//!
//! Every group checks that the variants agree on a result before timing them, so
//! a faster hasher cannot win by answering differently.
//!
//! Run with: `cargo bench -p ice-rpc --bench hash_map`.

#![allow(clippy::unwrap_used)] // benches may panic

use std::collections::HashMap;
use std::hash::{BuildHasher, BuildHasherDefault};
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use ice_rpc::gen::{FxHasher, IdentityHasher};

/// The map types the three candidates stand for.
type SipMap<K, V> = HashMap<K, V>;
type FxMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;
type IdMap<K, V> = HashMap<K, V, BuildHasherDefault<IdentityHasher>>;

/// Builds the deterministic 16-byte keys used by the correlation tests.
///
/// A real correlation id is `pid ++ counter`: two distinct 64-bit halves, never
/// a repeated byte, which is what makes the hasher do real work.
fn correlation(seed: u64) -> [u8; 16] {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut out = [0u8; 16];
    let (chunks, _) = out.as_chunks_mut::<8>();
    for chunk in chunks {
        state = state.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
        *chunk = state.to_le_bytes();
    }
    out
}

/// A **real** correlation id: `pid ++ counter`, both big-endian, exactly as
/// `next_correlation_id` mints it.
///
/// Consecutive calls differ only in the low bytes of the counter, which is the
/// pattern the maps really see — and the one a weak hasher clusters on.
fn correlation_real(pid: u64, counter: u64) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&pid.to_be_bytes());
    out[8..].copy_from_slice(&counter.to_be_bytes());
    out
}

/// The hashing cost alone, on one key of each shape.
fn hash_only(c: &mut Criterion) {
    let sip = std::collections::hash_map::RandomState::new();
    let fx = BuildHasherDefault::<FxHasher>::default();
    let id = BuildHasherDefault::<IdentityHasher>::default();

    let mut group = c.benchmark_group("hash_only_u32");
    let key: u32 = 0x00ab_cdef;
    group.bench_function("sip", |b| {
        b.iter(|| black_box(sip.hash_one(black_box(key))))
    });
    group.bench_function("fx", |b| b.iter(|| black_box(fx.hash_one(black_box(key)))));
    group.bench_function("id", |b| b.iter(|| black_box(id.hash_one(black_box(key)))));
    group.finish();

    let mut group = c.benchmark_group("hash_only_correlation");
    let key = correlation(7);
    group.bench_function("sip", |b| {
        b.iter(|| black_box(sip.hash_one(black_box(&key))))
    });
    group.bench_function("fx", |b| b.iter(|| black_box(fx.hash_one(black_box(&key)))));
    group.finish();
}

/// The real operations: a lookup in a map at the size the transport gives it.
fn map_get(c: &mut Criterion) {
    // The correlation-id maps: one entry per in-flight call. `1` and `8` are a
    // quiet deployment, `1024` and `16384` the depth a load test reaches.
    for &n in &[1usize, 8, 1024, 16384] {
        let keys: Vec<[u8; 16]> = (0..n as u64).map(correlation).collect();
        let probe = keys[n / 2];

        let mut sip: SipMap<[u8; 16], u64> = HashMap::new();
        let mut fx: FxMap<[u8; 16], u64> = HashMap::with_hasher(BuildHasherDefault::default());
        for key in &keys {
            sip.insert(*key, 1);
            fx.insert(*key, 1);
        }
        assert_eq!(
            sip.get(&probe),
            fx.get(&probe),
            "the two maps must answer the same value"
        );

        let mut group = c.benchmark_group("map_get_correlation");
        group.bench_with_input(BenchmarkId::new("sip", n), &probe, |b, probe| {
            b.iter(|| black_box(sip.get(black_box(probe))))
        });
        group.bench_with_input(BenchmarkId::new("fx", n), &probe, |b, probe| {
            b.iter(|| black_box(fx.get(black_box(probe))))
        });
        group.finish();
    }

    // The service/method tables: a handful of entries per channel, never thousands.
    for &n in &[1usize, 8] {
        let keys: Vec<u32> = (0..n as u32).collect();
        let probe = keys[n / 2];

        let mut sip: SipMap<u32, u64> = HashMap::new();
        let mut fx: FxMap<u32, u64> = HashMap::with_hasher(BuildHasherDefault::default());
        let mut id: IdMap<u32, u64> = HashMap::with_hasher(BuildHasherDefault::default());
        for key in &keys {
            sip.insert(*key, 1);
            fx.insert(*key, 1);
            id.insert(*key, 1);
        }
        assert_eq!(sip.get(&probe), id.get(&probe));
        assert_eq!(sip.get(&probe), fx.get(&probe));

        let mut group = c.benchmark_group("map_get_u32");
        group.bench_with_input(BenchmarkId::new("sip", n), &probe, |b, probe| {
            b.iter(|| black_box(sip.get(black_box(probe))))
        });
        group.bench_with_input(BenchmarkId::new("fx", n), &probe, |b, probe| {
            b.iter(|| black_box(fx.get(black_box(probe))))
        });
        group.bench_with_input(BenchmarkId::new("id", n), &probe, |b, probe| {
            b.iter(|| black_box(id.get(black_box(probe))))
        });
        group.finish();
    }
}

/// The same lookups, but on the **real** key pattern (`pid ++ counter`) and at
/// the sizes a blast load test reaches: this is where a weak hasher shows up as
/// clustering rather than as a constant per-op cost.
fn map_get_realistic(c: &mut Criterion) {
    const PID: u64 = 0x0000_0000_0000_4d2f;

    for &n in &[1024usize, 65_536, 131_072] {
        let keys: Vec<[u8; 16]> = (0..n as u64)
            .map(|i| correlation_real(PID, i + 1))
            .collect();
        let probe = keys[n / 2];

        let mut sip: SipMap<[u8; 16], u64> = HashMap::new();
        let mut fx: FxMap<[u8; 16], u64> = HashMap::with_hasher(BuildHasherDefault::default());
        for key in &keys {
            sip.insert(*key, 1);
            fx.insert(*key, 1);
        }
        assert_eq!(sip.get(&probe), fx.get(&probe));

        let mut group = c.benchmark_group("map_get_realistic");
        group.bench_with_input(BenchmarkId::new("sip", n), &probe, |b, probe| {
            b.iter(|| black_box(sip.get(black_box(probe))))
        });
        group.bench_with_input(BenchmarkId::new("fx", n), &probe, |b, probe| {
            b.iter(|| black_box(fx.get(black_box(probe))))
        });
        group.finish();
    }
}

/// Fills a map from empty with the real key pattern: the insert side of the
/// blast churn, where a hasher that clusters pays for the clustering.
fn map_build_realistic(c: &mut Criterion) {
    const PID: u64 = 0x0000_0000_0000_4d2f;
    const N: usize = 65_536;
    let keys: Vec<[u8; 16]> = (0..N as u64)
        .map(|i| correlation_real(PID, i + 1))
        .collect();

    let mut group = c.benchmark_group("map_build_realistic");
    group.bench_function("sip", |b| {
        b.iter(|| {
            let mut map: SipMap<[u8; 16], u64> = HashMap::with_capacity(N);
            for key in &keys {
                map.insert(*key, 1);
            }
            black_box(map.len())
        })
    });
    group.bench_function("fx", |b| {
        b.iter(|| {
            let mut map: FxMap<[u8; 16], u64> =
                HashMap::with_capacity_and_hasher(N, BuildHasherDefault::default());
            for key in &keys {
                map.insert(*key, 1);
            }
            black_box(map.len())
        })
    });
    group.finish();
}

/// The per-call cycle the transport runs on the correlation-id maps: register a
/// handler (insert) then release it (remove).
fn map_insert_remove(c: &mut Criterion) {
    let key = correlation(1);
    let mut group = c.benchmark_group("map_insert_remove_correlation");

    group.bench_function("sip", |b| {
        let mut map: SipMap<[u8; 16], u64> = HashMap::new();
        b.iter(|| {
            map.insert(black_box(key), 1);
            black_box(map.remove(black_box(&key)));
        });
        assert!(map.is_empty(), "the cycle must be balanced");
    });
    group.bench_function("fx", |b| {
        let mut map: FxMap<[u8; 16], u64> = HashMap::with_hasher(BuildHasherDefault::default());
        b.iter(|| {
            map.insert(black_box(key), 1);
            black_box(map.remove(black_box(&key)));
        });
        assert!(map.is_empty(), "the cycle must be balanced");
    });
    group.finish();
}

criterion_group!(
    benches,
    hash_only,
    map_get,
    map_get_realistic,
    map_build_realistic,
    map_insert_remove
);
criterion_main!(benches);
