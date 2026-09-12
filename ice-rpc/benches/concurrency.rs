//! Micro-benchmarks for the concurrency-sensitive primitives of `ice-rpc`.
//!
//! These benchmarks target the rkyv serialization path used by the native
//! request/response transport, comparing a per-RPC local buffer against a
//! shared scratch buffer under contention.
//!
//! Run with: `cargo bench -p ice-rpc`.

#![allow(clippy::unwrap_used)] // tests/examples/benches may panic
use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::{Arc, Barrier};

use ice_rpc::gen::async_lock::Mutex as AsyncMutex;
use ice_rpc::gen::futures_lite::future::block_on;
use ice_rpc::gen::rkyv::api::high::to_bytes_in;
use ice_rpc::gen::rkyv::rancor::Error as RkyvError;
use ice_rpc::gen::rkyv::util::AlignedVec;
use ice_rpc::gen::rkyv::{Archive, Deserialize, Serialize};

/// Serializations executed per thread in each iteration.
const PER_THREAD: usize = 10_000;

#[derive(Archive, Serialize, Deserialize)]
struct BenchPayload {
    id: u64,
    values: [u32; 16],
}

fn payload() -> BenchPayload {
    BenchPayload {
        id: 0x1234_5678_9abc_def0,
        values: [7; 16],
    }
}

fn serialize_into(buf: &mut AlignedVec<8>) {
    let _ = to_bytes_in::<_, RkyvError>(&payload(), buf);
}

/// Single-threaded baseline for the rkyv serialization path.
///
/// Purpose: establish the reference cost of `to_bytes_in` into a reusable
/// `AlignedVec<8>` with no contention. Every concurrent serialization
/// benchmark is compared against this baseline.
fn bench_serialize_single(c: &mut Criterion) {
    c.bench_function("serialize/single", |b| {
        b.iter(|| {
            let mut buf = AlignedVec::<8>::with_capacity(4096);
            for _ in 0..PER_THREAD {
                buf.clear();
                serialize_into(&mut buf);
            }
        });
    });
}

/// Runs `PER_THREAD` serializations per thread, each thread owning its own
/// buffer (no shared state).
///
/// This is the contention-free concurrent pattern: it represents the
/// "one buffer per RPC task" architecture, where each task reuses its local
/// `AlignedVec` without any lock.
fn run_local(threads: usize) {
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut handles = Vec::with_capacity(threads);
    for _ in 0..threads {
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let mut buf = AlignedVec::<8>::with_capacity(4096);
            barrier.wait();
            for _ in 0..PER_THREAD {
                buf.clear();
                serialize_into(&mut buf);
            }
        }));
    }
    barrier.wait();
    for handle in handles {
        let _ = handle.join();
    }
}

/// Runs `PER_THREAD` serializations per thread through a single shared
/// `AsyncMutex<AlignedVec<8>>` buffer.
///
/// It quantifies the serialization cost under contention when every RPC task
/// shares one global buffer.
fn run_shared(threads: usize) {
    let scratch = Arc::new(AsyncMutex::new(AlignedVec::<8>::with_capacity(4096)));
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut handles = Vec::with_capacity(threads);
    for _ in 0..threads {
        let scratch = scratch.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            for _ in 0..PER_THREAD {
                let mut guard = block_on(scratch.lock());
                guard.clear();
                serialize_into(&mut guard);
            }
        }));
    }
    barrier.wait();
    for handle in handles {
        let _ = handle.join();
    }
}

/// Concurrent serialization with a per-thread (per-task) local buffer.
///
/// Comparing `serialize/local_N` against `serialize/shared_N` reveals the
/// contention cost of a shared scratch.
fn bench_serialize_local(c: &mut Criterion) {
    for threads in [2usize, 4, 8] {
        c.bench_function(&format!("serialize/local_{}", threads), |b| {
            b.iter(|| run_local(threads));
        });
    }
}

/// Concurrent serialization through a shared scratch buffer.
fn bench_serialize_shared(c: &mut Criterion) {
    for threads in [2usize, 4, 8] {
        c.bench_function(&format!("serialize/shared_{}", threads), |b| {
            b.iter(|| run_shared(threads));
        });
    }
}

criterion_group!(
    benches,
    bench_serialize_single,
    bench_serialize_local,
    bench_serialize_shared,
);
criterion_main!(benches);
