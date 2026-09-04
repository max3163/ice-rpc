//! Micro-benchmarks for the concurrency-sensitive primitives of `ice-rpc`.
//!
//! These benchmarks target the patterns modified in the recent refactors:
//! - the server scratch serialization buffer (shared vs local under contention);
//! - the `NodeSupervisor` broadcast;
//! - the `ReconnectManager` per-service deduplication.
//!
//! Run with: `cargo bench -p ice-rpc`.

use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

use ice_rpc::async_lock::Mutex as AsyncMutex;
use ice_rpc::futures_lite::future::block_on;
use ice_rpc::gen::{NodeSupervisor, PendingService, ReconnectCallback, ReconnectManager};
use ice_rpc::rkyv::api::high::to_bytes_in;
use ice_rpc::rkyv::rancor::Error as RkyvError;
use ice_rpc::rkyv::util::AlignedVec;
use ice_rpc::rkyv::{Archive, Deserialize, Serialize};

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
///
/// Corresponds to: the `to_bytes_in(&event, &mut *guard)` step emitted by the
/// server codegen when a single RPC is in flight.
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
/// "one buffer per RPC task" architecture recommended for the server scratch,
/// where each task reuses its local `AlignedVec` without any lock.
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
/// This mirrors the historical server scratch pattern, where every RPC task
/// contended on one global buffer. It quantifies the serialization cost under
/// contention and is the reference used to detect the regression fixed by
/// `gen_server_match_arm` (the lock is now held only during serialize -> send).
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
                serialize_into(&mut *guard);
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
/// Purpose: measure how rkyv serialization scales when each worker owns its
/// own buffer. Comparing `serialize/local_N` against `serialize/shared_N`
/// reveals the contention cost of a shared scratch.
///
/// Corresponds to: the recommended per-task/RPC server buffer design.
fn bench_serialize_local(c: &mut Criterion) {
    for threads in [2usize, 4, 8] {
        c.bench_function(&format!("serialize/local_{}", threads), |b| {
            b.iter(|| run_local(threads));
        });
    }
}

/// Concurrent serialization through a shared scratch buffer.
///
/// Purpose: measure the throughput collapse when N threads serialize through
/// one `AsyncMutex<AlignedVec>`. The larger the gap with `serialize/local_N`,
/// the more important the per-task buffer optimization is.
///
/// Corresponds to: the old generated server code that acquired the scratch
/// mutex around the business call and kept it for the whole stream duration.
fn bench_serialize_shared(c: &mut Criterion) {
    for threads in [2usize, 4, 8] {
        c.bench_function(&format!("serialize/shared_{}", threads), |b| {
            b.iter(|| run_shared(threads));
        });
    }
}

/// Broadcast of a node-death event to N subscribers.
///
/// Purpose: measure the cost of `notify_node_dead` (subscribe + clone + fire)
/// as the number of clients watching the same remote node grows.
///
/// Corresponds to: the `NodeSupervisor` introduced to fix the multi-service
/// reconnection bug, where a dead node must notify every subscribed client
/// instead of a single per-NodeId callback.
fn bench_node_supervisor_broadcast(c: &mut Criterion) {
    for subscribers in [1usize, 8, 64] {
        c.bench_function(&format!("node_supervisor/broadcast_{}", subscribers), |b| {
            b.iter(|| {
                let supervisor = NodeSupervisor::global();
                let node_id = unique_node_id();
                let mut subs = Vec::with_capacity(subscribers);
                for _ in 0..subscribers {
                    let cb: ReconnectCallback = Arc::new(|_: u32| {});
                    subs.push(supervisor.subscribe(node_id, cb));
                }
                supervisor.notify_node_dead(node_id);
                drop(subs);
            });
        });
    }
}

/// Deduplication of N pending services/instances on the same dead node.
///
/// Purpose: measure the cost of `ReconnectManager::insert_pending` when many
/// services or instances register for reconnection against one node. The
/// deduplication is by `Arc` identity, so distinct instances of the same
/// service are all accepted.
///
/// Corresponds to: the centralized retry manager that replaced the previous
/// one-thread-per-service reconnection loop.
fn bench_reconnect_manager_dedup(c: &mut Criterion) {
    for services in [1usize, 16, 128] {
        c.bench_function(&format!("reconnect_manager/dedup_{}", services), |b| {
            b.iter(|| {
                let manager = ReconnectManager::new();
                for _ in 0..services {
                    let service = Arc::new(PendingService::new(
                        "bench-service",
                        Arc::new(AtomicU64::new(0)),
                        Arc::new(AtomicBool::new(false)),
                        Arc::new(AtomicBool::new(false)),
                    ));
                    manager.insert_pending(1, service);
                }
            });
        });
    }
}

/// Produces a unique node id per iteration to avoid cross-iteration collisions.
fn unique_node_id() -> u32 {
    static NEXT: AtomicU32 = AtomicU32::new(0x0BEE_0000);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

criterion_group!(
    benches,
    bench_serialize_single,
    bench_serialize_local,
    bench_serialize_shared,
    bench_node_supervisor_broadcast,
    bench_reconnect_manager_dedup,
);
criterion_main!(benches);
