//! Micro-benchmark: what `handle_request` does between the sample and the task.
//!
//! Two costs sit there, before the dispatcher is even called:
//!
//! ```text
//! let emitter: OwnedEmitter = Box::new(hub.emitter(header));
//! dispatcher.dispatch(method_id, *header, payload.to_vec(), emitter)
//! ```
//!
//! **The payload copy.** [`handle_request`](crate::transport) receives a payload
//! that lives in an iceoryx2 sample — released as soon as the receive loop moves
//! on — and hands it to a task that **outlives** the sample. The copy buys that
//! decoupling. Four strategies are priced:
//!
//! - `decode_borrowed` — rkyv decodes straight from the sample: no allocation for
//!   the payload, the floor a redesign could aim at. Only reachable if the sample
//!   is carried *into* the future, so the chunk stays loaned for the whole call.
//! - `copy_then_decode` — what the transport does today: one `to_vec()`, then the
//!   rkyv decode the generated handler performs anyway.
//! - `copy_only` — the copy alone, to read the gain of removing just the
//!   allocation while keeping an owned buffer.
//! - `pooled_buffer` — a recycled `Vec` (cleared and refilled) instead of a fresh
//!   one: keeps the owned buffer, drops the `malloc`/`free` traffic.
//!
//! Every payload is a real rkyv-encoded request, and the three sizes bracket the
//! demo services: a small query, a middle one, and a 4 KiB blob where the `memcpy`
//! starts to dominate.
//!
//! **The emitter box.** `CallEmitter` is private to the transport, but its layout
//! is public knowledge — an `Arc` hub plus the request header — and so is the
//! `Box<dyn ResponseEmitter + Send>` it is put behind. [`EmitterLike`] reproduces
//! that layout, and the three variants price the box against carrying the same
//! data inline in an enum (`Emitter::Call(..)`), which is what removing the
//! allocation would look like. The two `emit_*` variants check the refactor does
//! not move the cost to the call site: a `match` against a vtable call.
//!
//! Run with: `cargo bench -p ice-rpc --bench handler_handoff`.

#![allow(clippy::unwrap_used)] // tests/examples/benches may panic

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rkyv::rancor::Error;
use rkyv::util::AlignedVec;

use ice_rpc::gen::{EventKind, OwnedEmitter, ResponseEmitter, RpcHeader};

/// The header travels **inline** in the emitter, so its size is the copy the
/// builder pays per request. Pinned: the bench's model would be wrong otherwise.
const _: () = assert!(std::mem::size_of::<RpcHeader>() == 80);

/// A request shape close to the demos': one owned field.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct Request {
    key: String,
}

/// Encodes a request of roughly `size` bytes.
fn sample(size: usize) -> AlignedVec<16> {
    let key = "k".repeat(size.saturating_sub(40));
    rkyv::to_bytes::<Error>(&Request { key }).unwrap()
}

/// Stand-in for the transport's private `CallEmitter`: same field types, same
/// layout, same trait object.
struct EmitterLike {
    hub: Arc<u64>,
    request: RpcHeader,
}

impl ResponseEmitter for EmitterLike {
    fn emit(&mut self, kind: EventKind, _payload: &[u8]) -> bool {
        // Reads both fields, so the compiler keeps the layout the bench prices.
        // Identical on both sides of every comparison.
        black_box(Arc::strong_count(&self.hub));
        self.request.event_kind() == kind
    }
}

/// The candidate shape: the call sink inline, the type-erased one boxed.
enum Emitter {
    Call(EmitterLike),
    Boxed(Box<dyn ResponseEmitter + Send>),
}

impl ResponseEmitter for Emitter {
    fn emit(&mut self, kind: EventKind, payload: &[u8]) -> bool {
        match self {
            Emitter::Call(emitter) => emitter.emit(kind, payload),
            Emitter::Boxed(emitter) => emitter.emit(kind, payload),
        }
    }
}

/// The four hand-off strategies, on one payload.
fn handoff(c: &mut Criterion) {
    let mut group = c.benchmark_group("handler_handoff");

    for target in [64usize, 256, 4096] {
        let bytes = sample(target);
        let label = format!("{}B", bytes.len());
        group.throughput(Throughput::Bytes(bytes.len() as u64));

        group.bench_with_input(
            BenchmarkId::new("decode_borrowed", &label),
            &bytes,
            |b, bytes| {
                b.iter(|| {
                    black_box(rkyv::from_bytes::<Request, Error>(black_box(bytes)).unwrap());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("copy_then_decode", &label),
            &bytes,
            |b, bytes| {
                b.iter(|| {
                    let owned = bytes.to_vec();
                    black_box(rkyv::from_bytes::<Request, Error>(&owned).unwrap());
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("copy_only", &label), &bytes, |b, bytes| {
            b.iter(|| {
                black_box(bytes.to_vec());
            });
        });

        group.bench_with_input(
            BenchmarkId::new("pooled_buffer", &label),
            &bytes,
            |b, bytes| {
                // A buffer held across iterations: the `malloc`/`free` of
                // `to_vec()` disappears, the copy does not. A thread-local pool
                // is this plus a TLS lookup.
                let mut scratch = Vec::with_capacity(bytes.len());
                b.iter(|| {
                    scratch.clear();
                    scratch.extend_from_slice(bytes);
                    black_box(rkyv::from_bytes::<Request, Error>(&scratch).unwrap());
                });
            },
        );
    }

    group.finish();
}

/// Building the emitter, then emitting through it.
fn emitter(c: &mut Criterion) {
    let mut group = c.benchmark_group("emitter");
    let hub = Arc::new(0u64);
    let request = RpcHeader::default();

    // What the transport does per request today: one heap allocation, one atomic
    // increment, one 80-byte copy, then the `free` when the task drops it.
    group.bench_function("box_dyn", |b| {
        b.iter(|| {
            let emitter: OwnedEmitter = Box::new(EmitterLike {
                hub: Arc::clone(&hub),
                request,
            });
            black_box(emitter);
        });
    });

    // The candidate: the same data, inline in the enum. No allocation.
    group.bench_function("enum_inline", |b| {
        b.iter(|| {
            black_box(Emitter::Call(EmitterLike {
                hub: Arc::clone(&hub),
                request,
            }));
        });
    });

    // The variant the enum must still carry: a handler a test built
    // (`CollectEmitter`) is boxed, and that path pays what the current one pays.
    group.bench_function("enum_boxed_variant", |b| {
        b.iter(|| {
            black_box(Emitter::Boxed(Box::new(EmitterLike {
                hub: Arc::clone(&hub),
                request,
            })));
        });
    });

    // The floor the candidate still pays.
    group.bench_function("arc_clone_and_copy", |b| {
        b.iter(|| {
            black_box(EmitterLike {
                hub: Arc::clone(&hub),
                request,
            });
        });
    });

    // The call site: a `match` must not cost more than a vtable call.
    group.bench_function("emit_dyn", |b| {
        let mut emitter: OwnedEmitter = Box::new(EmitterLike {
            hub: Arc::clone(&hub),
            request,
        });
        b.iter(|| black_box(emitter.emit(EventKind::Complete, &[])));
    });
    group.bench_function("emit_enum", |b| {
        let mut emitter = Emitter::Call(EmitterLike {
            hub: Arc::clone(&hub),
            request,
        });
        b.iter(|| black_box(emitter.emit(EventKind::Complete, &[])));
    });

    group.finish();
}

criterion_group!(benches, handoff, emitter);
criterion_main!(benches);
