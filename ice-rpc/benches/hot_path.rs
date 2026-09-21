//! Micro-benchmarks of the costs the generated client pays **per call**, and of
//! the clock read the notification coalescer pays **per call**.
//!
//! They exist to settle a question the end-to-end load benchmark cannot answer.
//! `scripts/bench-load.sh` measures a round trip at roughly 3 µs, and its
//! `sequential` mode is stable to about ±1.5 %: far too coarse to see a single
//! allocation or the `memcpy` of a 64-byte payload. Each group below isolates
//! one of them, so a change can be judged on its own rather than on a throughput
//! number whose noise is the same size as the effect.
//!
//! - `request_encoding` — the generated method called `to_bytes`, which
//!   allocates a buffer per call; the alternative reuses one buffer, emptied and
//!   handed back by `to_bytes_in` (the pattern the response path already uses);
//! - `response_decoding` — `decode_aligned` copies the payload into an aligned
//!   buffer before decoding, although the transport guarantees the alignment; the
//!   alternative decodes in place;
//! - `misc` — the cost of a clock read (`Instant::now()`), which bounds what the
//!   coalescer may spend per call before deciding to notify, and the cost of a
//!   `service_id_of("literal")` call site as the generated code writes it.
//!
//! Every pair is checked to **agree** before being measured, so a variant cannot
//! win by doing less work.
//!
//! Run with: `cargo bench -p ice-rpc --bench hot_path`.

#![allow(clippy::unwrap_used)] // tests/examples/benches may panic

use std::hint::black_box;
use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion};
use rkyv::api::high::to_bytes_in;
use rkyv::rancor::Error;
use rkyv::util::AlignedVec;

/// Request shape of the `db` demo service: one owned field.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct Request {
    key: String,
}

/// Reply shape of the same service.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct Reply {
    value: i32,
}

fn request() -> Request {
    Request {
        key: "Alice".to_owned(),
    }
}

/// Takes the buffer out, leaving behind an empty, allocation-free placeholder.
///
/// This is what the production code does to a thread-local buffer: the
/// allocation travels through the encoder and comes back, instead of being
/// recreated.
fn take_buffer(buffer: &mut AlignedVec<16>) -> AlignedVec<16> {
    std::mem::replace(buffer, AlignedVec::<16>::with_capacity(0))
}

/// What the generated client method did: a fresh buffer per call.
fn encode_per_call(req: &Request) -> AlignedVec<16> {
    rkyv::to_bytes::<Error>(req).unwrap()
}

/// What it does with a reusable buffer: emptied, then handed back by the call.
fn encode_reused(req: &Request, buffer: AlignedVec<16>) -> AlignedVec<16> {
    let mut buffer = buffer;
    buffer.clear();
    let result: Result<AlignedVec<16>, Error> = to_bytes_in(req, buffer);
    result.unwrap()
}

/// What `decode_aligned` did: copy into an aligned buffer, then decode.
fn decode_copied(bytes: &[u8]) -> Reply {
    let mut aligned = AlignedVec::<16>::with_capacity(bytes.len());
    aligned.extend_from_slice(bytes);
    rkyv::from_bytes::<Reply, Error>(&aligned).unwrap()
}

/// What it does when the payload is already aligned: decode in place.
fn decode_in_place(bytes: &[u8]) -> Reply {
    rkyv::from_bytes::<Reply, Error>(bytes).unwrap()
}

fn bench_request_encoding(c: &mut Criterion) {
    let req = request();

    // A buffer is one pointer, one length and one capacity, so the two variants
    // must encode the same bytes — not merely both succeed.
    let per_call = encode_per_call(&req);
    let mut buffer = AlignedVec::<16>::with_capacity(256);
    let reused = encode_reused(&req, take_buffer(&mut buffer));
    assert_eq!(
        per_call.as_slice(),
        reused.as_slice(),
        "the reused buffer must produce the same encoding"
    );
    buffer = reused;

    let mut group = c.benchmark_group("request_encoding");
    group.bench_function("allocated_per_call", |b| {
        b.iter(|| black_box(encode_per_call(black_box(&req))))
    });
    group.bench_function("reused_buffer", |b| {
        b.iter(|| {
            let taken = take_buffer(&mut buffer);
            buffer = encode_reused(black_box(&req), taken);
            black_box(&buffer);
        })
    });
    group.finish();
}

fn bench_response_decoding(c: &mut Criterion) {
    let bytes = rkyv::to_bytes::<Error>(&Reply { value: 42 }).unwrap();
    // The premise of the in-place variant: `to_bytes` hands back a buffer aligned
    // the same way the transport aligns a sample payload.
    assert_eq!(
        bytes.as_ptr() as usize % 16,
        0,
        "the fixture must be 16-byte aligned, like a sample payload"
    );
    assert_eq!(
        decode_copied(&bytes).value,
        black_box(decode_in_place(&bytes)).value,
        "both variants must decode the same value"
    );

    let aligned = bytes.as_slice();
    let mut group = c.benchmark_group("response_decoding");
    group.bench_function("copied_then_decoded", |b| {
        b.iter(|| black_box(decode_copied(black_box(aligned))))
    });
    group.bench_function("decoded_in_place", |b| {
        b.iter(|| black_box(decode_in_place(black_box(aligned))))
    });
    group.finish();
}

/// The per-poll costs PF-1 targets: what a handler pays at **every** poll of its
/// task, before anything is cancelled.
///
/// These are the numbers that decide whether PF-1 is worth doing, so they are
/// measured one by one, in nanoseconds. The end-to-end benches cannot settle
/// this: a 3 % change is below their noise floor (see `pipeline`), and a per-poll
/// saving of a few nanoseconds disappears entirely in a 3 µs round trip. What the
/// end-to-end bench will be good for is confirming the absence of a regression.
///
/// `poll_cancelled_already_cancelled` is the floor the nominal path should reach:
/// it is the same call with the flag already set, i.e. the cost of one relaxed
/// load and a branch. The gap between it and `poll_cancelled_pending` is the
/// price of the `Mutex` and the waker-list scan on the nominal path — the whole
/// subject of PF-1.
fn bench_per_poll(c: &mut Criterion) {
    use std::task::{Context, Waker};

    let header = ice_rpc::gen::RpcHeader::request("get_user_age", 7, 1);
    let ctx = ice_rpc::CallContext::new(&header, "Calculator", "get_user_age");

    let token = ice_rpc::CancellationToken::new();
    let cancelled = ice_rpc::CancellationToken::new();
    cancelled.cancel();

    let mut group = c.benchmark_group("per_poll");

    // The thread-local swap of `CallContext::enter_ambient`, installed around
    // every poll of a handler task, plus its restore on drop.
    group.bench_function("ambient_install_restore", |b| {
        b.iter(|| {
            let scope = black_box(ctx).enter_ambient();
            drop(scope);
        })
    });

    // The `Arc` clone that `install_call_cancellation` performs per poll, and the
    // atomic decrement of its drop.
    group.bench_function("cancellation_token_clone_drop", |b| {
        b.iter(|| black_box(token.clone()))
    });

    // The nominal `poll_cancelled`: a `Mutex` lock and a scan of the waker list,
    // for a token that is not cancelled.
    group.bench_function("poll_cancelled_pending", |b| {
        let mut cx = Context::from_waker(Waker::noop());
        b.iter(|| black_box(token.poll_cancelled(&mut cx)))
    });

    // The floor: the same call once the token is cancelled.
    group.bench_function("poll_cancelled_already_cancelled", |b| {
        let mut cx = Context::from_waker(Waker::noop());
        b.iter(|| black_box(cancelled.poll_cancelled(&mut cx)))
    });

    // What PF-1 brings to the nominal path: the waker the task registered is
    // cached, so the lock and the waker-list scan are gone. The gap with
    // `poll_cancelled_pending` above is exactly what the change removes from a
    // task that polls on every poll.
    group.bench_function("poll_cancelled_cached_steady_state", |b| {
        let mut cx = Context::from_waker(Waker::noop());
        let mut registered = None;
        assert!(
            token
                .poll_cancelled_cached(&mut cx, &mut registered)
                .is_pending(),
            "the token must not be cancelled"
        );
        assert!(
            registered.is_some(),
            "the priming poll must have cached the waker"
        );
        b.iter(|| black_box(token.poll_cancelled_cached(&mut cx, &mut registered)))
    });

    group.finish();
}

fn bench_misc(c: &mut Criterion) {
    let mut group = c.benchmark_group("misc");

    // Lower bound of what `Coalescer::should_notify` costs per call: it reads
    // this clock, then two relaxed atomics.
    group.bench_function("clock_read", |b| {
        b.iter(|| black_box(Instant::now().elapsed().as_micros()))
    });

    // `Instant::now().elapsed()` calls `Instant::now()` a **second** time, so the
    // version that keeps its own base reads the clock once instead of twice.
    group.bench_function("clock_read_once", |b| {
        let base = Instant::now();
        b.iter(|| black_box(Instant::now().saturating_duration_since(base).as_micros()))
    });

    // `black_box` around the call prevents the constant folding that LTO would
    // otherwise apply, so this is the cost of the call **before** optimization:
    // an upper bound for the call sites of the generated code.
    group.bench_function("service_id_of_literal_call", |b| {
        b.iter(|| black_box(ice_rpc::gen::service_id_of("calculator")))
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_request_encoding,
    bench_response_decoding,
    bench_per_poll,
    bench_misc
);
criterion_main!(benches);
