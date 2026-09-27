//! Micro-benchmark: what `ipc_threadsafe`'s `MutexProtected` costs on a real
//! iceoryx2 port, against `ipc`'s `SingleThreaded`.
//!
//! `perf` attributes roughly 7 % of the provider's cycles to the mutex cluster
//! (`MutexProtected::lock`, the guards it drops, `pthread_mutex_lock/unlock`),
//! even though the payload channel is a lock-free SPSC ring and the provider
//! reads it from a single thread. The lock is not on the sample:
//! `ArcThreadSafetyPolicy` guards the **port handle** — the subscriber's
//! connection list, the publisher's returned-chunk queue, and the reference
//! count each `Sample` carries back. `ipc_threadsafe::Service` selects
//! `MutexProtected<T>`, `ipc::Service` selects `SingleThreaded<T>`, i.e. an
//! `Rc<T>`.
//!
//! Both flavours get the same two services, and the same two measurements:
//!
//! - `receive_empty` — `Subscriber::receive()` on an empty queue. This is where
//!   a lock is least defensible: `receive_impl` takes the shared state **twice**
//!   per call (`update_connections()` then `receive()`), on a thread that is
//!   alone. The delta between the two flavours is that pair of locks.
//! - `provider_cycle` — one `loan_slice_uninit` + `write_from_slice` + `send`,
//!   then the matching `receive`: the exact per-call sequence of the provider.
//!   The delta is the whole port cost of a call, publishing and receiving.
//!
//! Both use `criterion`'s plain `iter`: every iteration is one sample in flight,
//! which keeps the publisher's chunk pool balanced on the one thread both
//! flavours are confined to. Batching the publications first (`iter_batched`)
//! exhausts the pool — the returned chunks are only reclaimed lazily.
//!
//! `ipc` ports are `!Send`/`!Sync` — which is why the transport cannot use them
//! today, its ports live in process-wide `Arc`s. This bench only prices the gap;
//! it does not claim the switch is available.
//!
//! Run with: `cargo bench -p ice-rpc --bench iox_port_mutex`.

#![allow(clippy::unwrap_used)] // tests/examples/benches may panic

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use iceoryx2::prelude::*;
use iceoryx2::service::{ipc, ipc_threadsafe};

use ice_rpc::gen::RpcHeader;

/// The alignment the transport requests for its payloads.
const PAYLOAD_ALIGNMENT: usize = 16;

/// What the load benchmark moves per request.
const PAYLOAD_LEN: usize = 64;

/// Opens a node, a pub/sub service and both ports, for one service flavour.
///
/// The service name carries the pid, so a run never inherits the state left by a
/// run that was killed, and each flavour gets its own name: two flavours are two
/// distinct services on the bus.
macro_rules! open_ports {
    ($variant:literal, $svc:ty) => {{
        let node = NodeBuilder::new().create::<$svc>().unwrap();
        let name = ServiceName::new(&format!(
            "/ice_rpc_bench/{variant}/{pid}",
            variant = $variant,
            pid = std::process::id()
        ))
        .unwrap();
        let service = node
            .service_builder(&name)
            .publish_subscribe::<[u8]>()
            .user_header::<RpcHeader>()
            .payload_alignment(Alignment::new(PAYLOAD_ALIGNMENT).unwrap())
            .enable_safe_overflow(false)
            .open_or_create()
            .unwrap();
        // `initial_max_slice_len` is what the transport passes too: the service's
        // own default is 1, so a 64-byte payload would be refused by `loan`.
        let publisher = service
            .publisher_builder()
            .initial_max_slice_len(PAYLOAD_LEN)
            .create()
            .unwrap();
        let subscriber = service.subscriber_builder().create().unwrap();
        // Connect the two once, outside any measurement: the first `receive` is
        // the one that walks the publisher list.
        let _ = subscriber.receive();
        (node, publisher, subscriber)
    }};
}

/// `try_publish`'s sequence, verbatim: loan, write, send.
macro_rules! publish_one {
    ($publisher:expr, $payload:expr) => {{
        let sample = $publisher.loan_slice_uninit($payload.len()).unwrap();
        let mut sample = sample.write_from_slice($payload);
        *sample.user_header_mut() = RpcHeader::default();
        black_box(sample.send().unwrap())
    }};
}

fn receive_empty(c: &mut Criterion) {
    let mut group = c.benchmark_group("iox_port_mutex/receive_empty");

    {
        let (_node, _publisher, subscriber) =
            open_ports!("threadsafe_empty", ipc_threadsafe::Service);
        group.bench_function("ipc_threadsafe", |b| {
            b.iter(|| black_box(subscriber.receive()));
        });
    }
    {
        let (_node, _publisher, subscriber) = open_ports!("single_empty", ipc::Service);
        group.bench_function("ipc", |b| {
            b.iter(|| black_box(subscriber.receive()));
        });
    }

    group.finish();
}

fn provider_cycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("iox_port_mutex/provider_cycle");
    let payload = [7u8; PAYLOAD_LEN];

    {
        let (_node, publisher, subscriber) =
            open_ports!("threadsafe_cycle", ipc_threadsafe::Service);
        group.bench_function("ipc_threadsafe", |b| {
            b.iter(|| {
                publish_one!(&publisher, &payload);
                black_box(subscriber.receive().unwrap());
            });
        });
    }
    {
        let (_node, publisher, subscriber) = open_ports!("single_cycle", ipc::Service);
        group.bench_function("ipc", |b| {
            b.iter(|| {
                publish_one!(&publisher, &payload);
                black_box(subscriber.receive().unwrap());
            });
        });
    }

    group.finish();
}

criterion_group!(benches, receive_empty, provider_cycle);
criterion_main!(benches);
