//! Call tracing over ice-rpc: one span per provider call, propagated hop by hop.
//!
//! Two services are hosted by the same provider process:
//!
//! - **InventoryService** (the leaf): `reserve(item) -> u32`;
//! - **OrderService**: `place_order(item) -> Receipt`, which reserves through the
//!   transport *before* it answers.
//!
//! The point of the example is what the provider writes, on the two hops of one
//! consumer call:
//!
//! ```text
//! [ctx] OrderService.place_order      corr=<c1> trace=<T> span=0x<A> parent=0x0
//! [ctx] InventoryService.reserve      corr=<c2> trace=<T> span=0x<B> parent=0x<A>
//! ```
//!
//! Both hops carry the same trace id `<T>` — they belong to the same call tree —
//! and the leaf parents on the span of the hop that emitted it (`parent = <A>`).
//! The chain is reconstructed without the implementation passing anything around:
//! the context is *ambient* for the duration of the handler, and the client reads
//! it to fill the request header of its own call.
//!
//! Run the provider in one terminal:
//!
//! ```bash
//! cargo run -p ice-rpc --example tracing-demo --features tokio,tracing -- provider
//! ```
//!
//! and the consumer in another:
//!
//! ```bash
//! cargo run -p ice-rpc --example tracing-demo --features tokio,tracing -- consumer
//! ```
//!
//! `RUST_LOG` drives the subscriber (default: `info`).
//!
//! On Windows, `iceoryx2` prints `< Win32 API error >` lines on stderr while it
//! scans the shared-memory directory: they come from `iceoryx2-pal-posix` and have
//! nothing to do with this demo.
//!
//! # The two halves of the feature
//!
//! The `[ctx]` lines above come from [`CallContext::current`], and they work with
//! the `tracing` feature **off**: the generated handler always installs the
//! ambient context, so a deployment that collects no spans still gets the ids in
//! its ordinary logs.
//!
//! The `tracing` feature adds the other half: the same handler enters one
//! `tracing` span per call (`rpc`, carrying the service, the version, the method,
//! the correlation id, the trace and span ids and the parent), so every `tracing`
//! **and** `log` event the implementation emits is attached to the call without it
//! writing anything. The `log` side works because the subscriber installed below
//! bridges `log` records into the current span.
//!
//! The caller side gets no span of its own on purpose: the ids travel in the
//! request header, and the span a caller wants around its own work is its own
//! business.
//!
//! # The second hop is a direct call
//!
//! [`OrderServiceImpl::place_order`] delegates to a service hosted by the **same
//! process**, which it resolves through the locator the way any service reaches
//! another. That call carries no header, so it used to be the one hop a trace
//! could not see: the leaf read the caller's ids as if they were its own. With
//! `tracing` on, the generated proxy now gives it its own `CallContext` — the
//! leaf's service and method — and a span parented on the caller's, marked
//! `kind = "local"` so an observer does not count it as a network round trip.
//!
//! Because the leaf is registered in this process, the locator hands back the
//! **Provider** proxy and the call stays here. Were it not registered, the locator
//! would build a `Consumer` proxy and the same call would cross the transport — a
//! span without `kind = "local"`.
//!
//! With `tracing` off, that hop is exactly what it was: a plain in-process call,
//! no context of its own, no allocation.

#![allow(missing_docs)] // test/example target: documented by Readme.md, not part of a published API
#![allow(clippy::unwrap_used)] // tests/examples/benches may panic
use async_trait::async_trait;
use ice_rpc::{service, CallContext, Observable, ServiceInit};
use rkyv::{Archive, Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, Ordering};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

/// Acknowledgment returned by [`OrderService::place_order`].
#[derive(
    Debug, Clone, PartialEq, Archive, Serialize, Deserialize, serde::Serialize, serde::Deserialize,
)]
pub struct Receipt {
    /// The item that was ordered.
    pub item: String,
    /// The reservation the leaf handed out.
    pub reservation: u32,
}

/// The leaf: hands out one reservation per item.
#[service("InventoryService")]
pub trait InventoryService {
    /// Reserves `item` and returns the reservation number.
    async fn reserve(&self, item: String) -> Observable<u32, String>;
}

/// The service the consumer calls; it reserves through the transport.
#[service("OrderService")]
pub trait OrderService {
    /// Places an order for `item`.
    async fn place_order(&self, item: String) -> Observable<Receipt, String>;
}

/// Leaf implementation: a counter, nothing else.
struct InventoryServiceImpl {
    next_reservation: AtomicU32,
}

impl InventoryServiceImpl {
    fn new() -> Self {
        Self {
            next_reservation: AtomicU32::new(1000),
        }
    }
}

#[async_trait]
impl InventoryService for InventoryServiceImpl {
    async fn reserve(&self, item: String) -> Observable<u32, String> {
        // Second hop: the trace was carried by the request header, and the parent
        // is the span of the call that emitted it.
        report("InventoryService", "reserve");
        log::info!("[inventory] reserving '{item}'");

        let reservation = self.next_reservation.fetch_add(1, Ordering::Relaxed);
        ice_rpc::of(reservation)
    }
}

/// Implementation of the service that calls its dependency.
///
/// It holds no proxy of its own: it asks the locator for the leaf, the way any
/// service reaches another. Because the leaf is hosted by this very process, the
/// locator hands back the **Provider** proxy, so the call stays in-process — the
/// direct hop this example is about.
struct OrderServiceImpl;

#[async_trait]
impl OrderService for OrderServiceImpl {
    async fn place_order(&self, item: String) -> Observable<Receipt, String> {
        // First hop: whatever the consumer sent. `parent` is zero — the consumer
        // started the trace, it did not continue one.
        report("OrderService", "place_order");
        log::info!("[order] placing an order for '{item}'");

        // Resolved here, by the locator, rather than threaded in from outside.
        // This is the difference that matters for the trace: a leaf registered in
        // *this* process comes back as a `Provider` proxy, so the call below is a
        // direct, in-process hop — `kind = "local"`. Had it not been registered,
        // the locator would have built a `Consumer` proxy instead, and the same
        // call would have crossed the transport.
        let inventory = ice_rpc::locator()
            .get::<InventoryServiceProxy>()
            .await
            .expect("InventoryService is hosted here, and declared as a dependency");

        // The child call. The client reads the ambient context — still installed,
        // the handler has not returned — and parents the outgoing trace on the
        // span of *this* call.
        let reserved = inventory.reserve(item.clone()).await.first_value().await;

        let reservation = match reserved {
            Ok(value) => value,
            Err(error) => {
                log::error!("[order] the inventory refused '{item}': {error}");
                return ice_rpc::throw_error(format!("inventory refused '{item}': {error}"));
            }
        };

        log::info!("[order] '{item}' reserved as #{reservation}");
        ice_rpc::of(Receipt { item, reservation })
    }
}

/// Declares the hop of this example to the locator.
///
/// [`dependencies`](ServiceInit::dependencies) is what puts `InventoryService`
/// **before** `OrderService` in the topological order of `initialize_all`: the leaf
/// is initialized first, so the locator has it registered by the time the first
/// order arrives. Without this line the order would depend on the registration
/// order, and a call arriving early could silently resolve to a `Consumer` proxy.
#[async_trait]
impl ServiceInit for OrderServiceImpl {
    fn dependencies(&self) -> Vec<&'static str> {
        vec![InventoryServiceProxy::SERVICE_NAME]
    }
}

/// Prints the ambient context of the call being served, if any.
///
/// Rendering allocates, which is exactly why the framework does not do it for
/// you: this is application logging, not the hot path.
fn report(service: &str, method: &str) {
    match CallContext::current() {
        Some(ctx) => {
            let trace = ctx.trace();
            log::info!(
                "[ctx] {service}.{method}: corr={} trace={} span={:#018x} parent={:#018x}",
                ctx.correlation(),
                trace.trace_id_hex(),
                ctx.span_id(),
                trace.parent_span_id,
            );
        }
        // Outside a handler, and inside a task the implementation spawned itself:
        // the context is ambient, not global.
        None => log::info!("[ctx] {service}.{method}: no ambient context"),
    }
}

/// Installs the subscriber the whole demo is read through.
///
/// `with_span_events` prints the entry and the exit of every span, which is what
/// makes "one span per call" visible. The other half comes for free: the
/// subscriber bridges the `log` records the implementation already writes into
/// the span that is current when they are emitted.
fn init_subscriber() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
        .init();
}

/// Hosts the leaf and the service that consumes it.
async fn run_provider() -> Result<(), Box<dyn std::error::Error>> {
    ice_rpc::run_provider!(
        InventoryServiceProxy::provide(InventoryServiceImpl::new()),
        OrderServiceProxy::provide_with_init(OrderServiceImpl),
    )
    .await
}

/// Places two orders against the provider, then returns.
async fn run_consumer() {
    let orders = OrderServiceProxy::consume();

    for item in ["widget", "gadget"] {
        log::info!("[consumer] place_order({item})");
        match orders
            .place_order(item.to_string())
            .await
            .first_value()
            .await
        {
            Ok(receipt) => log::info!("[consumer] {receipt:?}"),
            Err(error) => log::error!("[consumer] the order failed: {error}"),
        }
    }
}

#[ice_rpc::main(tokio)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_subscriber();

    match std::env::args().nth(1).as_deref() {
        Some("provider") | None => run_provider().await,
        Some("consumer") => {
            run_consumer().await;
            Ok(())
        }
        Some(other) => {
            eprintln!("usage: tracing-demo [provider|consumer] (got {other})");
            Ok(())
        }
    }
}
