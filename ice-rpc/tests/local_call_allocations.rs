//! Allocations per **direct** (in-process) service call.
//!
//! Companion of `benches/local_call.rs`: the benchmark prices the envelope in
//! nanoseconds, this counts what it allocates. The two are needed together — a
//! variant that is fast because it allocates less, and one that is slow because
//! it allocates more, cannot be told apart by a clock at this scale.
//!
//! The protocol is the one established by `plans/lot2-allocations-par-appel.md`:
//!
//! - the readable figure is a **batch**, not a single call, so the window's edge
//!   effects stay bounded;
//! - the idle control is *inside* the test — a second `#[test]` would run in
//!   parallel with this one and count the other's allocations.
//!
//! See `plans/spans-appels-internes-provider.md`.

#![allow(clippy::unwrap_used)] // test target: it may panic
#![allow(missing_docs)] // the rkyv `Archive` derive emits an undocumented struct

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use futures_lite::future::poll_fn;
use ice_rpc::gen::RpcHeader;
use ice_rpc::rt::block_on;
use ice_rpc::{service, CallContext, Observable};

/// Counts the allocations the test window performs.
struct CountingAlloc;

/// Whether the window is open; the counter is only touched while it is.
static COUNTING: AtomicBool = AtomicBool::new(false);
/// Number of `alloc`/`alloc_zeroed`/`realloc` calls made while it was open.
static ALLOCS: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every method forwards to `System` with the same arguments it received,
// so the allocation contract is the system allocator's own; the counter is a
// plain relaxed atomic that allocates nothing and cannot panic.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: `layout` is the caller's, forwarded unchanged.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: `layout` is the caller's, forwarded unchanged.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: the caller guarantees `ptr` came from this allocator with
        // `layout`, which is what `System::realloc` requires.
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` and `layout` are the caller's, forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

/// The service every variant calls.
///
/// Declared with the real `#[service]` macro, like the benchmark: the `Provider`
/// arm being counted is the generated one.
#[service("BenchLocalEchoAllocs")]
#[async_trait::async_trait]
pub trait LocalEcho: Send + Sync + 'static {
    /// Returns its argument.
    async fn echo(&self, value: i32) -> Observable<i32, String>;
}

/// Leaf implementation: builds a single-value stream.
struct EchoImpl;

#[async_trait::async_trait]
impl LocalEcho for EchoImpl {
    async fn echo(&self, value: i32) -> Observable<i32, String> {
        ice_rpc::of(value)
    }
}

/// Value every variant carries.
const VALUE: i32 = 7;

/// Calls per batch: large enough that the window's edge effects are amortised.
const CALLS: usize = 1_000;

/// The context `CallContext::local` would build for a direct call.
fn local_context() -> CallContext {
    let header = RpcHeader::request(
        "echo",
        ice_rpc::gen::service_id_of(LocalEchoProxy::SERVICE_NAME),
        1,
    );
    CallContext::new(&header, LocalEchoProxy::SERVICE_NAME, "echo")
}

/// The zero-allocation envelope: the context is entered around each `poll`.
///
/// `pin!` on an `async fn` parameter is what removes the `Box` `call_scoped`
/// needs to erase its future; the shape is an `async fn` rather than a
/// `fn -> impl Future` so the compiler keeps the state on the stack.
async fn local_scope<F>(ctx: CallContext, future: F) -> F::Output
where
    F: std::future::Future,
{
    let mut future = std::pin::pin!(future);
    poll_fn(move |cx| {
        let _ambient = ctx.enter_ambient();
        future.as_mut().poll(cx)
    })
    .await
}

/// Runs `body` with the counter open and returns how many allocations it made.
fn count_allocations<T>(body: impl FnOnce() -> T) -> (T, usize) {
    ALLOCS.store(0, Ordering::Relaxed);
    COUNTING.store(true, Ordering::Relaxed);
    let out = body();
    COUNTING.store(false, Ordering::Relaxed);
    (out, ALLOCS.load(Ordering::Relaxed))
}

/// Direct call, drained to a value.
fn direct(proxy: &Arc<LocalEchoProxy>, value: i32) -> i32 {
    let stream = block_on(proxy.echo(value));
    block_on(stream.first_value()).unwrap()
}

/// Direct call wrapped by the zero-allocation envelope.
fn wrapped_generic(proxy: &Arc<LocalEchoProxy>, value: i32) -> i32 {
    let ctx = local_context();
    block_on(local_scope(ctx, async move {
        let stream = proxy.echo(value).await;
        stream.first_value().await.unwrap()
    }))
}

/// Direct call wrapped by `call_scoped`, which erases the future.
fn wrapped_boxed(proxy: &Arc<LocalEchoProxy>) {
    let ctx = local_context();
    let proxy = Arc::clone(proxy);
    let future = ice_rpc::gen::call_scoped(ctx, async move {
        let stream = proxy.echo(VALUE).await;
        let _ = stream.first_value().await;
    });
    block_on(future);
}

/// Counts, per batch, what each envelope adds to a direct call.
#[test]
fn the_zero_allocation_envelope_adds_no_allocation_and_call_scoped_adds_one() {
    let proxy = LocalEchoProxy::provide(EchoImpl);

    // Warm-up: the first calls may allocate one-off state (the proxy's mode lock
    // is already built, but the executor and the stream machinery are not).
    for _ in 0..CALLS {
        std::hint::black_box(direct(&proxy, VALUE));
    }

    // Idle control, in the same test on purpose (see the module documentation).
    let (_, idle) = count_allocations(|| {});
    assert_eq!(idle, 0, "an empty window must count nothing");

    let batch = |body: &dyn Fn()| {
        let (_, allocations) = count_allocations(body);
        allocations
    };

    let baseline = batch(&|| {
        for _ in 0..CALLS {
            std::hint::black_box(direct(&proxy, VALUE));
        }
    });
    let generic = batch(&|| {
        for _ in 0..CALLS {
            std::hint::black_box(wrapped_generic(&proxy, VALUE));
        }
    });
    let boxed = batch(&|| {
        for _ in 0..CALLS {
            wrapped_boxed(&proxy);
        }
    });

    println!(
        "allocations for {CALLS} calls — direct: {baseline}, generic: {generic}, boxed: {boxed}"
    );
    println!(
        "per call — direct: {:.2}, generic: {:.2}, boxed: {:.2}",
        baseline as f64 / CALLS as f64,
        generic as f64 / CALLS as f64,
        boxed as f64 / CALLS as f64,
    );

    // A call is a multiple of the batch size, so the count must divide evenly —
    // otherwise the window caught work that does not belong to a call.
    assert_eq!(
        baseline % CALLS,
        0,
        "the direct call counts {baseline} for {CALLS} calls: not a whole number per call"
    );
    assert_eq!(
        generic, baseline,
        "the generic envelope must cost no allocation at all: {generic} against {baseline}"
    );
    assert!(
        boxed > baseline,
        "`call_scoped` must pay for the `Box` it erases the future with: {boxed} against {baseline}"
    );
}
