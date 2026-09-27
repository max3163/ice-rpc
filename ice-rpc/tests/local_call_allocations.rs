//! Allocations per **direct** (in-process) service call.
//!
//! Companion of `benches/local_call.rs`: the benchmark prices the envelope in
//! nanoseconds, this counts what it allocates. The two are needed together — a
//! variant that is fast because it allocates less, and one that is slow because
//! it allocates more, cannot be told apart by a clock at this scale.
//!
//! The protocol follows `plans/lot2-allocations-par-appel.md`, with the two
//! corrections the CI required:
//!
//! - the window is **this thread's**. A process-wide counter attributes the
//!   allocations of every thread to the call under measurement, and the process is
//!   not quiet: the coverage job caught the runner's own work — four allocations,
//!   in one window and in no other — and reported it as a call that does not cost
//!   what its neighbours cost;
//! - the reading is one **window per call**, reduced by the *mode* of the
//!   histogram. The batch the plan established was written for a transported call,
//!   served on several threads: there, only a batch bounds the window's edges. A
//!   direct call never leaves this thread, so its window has no edge to bound —
//!   and work that is not the call's can only ever *add* to a window, so it moves
//!   a histogram's tail, never its most frequent value, where a batch sum is
//!   broken by a single outlier;
//! - the idle control stays *inside* the test: it must open the same window, on
//!   the same thread, as the calls it controls.
//!
//! See `plans/spans-appels-internes-provider.md`.

#![allow(clippy::unwrap_used)] // test target: it may panic
#![allow(missing_docs)] // the rkyv `Archive` derive emits an undocumented struct

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures_lite::future::poll_fn;
use ice_rpc::gen::RpcHeader;
use ice_rpc::rt::block_on;
use ice_rpc::{service, CallContext, Observable};

/// Counts the allocations the calling thread makes inside its window.
struct CountingAlloc;

thread_local! {
    /// `(window open, allocations counted while it was open)`, for this thread.
    ///
    /// A plain `Cell`: it allocates nothing, needs no destructor and has no panic
    /// path, so the allocator can read and write it while counting an allocation
    /// without re-entering itself.
    static WINDOW: Cell<(bool, usize)> = const { Cell::new((false, 0)) };
}

// SAFETY: every method forwards to `System` with the same arguments it received,
// so the allocation contract is the system allocator's own; the thread-local above
// is a `Cell` that allocates nothing and cannot panic.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_one();
        // SAFETY: `layout` is the caller's, forwarded unchanged.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_one();
        // SAFETY: `layout` is the caller's, forwarded unchanged.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count_one();
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

/// Records one allocation on the calling thread, when its window is open.
///
/// Only the window's own thread is counted: an allocation of another thread is not
/// this call's, and the CI runner does have threads that allocate.
#[inline]
fn count_one() {
    WINDOW.with(|window| {
        let (open, count) = window.get();
        if open {
            window.set((true, count + 1));
        }
    });
}

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

/// Windows taken per variant, i.e. calls measured per variant.
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

/// Runs `body` with **this thread's** window open, and returns what it counted.
///
/// The window is closed here, before the result is inspected: whatever the caller
/// does with it is not part of the measurement.
fn count_allocations<T>(body: impl FnOnce() -> T) -> (T, usize) {
    WINDOW.with(|window| window.set((true, 0)));
    let out = body();
    let allocations = WINDOW.with(|window| window.replace((false, 0)).1);
    (out, allocations)
}

/// The count of one call, measured on `CALLS` successive windows.
///
/// One window per call rather than one window for the batch: a direct call never
/// leaves this thread, so its window has no edge another thread could cross — the
/// edges the batch protocol of `plans/lot2-allocations-par-appel.md` bounds only
/// exist for a transported call, served on several threads.
///
/// The histogram is the reading; [`mode`] reduces it. Work that is not the call's
/// can only ever *add* to a window, so it lands in the tail of the distribution and
/// cannot move its most frequent value.
fn per_call_profile<T>(mut body: impl FnMut() -> T) -> BTreeMap<usize, usize> {
    let mut histogram = BTreeMap::new();
    for _ in 0..CALLS {
        let (_, allocations) = count_allocations(&mut body);
        *histogram.entry(allocations).or_insert(0) += 1;
    }
    histogram
}

/// The cost of a call: the count the windows agreed on most often.
///
/// A deterministic count sampled `CALLS` times makes the mode that count itself;
/// the rare window that caught foreign work cannot tie it. The lower count wins a
/// tie, because work that is not the call's can only ever add.
fn mode(histogram: &BTreeMap<usize, usize>) -> usize {
    histogram
        .iter()
        .max_by_key(|(cost, seen)| (**seen, std::cmp::Reverse(**cost)))
        .map(|(cost, _)| *cost)
        .expect("`CALLS` is not zero, so the histogram is never empty")
}

/// Allocations a window counts while **another** thread allocates.
///
/// The other thread is started before the window opens (`spawn` allocates on this
/// thread) and is released inside it. The two threads synchronize on atomics rather
/// than on a channel or a barrier: neither allocates on the counting thread, so the
/// handshake cannot pollute the very window it is meant to control.
fn allocations_of_another_thread_are_not_counted() -> usize {
    /// Allocations the other thread makes while the window is open.
    const FOREIGN: usize = 64;

    let go = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let worker = std::thread::spawn({
        let go = Arc::clone(&go);
        let done = Arc::clone(&done);
        move || {
            while !go.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
            for _ in 0..FOREIGN {
                std::hint::black_box(Vec::<u8>::with_capacity(1024));
            }
            done.store(true, Ordering::Release);
        }
    });

    let (_, counted) = count_allocations(|| {
        go.store(true, Ordering::Release);
        while !done.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
    });
    worker.join().expect("the noise thread must not panic");
    counted
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

/// Counts, per call, what each envelope adds to a direct call.
#[test]
fn the_zero_allocation_envelope_adds_no_allocation_and_call_scoped_adds_one() {
    let proxy = LocalEchoProxy::provide(EchoImpl);

    // Warm-up: the first calls may allocate one-off state (the proxy's mode lock
    // is already built, but the executor and the stream machinery are not). The
    // mode of the histogram would absorb such a one-off anyway; warming up keeps
    // it out of the reading instead of hiding it in the tail.
    for _ in 0..CALLS {
        std::hint::black_box(direct(&proxy, VALUE));
    }

    // Idle control, inside the test on purpose (see the module documentation).
    let (_, idle) = count_allocations(|| {});
    assert_eq!(idle, 0, "an empty window must count nothing");

    // Control of the window's scope: this is what the CI needed. Without it, the
    // runner's own threads broke the reading of the very first batch.
    assert_eq!(
        allocations_of_another_thread_are_not_counted(),
        0,
        "the window is this thread's: another thread's allocations are not a call's"
    );

    let direct_profile = per_call_profile(|| std::hint::black_box(direct(&proxy, VALUE)));
    let generic_profile = per_call_profile(|| std::hint::black_box(wrapped_generic(&proxy, VALUE)));
    let boxed_profile = per_call_profile(|| wrapped_boxed(&proxy));

    let direct_cost = mode(&direct_profile);
    let generic_cost = mode(&generic_profile);
    let boxed_cost = mode(&boxed_profile);

    println!(
        "allocations per call over {CALLS} calls — direct: {direct_cost}, generic: {generic_cost}, \
         boxed: {boxed_cost}"
    );
    // The raw reading: a count that appears once or twice and sits above the rest
    // is foreign work the window caught, not the price of a call.
    println!(
        "histograms — direct: {direct_profile:?}, generic: {generic_profile:?}, \
         boxed: {boxed_profile:?}"
    );

    // Guards the instrument: a counter that sees nothing would make the two
    // comparisons below hold for the wrong reason.
    assert!(
        direct_cost > 0,
        "the instrument must see a direct call's own allocations, it saw {direct_cost}"
    );
    assert_eq!(
        generic_cost, direct_cost,
        "the generic envelope must cost no allocation at all: {generic_cost} against {direct_cost}"
    );
    assert!(
        boxed_cost > direct_cost,
        "`call_scoped` must pay for the `Box` it erases the future with: {boxed_cost} against \
         {direct_cost}"
    );
}
