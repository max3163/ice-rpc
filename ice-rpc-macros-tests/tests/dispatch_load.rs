//! Load test of one channel under concurrent clients, with a **delayed** method.
//!
//! Complementary to `ice-rpc/examples/benchmark-app.rs`, which measures throughput
//! on instant handlers: through that bench an extra scheduler hand-off is a few
//! microseconds, and a handler that *awaits* cannot be distinguished from one that
//! answers at once. This test loads a method that awaits 2 ms — a database call —
//! because that is where a channel-wide dispatcher and a task-per-request
//! dispatcher differ by an order of magnitude.
//!
//! Each call is timed in two pieces — the request publication and the response
//! wait — because that split is what tells a **provider-side** serialization from
//! a **client-side** one: if publishing the request already costs the awaited
//! time, the delay is not in the dispatcher.
//!
//! Phases:
//!
//! - `echo` — the hot path. Reported, not asserted: its absolute throughput is a
//!   property of the machine.
//! - `wait` — `C` clients each awaiting `WAIT_MS`. Asserted: a dispatcher that runs
//!   the handler on the channel's thread serializes them, so the mean lands near
//!   `C × WAIT_MS`.
//!
//! It also reports how many `wait` handlers were inside their await at the same
//! time, and on which threads.
//!
//! Run with:
//! `cargo test -p ice-rpc-macros-tests --test dispatch_load -- --nocapture --test-threads=1`

#![allow(missing_docs)]
#![allow(clippy::unwrap_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use ice_rpc::{service, Event, Observable};

/// Await of the `wait` method.
///
/// Comfortably above the **OS timer granularity** — 15.6 ms on Windows, which is
/// what a 2 ms `sleep` actually costs there. The assertion uses the *measured*
/// await, so it holds on any platform.
const WAIT_MS: u64 = 30;

/// Client threads of a loaded phase.
const CLIENTS: usize = 8;

/// Calls per client in an `echo` phase.
const CALLS: usize = 200;

/// Calls per client in a `wait` phase: each one costs `WAIT_MS` of wall time.
const WAIT_CALLS: usize = 30;

/// Calls of the single-client phase.
const SINGLE_CALLS: usize = 2_000;

/// Calls spent warming the channel up before a measured phase.
const WARMUP: usize = 32;

#[service("LoadProbe")]
#[async_trait::async_trait]
pub trait LoadProbe: Send + Sync + 'static {
    /// Answers without ever yielding: the hot path.
    async fn echo(&self, value: i32) -> Observable<i32, String>;
    /// Awaits `ms` before answering: the head-of-line scenario.
    async fn wait(&self, ms: i32) -> Observable<i32, String>;
}

// ---------------------------------------------------------------------------
// Provider-side observation: is the await overlapped?
// ---------------------------------------------------------------------------

/// `wait` handlers currently inside their await.
static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
/// Most `wait` handlers that were inside their await at the same time.
static MAX_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
/// Threads a `wait` handler entered on, in first-seen order.
static WAIT_THREADS: Mutex<Vec<String>> = Mutex::new(Vec::new());
/// Total time the `wait` handlers spent inside their await.
static AWAIT_TOTAL_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Number of awaits measured.
static AWAIT_COUNT: AtomicUsize = AtomicUsize::new(0);

fn reset_wait_observation() {
    IN_FLIGHT.store(0, Ordering::SeqCst);
    MAX_IN_FLIGHT.store(0, Ordering::SeqCst);
    AWAIT_TOTAL_NS.store(0, Ordering::SeqCst);
    AWAIT_COUNT.store(0, Ordering::SeqCst);
    WAIT_THREADS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clear();
}

/// Mean time a `wait` handler actually spent inside its await.
///
/// Measured rather than assumed: on Windows a 2 ms `sleep` costs 15.6 ms, so an
/// assertion written against the requested duration would be wrong there.
fn observed_await() -> Duration {
    let count = AWAIT_COUNT.load(Ordering::SeqCst).max(1) as u64;
    Duration::from_nanos(AWAIT_TOTAL_NS.load(Ordering::SeqCst) / count)
}

/// Records the entry of a `wait` handler.
fn wait_enter() {
    let in_flight = IN_FLIGHT.fetch_add(1, Ordering::SeqCst) + 1;
    MAX_IN_FLIGHT.fetch_max(in_flight, Ordering::SeqCst);

    let thread = std::thread::current();
    let label = format!("{:?}/{}", thread.id(), thread.name().unwrap_or("unnamed"));
    let mut threads = WAIT_THREADS.lock().unwrap_or_else(|e| e.into_inner());
    if !threads.contains(&label) {
        threads.push(label);
    }
}

/// Records the exit of a `wait` handler.
fn wait_exit() {
    IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
}

struct ProbeImpl;

#[async_trait::async_trait]
impl LoadProbe for ProbeImpl {
    async fn echo(&self, value: i32) -> Observable<i32, String> {
        Observable::from_events([Event::Next(value + 1), Event::Complete])
    }

    async fn wait(&self, ms: i32) -> Observable<i32, String> {
        wait_enter();
        let entered_at = Instant::now();
        ice_rpc::rt::sleep(Duration::from_millis(ms.max(0) as u64)).await;
        AWAIT_TOTAL_NS.fetch_add(entered_at.elapsed().as_nanos() as u64, Ordering::SeqCst);
        AWAIT_COUNT.fetch_add(1, Ordering::SeqCst);
        wait_exit();
        Observable::from_events([Event::Next(ms), Event::Complete])
    }
}

/// Boots ice-rpc once for the whole test binary.
fn init_global() {
    static GUARD: std::sync::OnceLock<ice_rpc::gen::ShutdownGuard> = std::sync::OnceLock::new();
    GUARD.get_or_init(ice_rpc::gen::init_without_ctrl_c);
}

/// Where the time of one call went.
#[derive(Clone, Copy)]
struct CallTiming {
    /// Request publication, up to the stream being returned.
    publish: Duration,
    /// Whole round trip.
    total: Duration,
}

impl CallTiming {
    /// Waiting for the response, excluding the request publication.
    fn response(&self) -> Duration {
        self.total.saturating_sub(self.publish)
    }
}

/// Which method a phase loads.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Echo,
    Wait,
}

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::Echo => "echo",
            Self::Wait => "wait",
        }
    }

    /// One synchronous call, timed in two pieces.
    fn call(self, proxy: &LoadProbeProxy, value: i32) -> CallTiming {
        let started = Instant::now();
        let stream = match self {
            Self::Echo => ice_rpc::rt::block_on(proxy.echo(value)),
            Self::Wait => ice_rpc::rt::block_on(proxy.wait(WAIT_MS as i32)),
        };
        let publish = started.elapsed();
        let values = ice_rpc::rt::block_on(stream.collect()).expect("collect");
        assert!(!values.is_empty(), "every call must be answered");
        CallTiming {
            publish,
            total: started.elapsed(),
        }
    }
}

/// What one measured phase produced.
struct Stats {
    clients: usize,
    calls: usize,
    total: Duration,
    mean: Duration,
    p50: Duration,
    p99: Duration,
    max: Duration,
    /// Mean request publication.
    mean_publish: Duration,
    /// Mean response wait.
    mean_response: Duration,
}

impl Stats {
    fn rps(&self) -> f64 {
        self.calls as f64 / self.total.as_secs_f64()
    }

    fn report(&self, phase: Phase) {
        eprintln!(
            "[load] {:<4} clients={} calls={:<5} wall={:>8.2?} {:>9.0} req/s  \
             mean={:>8.3?} p50={:>8.3?} p99={:>8.3?} max={:>8.3?}  \
             publish={:>8.3?} response={:>8.3?}",
            phase.name(),
            self.clients,
            self.calls,
            self.total,
            self.rps(),
            self.mean,
            self.p50,
            self.p99,
            self.max,
            self.mean_publish,
            self.mean_response,
        );
    }
}

/// Runs one phase with `clients` threads, released together by a barrier.
fn bench(proxy: &Arc<LoadProbeProxy>, phase: Phase, clients: usize, calls: usize) -> Stats {
    for _ in 0..WARMUP {
        let _ = phase.call(proxy, 0);
    }

    if phase == Phase::Wait {
        reset_wait_observation();
    }

    let barrier = Arc::new(Barrier::new(clients + 1));
    let mut handles = Vec::with_capacity(clients);

    for worker in 0..clients {
        let proxy = Arc::clone(proxy);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            let mut timings = Vec::with_capacity(calls);
            for call in 0..calls {
                timings.push(phase.call(&proxy, (worker * calls + call) as i32));
            }
            timings
        }));
    }

    barrier.wait();
    let started = Instant::now();
    let timings: Vec<CallTiming> = handles
        .into_iter()
        .flat_map(|handle| handle.join().expect("client thread"))
        .collect();
    let total = started.elapsed();

    let mut latencies: Vec<Duration> = timings.iter().map(|t| t.total).collect();
    latencies.sort_unstable();
    let sum: Duration = latencies.iter().sum();
    let percentile = |p: f64| latencies[((latencies.len() - 1) as f64 * p) as usize];

    let calls = timings.len() as u32;
    Stats {
        clients,
        calls: latencies.len(),
        total,
        mean: sum / calls,
        p50: percentile(0.50),
        p99: percentile(0.99),
        max: *latencies.last().expect("at least one call"),
        mean_publish: timings.iter().map(|t| t.publish).sum::<Duration>() / calls,
        mean_response: timings.iter().map(|t| t.response()).sum::<Duration>() / calls,
    }
}

/// Ignored by default: this is a load measurement, it spins eight client threads
/// for several seconds, and running it inside the suite disturbs the
/// timing-sensitive tests of the other packages. `dispatch_serialization.rs`
/// guards the same property in one second and *does* run by default.
///
/// `cargo test -p ice-rpc-macros-tests --test dispatch_load -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore = "load measurement: run with --ignored --nocapture"]
fn load_profile_of_one_channel() {
    init_global();

    let locator = ice_rpc::locator();
    ice_rpc::rt::block_on(async {
        locator.register(LoadProbeProxy::provide(ProbeImpl)).await;
        locator.initialize_all().await.expect("initialize_all");
    });

    // Let the channel thread create the iceoryx2 services.
    std::thread::sleep(Duration::from_millis(500));

    let proxy = Arc::new(LoadProbeProxy::consume());

    eprintln!("---- load profile (wait = {WAIT_MS} ms awaited) ----");

    // The hot path: what the task model costs.
    bench(&proxy, Phase::Echo, 1, SINGLE_CALLS).report(Phase::Echo);
    let echo_loaded = bench(&proxy, Phase::Echo, CLIENTS, CALLS);
    echo_loaded.report(Phase::Echo);

    // The head-of-line scenario: what the task model buys. The scaling curve is
    // what locates the remaining serialization: a cost that does not grow with
    // the client count is per call, one that grows linearly is per round.
    // The scaling curve is what separates a per-call cost from a per-round one:
    // a constant that does not grow with the client count is per call, a linear
    // growth is the serialization the model is supposed to remove.
    let mut wait_loaded = bench(&proxy, Phase::Wait, 1, WAIT_CALLS);
    wait_loaded.report(Phase::Wait);
    for clients in [2usize, 4, CLIENTS] {
        let stats = bench(&proxy, Phase::Wait, clients, WAIT_CALLS);
        stats.report(Phase::Wait);
        if clients == CLIENTS {
            wait_loaded = stats;
        }
    }
    let await_cost = observed_await();

    let overlapped = MAX_IN_FLIGHT.load(Ordering::SeqCst);
    let threads = WAIT_THREADS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    eprintln!(
        "[load] provider: max {overlapped} `wait` handler(s) inside their await, \
         on {} thread(s) {:?}; measured await {await_cost:?} (requested {WAIT_MS} ms)",
        threads.len(),
        threads
    );

    // ── The assertion ──────────────────────────────────────────────────────
    //
    // `C` clients each await. A dispatcher that runs the handler on the channel's
    // thread serializes them: one call waits for the `C - 1` that started before
    // it, so the mean lands near `C × await` and the last client of a round waits
    // the whole round. A task per request overlaps them, so the mean stays within
    // a small multiple of a single await.
    let ceiling = await_cost * 3;
    assert!(
        wait_loaded.mean < ceiling,
        "the {} concurrent awaits were serialized: mean {:?} for a measured await of \
         {await_cost:?} (expected < {ceiling:?}; a channel-wide dispatcher gives ~{:?}). \
         Provider observation: max {overlapped} handler(s) in flight on {} thread(s) {:?}; \
         client observation: publish {:?}, response {:?}",
        wait_loaded.clients,
        wait_loaded.mean,
        await_cost * CLIENTS as u32,
        threads.len(),
        threads,
        wait_loaded.mean_publish,
        wait_loaded.mean_response,
    );

    eprintln!(
        "[load] RESULT: wait mean {:?} for a measured await of {await_cost:?} (x{:.2}), \
         {} handler(s) in flight at most on {} thread(s)",
        wait_loaded.mean,
        wait_loaded.mean.as_secs_f64() / await_cost.as_secs_f64(),
        overlapped,
        threads.len()
    );
}
