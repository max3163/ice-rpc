//! Non-regression test of the dispatch model: **a slow call must not hold back
//! the next one**.
//!
//! The unit that owns a dispatch thread is the `group`, and that thread used to
//! *run* the handler: with one handler per channel polled inline, a `query_db`
//! awaiting 100 ms pushed every other call of the same group behind it.
//!
//! Measured before the fix, on this very scenario:
//!
//! ```text
//! idle fast call                    :  288 µs
//! fast call behind slow(400 ms)     :  292 ms
//!   — of which the fast handler ran :   39 µs
//! one ThreadId, holds disjoint
//! ```
//!
//! The same 39 µs of work waited 292 ms. A task per request removes the wait: the
//! channel's thread only receives and publishes, and the two handlers are alive
//! at the same time on the executor.
//!
//! Two proofs are asserted, and they are independent:
//!
//! 1. **Latency** — the fast call returns in well under the time `slow` spends
//!    in flight. This is the criterion the regression is named after.
//! 2. **Overlap** — the handlers' own records show `slow` and `fast` entered and
//!    left in a nested order, which no serialized dispatcher can produce.
//!
//! Dedicated binary: the channel registry is sealed once `initialize_all` has
//! run, so only a fresh process guarantees a single grouped channel.
//!
//! Run with:
//! `cargo test -p ice-rpc-macros-tests --test dispatch_serialization -- --nocapture`

#![allow(missing_docs)]
#![allow(clippy::unwrap_used)]

use std::sync::Mutex;
use std::time::{Duration, Instant};

use ice_rpc::{service, Event, Observable};

/// In-flight time of the deliberately slow method.
const SLOW_MS: i32 = 400;

/// How long the slow call is left in flight before the fast one is issued.
const HEAD_START: Duration = Duration::from_millis(120);

/// Ceiling of the fast call, **including while a slow one is in flight**.
///
/// The pre-fix measurement was 292 ms; the point of the test is that this ceiling
/// is not reachable by a serialized dispatcher.
const FAST_CEILING: Duration = Duration::from_millis(60);

// ---------------------------------------------------------------------------
// Capturing logger
// ---------------------------------------------------------------------------

/// Every `log` record, in emission order.
static RECORDS: Mutex<Vec<String>> = Mutex::new(Vec::new());

struct CaptureLogger;

impl log::Log for CaptureLogger {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        let line = format!("[{:5}] {}", record.level(), record.args());
        if let Ok(mut records) = RECORDS.lock() {
            records.push(line);
        }
    }

    fn flush(&self) {}
}

fn install_capture_logger() {
    static LOGGER: CaptureLogger = CaptureLogger;
    // A second install in the same process is not an error here.
    let _ = log::set_logger(&LOGGER);
    log::set_max_level(log::LevelFilter::Debug);
}

fn clear_records() {
    RECORDS.lock().unwrap_or_else(|e| e.into_inner()).clear();
}

/// One `probe-enter` / `probe-exit` record emitted by the service itself.
#[derive(Debug)]
struct ProbeEvent {
    kind: &'static str,
    method: String,
    line: String,
}

/// Parses the records the service implementation wrote.
///
/// The service logs its own entries and exits: the framework's dispatch path is
/// deliberately left without instrumentation, so this costs the hot path nothing.
fn probe_events() -> Vec<ProbeEvent> {
    RECORDS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter(|line| line.contains("probe-"))
        .map(|line| ProbeEvent {
            kind: if line.contains("probe-enter") {
                "enter"
            } else {
                "exit"
            },
            method: line
                .split("method=")
                .nth(1)
                .and_then(|rest| rest.split_whitespace().next())
                .unwrap_or("?")
                .to_owned(),
            line: line.clone(),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Probe service
// ---------------------------------------------------------------------------

#[service("SerialProbe", group = "dispatch_probe_group")]
#[async_trait::async_trait]
pub trait SerialProbe: Send + Sync + 'static {
    /// Holds the channel for `ms` — by **awaiting**, which is what a database or
    /// an HTTP call does.
    async fn slow(&self, ms: i32) -> Observable<i32, String>;
    /// Answers at once; its latency is the clock that measures the hold.
    async fn fast(&self, value: i32) -> Observable<i32, String>;
}

struct ProbeImpl;

#[async_trait::async_trait]
impl SerialProbe for ProbeImpl {
    async fn slow(&self, ms: i32) -> Observable<i32, String> {
        log::debug!("probe-enter method=slow");
        // A real await: the time passes without any thread being held.
        ice_rpc::rt::sleep(Duration::from_millis(ms.max(0) as u64)).await;
        log::debug!("probe-exit method=slow");
        Observable::from_events([Event::Next(ms), Event::Complete])
    }

    async fn fast(&self, value: i32) -> Observable<i32, String> {
        log::debug!("probe-enter method=fast");
        log::debug!("probe-exit method=fast");
        Observable::from_events([Event::Next(value + 1), Event::Complete])
    }
}

/// Boots ice-rpc once for the whole test binary.
fn init_global() {
    static GUARD: std::sync::OnceLock<ice_rpc::gen::ShutdownGuard> = std::sync::OnceLock::new();
    GUARD.get_or_init(ice_rpc::gen::init_without_ctrl_c);
}

/// One synchronous call, returning its latency and its values.
fn timed_call(proxy: &SerialProbeProxy, value: i32) -> (Duration, Vec<i32>) {
    let started = Instant::now();
    let stream = ice_rpc::rt::block_on(proxy.fast(value));
    let values = ice_rpc::rt::block_on(stream.collect()).expect("fast collect");
    (started.elapsed(), values)
}

#[test]
fn a_slow_call_does_not_hold_back_the_next_one_of_the_group() {
    install_capture_logger();
    init_global();

    let locator = ice_rpc::locator();
    ice_rpc::rt::block_on(async {
        locator.register(SerialProbeProxy::provide(ProbeImpl)).await;
        locator.initialize_all().await.expect("initialize_all");
    });

    // Let the channel thread create the iceoryx2 services.
    std::thread::sleep(Duration::from_millis(500));

    let consumer = std::sync::Arc::new(SerialProbeProxy::consume());

    // Warm-up: creates the cached client and connects it, so the measured calls
    // pay neither cost.
    let (warmup, values) = timed_call(&consumer, 0);
    assert_eq!(values, vec![1], "warm-up answered the wrong value");
    eprintln!("[probe] warm-up fast call: {warmup:?}");

    // ── Control: the same fast call on an idle channel ──────────────────────
    std::thread::sleep(Duration::from_millis(50));
    let (idle, values) = timed_call(&consumer, 41);
    assert_eq!(values, vec![42]);
    eprintln!("[probe] CONTROL  idle fast call: {idle:?}");

    // ── Measurement: fast call issued while slow is in flight ───────────────
    clear_records();

    let slow_consumer = std::sync::Arc::clone(&consumer);
    let slow_thread = std::thread::spawn(move || {
        let stream = ice_rpc::rt::block_on(slow_consumer.slow(SLOW_MS));
        ice_rpc::rt::block_on(stream.collect())
    });

    std::thread::sleep(HEAD_START);

    let (blocked, values) = timed_call(&consumer, 100);
    assert_eq!(values, vec![101], "the fast call must still be answered");
    eprintln!("[probe] BLOCKED  fast call while slow({SLOW_MS}ms) runs: {blocked:?}");

    let slow_values = slow_thread
        .join()
        .expect("slow thread")
        .expect("slow collect");
    assert_eq!(slow_values, vec![SLOW_MS], "the slow call must be answered");

    // ── Proof 1: the latency ceiling ───────────────────────────────────────
    assert!(
        idle < FAST_CEILING,
        "control: an idle fast call took {idle:?} (expected < {FAST_CEILING:?})"
    );
    assert!(
        blocked < FAST_CEILING,
        "head-of-line blocking is back: the fast call waited {blocked:?} behind slow({SLOW_MS}ms), \
         which is the time slow spends in flight (expected < {FAST_CEILING:?} — it was 292 ms \
         when the channel's thread ran the handler)"
    );

    // ── Proof 2: the two handlers overlapped ───────────────────────────────
    let events = probe_events();
    eprintln!("---- probe records ({}) ----", events.len());
    for event in &events {
        eprintln!("{}", event.line);
    }

    let position = |method: &str, kind: &str| {
        events
            .iter()
            .position(|event| event.method == method && event.kind == kind)
    };
    let enter_slow = position("slow", "enter").expect("slow entered");
    let exit_slow = position("slow", "exit").expect("slow left");
    let enter_fast = position("fast", "enter").expect("fast entered");
    let exit_fast = position("fast", "exit").expect("fast left");

    assert!(
        enter_slow < enter_fast && enter_fast < exit_fast && exit_fast < exit_slow,
        "the handlers must be nested — fast inside slow — while they ran concurrently; \
         observed: slow[{enter_slow}..{exit_slow}] fast[{enter_fast}..{exit_fast}]"
    );

    eprintln!(
        "[probe] RESULT: fast call {blocked:?} while slow is in flight ({idle:?} idle), \
         and the two handlers overlapped: slow[{enter_slow}..{exit_slow}] contains fast[{enter_fast}..{exit_fast}]"
    );
}
