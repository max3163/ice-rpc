//! Execution facade: one full mode per host runtime, and a fallback for a
//! deployment that has none.
//!
//! Every concurrency primitive of the ice-rpc core goes through this module, so
//! the rest of the crate never names a runtime. What this facade cannot be is
//! *neutral*: a detached task must be polled by someone, and "who polls" is the
//! only thing a runtime is asked for here. That is why there is one mode per
//! host runtime rather than a single lowest-common-denominator executor, and why
//! the modes are exclusive — the point of a full mode is that the process runs
//! *one* pool, not three.
//!
//! | Mode | `spawn` | `sleep` | blocking pool |
//! |---|---|---|---|
//! | `rt-threads` (default) | owned `async-executor` instance on OS threads | `futures-timer` | `blocking` |
//! | `tokio` | `tokio::spawn` | `tokio::time::sleep` | tokio's pool |
//! | `smol` | `smol::spawn` (its global executor) | `smol::Timer` | `smol::unblock` |
//!
//! The two long-lived IPC loops do not go through a runtime at all: they own a
//! `std::thread` and block on an iceoryx2 `WaitSet` (see [`spawn_blocking`]).
//! Nothing in the core performs async I/O, which is why the default mode needs
//! no I/O reactor.
//!
//! The modes are selected by feature, by priority: `tokio` > `smol` >
//! `rt-threads`. Enabling a mode does not remove the fallback from the
//! dependency graph, because Cargo features are additive; a build with no
//! residue of the other modes uses `default-features = false`.

// Cargo features are additive and cannot express "if tokio then not smol", so
// the exclusivity the modes require is enforced here rather than in the
// manifest. Two full runtimes in one process is precisely what these modes exist
// to avoid: the first one to start would silently own the tasks of both.
#[cfg(all(feature = "tokio", feature = "smol"))]
compile_error!(
    "features `tokio` and `smol` are mutually exclusive: pick one execution facade. \
     The `ice-rpc` crate exposes both under the same names."
);

#[cfg(not(any(feature = "tokio", feature = "smol", feature = "rt-threads")))]
compile_error!(
    "no execution facade selected: enable `rt-threads` (the default), `tokio` or `smol`."
);

mod cancel;

pub use cancel::{CancellationToken, WaitForCancel};

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// Error returned by [`timeout`] when the deadline elapses first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed;

/// An awaitable handle to a blocking task.
///
/// Produced by [`spawn_blocking`]. Awaiting it waits for the underlying
/// blocking thread to finish. The result of the closure is intentionally
/// discarded: the ice-rpc core only needs termination notification.
pub struct BlockingHandle {
    inner: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
}

impl BlockingHandle {
    fn new(fut: impl Future<Output = ()> + Send + 'static) -> Self {
        Self {
            inner: Box::pin(fut),
        }
    }
}

impl Future for BlockingHandle {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

// ── Mode 1: tokio ─────────────────────────────────────────────────────────
#[cfg(feature = "tokio")]
mod imp {
    use super::*;

    pub fn spawn<F>(future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(future);
    }

    pub fn sleep(dur: Duration) -> impl Future<Output = ()> + Send + 'static {
        tokio::time::sleep(dur)
    }

    /// Pooled variant of [`super::spawn_blocking`].
    ///
    /// Requires an active tokio runtime; a panicking closure is logged.
    pub async fn spawn_blocking<F, R>(f: F)
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        if let Err(e) = tokio::task::spawn_blocking(f).await {
            log::error!("[ice-rpc] pooled blocking task failed: {e}");
        }
    }

    /// Pooled variant of [`super::spawn_blocking_value`].
    ///
    /// `tokio::task::spawn_blocking` already catches panics (reported as a
    /// `JoinError`).
    pub async fn spawn_blocking_value<F, R>(f: F) -> Result<R, String>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        match tokio::task::spawn_blocking(f).await {
            Ok(value) => Ok(value),
            Err(e) => Err(format!("pooled blocking task failed: {e}")),
        }
    }
}

// ── Mode 2: smol ──────────────────────────────────────────────────────────
#[cfg(all(feature = "smol", not(feature = "tokio")))]
mod imp {
    use super::*;

    /// Spawns onto smol's **global** executor.
    ///
    /// That global executor is the point of this mode: the task runs on the very
    /// executor the application's own `smol::spawn` uses, so ice-rpc and the
    /// application share one pool instead of running two. It starts lazily, on
    /// the first spawn, and runs `SMOL_THREADS` threads — **one** by default,
    /// unlike the `available_parallelism()` threads of the fallback mode. A
    /// provider that serves concurrent calls has to set it.
    pub fn spawn<F>(future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // `detach` is mandatory: dropping an `async_executor::Task` cancels its
        // future, and the core spawns tasks it never joins.
        smol::spawn(future).detach();
    }

    /// Sleeps through smol's timer, whose `async-io` reactor owns a background
    /// thread: nothing to enter, from any thread.
    pub fn sleep(dur: Duration) -> impl Future<Output = ()> + Send + 'static {
        // `smol::Timer` resolves to the deadline instant; the facade exposes `()`.
        async move {
            smol::Timer::after(dur).await;
        }
    }

    /// Pooled variant of [`super::spawn_blocking`].
    ///
    /// `smol::unblock` *is* `blocking::unblock`, the pool the fallback mode uses.
    /// It re-raises a panic in the awaiting task, so the closure is caught here
    /// exactly as in the fallback mode.
    pub fn spawn_blocking<F, R>(f: F) -> impl Future<Output = ()> + Send + 'static
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        smol::unblock(move || {
            if let Err(message) = catch_panic(f) {
                log::error!("[ice-rpc] pooled blocking task panicked: {message}");
            }
        })
    }

    /// Pooled variant of [`super::spawn_blocking_value`].
    pub async fn spawn_blocking_value<F, R>(f: F) -> Result<R, String>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        smol::unblock(move || catch_panic(f)).await
    }
}

// ── Mode 3: OS threads (the fallback) ─────────────────────────────────────
#[cfg(all(feature = "rt-threads", not(feature = "tokio"), not(feature = "smol")))]
mod imp {
    use super::*;
    use std::sync::{Arc, OnceLock};

    /// Number of threads running the owned executor.
    ///
    /// `available_parallelism()` matches the thread count of the executor this
    /// mode replaced (`async-global-executor`); `ICE_RPC_THREADS` overrides it,
    /// which is what a deployment pinning a core — or one that must not let a
    /// dependency start a pool of its own — sets.
    fn worker_threads() -> usize {
        std::env::var("ICE_RPC_THREADS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|count| *count > 0)
            .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |count| count.get()))
    }

    /// The process-wide executor, started on first use.
    ///
    /// Unlike the global of a third-party runtime, this one is an instance this
    /// crate owns: no `static` executor buried in a dependency, no `thread_local`
    /// to enter, no I/O reactor to start — nothing in the core performs async
    /// I/O. The threads below live for the rest of the process, like the pool of
    /// the executor this mode replaced.
    fn executor() -> &'static Arc<async_executor::Executor<'static>> {
        static EXECUTOR: OnceLock<Arc<async_executor::Executor<'static>>> = OnceLock::new();
        EXECUTOR.get_or_init(|| {
            let executor = Arc::new(async_executor::Executor::new());
            for index in 1..=worker_threads() {
                let worker = Arc::clone(&executor);
                std::thread::Builder::new()
                    .name(format!("ice-rpc-rt-{index}"))
                    .spawn(move || {
                        // `pending()` never completes, so the thread polls the
                        // executor for the whole process lifetime.
                        futures_lite::future::block_on(
                            worker.run(futures_lite::future::pending::<()>()),
                        )
                    })
                    .expect("failed to start an ice-rpc runtime worker thread");
            }
            executor
        })
    }

    /// Spawns a task onto the owned executor.
    pub fn spawn<F>(future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // `detach` is mandatory: dropping an `async_executor::Task` cancels its
        // future, and the core spawns tasks it never joins.
        executor().spawn(future).detach();
    }

    /// Sleeps through `futures-timer`, which owns its thread: no runtime, no
    /// reactor, nothing to enter.
    pub fn sleep(dur: Duration) -> impl Future<Output = ()> + Send + 'static {
        futures_timer::Delay::new(dur)
    }

    /// Pooled variant of [`super::spawn_blocking`].
    ///
    /// `blocking::unblock` re-raises a panic in the awaiting task, so the closure
    /// is caught here and logged.
    pub fn spawn_blocking<F, R>(f: F) -> impl Future<Output = ()> + Send + 'static
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        blocking::unblock(move || {
            if let Err(message) = catch_panic(f) {
                log::error!("[ice-rpc] pooled blocking task panicked: {message}");
            }
        })
    }

    /// Pooled variant of [`super::spawn_blocking_value`].
    pub async fn spawn_blocking_value<F, R>(f: F) -> Result<R, String>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        blocking::unblock(move || catch_panic(f)).await
    }
}

/// Spawns a future onto the configured runtime.
///
/// The task is detached: its result is discarded. Under the `rt-threads` and
/// `smol` facades this works from any context, including one without an active
/// async runtime: those executors start lazily and accept a spawn from any
/// thread. Under the `tokio` facade an active runtime is required.
pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    imp::spawn(future)
}

/// A spawner captured on a thread that **has** a runtime context, usable later
/// from a thread that has none.
///
/// Under the `tokio` facade [`spawn`] requires an active runtime and panics
/// without one. The transport boots its channel threads with `std::thread` —
/// they never run inside a runtime — so it captures a [`Spawner`] on the thread
/// that starts the channel (which is inside the runtime) and moves it into the
/// channel thread. Under the `rt-threads` and `smol` facades nothing is
/// captured: both executors accept a spawn from any thread.
#[derive(Clone, Default)]
pub struct Spawner {
    /// Runtime captured by [`Spawner::capture`] (`tokio` facade only).
    #[cfg(feature = "tokio")]
    handle: Option<tokio::runtime::Handle>,
}

impl Spawner {
    /// Captures the runtime of the calling thread, if it has one.
    pub fn capture() -> Self {
        Self {
            #[cfg(feature = "tokio")]
            handle: tokio::runtime::Handle::try_current().ok(),
        }
    }

    /// Spawns `future`, detached, wherever this spawner was captured from.
    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        #[cfg(feature = "tokio")]
        match &self.handle {
            Some(handle) => {
                handle.spawn(future);
            }
            // A provider booted outside any runtime (a bare `fn main`, an N-API
            // callback) still needs somewhere to run its calls.
            None => {
                fallback_runtime().spawn(future);
            }
        }
        #[cfg(not(feature = "tokio"))]
        spawn(future);
    }

    /// Runs `future` on the calling thread when it completes without yielding,
    /// and hands it to the runtime otherwise.
    ///
    /// A future that is ready on its first poll then costs **nothing more** than
    /// calling it: no queue, no wake-up, no second cache. One that yields is
    /// spawned at that very `Pending`, so it keeps every benefit of running as a
    /// task — this is what makes a handler that awaits a database overlap with
    /// the next request, while a handler that answers from memory is not taxed
    /// for a hop it does not need.
    ///
    /// # Why the first poll may use a waker that does nothing
    ///
    /// A wake-up carries no information beyond "poll again": a [`Waker`] is
    /// opaque and a future can only observe its own readiness by being polled.
    /// The only requirement is therefore that the task is polled again, which
    /// [`Spawner::spawn`] guarantees — it schedules an initial poll. A completion
    /// that fires in the window between the inline poll and that first scheduled
    /// poll is not lost: it is simply observed by the scheduled poll.
    ///
    /// # The calling thread is not a runtime thread
    ///
    /// The first poll of a future may touch a runtime resource — a timer started
    /// before its first `await`, a socket, a `tokio::spawn` of its own — and under
    /// the `tokio` facade such a call requires a runtime context. The captured
    /// handle is therefore **entered** around the inline poll, which gives the
    /// calling thread the context a worker thread would have had. When no runtime
    /// could be captured there is nothing to enter, and polling here would break
    /// the future instead of speeding it up: it is spawned instead.
    ///
    /// # Blocking
    ///
    /// The inline poll runs on the caller's thread, so a future that blocks
    /// *before* its first `await` blocks that thread. Offloading a blocking body
    /// stays the caller's decision, through [`blocking_call`].
    ///
    /// # Allocation
    ///
    /// Takes the future **already boxed**, so that a task which completes inline
    /// costs no allocation at all: re-boxing a `Pin<Box<dyn Future>>` would add a
    /// second indirection on the path of every request.
    ///
    /// [`Waker`]: std::task::Waker
    pub fn run_or_spawn(&self, mut task: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
        #[cfg(feature = "tokio")]
        let _entered = match &self.handle {
            Some(handle) => handle.enter(),
            None => {
                self.spawn(task);
                return;
            }
        };

        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        if task.as_mut().poll(&mut cx).is_pending() {
            self.spawn(task);
        }
    }
}

/// Runtime used by [`Spawner`] when no runtime was captured.
///
/// Lazily created and process-wide: the alternative is a provider that works
/// inside `#[ice_rpc::main]` and panics outside it.
#[cfg(feature = "tokio")]
fn fallback_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build the fallback tokio runtime for spawned tasks")
    })
}

/// Runs a blocking closure on a **dedicated** thread and returns an awaitable
/// handle.
///
/// Reserved for the long-lived IPC loops: they run for the whole process
/// lifetime and must own a thread instead of occupying a slot in a shared pool.
/// For short blocking work, use [`blocking_call`] or [`spawn_blocking_value`].
///
/// Awaiting the handle waits for the thread's completion.
pub fn spawn_blocking<F, R>(f: F) -> BlockingHandle
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (tx, rx) = async_channel::bounded::<()>(1);
    std::thread::spawn(move || {
        let _ = f();
        let _ = tx.try_send(());
    });
    BlockingHandle::new(async move {
        let _ = rx.recv().await;
    })
}

/// Runs a short blocking closure on the runtime's **bounded** thread pool and
/// returns an awaitable handle.
///
/// The pool belongs to the mode: `blocking` in the `rt-threads` and `smol`
/// facades, tokio's blocking pool under the `tokio` feature. A panicking closure
/// is caught and logged, never propagated to the awaiter.
pub fn blocking_call<F, R>(f: F) -> BlockingHandle
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    BlockingHandle::new(imp::spawn_blocking(f))
}

/// Runs a short blocking closure on the runtime's bounded pool and returns its
/// result (pooled counterpart of [`blocking_call`]).
///
/// # Errors
///
/// Returns `Err` when the closure panics (the panic is caught and its message
/// returned) or when the task is cancelled before reporting a result.
pub async fn spawn_blocking_value<F, R>(f: F) -> Result<R, String>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    imp::spawn_blocking_value(f).await
}

/// Runs a pooled blocking closure, turning a panic into an `Err(message)`.
///
/// The pooled variants of the `rt-threads` and `smol` facades are built on
/// `blocking::unblock`, which re-raises a panic in the awaiting task: catching it
/// here is what keeps a panicking task from taking its awaiter down with it.
/// Under the `tokio` feature the pool reports panics through `JoinError`
/// instead, so none of this is compiled there.
#[cfg(not(feature = "tokio"))]
fn catch_panic<F, R>(f: F) -> Result<R, String>
where
    F: FnOnce() -> R,
{
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).map_err(panic_payload_message)
}

/// Formats the payload of a caught panic into a human-readable message.
///
/// Only used by the modes whose pool re-raises panics.
#[cfg(not(feature = "tokio"))]
fn panic_payload_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "blocking task panicked with a non-string payload".to_string()
    }
}

/// Sleeps for the given duration.
pub fn sleep(dur: Duration) -> impl Future<Output = ()> + Send + 'static {
    imp::sleep(dur)
}

/// Waits for `fut` to complete, or returns [`Elapsed`] once `dur` elapses.
pub async fn timeout<F>(dur: Duration, fut: F) -> Result<F::Output, Elapsed>
where
    F: Future,
{
    enum Outcome<T> {
        Value(T),
        TimedOut,
    }

    match futures_lite::future::race(async { Outcome::Value(fut.await) }, async {
        sleep(dur).await;
        Outcome::TimedOut
    })
    .await
    {
        Outcome::Value(output) => Ok(output),
        Outcome::TimedOut => Err(Elapsed),
    }
}

/// Runs a future to completion on the current thread.
///
/// No async runtime is required. Useful in synchronous entry points
/// (N-API callbacks, tests, `fn main`).
pub fn block_on<F: Future>(future: F) -> F::Output {
    futures_lite::future::block_on(future)
}

/// Runs a future on the facade's own runtime.
///
/// Under the `tokio` facade, [`spawn`] and [`sleep`] require an active runtime:
/// a test that drives them has to supply one for the whole poll.
///
/// `#[doc(hidden)]`: this is a test helper shared by `ice-rpc-rx` and `ice-rpc`,
/// not part of the API.
#[doc(hidden)]
pub fn test_block_on<F: Future>(future: F) -> F::Output {
    #[cfg(feature = "tokio")]
    {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build the tokio runtime for tests")
            .block_on(future)
    }
    #[cfg(not(feature = "tokio"))]
    {
        block_on(future)
    }
}

/// Runtime-agnostic oneshot channel.
///
/// `futures-lite` carries no channel of its own, and pulling the `futures`
/// facade in for the one primitive would compile eight crates — the
/// runtime-agnostic `oneshot` crate needs none.
///
/// Upstream splits the receiving end in two: its `Receiver` is only
/// `IntoFuture`, because it also offers a blocking `recv` meant for OS threads,
/// and the `Future` is `AsyncReceiver`. The facade hands the latter out under
/// the historical name, so `channel()` still yields a receiver an executor can
/// poll directly and `timeout(.., rx).await` keeps working unchanged.
pub mod oneshot {
    /// Creates a oneshot channel whose receiving end is directly a future.
    pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
        ::oneshot::async_channel::<T>()
    }

    /// The receiving half, as a future (upstream `AsyncReceiver`).
    pub use ::oneshot::AsyncReceiver as Receiver;
    /// Error returned when the sending half is dropped before it sends.
    pub use ::oneshot::RecvError as Canceled;
    /// The sending half.
    pub use ::oneshot::Sender;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn block_on_runs_future_to_completion() {
        let value = block_on(async { 7 });
        assert_eq!(value, 7);
    }

    #[test]
    fn spawn_blocking_value_returns_result() {
        let value = test_block_on(spawn_blocking_value(|| 21 * 2));
        assert_eq!(value, Ok(42));
    }

    #[test]
    fn spawn_blocking_value_reports_panic_as_error() {
        // The wording depends on the facade: the agnostic pool returns the panic
        // payload, tokio wraps it in a `JoinError`.
        let result = test_block_on(spawn_blocking_value(|| -> i32 { panic!("boom") }));
        let message = result.expect_err("a panicking task must surface as Err");
        assert!(
            message.contains("boom") || message.contains("panic"),
            "unexpected message: {message}"
        );
    }

    #[test]
    fn blocking_call_runs_the_closure_on_the_pool() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();
        test_block_on(blocking_call(move || {
            flag_clone.store(true, Ordering::SeqCst);
        }));
        assert!(flag.load(Ordering::SeqCst));
    }

    #[test]
    fn spawn_blocking_handle_completes() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();
        let handle = spawn_blocking(move || {
            flag_clone.store(true, Ordering::SeqCst);
        });
        block_on(handle);
        assert!(flag.load(Ordering::SeqCst));
    }

    #[test]
    fn timeout_resolves_value_when_future_completes_first() {
        let result = test_block_on(timeout(Duration::from_secs(5), async { 42 }));
        assert_eq!(result, Ok(42));
    }

    #[test]
    fn timeout_returns_elapsed_on_deadline() {
        let result = test_block_on(timeout(Duration::from_millis(10), async {
            futures_lite::future::pending::<()>().await;
        }));
        assert_eq!(result, Err(Elapsed));
    }

    #[test]
    fn spawn_runs_detached_future() {
        // `spawn` requires an active runtime under the `tokio` facade.
        let flag = Arc::new(AtomicBool::new(false));
        let flag_spawn = flag.clone();
        let flag_wait = flag.clone();
        test_block_on(async move {
            spawn(async move {
                flag_spawn.store(true, Ordering::SeqCst);
            });
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while !flag_wait.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
        });
        assert!(flag.load(Ordering::SeqCst));
    }
}
