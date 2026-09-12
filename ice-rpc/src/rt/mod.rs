//! Runtime-agnostic execution facade.
//!
//! All the concurrency primitives used by the ice-rpc core go through this
//! module so that the crate has no direct dependency on a particular async
//! runtime. By default the facade is backed by `async-global-executor`,
//! `std::thread` and `futures-timer`; the `tokio` feature switches it to tokio.

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

// ── Default (agnostic) implementation ─────────────────────────────────────
#[cfg(not(feature = "tokio"))]
mod imp {
    use super::*;

    pub fn spawn<F>(future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        async_global_executor::spawn(future).detach();
    }

    pub fn sleep(dur: Duration) -> impl Future<Output = ()> + Send + 'static {
        futures_timer::Delay::new(dur)
    }

    /// Pooled variant of [`super::spawn_blocking`].
    ///
    /// Backed by the `blocking` crate's pool; a panicking closure is caught here.
    pub fn spawn_blocking<F, R>(f: F) -> impl Future<Output = ()> + Send + 'static
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        async_global_executor::spawn_blocking(move || {
            if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
                log::error!(
                    "[ice-rpc] pooled blocking task panicked: {}",
                    super::panic_payload_message(payload)
                );
            }
        })
    }

    /// Pooled variant of [`super::spawn_blocking_value`].
    pub async fn spawn_blocking_value<F, R>(f: F) -> Result<R, String>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        async_global_executor::spawn_blocking(move || {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
        })
        .await
        .map_err(super::panic_payload_message)
    }
}

// ── Tokio implementation ──────────────────────────────────────────────────
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

/// Spawns a future onto the configured runtime.
///
/// The task is detached: its result is discarded. Works from any context,
/// including one without an active async runtime (the default agnostic
/// facade starts its own global executor lazily).
pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    imp::spawn(future)
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
/// The pool is the executor's own (the `blocking` crate by default, tokio's
/// blocking pool under the `tokio` feature). A panicking closure is caught and
/// logged, never propagated to the awaiter.
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

/// Formats the payload of a caught panic into a human-readable message.
///
/// Only used by the agnostic facade: under the `tokio` feature the pool reports
/// panics through `JoinError` instead.
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

/// Runs a future on the facade's own runtime (unit tests only).
///
/// Under the `tokio` facade, [`spawn`] and [`sleep`] require an active runtime.
#[cfg(test)]
pub(crate) fn test_block_on<F: Future>(future: F) -> F::Output {
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
pub mod oneshot {
    pub use futures::channel::oneshot::{channel, Canceled, Receiver, Sender};
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
            futures::future::pending::<()>().await;
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
