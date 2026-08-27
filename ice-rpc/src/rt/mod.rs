//! Runtime-agnostic execution facade.
//!
//! All the concurrency primitives used by the ice-rpc core go through this
//! module so that the crate has no direct dependency on a particular async
//! runtime. By default (no feature), the facade is backed by
//! `async-global-executor` (task spawning), `std::thread` (blocking threads)
//! and `futures-timer` (timers), which work on top of any executor: tokio,
//! smol, pollster, etc.
//!
//! Optional features switch the facade to a dedicated runtime:
//! - `tokio` → tokio runtime primitives.
//! - `smol`  → the default agnostic facade, which smol uses natively.

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

/// Runs a blocking closure on a dedicated thread and returns an awaitable
/// handle. The thread is started eagerly; awaiting the handle waits for its
/// completion. This is runtime-agnostic: it relies on `std::thread` plus an
/// `async-channel` completion notification.
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

/// Runs a blocking closure on a dedicated thread and returns its result.
///
/// Runtime-agnostic: the closure is executed eagerly on a `std::thread`, and
/// the result is sent back through an `async-channel`.
pub async fn spawn_blocking_value<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (tx, rx) = async_channel::bounded::<R>(1);
    std::thread::spawn(move || {
        let _ = tx.send_blocking(f());
    });
    rx.recv().await.expect("blocking task panicked")
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

    match futures_lite::future::race(
        async { Outcome::Value(fut.await) },
        async {
            sleep(dur).await;
            Outcome::TimedOut
        },
    )
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

/// Runtime-agnostic oneshot channel.
pub mod oneshot {
    pub use futures::channel::oneshot::{channel, Canceled, Receiver, Sender};
}
