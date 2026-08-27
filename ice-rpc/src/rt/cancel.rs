//! Runtime-agnostic cancellation token.
//!
//! A minimal, dependency-free replacement for `tokio_util::sync::CancellationToken`.
//! It is cheap to clone (shared `Arc`) and can be awaited through
//! [`CancellationToken::cancelled`].

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct Inner {
    cancelled: AtomicBool,
    wakers: Mutex<Vec<Waker>>,
}

/// A token that can be awaited and cancelled from any thread.
///
/// Unlike a single-use channel, cancellation is idempotent and the token can
/// be shared by reference or by value between tasks and blocking threads.
pub struct CancellationToken {
    inner: Arc<Inner>,
}

impl CancellationToken {
    /// Creates a new, non-cancelled token.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                cancelled: AtomicBool::new(false),
                wakers: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Cancels the token, waking all the tasks currently waiting on it.
    pub fn cancel(&self) {
        if self.inner.cancelled.swap(true, Ordering::SeqCst) {
            return;
        }
        let wakers = std::mem::take(
            &mut *self
                .inner
                .wakers
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
        );
        for waker in wakers {
            waker.wake();
        }
    }

    /// Returns `true` if the token has been cancelled.
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::SeqCst)
    }

    /// Returns a future that completes when the token is cancelled.
    ///
    /// If the token is already cancelled, the future resolves immediately.
    pub fn cancelled(&self) -> WaitForCancel<'_> {
        WaitForCancel { token: self }
    }
}

impl Clone for CancellationToken {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for CancellationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

/// Future returned by [`CancellationToken::cancelled`].
pub struct WaitForCancel<'a> {
    token: &'a CancellationToken,
}

impl Future for WaitForCancel<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.token.is_cancelled() {
            return Poll::Ready(());
        }

        let mut wakers = match self.token.inner.wakers.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if !wakers.iter().any(|w| w.will_wake(cx.waker())) {
            wakers.push(cx.waker().clone());
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_starts_not_cancelled() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
    }

    #[test]
    fn token_cancel_sets_flag() {
        let token = CancellationToken::new();
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn token_clone_shares_state() {
        let token = CancellationToken::new();
        let clone = token.clone();
        clone.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn cancelled_resolves_immediately_when_already_cancelled() {
        let token = CancellationToken::new();
        token.cancel();
        futures_lite::future::block_on(token.cancelled());
    }

    #[test]
    fn cancelled_wakes_up_on_cancel() {
        let token = CancellationToken::new();
        let token_clone = token.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(10));
            token_clone.cancel();
        });
        futures_lite::future::block_on(token.cancelled());
        assert!(token.is_cancelled());
    }
}
