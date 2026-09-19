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
        let wakers =
            std::mem::take(&mut *self.inner.wakers.lock().unwrap_or_else(|e| e.into_inner()));
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

    /// Polls the token, registering the waker of `cx` while it is not cancelled.
    ///
    /// This is the building block of a future that must react to a cancellation
    /// *while* driving something else — driving a handler task, for instance. A
    /// caller that got `Pending` from its own work registers here and is woken the
    /// instant the token fires, instead of re-polling on a timer.
    ///
    /// The waker is registered **at most once per task**: the same token is
    /// polled on every poll of the future that awaits it, and a cancellation must
    /// wake that future once, not once per poll. A different waker replaces
    /// nothing — it is simply added, because two tasks can wait on one token.
    ///
    /// The waker is registered **before** the flag is read, and the flag is read
    /// again afterwards. The other order has a lost-wakeup window: a cancellation
    /// that fires between the read and the registration would find an empty waker
    /// list and the future would be parked on a wake-up nobody will ever send. Any
    /// cancellation that lands after this point is either seen by the second read
    /// or wakes the registered waker.
    pub fn poll_cancelled(&self, cx: &mut Context<'_>) -> Poll<()> {
        // The already-cancelled case needs no registration, and the token is
        // polled on every poll of the future that waits on it.
        if self.is_cancelled() {
            return Poll::Ready(());
        }

        let mut wakers = match self.inner.wakers.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if !wakers.iter().any(|w| w.will_wake(cx.waker())) {
            wakers.push(cx.waker().clone());
        }
        drop(wakers);

        // Second read, closing the window opened by the first one.
        if self.is_cancelled() {
            return Poll::Ready(());
        }
        Poll::Pending
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

    /// Delegates to [`CancellationToken::poll_cancelled`], the single
    /// implementation of "wait for this token".
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.token.poll_cancelled(cx)
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

    #[test]
    fn default_is_not_cancelled() {
        let token = CancellationToken::default();
        assert!(!token.is_cancelled());
    }

    #[test]
    fn cancel_is_idempotent() {
        let token = CancellationToken::new();
        token.cancel();
        token.cancel();
        assert!(token.is_cancelled());
    }

    /// A counting waker: what the token calls when it fires.
    #[derive(Default)]
    struct CountWakes(std::sync::atomic::AtomicUsize);

    impl std::task::Wake for CountWakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn poll_cancelled_is_ready_once_the_token_is_cancelled() {
        let token = CancellationToken::new();
        let mut cx = Context::from_waker(Waker::noop());

        assert!(token.poll_cancelled(&mut cx).is_pending());
        token.cancel();
        assert!(token.poll_cancelled(&mut cx).is_ready());
    }

    /// The token is polled on every poll of the future that waits on it: it must
    /// register one waker per task, not one per poll, and wake that task once.
    #[test]
    fn poll_cancelled_registers_one_waker_per_task() {
        let token = CancellationToken::new();
        let counter = Arc::new(CountWakes::default());
        let waker = Waker::from(Arc::clone(&counter));
        let mut cx = Context::from_waker(&waker);

        assert!(token.poll_cancelled(&mut cx).is_pending());
        assert!(token.poll_cancelled(&mut cx).is_pending());
        assert_eq!(counter.0.load(Ordering::SeqCst), 0, "nothing has fired yet");

        token.cancel();
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            1,
            "one wake for the task, not one per poll"
        );
        assert!(token.poll_cancelled(&mut cx).is_ready());
    }

    #[test]
    fn debug_format_contains_state() {
        let token = CancellationToken::new();
        let dbg = format!("{:?}", token);
        assert!(dbg.contains("CancellationToken"));
        assert!(dbg.contains("false"));
    }
}
