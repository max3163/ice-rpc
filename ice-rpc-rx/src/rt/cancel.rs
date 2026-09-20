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
    /// [`poll_cancelled`](Self::poll_cancelled) for a task that polls this token
    /// on every poll of its future, and **caches the waker it registered**.
    ///
    /// The plain call takes the lock on every poll: it must compare the waker it
    /// is given with the registered ones, and that comparison needs the list.
    /// Measured at 10.6 ns per poll, against 0.73 ns for this variant once the
    /// waker is cached (`benches/hot_path.rs`, group `per_poll`).
    ///
    /// `registered` is that cache. It belongs to the caller, travels across polls,
    /// and is *the waker this task registered*, not a boolean: a boolean would be
    /// wrong, because a task's waker is **not** stable in this codebase.
    /// [`Spawner::run_or_spawn`] polls a task once with a no-op waker before
    /// handing it to the executor, so the first poll of a provider handler
    /// registers a waker the task never uses again — a boolean would then skip the
    /// registration that matters, and the `Cancel` would wake nobody. The cached
    /// comparison makes that case an ordinary one: a different waker replaces the
    /// cache and is registered.
    ///
    /// # Cost model
    ///
    /// The lock is paid once per **waker**, not once per poll: at the first poll,
    /// or when the waker changes. Everything in between is a comparison of two
    /// pointers. The `Waker::clone` that fills the cache is paid once per waker
    /// too, and never on the path of a handler that answers during its inline
    /// poll — that path returns before this call.
    ///
    /// # Example
    /// ```rust,ignore
    /// let mut registered = None;
    /// // ... at every poll of the task:
    /// if token.poll_cancelled_cached(cx, &mut registered).is_ready() {
    ///     return Poll::Ready(());
    /// }
    /// ```
    #[inline]
    #[doc(hidden)]
    pub fn poll_cancelled_cached(
        &self,
        cx: &mut Context<'_>,
        registered: &mut Option<Waker>,
    ) -> Poll<()> {
        // Read first: a cancelled token is ready whatever the registration state.
        if self.is_cancelled() {
            return Poll::Ready(());
        }
        // The fast path: the waker is the one this task already registered, so
        // the list cannot tell us anything the flag above did not.
        if let Some(cached) = registered.as_ref() {
            if cached.will_wake(cx.waker()) {
                return Poll::Pending;
            }
        }
        // First poll of this task, or a different waker: the lock is paid here.
        let ready = self.poll_cancelled(cx);
        if ready.is_ready() {
            return ready;
        }
        *registered = Some(cx.waker().clone());
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

    /// The point of `poll_cancelled_cached`: after the first poll, later polls
    /// register nothing — and the one registration still wakes the task.
    #[test]
    fn poll_cancelled_cached_registers_the_waker_once_and_still_wakes_it() {
        let token = CancellationToken::new();
        let counter = Arc::new(CountWakes::default());
        let waker = Waker::from(Arc::clone(&counter));
        let mut cx = Context::from_waker(&waker);
        let mut registered = None;

        for _ in 0..5 {
            assert!(token
                .poll_cancelled_cached(&mut cx, &mut registered)
                .is_pending());
        }
        assert!(registered.is_some(), "the first poll must cache the waker");
        assert_eq!(counter.0.load(Ordering::SeqCst), 0, "nothing has fired yet");

        token.cancel();
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            1,
            "one wake for the task, whatever the number of polls"
        );
        assert!(token
            .poll_cancelled_cached(&mut cx, &mut registered)
            .is_ready());
    }

    /// A **changed** waker is registered, and wakes the task that owns it.
    ///
    /// This is the case a boolean would lose, and it is not a corner case:
    /// `Spawner::run_or_spawn` polls a task once with a no-op waker before the
    /// executor polls it with its own. A token that stopped registering after the
    /// first poll would then wake nobody, and the remote `Cancel` would be lost —
    /// which `tests/remote_cancel.rs` catches.
    #[test]
    fn poll_cancelled_cached_registers_a_waker_that_replaces_the_cached_one() {
        let token = CancellationToken::new();
        let mut registered = None;

        // First poll, with a no-op waker: the inline poll of `run_or_spawn`.
        let mut inline_cx = Context::from_waker(Waker::noop());
        assert!(token
            .poll_cancelled_cached(&mut inline_cx, &mut registered)
            .is_pending());

        // The task is then polled by the executor, which hands it another waker.
        let counter = Arc::new(CountWakes::default());
        let waker = Waker::from(Arc::clone(&counter));
        let mut cx = Context::from_waker(&waker);
        assert!(token
            .poll_cancelled_cached(&mut cx, &mut registered)
            .is_pending());

        token.cancel();
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            1,
            "the executor waker must have been registered, not the no-op one"
        );
    }

    /// A token already cancelled needs no registration at all: nothing is cached,
    /// and the call is ready on the first poll.
    #[test]
    fn poll_cancelled_cached_is_ready_without_registering_when_already_cancelled() {
        let token = CancellationToken::new();
        token.cancel();

        let mut cx = Context::from_waker(Waker::noop());
        let mut registered = None;
        assert!(token
            .poll_cancelled_cached(&mut cx, &mut registered)
            .is_ready());
        assert!(
            registered.is_none(),
            "a cancelled token has nothing to cache"
        );
    }

    #[test]
    fn debug_format_contains_state() {
        let token = CancellationToken::new();
        let dbg = format!("{:?}", token);
        assert!(dbg.contains("CancellationToken"));
        assert!(dbg.contains("false"));
    }
}
