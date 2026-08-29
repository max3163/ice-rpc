//! Chainable wrapper for RPC method calls with a configurable timeout.
//!
//! Allows chaining `.with_timeout(Duration)` before the `.await`.
//!
//! # Example
//! ```rust,ignore
//! proxy.get_user_age("Alice".into())
//!     .with_timeout(Duration::from_secs(5))
//!     .await
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::RpcError;

/// Alias for the boxed future returned by generated RPC client methods.
type RpcCallFuture<T, E> =
    Pin<Box<dyn Future<Output = Result<crate::Stream<T, E>, RpcError>> + Send>>;

/// Future for an RPC method call with a configurable timeout.
///
/// Built by the generated client methods. Supports chaining
/// `.with_timeout()` to override the service location timeout.
pub struct MethodCall<T, E> {
    inner: RpcCallFuture<T, E>,
    /// Timeout in seconds, shared with the inner future.
    timeout_secs: Arc<AtomicU64>,
}

#[allow(dead_code)]
impl<T, E> MethodCall<T, E> {
    /// Creates a new `MethodCall` with the specified timeout (in seconds).
    pub(crate) fn new(
        future: impl Future<Output = Result<crate::Stream<T, E>, RpcError>> + Send + 'static,
        timeout_secs: u64,
    ) -> Self {
        Self {
            inner: Box::pin(future),
            timeout_secs: Arc::new(AtomicU64::new(timeout_secs)),
        }
    }

    /// Creates with a shared `Arc<AtomicU64>` for the timeout.
    pub(crate) fn with_shared_timeout(
        future: impl Future<Output = Result<crate::Stream<T, E>, RpcError>> + Send + 'static,
        timeout_secs: Arc<AtomicU64>,
    ) -> Self {
        Self {
            inner: Box::pin(future),
            timeout_secs,
        }
    }

    /// Sets a custom timeout for the service location.
    ///
    /// Replaces the default timeout (global `RPC_CALL_TIMEOUT_SECS` or `#[timeout]`).
    ///
    /// # Example
    /// ```rust,ignore
    /// proxy.get_user_age("Alice".into())
    ///     .with_timeout(Duration::from_secs(5))
    ///     .await
    /// ```
    pub fn with_timeout(self, timeout: Duration) -> Self {
        self.timeout_secs
            .store(timeout.as_secs(), Ordering::Relaxed);
        self
    }

    /// Clones the timeout `Arc<AtomicU64>` to pass it to the future.
    pub(crate) fn timeout_arc(&self) -> Arc<AtomicU64> {
        self.timeout_secs.clone()
    }
}

impl<T, E> Future for MethodCall<T, E> {
    type Output = Result<crate::Stream<T, E>, RpcError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    type PendingResult = Result<crate::Stream<(), ()>, crate::RpcError>;

    fn pending_future() -> impl Future<Output = PendingResult> + Send + 'static {
        futures::future::pending::<PendingResult>()
    }

    #[test]
    fn new_initializes_timeout() {
        let call = MethodCall::new(pending_future(), 3);
        assert_eq!(call.timeout_arc().load(Ordering::Relaxed), 3);
    }

    #[test]
    fn with_shared_timeout_shares_arc() {
        let shared = Arc::new(AtomicU64::new(9));
        let call = MethodCall::with_shared_timeout(pending_future(), shared.clone());
        assert_eq!(call.timeout_arc().load(Ordering::Relaxed), 9);
        shared.store(11, Ordering::Relaxed);
        assert_eq!(call.timeout_arc().load(Ordering::Relaxed), 11);
    }

    #[test]
    fn with_timeout_overrides_value() {
        let call = MethodCall::new(pending_future(), 1).with_timeout(Duration::from_secs(5));
        assert_eq!(call.timeout_arc().load(Ordering::Relaxed), 5);
    }

    #[test]
    fn poll_forwards_inner_ready_future() {
        let (_tx, rx) = async_channel::unbounded::<crate::Event<(), ()>>();
        let call = MethodCall::new(futures::future::ready(Ok(rx)), 1);
        let result = futures::executor::block_on(call);
        assert!(result.is_ok());
    }
}
