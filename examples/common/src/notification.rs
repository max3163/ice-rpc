//! Notification service demonstrating the `subscribe` (push) mechanism.
//!
//! A service that streams several values to its consumer is exactly the use case
//! `subscribe` was designed for: the consumer subscribes once and receives
//! `next` calls until `complete`.

use ice_rpc::{service, Observable};

/// Streams notifications to whoever subscribes to `watch`.
#[service("NotificationService", discovery_timeout = "5s")]
pub trait NotificationService {
    /// Emits `count` notifications, one every 100 ms, then completes.
    ///
    /// This is a **multi-value** stream: on the wire each value travels as its
    /// own sample, followed by a terminal `Complete`.
    async fn watch(&self, count: u32) -> Observable<u32, String>;

    /// Single-value health check.
    ///
    /// Implemented with `of(value)`, so it travels as **one** wire sample.
    async fn ping(&self) -> Observable<u32, String>;
}
