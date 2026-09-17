//! Coalesced wake-up notifications.
//!
//! The thread to wake runs in another process, so its parked state is not
//! observable from the sender: the notification is emitted for the pair, at most
//! once per [`NOTIFY_COALESCE_WINDOW`] and per channel.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::global::Global;

/// Minimum delay between two wake-up notifications of the same channel.
///
/// Far shorter than the time a receiver keeps polling before it parks
/// ([`super::IDLE_SPINS`] yields), so a coalesced wake-up is only ever skipped
/// while the receiver is still awake.
pub(super) const NOTIFY_COALESCE_WINDOW: Duration = Duration::from_micros(100);

/// Coalescing window of the wake-up notifications of one channel.
///
/// One instance per notification direction: the consumer throttles the wake-up
/// of the provider, the provider the wake-up of the consumers. It is shared
/// between the threads that publish on a channel, hence the atomic counter.
pub(super) struct Coalescer(AtomicU64);

/// Value meaning "this coalescer has never notified".
///
/// Timestamps are stored shifted by one ([`Coalescer::should_notify`]), so `0`
/// stays free to mark the initial state: without it, a notification emitted
/// during the first microseconds of the process would be coalesced against a
/// timestamp of zero and lost.
const NEVER: u64 = 0;

impl Coalescer {
    /// Creates a coalescer that has never notified.
    pub(super) const fn new() -> Self {
        Self(AtomicU64::new(NEVER))
    }

    /// Returns `true` when a notification must be emitted now, and records it.
    ///
    /// `Relaxed` ordering is enough: the value only throttles a redundant
    /// wake-up, and a lost update costs one extra notification at worst.
    pub(super) fn should_notify(&self) -> bool {
        let now = clock_us() + 1;
        let previous = self.0.load(Ordering::Relaxed);
        if previous != NEVER
            && now.saturating_sub(previous) < NOTIFY_COALESCE_WINDOW.as_micros() as u64
        {
            return false;
        }
        self.0.store(now, Ordering::Relaxed);
        true
    }
}

impl Default for Coalescer {
    fn default() -> Self {
        Self::new()
    }
}

/// Monotonic microsecond clock shared by every coalescer.
fn clock_us() -> u64 {
    static BASE: Global<Instant> = Global::new();
    BASE.get_or_init(Instant::now).elapsed().as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_notification_is_always_emitted() {
        let coalescer = Coalescer::new();
        assert!(coalescer.should_notify());
    }

    #[test]
    fn a_second_notification_inside_the_window_is_coalesced() {
        let coalescer = Coalescer::new();
        assert!(coalescer.should_notify());
        assert!(!coalescer.should_notify());
    }

    #[test]
    fn a_notification_after_the_window_is_emitted() {
        let coalescer = Coalescer::new();
        let _ = coalescer.should_notify();
        std::thread::sleep(NOTIFY_COALESCE_WINDOW * 2);
        assert!(coalescer.should_notify());
    }
}
