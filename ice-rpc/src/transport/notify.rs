//! Coalesced wake-up notifications.
//!
//! The thread to wake runs in another process, so its parked state is not
//! observable from the sender: the notification is emitted for the pair, at most
//! once per [`NOTIFY_COALESCE_WINDOW`] and per channel.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// Minimum delay between two wake-up notifications of the same channel.
///
/// Far shorter than the time a receiver keeps polling before it parks
/// ([`super::IDLE_SPINS`] yields), so a coalesced wake-up is only ever skipped
/// while the receiver is still awake.
pub(super) const NOTIFY_COALESCE_WINDOW: Duration = Duration::from_micros(100);

/// Microsecond timestamp of the last wake-up, for the single-threaded callers.
pub(super) fn should_notify_local(last: &mut u64) -> bool {
    let now = clock_us();
    if now.saturating_sub(*last) < NOTIFY_COALESCE_WINDOW.as_micros() as u64 {
        return false;
    }
    *last = now;
    true
}

/// Same as [`should_notify_local`], for callers that share the timestamp
/// across threads.
pub(super) fn should_notify(last: &AtomicU64) -> bool {
    let now = clock_us();
    let previous = last.load(Ordering::Relaxed);
    if now.saturating_sub(previous) < NOTIFY_COALESCE_WINDOW.as_micros() as u64 {
        return false;
    }
    last.store(now, Ordering::Relaxed);
    true
}

/// Monotonic microsecond clock shared by every coalescer.
fn clock_us() -> u64 {
    static BASE: OnceLock<Instant> = OnceLock::new();
    BASE.get_or_init(Instant::now).elapsed().as_micros() as u64
}
