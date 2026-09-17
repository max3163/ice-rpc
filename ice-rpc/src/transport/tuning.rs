//! Every tunable value of the transport, in one place.
//!
//! The table below mirrors section 4.5 of the `Readme.md`. Keeping the values
//! here, next to their justification, is what makes the memory budget of a
//! channel readable: a reader no longer has to grep half a dozen modules to
//! reconstruct it.
//!
//! | Setting | Value | Why |
//! |---|---|---|
//! | `SUBSCRIBER_BUFFER` | 1 024 | one term of the memory budget of a channel |
//! | `enable_safe_overflow` | **false** | enabled, a full subscriber buffer silently overwrites its oldest pending sample, losing a request that is never answered; disabled, `send()` reports `0` delivered and the publisher retries, turning the overflow into backpressure |
//! | `MAX_SLICE_LEN` | 256 | `slice` memory per sample; larger payloads grow the segment |
//! | `MAX_LOANED_SAMPLES` | 1 024 | sizes the data segment of a publisher: ~400 KB, against ~6.5 MB at the iceoryx2 default, untenable with dozens of services |
//! | `MAX_PUBLISHERS` / `MAX_SUBSCRIBERS` | 16 | a channel is shared: every consuming process publishes on it, every provider subscribes to it |
//! | `MAX_NODES` | 32 | processes that can open the same channel at once |
//! | `WAITSET_DEADLINE` | 1 ms | bounds the cost of a missed notification |
//! | `IDLE_SPINS` | 2 000 yields | the hot path stays at polling speed, the idle path blocks on the `WaitSet` |

use std::time::Duration;

/// Suffixes of the iceoryx2 services backing one channel.
pub(super) const REQUEST_SUFFIX: &str = "_req";
pub(super) const RESPONSE_SUFFIX: &str = "_resp";
pub(super) const REQUEST_NOTIFY_SUFFIX: &str = "_req_notify";
pub(super) const RESPONSE_NOTIFY_SUFFIX: &str = "_resp_notify";

/// Samples a subscriber can buffer before backpressure is reported.
pub(super) const SUBSCRIBER_BUFFER: usize = 1024;

/// Publishers accepted on one channel: one per process that sends on it.
pub(super) const MAX_PUBLISHERS: usize = 16;

/// Subscribers accepted on one channel: one per process and per channel.
pub(super) const MAX_SUBSCRIBERS: usize = 16;

/// Processes that can open the same channel at once.
pub(super) const MAX_NODES: usize = 32;

/// Samples a publisher can keep loaned at once; sizes its data segment.
pub(super) const MAX_LOANED_SAMPLES: usize = 1024;

/// Initial slice length of a sample; large payloads grow the segment on demand.
pub(super) const MAX_SLICE_LEN: usize = 256;

/// Payload alignment requested from iceoryx2, the alignment `rkyv::to_bytes`
/// produces, so a sample is decodable in place.
pub(super) const PAYLOAD_ALIGNMENT: usize = 16;

/// How long a call waits for the provider to be connected before failing.
///
/// Overridable with `ICE_RPC_PROVIDER_WAIT_MS`.
pub(super) const PROVIDER_WAIT_DEFAULT: Duration = Duration::from_secs(30);

/// How long a response waits for the consumer to be connected.
pub(super) const CONSUMER_WAIT_TIMEOUT: Duration = Duration::from_millis(500);

/// Sleep between two delivery attempts, once the spin budget is exhausted.
pub(super) const PUBLISH_RETRY_SLEEP: Duration = Duration::from_millis(1);

/// Consecutive delivery attempts spent yielding before the retry loop sleeps.
///
/// A full channel is the normal case of a burst; a sleep costs the system timer.
pub(super) const PUBLISH_SPIN_ATTEMPTS: u32 = 4_096;

/// Upper bound on how long a dispatch thread blocks before it drains again.
///
/// The wait itself is event-driven; this deadline is the safety net that bounds
/// the cost of a missed notification.
pub(super) const WAITSET_DEADLINE: Duration = Duration::from_millis(1);

/// Attempts made to open the ports of a channel before giving up on it.
///
/// iceoryx2 answers `SystemInFlux` when another process is creating or removing
/// that exact service at this instant — a race the next attempt wins. Without a
/// retry, one such blip left the channel dead for the rest of the process
/// lifetime, although the error is classified as retryable.
pub(super) const OPEN_RETRY_ATTEMPTS: u32 = 20;

/// Delay between two attempts at opening the ports of a channel.
///
/// 20 × 50 ms is a one-second budget: wider than a process teardown, and the
/// price is paid only by a channel that recovers. A non-retryable failure (a
/// service left by another build) returns immediately instead of waiting.
pub(super) const OPEN_RETRY_SLEEP: Duration = Duration::from_millis(50);

/// Processed samples between two termination checks on the busy path.
///
/// `SignalHandler::termination_requested()` takes a process-wide mutex.
pub(super) const SIGNAL_CHECK_SAMPLES: u32 = 256;

/// Consecutive empty polls spent spinning before a thread blocks on its
/// `WaitSet`.
pub(super) const IDLE_SPINS: u32 = 2_000;

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the values documented in section 4.5 of the `Readme.md`, so a
    /// change of tuning cannot slip through without updating the table.
    #[test]
    fn tuning_values_match_the_documented_table() {
        assert_eq!(SUBSCRIBER_BUFFER, 1024);
        assert_eq!(MAX_LOANED_SAMPLES, 1024);
        assert_eq!(MAX_SLICE_LEN, 256);
        assert_eq!(MAX_PUBLISHERS, 16);
        assert_eq!(MAX_SUBSCRIBERS, 16);
        assert_eq!(MAX_NODES, 32);
        assert_eq!(WAITSET_DEADLINE, Duration::from_millis(1));
        assert_eq!(IDLE_SPINS, 2_000);
    }

    /// The payload alignment must stay the one `rkyv::to_bytes` produces:
    /// a 16-byte-aligned sample is what lets a response be decoded in place.
    #[test]
    fn the_payload_alignment_is_the_rkyv_one() {
        assert_eq!(PAYLOAD_ALIGNMENT, 16);
    }

    /// The signal check masks the sample counter, so the constant must be a
    /// power of two for `& (N - 1)` to mean "every N samples".
    #[test]
    fn the_signal_check_interval_is_a_power_of_two() {
        assert!(SIGNAL_CHECK_SAMPLES.is_power_of_two());
    }
}
