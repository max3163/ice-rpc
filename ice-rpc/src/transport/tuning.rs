//! The values the transport truly owns, in one place.
//!
//! Two families live here:
//!
//! - the **protocol invariants** (`PAYLOAD_ALIGNMENT`, `enable_safe_overflow`):
//!   changing one is a wire-protocol change and requires bumping
//!   [`PROTOCOL_VERSION`](crate::types::consts::PROTOCOL_VERSION), so they are
//!   compiled in and never exposed;
//! - the **runtime tuning** (`WAITSET_DEADLINE`, `IDLE_SPINS`, ...): invisible to
//!   a peer, so a process may adjust them on its own.
//!
//! The service limits one might expect here — publishers, subscribers, nodes,
//! subscriber buffer size, slice length and loaned samples — are deliberately
//! **absent**. They are deployment policy, not implementation choices: every
//! participant inherits them from the iceoryx2 configuration it resolves. The one
//! exception is the slice length, which iceoryx2 0.10 does not expose in its
//! configuration and whose builder default is `1`; the developer declares it per
//! channel with `#[service(max_slice_len = N)]`, see [`DEFAULT_MAX_SLICE_LEN`].
//!
//! | Setting | Value | Why |
//! |---|---|---|
//! | `PAYLOAD_ALIGNMENT` | 16 | the alignment `rkyv::to_bytes` produces, so a sample decodes in place |
//! | `REQUEST_SCRATCH_CAPACITY` | 256 bytes | starting size of the per-thread request buffer; rkyv grows it when a request is larger |
//! | `enable_safe_overflow` | **false** | enabled, a full subscriber buffer silently overwrites its oldest pending sample, losing a request that is never answered; disabled, `send()` reports `0` delivered and the publisher retries, turning the overflow into backpressure |
//! | `WAITSET_DEADLINE` | 1 ms | bounds the cost of a missed notification |
//! | `IDLE_SPINS` | 2 000 yields | the hot path stays at polling speed, the idle path blocks on the `WaitSet` |

use std::time::Duration;

/// Suffixes of the iceoryx2 services backing one channel.
pub(super) const REQUEST_SUFFIX: &str = "_req";
pub(super) const RESPONSE_SUFFIX: &str = "_resp";
pub(super) const REQUEST_NOTIFY_SUFFIX: &str = "_req_notify";
pub(super) const RESPONSE_NOTIFY_SUFFIX: &str = "_resp_notify";
pub const DEFAULT_MAX_SLICE_LEN: usize = 256;

/// Payload alignment requested from iceoryx2, the alignment `rkyv::to_bytes`
/// produces, so a sample is decodable in place. It is also the alignment every
/// buffer that holds an encoded request uses, so the payload stays aligned when
/// it is written.
pub(super) const PAYLOAD_ALIGNMENT: usize = 16;

/// Initial capacity of the per-thread request-encoding buffer, in bytes.
///
/// A starting size, not a limit: rkyv grows the buffer when a request is larger.
/// 256 covers the common small request, so the hot path allocates once per
/// **thread** instead of once per call.
pub(super) const REQUEST_SCRATCH_CAPACITY: usize = 256;

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
        assert_eq!(DEFAULT_MAX_SLICE_LEN, 256);
        assert_eq!(REQUEST_SCRATCH_CAPACITY, 256);
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
