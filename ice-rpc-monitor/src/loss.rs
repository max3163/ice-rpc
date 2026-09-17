//! Sample-loss detection from the `seq` field of the header.
//!
//! `seq` is monotonic per publisher port, and a port publishes on exactly one
//! `(channel, direction)` pair — the consumer's publisher for `_req`, the
//! provider's for `_resp`. A hole in the observed sequence therefore proves that
//! samples were lost between two observations.
//!
//! Two properties make the count trustworthy:
//!
//! - the publisher is keyed on iceoryx2's **native** `publisher_id`, not on the
//!   emitter PID: the id is unique per publisher port and survives a PID reuse, so
//!   a process restart can never alias onto the sequence of its predecessor;
//! - keying on the id rather than on the channel keeps several publishers of one
//!   channel — two consumers of the same service — from looking like a single
//!   interrupted sequence, which would fabricate gaps.
//!
//! What is measured here is only the **observer's own** loss: the observer
//! attaches as one subscriber more and cannot back-pressure the publisher, so a
//! lagging observer is skipped instead of slowing the application down. This
//! counter is what tells it whether the numbers it renders are complete, which no
//! error can tell it — iceoryx2 drops samples per subscriber, silently.

use std::collections::HashMap;

/// Last observed sequence of every publisher port.
#[derive(Default)]
pub struct LossTracker {
    last: HashMap<u128, u64>,
}

impl LossTracker {
    /// Records one observation and returns how many samples were missed since the
    /// previous one of the same publisher.
    ///
    /// The first observation of a publisher reports `0` (nothing to compare
    /// against). A non-increasing sequence (reordering, or a publisher restart
    /// that resets its counter) also reports `0` rather than a bogus huge gap.
    pub fn observe(&mut self, publisher_id: u128, seq: u64) -> u64 {
        match self.last.insert(publisher_id, seq) {
            Some(previous) if seq > previous.saturating_add(1) => seq - previous - 1,
            _ => 0,
        }
    }

    /// Number of publishers currently tracked.
    pub fn tracked(&self) -> usize {
        self.last.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contiguous_sequences_report_no_gap() {
        let mut tracker = LossTracker::default();
        assert_eq!(tracker.observe(1, 0), 0);
        assert_eq!(tracker.observe(1, 1), 0);
        assert_eq!(tracker.observe(1, 2), 0);
    }

    #[test]
    fn a_hole_is_reported_as_the_number_of_missed_samples() {
        let mut tracker = LossTracker::default();
        tracker.observe(7, 10);
        assert_eq!(tracker.observe(7, 15), 4);
    }

    #[test]
    fn publishers_are_tracked_independently() {
        let mut tracker = LossTracker::default();
        tracker.observe(1, 0);
        tracker.observe(2, 5);
        // A second publisher of the same channel (another consumer) must never be
        // compared with the first one.
        assert_eq!(tracker.observe(1, 1), 0);
        assert_eq!(tracker.observe(2, 6), 0);
        assert_eq!(tracker.tracked(), 2);
    }

    #[test]
    fn a_restart_under_a_new_publisher_id_starts_fresh() {
        let mut tracker = LossTracker::default();
        tracker.observe(7, 42);
        // Same process, same PID, but a new publisher port: no bogus gap.
        assert_eq!(tracker.observe(8, 0), 0);
        assert_eq!(tracker.tracked(), 2);
    }

    #[test]
    fn a_reset_sequence_does_not_report_a_bogus_gap() {
        let mut tracker = LossTracker::default();
        tracker.observe(1, 100);
        assert_eq!(tracker.observe(1, 0), 0);
        assert_eq!(tracker.observe(1, 1), 0);
    }
}
