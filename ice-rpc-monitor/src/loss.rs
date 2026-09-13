//! Sample-loss detection from the per-publisher `seq` field of the header.
//!
//! `seq` is monotonic per publisher, and a channel hosts exactly one publisher
//! per `(direction, process)` — the consumer's publisher for `_req`, the
//! provider's publisher for `_resp`. A hole in the observed sequence therefore
//! proves that samples were lost between two observations.

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use ice_rpc::monitor::Direction;

/// Identity of one publisher on one channel.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Publisher {
    channel: String,
    direction: Direction,
    emitter_pid: u32,
}

/// Tracks the last observed sequence of every publisher.
#[derive(Default)]
pub struct LossTracker {
    last: HashMap<Publisher, u64>,
}

impl LossTracker {
    /// Records one observation and returns how many samples were missed since the
    /// previous one.
    ///
    /// The first observation of a publisher reports `0` (nothing to compare
    /// against). A non-increasing sequence (reordering, or a publisher restart
    /// that resets its counter) also reports `0` rather than a bogus huge gap.
    pub fn observe(
        &mut self,
        channel: &str,
        direction: Direction,
        emitter_pid: u32,
        seq: u64,
    ) -> u64 {
        let key = Publisher {
            channel: channel.to_owned(),
            direction,
            emitter_pid,
        };
        match self.last.entry(key) {
            Entry::Occupied(mut entry) => {
                let previous = *entry.get();
                entry.insert(seq);
                if seq > previous.saturating_add(1) {
                    seq - previous - 1
                } else {
                    0
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(seq);
                0
            }
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
        assert_eq!(tracker.observe("c", Direction::Request, 1, 0), 0);
        assert_eq!(tracker.observe("c", Direction::Request, 1, 1), 0);
        assert_eq!(tracker.observe("c", Direction::Request, 1, 2), 0);
    }

    #[test]
    fn a_hole_is_reported_as_the_number_of_missed_samples() {
        let mut tracker = LossTracker::default();
        tracker.observe("c", Direction::Response, 7, 10);
        assert_eq!(tracker.observe("c", Direction::Response, 7, 15), 4);
    }

    #[test]
    fn publishers_are_tracked_independently() {
        let mut tracker = LossTracker::default();
        tracker.observe("c", Direction::Request, 1, 0);
        tracker.observe("c", Direction::Request, 2, 5);
        // A different process, direction or channel must not be compared.
        assert_eq!(tracker.observe("c", Direction::Request, 1, 1), 0);
        assert_eq!(tracker.observe("c", Direction::Response, 1, 3), 0);
        assert_eq!(tracker.observe("d", Direction::Request, 1, 3), 0);
        assert_eq!(tracker.tracked(), 4);
    }

    #[test]
    fn a_reset_sequence_does_not_report_a_bogus_gap() {
        let mut tracker = LossTracker::default();
        tracker.observe("c", Direction::Request, 1, 100);
        assert_eq!(tracker.observe("c", Direction::Request, 1, 0), 0);
        assert_eq!(tracker.observe("c", Direction::Request, 1, 1), 0);
    }
}
