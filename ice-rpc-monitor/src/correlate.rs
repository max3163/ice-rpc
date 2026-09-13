//! Correlation of requests with their responses, keyed by `correlation_id`.
//!
//! A response stream can carry several `Next` samples followed by one terminal
//! (`Complete` / `Error`). The entry is therefore kept until the terminal sample:
//! the latency is measured on the **first** response, and the call is closed only
//! when the stream ends.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use ice_rpc::gen::CORRELATION_ID_LEN;

/// A call awaiting its response stream.
#[derive(Debug, Clone)]
pub struct PendingCall {
    /// Channel the request was observed on.
    pub channel: String,
    /// Service id carried by the request header.
    pub service_id: u32,
    /// Method carried by the request header.
    pub method: String,
    /// Request emission timestamp (ns since the Unix epoch).
    pub request_ts_ns: u64,
    /// Request payload length in bytes.
    pub request_bytes: usize,
    /// Whether a first response has already been observed.
    pub first_response_seen: bool,
    started: Instant,
}

impl PendingCall {
    /// Creates a pending entry for a freshly observed request.
    pub fn new(
        channel: String,
        service_id: u32,
        method: String,
        request_ts_ns: u64,
        request_bytes: usize,
    ) -> Self {
        Self {
            channel,
            service_id,
            method,
            request_ts_ns,
            request_bytes,
            first_response_seen: false,
            started: Instant::now(),
        }
    }

    /// How long this call has been tracked.
    pub fn age(&self) -> Duration {
        self.started.elapsed()
    }
}

/// Outcome of resolving a response against the table.
#[derive(Debug)]
pub enum Resolution {
    /// The response belongs to a tracked call; the returned value is the state
    /// **before** this response was taken into account.
    Matched(PendingCall),
    /// No call is tracked for this correlation id.
    Unknown,
}

/// Bounded table of in-flight calls.
pub struct Correlate {
    entries: HashMap<[u8; CORRELATION_ID_LEN], PendingCall>,
    max: usize,
    ttl: Duration,
}

impl Correlate {
    /// Creates a table holding at most `max` calls, each evicted after `ttl`.
    pub fn new(max: usize, ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            max,
            ttl,
        }
    }

    /// Number of calls currently tracked.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Registers a new call; returns the calls evicted to stay under capacity.
    pub fn insert(&mut self, cid: [u8; CORRELATION_ID_LEN], call: PendingCall) -> Vec<PendingCall> {
        let mut evicted = Vec::new();
        if self.entries.len() >= self.max {
            if let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, call)| call.started)
                .map(|(cid, _)| *cid)
            {
                if let Some(call) = self.entries.remove(&oldest) {
                    evicted.push(call);
                }
            }
        }
        self.entries.insert(cid, call);
        evicted
    }

    /// Resolves a response; a `terminal` response closes the call.
    pub fn on_response(&mut self, cid: &[u8; CORRELATION_ID_LEN], terminal: bool) -> Resolution {
        if terminal {
            match self.entries.remove(cid) {
                Some(call) => Resolution::Matched(call),
                None => Resolution::Unknown,
            }
        } else {
            match self.entries.get_mut(cid) {
                Some(call) => {
                    let previous = call.clone();
                    call.first_response_seen = true;
                    Resolution::Matched(previous)
                }
                None => Resolution::Unknown,
            }
        }
    }

    /// Evicts every call older than the configured TTL.
    pub fn sweep(&mut self) -> Vec<PendingCall> {
        let ttl = self.ttl;
        let mut evicted = Vec::new();
        self.entries.retain(|_, call| {
            if call.age() >= ttl {
                evicted.push(call.clone());
                false
            } else {
                true
            }
        });
        evicted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call() -> PendingCall {
        PendingCall::new("c".into(), 1, "m".into(), 0, 0)
    }

    #[test]
    fn a_clean_call_is_matched_then_closed() {
        let mut table = Correlate::new(8, Duration::from_secs(60));
        let cid = [1u8; CORRELATION_ID_LEN];
        table.insert(cid, call());

        match table.on_response(&cid, true) {
            Resolution::Matched(previous) => assert!(!previous.first_response_seen),
            other => panic!("expected a match, got {other:?}"),
        }
        assert_eq!(table.len(), 0);
        assert!(matches!(table.on_response(&cid, true), Resolution::Unknown));
    }

    #[test]
    fn a_stream_keeps_the_call_until_the_terminal_sample() {
        let mut table = Correlate::new(8, Duration::from_secs(60));
        let cid = [2u8; CORRELATION_ID_LEN];
        table.insert(cid, call());

        // First (non-terminal) response: kept, and flagged as first.
        assert!(matches!(
            table.on_response(&cid, false),
            Resolution::Matched(_)
        ));
        assert_eq!(table.len(), 1);
        // Second response is no longer the first one.
        match table.on_response(&cid, false) {
            Resolution::Matched(previous) => assert!(previous.first_response_seen),
            other => panic!("expected a match, got {other:?}"),
        }
        // Terminal closes it.
        assert!(matches!(
            table.on_response(&cid, true),
            Resolution::Matched(_)
        ));
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn capacity_evicts_the_oldest_call() {
        let mut table = Correlate::new(1, Duration::from_secs(60));
        table.insert([1u8; CORRELATION_ID_LEN], call());
        let evicted = table.insert([2u8; CORRELATION_ID_LEN], call());
        assert_eq!(evicted.len(), 1);
        assert_eq!(table.len(), 1);
    }
}
