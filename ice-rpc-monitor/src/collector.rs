//! Acquisition loop: channel discovery, read-only draining and aggregation.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ice_rpc::gen::{fmt_correlation_id, is_pid_alive, EventKind, RpcHeader};
use ice_rpc::monitor::{discover_channels, Direction, DirectionView};

use crate::config::Config;
use crate::correlate::{Correlate, PendingCall, Resolution};
use crate::loss::LossTracker;
use crate::metrics::Metrics;
use crate::traces::{TraceRecord, TraceSink};

/// Samples drained from one view per iteration, bounding the loop latency.
const DRAIN_BUDGET: usize = 4096;
/// Idle wait before draining again when nothing was received.
const IDLE_WAIT: Duration = Duration::from_millis(5);
/// Capacity of the trace queue (records are dropped beyond it).
const TRACE_QUEUE: usize = 16_384;

/// One subscribed direction of one channel.
struct View {
    channel: String,
    direction: Direction,
    view: DirectionView,
}

/// Maps a header event kind to its label.
fn kind_name(kind: EventKind) -> &'static str {
    match kind {
        EventKind::Request => "request",
        EventKind::Next => "next",
        EventKind::Complete => "complete",
        EventKind::Error => "error",
    }
}

/// The out-of-band observer.
pub struct Monitor {
    config: Config,
    metrics: Arc<Metrics>,
    correlate: Correlate,
    loss: LossTracker,
    views: Vec<View>,
    trace: Option<TraceSink>,
    traced: u64,
    /// Emitter pids seen so far, with their last known liveness.
    pids: HashMap<u32, bool>,
    last_discover: Instant,
    last_sweep: Instant,
    last_liveness: Instant,
    wait_cursor: usize,
}

impl Monitor {
    /// Builds the observer, opening the trace sink when tracing is enabled.
    ///
    /// # Errors
    /// Returns a message when the trace file cannot be created.
    pub fn new(config: Config, metrics: Arc<Metrics>) -> Result<Self, String> {
        let trace = if config.trace_sample_rate == 0 {
            None
        } else {
            match &config.trace_file {
                Some(path) => Some(
                    TraceSink::file(path, TRACE_QUEUE)
                        .map_err(|e| format!("cannot open trace file '{}': {e}", path.display()))?,
                ),
                None => Some(TraceSink::stdout(TRACE_QUEUE)),
            }
        };

        let correlate = Correlate::new(config.max_inflight, config.call_ttl);
        let now = Instant::now();
        Ok(Self {
            config,
            metrics,
            correlate,
            loss: LossTracker::default(),
            views: Vec::new(),
            trace,
            traced: 0,
            pids: HashMap::new(),
            last_discover: now,
            last_sweep: now,
            last_liveness: now,
            wait_cursor: 0,
        })
    }

    /// Number of traces dropped by a saturated sink.
    pub fn dropped_traces(&self) -> u64 {
        self.trace.as_ref().map(TraceSink::dropped).unwrap_or(0)
    }

    /// Runs the acquisition loop until `cancel` is set.
    ///
    /// # Errors
    /// Currently infallible; kept as a `Result` so callers handle startup
    /// failures uniformly.
    pub fn run(mut self, cancel: &AtomicBool) -> Result<(), String> {
        log::info!(
            "[monitor] observing {} channel(s) requested",
            self.config.channels.len()
        );
        self.refresh_channels();

        while !cancel.load(Ordering::Relaxed) {
            if self.last_discover.elapsed() >= self.config.discover_interval {
                self.last_discover = Instant::now();
                self.refresh_channels();
            }

            let received = self.drain_all();

            if self.last_sweep.elapsed() >= self.config.sweep_interval {
                self.last_sweep = Instant::now();
                self.sweep();
            }
            if self.last_liveness.elapsed() >= self.config.liveness_interval {
                self.last_liveness = Instant::now();
                self.check_liveness();
            }

            if !received {
                self.wait_idle();
            }
        }

        log::info!(
            "[monitor] stopping: {} view(s), {} publisher(s) tracked, {} call(s) in flight, {} trace(s) dropped",
            self.views.len(),
            self.loss.tracked(),
            self.correlate.len(),
            self.dropped_traces()
        );
        Ok(())
    }

    /// Discovers the channels and attaches to the missing directions.
    fn refresh_channels(&mut self) {
        let targets = if self.config.channels.is_empty() {
            match discover_channels() {
                Ok(channels) => channels,
                Err(e) => {
                    log::warn!("[monitor] service discovery failed: {e}");
                    return;
                }
            }
        } else {
            self.config.channels.clone()
        };

        for channel in targets {
            self.open_if_missing(&channel, Direction::Request);
            self.open_if_missing(&channel, Direction::Response);
        }
    }

    /// Attaches to one direction of a channel, unless already attached.
    fn open_if_missing(&mut self, channel: &str, direction: Direction) {
        if self
            .views
            .iter()
            .any(|v| v.channel == channel && v.direction == direction)
        {
            return;
        }
        match DirectionView::open(channel, direction) {
            Ok(view) => {
                log::info!(
                    "[monitor] attached to '{channel}' ({direction:?}), buffer={} sample(s)",
                    view.buffer_size()
                );
                self.views.push(View {
                    channel: channel.to_owned(),
                    direction,
                    view,
                });
            }
            // The service may not exist yet: retried on the next discovery tick.
            Err(e) => log::debug!("[monitor] '{channel}' ({direction:?}) not available yet: {e}"),
        }
    }

    /// Drains every view once; returns whether at least one sample was read.
    fn drain_all(&mut self) -> bool {
        let mut received = false;
        for index in 0..self.views.len() {
            let channel = self.views[index].channel.clone();
            let direction = self.views[index].direction;
            for _ in 0..DRAIN_BUDGET {
                match self.views[index].view.try_receive() {
                    Ok(Some((header, payload_len))) => {
                        received = true;
                        self.process(&channel, direction, header, payload_len);
                    }
                    Ok(None) => break,
                    Err(e) => {
                        log::warn!("[monitor] receive error on '{channel}': {e}");
                        break;
                    }
                }
            }
        }
        received
    }

    /// Aggregates one observed sample.
    fn process(
        &mut self,
        channel: &str,
        direction: Direction,
        header: RpcHeader,
        payload_len: usize,
    ) {
        // Loss detection must see every sample, whatever its kind.
        let missed = self
            .loss
            .observe(channel, direction, header.emitter_pid, header.seq);
        self.metrics
            .on_sample_gap(channel, direction, header.emitter_pid, missed);
        self.metrics.on_payload(channel, direction, payload_len);
        self.pids.entry(header.emitter_pid).or_insert(true);

        match direction {
            Direction::Request => self.on_request(channel, &header, payload_len),
            Direction::Response => self.on_response(channel, &header, payload_len),
        }
    }

    /// Handles one request sample.
    fn on_request(&mut self, channel: &str, header: &RpcHeader, payload_len: usize) {
        if header.event_kind() != EventKind::Request {
            log::debug!(
                "[monitor] '{channel}': unexpected {:?} sample on the request channel",
                header.event_kind()
            );
            return;
        }

        let method = header.method();
        self.metrics.on_request(channel, header.service_id, method);
        self.metrics.on_inflight(channel, header.service_id, 1);

        let call = PendingCall::new(
            channel.to_owned(),
            header.service_id,
            method.to_owned(),
            header.timestamp_ns,
            payload_len,
        );
        for evicted in self.correlate.insert(header.correlation_id, call) {
            self.metrics.on_unmatched_request(&evicted.channel);
            self.metrics
                .on_inflight(&evicted.channel, evicted.service_id, -1);
        }
    }

    /// Handles one response sample, closing the call on a terminal kind.
    fn on_response(&mut self, channel: &str, header: &RpcHeader, payload_len: usize) {
        let kind = header.event_kind();
        let name = kind_name(kind);
        self.metrics.on_response(channel, header.service_id, name);
        let terminal = kind.is_terminal();

        match self.correlate.on_response(&header.correlation_id, terminal) {
            Resolution::Unknown => self.metrics.on_orphan_response(channel),
            Resolution::Matched(call) => {
                if !call.first_response_seen {
                    if header.timestamp_ns >= call.request_ts_ns {
                        let seconds = (header.timestamp_ns - call.request_ts_ns) as f64 / 1e9;
                        self.metrics.on_latency(
                            &call.channel,
                            call.service_id,
                            &call.method,
                            seconds,
                        );
                    } else {
                        // A clock adjustment made the response look older.
                        self.metrics.on_clock_skew();
                    }
                }
                if terminal {
                    self.metrics.on_inflight(&call.channel, call.service_id, -1);
                    self.emit_trace(channel, header, payload_len, &call, name);
                }
            }
        }
    }

    /// Emits one trace record for a completed call, subject to sampling.
    fn emit_trace(
        &mut self,
        channel: &str,
        header: &RpcHeader,
        payload_len: usize,
        call: &PendingCall,
        kind: &'static str,
    ) {
        if self.trace.is_none() {
            return;
        }
        self.traced = self.traced.wrapping_add(1);
        let rate = self.config.trace_sample_rate;
        if rate == 0 || self.traced % rate != 0 {
            return;
        }

        let latency_us = header.timestamp_ns.saturating_sub(call.request_ts_ns) / 1_000;
        let trace_id = fmt_correlation_id(&header.correlation_id);
        let record = TraceRecord {
            trace_id: &trace_id,
            channel,
            service_id: header.service_id,
            method: &call.method,
            event_kind: kind,
            emitter_pid: header.emitter_pid,
            request_bytes: call.request_bytes,
            response_bytes: payload_len,
            latency_us,
        };
        if let Some(sink) = &self.trace {
            sink.emit(&record);
        }
    }

    /// Evicts the calls that outlived their TTL.
    fn sweep(&mut self) {
        for evicted in self.correlate.sweep() {
            self.metrics.on_unmatched_request(&evicted.channel);
            self.metrics
                .on_inflight(&evicted.channel, evicted.service_id, -1);
        }
    }

    /// Refreshes the liveness gauge and counts the disappeared emitters.
    fn check_liveness(&mut self) {
        let mut alive = 0i64;
        let mut crashed = 0u64;
        for (pid, was_alive) in self.pids.iter_mut() {
            let now_alive = is_pid_alive(*pid);
            if now_alive {
                alive += 1;
            } else if *was_alive {
                crashed += 1;
                log::warn!("[monitor] emitter {pid} disappeared");
            }
            *was_alive = now_alive;
        }
        self.metrics.set_nodes_alive(alive);
        if crashed > 0 {
            self.metrics.add_node_crash(crashed);
        }
    }

    /// Blocks briefly on one listener (round-robin) when nothing was drained.
    fn wait_idle(&mut self) {
        if self.views.is_empty() {
            std::thread::sleep(Duration::from_millis(250));
            return;
        }
        self.wait_cursor = (self.wait_cursor + 1) % self.views.len();
        // A wake-up on any channel is followed by a drain of *every* view, so
        // blocking on a single listener is enough and avoids a WaitSet.
        let _ = self.views[self.wait_cursor].view.wait(IDLE_WAIT);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_event_kind_has_a_label() {
        assert_eq!(kind_name(EventKind::Request), "request");
        assert_eq!(kind_name(EventKind::Next), "next");
        assert_eq!(kind_name(EventKind::Complete), "complete");
        assert_eq!(kind_name(EventKind::Error), "error");
    }

    #[test]
    fn a_monitor_without_tracing_builds() {
        let monitor = Monitor::new(Config::default(), Arc::new(Metrics::new()));
        assert!(monitor.is_ok());
        assert_eq!(monitor.expect("built").dropped_traces(), 0);
    }
}
