//! Acquisition loop: channel discovery, read-only draining and aggregation.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ice_rpc::gen::{fmt_correlation_id, is_pid_alive, EventKind, RpcHeader, CORRELATION_ID_LEN};
use ice_rpc::monitor::{discover_channels, Direction, DirectionView, Emitter};

use crate::config::Config;
use crate::correlate::{Correlate, PendingCall, Resolution};
use crate::health::{ChannelHealth, Scanner};
use crate::loss::LossTracker;
use crate::metrics::Metrics;
use crate::traces::{RecentBuffer, TraceRecord, TraceSink};

/// Samples drained from one view per iteration, bounding the loop latency.
const DRAIN_BUDGET: usize = 4096;
/// Idle wait before draining again when nothing was received.
const IDLE_WAIT: Duration = Duration::from_millis(5);
/// Delay before re-attempting the channels that are known but not attached yet.
///
/// Independent of [`Config::discover_interval`], which paces the *listing* of the
/// channels: that one costs a scan of the service registry, while a channel whose
/// service does not exist yet appears within milliseconds of its provider's ports
/// being created. Retrying only on the discovery interval — seconds by default —
/// left the observer blind to the first calls of a provider that starts just
/// after it, which is the very traffic it was started for.
const ATTACH_RETRY: Duration = Duration::from_millis(50);
/// Capacity of the trace queue (records are dropped beyond it).
const TRACE_QUEUE: usize = 16_384;
/// How long a response waits for its still-unseen request.
///
/// A passive observer drains the two directions independently, so a response
/// can overtake its request; holding it briefly turns that race into a match
/// instead of a spurious orphan.
const ORPHAN_GRACE: Duration = Duration::from_millis(250);
/// Maximum number of deferred responses held at once.
const MAX_DEFERRED: usize = 16_384;
/// Maximum number of deferred samples kept for one correlation id.
const MAX_DEFERRED_SAMPLES: usize = 4_096;

/// One subscribed direction of one channel.
struct View {
    channel: String,
    direction: Direction,
    view: DirectionView,
    /// Whether the payload of the samples must be captured (detail mode).
    detail: bool,
}

/// One drained sample: metadata only, or the payload too in detail mode.
#[derive(Clone)]
enum Sample {
    /// Stats mode: only the payload length is known.
    Meta { payload_len: usize },
    /// Detail mode: the payload was copied out of the shared-memory sample.
    Full { payload: Vec<u8> },
}

impl Sample {
    /// Payload length in bytes.
    fn payload_len(&self) -> usize {
        match self {
            Sample::Meta { payload_len } => *payload_len,
            Sample::Full { payload } => payload.len(),
        }
    }
}

/// Fallback text for a payload whose service has no registered decoder.
fn undecoded(payload_len: usize) -> String {
    format!("<{payload_len} bytes, no decoder>")
}

/// Responses observed before their request was drained.
///
/// The two directions are drained independently and iceoryx2 gives no ordering
/// guarantee between separate services, so a response can be seen before its
/// request. **Every** sample of the stream is kept: dropping a later terminal
/// after a non-terminal one would leave the call in flight forever.
struct DeferredResponses {
    channel: String,
    /// Each sample keeps the identity of the process that emitted it, so the
    /// trace can still name its emitter once the call is finally resolved.
    samples: Vec<(RpcHeader, Emitter, Sample)>,
    since: Instant,
}

/// The out-of-band observer.
pub struct Monitor {
    config: Config,
    metrics: Arc<Metrics>,
    correlate: Correlate,
    loss: LossTracker,
    views: Vec<View>,
    /// Responses waiting for their request to be observed.
    deferred: HashMap<[u8; CORRELATION_ID_LEN], DeferredResponses>,
    trace: Option<TraceSink>,
    /// Last rendered messages, when the live console view keeps them in memory.
    recent: Option<RecentBuffer>,
    traced: u64,
    /// Emitter pids seen so far, with their last known liveness.
    pids: HashMap<u32, bool>,
    /// Throttled inventory of the nodes, services and processes.
    health: Option<Scanner>,
    /// Channels the observer knows about, attached or not.
    known_channels: Vec<String>,
    /// Failed channel-discovery attempts.
    discovery_errors: u64,
    /// When the channel *list* was last refreshed.
    last_discover: Instant,
    /// When the known channels were last probed for attachment.
    last_attach: Instant,
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
        // The live view cannot let the messages scroll, so it keeps them in a
        // bounded in-memory buffer instead of streaming them to stdout. An
        // explicit `--trace-file` still wins (the messages go to the file).
        let (trace, recent) = if config.trace_sample_rate == 0 {
            (None, None)
        } else if config.console_live && config.trace_file.is_none() {
            let (sink, buffer) = TraceSink::memory(TRACE_QUEUE, config.trace_format);
            (Some(sink), Some(buffer))
        } else {
            match &config.trace_file {
                Some(path) => (
                    Some(
                        TraceSink::file(path, TRACE_QUEUE, config.trace_format).map_err(|e| {
                            format!("cannot open trace file '{}': {e}", path.display())
                        })?,
                    ),
                    None,
                ),
                None => (
                    Some(TraceSink::stdout(TRACE_QUEUE, config.trace_format)),
                    None,
                ),
            }
        };

        let correlate = Correlate::new(config.max_inflight, config.call_ttl);
        let health = if config.health_interval.is_zero() {
            None
        } else {
            Some(Scanner::new(config.health_interval, config.health_shm))
        };
        let now = Instant::now();
        Ok(Self {
            config,
            metrics,
            correlate,
            loss: LossTracker::default(),
            views: Vec::new(),
            deferred: HashMap::new(),
            trace,
            recent,
            traced: 0,
            pids: HashMap::new(),
            health,
            known_channels: Vec::new(),
            discovery_errors: 0,
            last_discover: now,
            last_attach: now,
            last_sweep: now,
            last_liveness: now,
            wait_cursor: 0,
        })
    }

    /// Number of traces dropped by a saturated sink.
    pub fn dropped_traces(&self) -> u64 {
        self.trace.as_ref().map(TraceSink::dropped).unwrap_or(0)
    }

    /// The recent messages buffer, when the live view keeps them in memory.
    pub fn recent_messages(&self) -> Option<RecentBuffer> {
        self.recent.clone()
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
        self.refresh_channel_list();
        self.attach_known_channels();

        while !cancel.load(Ordering::Relaxed) {
            // The channel *list* is re-scanned on the discovery interval only: it
            // costs a walk of the service registry.
            if self.last_discover.elapsed() >= self.config.discover_interval {
                self.last_discover = Instant::now();
                self.refresh_channel_list();
            }
            // Attachment is retried far more often: a channel that appears late
            // must be observed from its first call, see [`ATTACH_RETRY`].
            if self.last_attach.elapsed() >= ATTACH_RETRY {
                self.last_attach = Instant::now();
                self.attach_known_channels();
            }
            self.refresh_health();

            let received = self.drain_all();
            // A response drained in this pass may have overtaken its request.
            self.resolve_deferred();

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

    /// Refreshes the list of the channels the observer must watch.
    ///
    /// Only the *list* is established here, because discovering it costs a scan of
    /// the service registry; attaching to the channels it names is retried much
    /// more often, see [`Monitor::attach_known_channels`].
    fn refresh_channel_list(&mut self) {
        let targets = if self.config.channels.is_empty() {
            match discover_channels() {
                Ok(channels) => channels,
                Err(e) => {
                    self.discovery_errors += 1;
                    log::warn!("[monitor] service discovery failed: {e}");
                    return;
                }
            }
        } else {
            self.config.channels.clone()
        };

        self.known_channels.clone_from(&targets);
    }

    /// Attaches to every known channel whose directions are not observed yet.
    ///
    /// A channel that does not exist yet is retried until its services appear.
    /// Every call published before the attachment is out of the observer's reach:
    /// iceoryx2 subscribers never receive past samples.
    fn attach_known_channels(&mut self) {
        // Indexed rather than iterated: `open_if_missing` borrows the whole
        // observer, the list of known channels included.
        for index in 0..self.known_channels.len() {
            let channel = self.known_channels[index].clone();
            self.open_if_missing(&channel, Direction::Request);
            self.open_if_missing(&channel, Direction::Response);
        }
    }

    /// Publishes the attachment state, and the inventory when a scan is due.
    fn refresh_health(&mut self) {
        let due = self
            .health
            .as_mut()
            .map(Scanner::refresh_if_due)
            .unwrap_or(false);

        if due {
            if let Some(snapshot) = self.health.as_ref().map(Scanner::snapshot) {
                self.metrics.set_health(snapshot);
            }
        }
        // The per-channel block is built from the observer's own views, so it is
        // published on every pass: the inventory scan is throttled, the truth
        // about what is attached must not be.
        self.metrics.set_channels(&self.channel_health());
        // One ice-rpc service per channel, as the bus shows it. Counting the
        // linked contract instead would include services that are not hosted
        // here (e.g. a `#[service]` this provider never started).
        self.metrics.set_services(self.known_channels.len() as i64);
        // The observer's own counters are published on every pass.
        self.metrics
            .set_observer(self.dropped_traces(), self.discovery_errors);
    }

    /// Builds the per-channel health block from the known channels and views.
    fn channel_health(&self) -> Vec<ChannelHealth> {
        let mut out = Vec::new();
        for channel in &self.known_channels {
            for (direction, label) in [(Direction::Request, "req"), (Direction::Response, "resp")] {
                let view = self
                    .views
                    .iter()
                    .find(|v| v.channel == *channel && v.direction == direction);
                out.push(match view {
                    Some(view) => ChannelHealth {
                        channel: channel.clone(),
                        direction: label,
                        attached: true,
                        publishers: view.view.publisher_count(),
                        subscribers: view.view.subscriber_count(),
                        max_publishers: view.view.max_publishers(),
                        max_subscribers: view.view.max_subscribers(),
                        subscriber_buffer: view.view.subscriber_max_buffer_size(),
                    },
                    None => ChannelHealth {
                        channel: channel.clone(),
                        direction: label,
                        attached: false,
                        publishers: 0,
                        subscribers: 0,
                        max_publishers: 0,
                        max_subscribers: 0,
                        subscriber_buffer: 0,
                    },
                });
            }
        }
        out
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
        let detail = self.config.detail_for(channel);
        match DirectionView::open(channel, direction) {
            Ok(view) => {
                log::info!(
                    "[monitor] attached to '{channel}' ({direction:?}, mode={}), buffer={} sample(s)",
                    if detail { "detail" } else { "stats" },
                    view.buffer_size()
                );
                self.views.push(View {
                    channel: channel.to_owned(),
                    direction,
                    view,
                    detail,
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
            let detail = self.views[index].detail;
            for _ in 0..DRAIN_BUDGET {
                let drained = if detail {
                    match self.views[index].view.try_receive_payload() {
                        Ok(Some((header, emitter, payload))) => {
                            Some((header, emitter, Sample::Full { payload }))
                        }
                        Ok(None) => None,
                        Err(e) => {
                            log::warn!("[monitor] receive error on '{channel}': {e}");
                            None
                        }
                    }
                } else {
                    match self.views[index].view.try_receive() {
                        Ok(Some((header, emitter, payload_len))) => {
                            Some((header, emitter, Sample::Meta { payload_len }))
                        }
                        Ok(None) => None,
                        Err(e) => {
                            log::warn!("[monitor] receive error on '{channel}': {e}");
                            None
                        }
                    }
                };

                match drained {
                    Some((header, emitter, sample)) => {
                        received = true;
                        self.process(&channel, direction, header, emitter, sample);
                    }
                    None => break,
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
        emitter: Emitter,
        sample: Sample,
    ) {
        log::trace!(
            "[monitor] sample direction={direction:?} seq={} kind={:?} len={} emitter_pid={}",
            header.seq,
            header.event_kind(),
            sample.payload_len(),
            emitter.pid
        );
        // Loss detection must see every sample, whatever its kind: the `seq` is
        // monotonic per publisher port, which the native `publisher_id` scopes.
        // What is counted is the observer's **own** loss: a lagging observer is
        // skipped by the publisher instead of slowing the application down.
        let missed = self.loss.observe(emitter.publisher_id, header.seq);
        self.metrics.on_sample_gap(missed);
        self.metrics
            .on_payload(channel, direction, sample.payload_len());
        self.pids.entry(emitter.pid).or_insert(true);

        match direction {
            Direction::Request => self.on_request(channel, &header, &sample),
            Direction::Response => self.on_response(channel, &header, emitter, &sample),
        }
    }

    /// Handles one request sample.
    fn on_request(&mut self, channel: &str, header: &RpcHeader, sample: &Sample) {
        if header.event_kind() != EventKind::Request {
            log::debug!(
                "[monitor] '{channel}': unexpected {:?} sample on the request channel",
                header.event_kind()
            );
            return;
        }

        // The wire carries only the method id: resolve it to the name the
        // observer was told about, or fall back to the hexadecimal id — the
        // same discipline as an unlinked service.
        let method = match self
            .config
            .decoders
            .method_name(header.service_id, header.method_id())
        {
            Some(name) => name.to_owned(),
            None => format!("{:#010x}", header.method_id()),
        };
        self.metrics.on_request(channel, header.service_id, &method);
        self.metrics.on_inflight(channel, header.service_id, 1);

        // Decode the request with the registry this observer was built with.
        let request_text = match sample {
            Sample::Full { payload } => Some(
                self.config
                    .decoders
                    .request(header.service_id, &method, payload)
                    .unwrap_or_else(|| undecoded(payload.len())),
            ),
            Sample::Meta { .. } => None,
        };

        let call = PendingCall::new(
            channel.to_owned(),
            header.service_id,
            method,
            header.timestamp_ns,
            sample.payload_len(),
            request_text,
        );
        for evicted in self.correlate.insert(header.correlation_id, call) {
            self.metrics.on_unmatched_request(&evicted.channel);
            self.metrics
                .on_inflight(&evicted.channel, evicted.service_id, -1);
        }
    }

    /// Handles one response sample, closing the call on a terminal kind.
    fn on_response(
        &mut self,
        channel: &str,
        header: &RpcHeader,
        emitter: Emitter,
        sample: &Sample,
    ) {
        let kind = header.event_kind();
        self.metrics
            .on_response(channel, header.service_id, kind.label());
        let terminal = kind.is_terminal();

        match self.correlate.on_response(&header.correlation_id, terminal) {
            Resolution::Matched(call) => {
                self.complete_response(channel, header, emitter, sample, call, kind)
            }
            // A passive observer drains the two directions independently: the
            // request may not have been observed yet. Hold the response briefly.
            Resolution::Unknown => self.defer_response(channel, header, emitter, sample),
        }
    }

    /// Accounts for a response whose request is known.
    fn complete_response(
        &mut self,
        channel: &str,
        header: &RpcHeader,
        emitter: Emitter,
        sample: &Sample,
        call: PendingCall,
        kind: EventKind,
    ) {
        // Derived rather than passed: one less argument to thread through the
        // deferred-replay path, and a single source of truth.
        let terminal = kind.is_terminal();
        if !call.first_response_seen {
            if header.timestamp_ns >= call.request_ts_ns {
                let seconds = (header.timestamp_ns - call.request_ts_ns) as f64 / 1e9;
                self.metrics
                    .on_latency(&call.channel, call.service_id, &call.method, seconds);
            } else {
                // A clock adjustment made the response look older.
                self.metrics.on_clock_skew();
            }
        }
        if terminal {
            self.metrics.on_inflight(&call.channel, call.service_id, -1);
            self.emit_trace(channel, header, emitter, sample, &call, kind.label());
        }
    }

    /// Holds a response whose request has not been observed yet.
    fn defer_response(
        &mut self,
        channel: &str,
        header: &RpcHeader,
        emitter: Emitter,
        sample: &Sample,
    ) {
        if self.deferred.len() >= MAX_DEFERRED {
            if let Some(oldest) = self
                .deferred
                .iter()
                .min_by_key(|(_, entry)| entry.since)
                .map(|(cid, _)| *cid)
            {
                if let Some(entry) = self.deferred.remove(&oldest) {
                    for _ in 0..entry.samples.len() {
                        self.metrics.on_orphan_response(&entry.channel);
                    }
                }
            }
        }

        let cid = header.correlation_id;
        // A pathological stream must not grow the entry without bound: give the
        // call up as orphan, the TTL sweep will then correct the in-flight gauge.
        let overflowing = self
            .deferred
            .get(&cid)
            .map(|entry| entry.samples.len() >= MAX_DEFERRED_SAMPLES)
            .unwrap_or(false);
        if overflowing {
            if let Some(entry) = self.deferred.remove(&cid) {
                for _ in 0..entry.samples.len() {
                    self.metrics.on_orphan_response(&entry.channel);
                }
            }
            return;
        }

        self.deferred
            .entry(cid)
            .or_insert_with(|| DeferredResponses {
                channel: channel.to_owned(),
                samples: Vec::new(),
                since: Instant::now(),
            })
            .samples
            .push((*header, emitter, sample.clone()));
    }

    /// Pairs the deferred responses whose request has just been observed, and
    /// gives up on the others after [`ORPHAN_GRACE`].
    fn resolve_deferred(&mut self) {
        if self.deferred.is_empty() {
            return;
        }

        let ready: Vec<[u8; CORRELATION_ID_LEN]> = self
            .deferred
            .keys()
            .filter(|cid| self.correlate.contains(cid))
            .copied()
            .collect();

        for cid in ready {
            let Some(entry) = self.deferred.remove(&cid) else {
                continue;
            };
            // Replay the whole stream in observation order: the first sample
            // carries the latency, the terminal one closes the call.
            for (header, emitter, sample) in &entry.samples {
                let kind = header.event_kind();
                let terminal = kind.is_terminal();
                if let Resolution::Matched(call) = self.correlate.on_response(&cid, terminal) {
                    self.complete_response(&entry.channel, header, *emitter, sample, call, kind);
                }
            }
        }

        let now = Instant::now();
        let mut expired: Vec<(String, usize)> = Vec::new();
        self.deferred.retain(|_, entry| {
            if now.duration_since(entry.since) >= ORPHAN_GRACE {
                expired.push((entry.channel.clone(), entry.samples.len()));
                false
            } else {
                true
            }
        });
        for (channel, count) in expired {
            for _ in 0..count {
                self.metrics.on_orphan_response(&channel);
            }
        }
    }

    /// Emits one trace record for a completed call, subject to sampling.
    fn emit_trace(
        &mut self,
        channel: &str,
        header: &RpcHeader,
        emitter: Emitter,
        sample: &Sample,
        call: &PendingCall,
        kind: &'static str,
    ) {
        if self.trace.is_none() {
            return;
        }
        self.traced = self.traced.wrapping_add(1);
        let rate = self.config.trace_sample_rate;
        if rate == 0 || !self.traced.is_multiple_of(rate) {
            return;
        }

        let latency_us = header.timestamp_ns.saturating_sub(call.request_ts_ns) / 1_000;
        let call_id = fmt_correlation_id(&header.correlation_id);
        // The distributed trace, when the caller propagated one. Read from the
        // header alone: the observer never decodes the payload, which is what
        // keeps the stats mode free of any decode cost.
        let trace = header.trace();
        let trace_id = trace.is_present().then(|| trace.trace_id_hex());
        // Decode the response with the registry this observer was built with.
        let response_text = match sample {
            Sample::Full { payload } => Some(if header.event_kind() == EventKind::RpcError {
                // A transport-level rejection carries a bare `RpcError`, not the
                // service's `WireEvent<T, E>`: it is rendered without the registry.
                ice_rpc::transport::decode_aligned::<ice_rpc::RpcError>(payload)
                    .map(|err| format!("rpc error: {err}"))
                    .unwrap_or_else(|_| undecoded(payload.len()))
            } else {
                self.config
                    .decoders
                    .response(call.service_id, &call.method, payload)
                    .unwrap_or_else(|| undecoded(payload.len()))
            }),
            Sample::Meta { .. } => None,
        };
        let record = TraceRecord {
            call_id: &call_id,
            trace_id: trace_id.as_deref(),
            parent_span_id: trace.parent_span_id,
            channel,
            service_id: header.service_id,
            method: &call.method,
            event_kind: kind,
            emitter_pid: emitter.pid,
            request_bytes: call.request_bytes,
            response_bytes: sample.payload_len(),
            latency_us,
            request_text: call.request_text.as_deref(),
            response_text: response_text.as_deref(),
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
            // Nothing to block on: wait only until the next attachment retry, so
            // a channel whose provider starts late is picked up within
            // [`ATTACH_RETRY`] of its appearance.
            std::thread::sleep(ATTACH_RETRY);
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
    use std::path::{Path, PathBuf};

    use ice_rpc::monitor::{ClosureDecoder, Decoders};

    use super::*;
    use crate::config::Mode;

    /// A header with a chosen correlation id, kind and timestamp.
    fn header(cid_byte: u8, kind: EventKind, timestamp_ns: u64) -> RpcHeader {
        let mut header = RpcHeader {
            correlation_id: [cid_byte; CORRELATION_ID_LEN],
            service_id: 7,
            timestamp_ns,
            ..RpcHeader::default()
        };
        header.event_kind = kind.as_u8();
        header
    }

    /// The emitter identity, as read from the native iceoryx2 sample header.
    fn emitter() -> Emitter {
        Emitter {
            pid: 4_242,
            node_id: 1,
            publisher_id: 9,
        }
    }

    /// A process-unique temporary trace path.
    fn trace_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "ice-rpc-monitor-{tag}-{}.ndjson",
            std::process::id()
        ))
    }

    /// Reads a trace file and removes it, so a failing assertion leaves none.
    fn read_and_remove(path: &Path) -> String {
        let content = std::fs::read_to_string(path).unwrap_or_default();
        let _ = std::fs::remove_file(path);
        content
    }

    #[test]
    fn every_event_kind_has_a_label() {
        // The labels live on `EventKind` itself now, so this only checks that
        // the observer's view of a sample stays the wire vocabulary.
        assert_eq!(EventKind::Request.label(), "request");
        assert_eq!(EventKind::Next.label(), "next");
        assert_eq!(EventKind::Complete.label(), "complete");
        assert_eq!(EventKind::Error.label(), "error");
    }

    #[test]
    fn a_monitor_without_tracing_builds() {
        let monitor = Monitor::new(Config::default(), Arc::new(Metrics::new()));
        assert!(monitor.is_ok());
        assert_eq!(monitor.expect("built").dropped_traces(), 0);
    }

    /// A response stream observed *before* its request must be replayed in full:
    /// keeping only the first sample would silently drop the terminal one and
    /// leave the call in flight forever (regression).
    #[test]
    fn a_response_stream_seen_before_its_request_is_replayed_in_full() {
        let metrics = Arc::new(Metrics::new());
        let mut monitor = Monitor::new(Config::default(), metrics.clone()).expect("built");
        const CHANNEL: &str = "TestChannel";
        let cid = [0xABu8; CORRELATION_ID_LEN];
        let emitter = Emitter {
            pid: 4_242,
            node_id: 1,
            publisher_id: 9,
        };

        // The three response samples are drained first: the request is not yet
        // tracked, so every one of them is deferred.
        let mut response = RpcHeader {
            correlation_id: cid,
            service_id: 7,
            timestamp_ns: 1_000_000_500,
            ..RpcHeader::default()
        };
        for kind in [EventKind::Next, EventKind::Next, EventKind::Complete] {
            response.event_kind = kind.as_u8();
            monitor.defer_response(
                CHANNEL,
                &response,
                emitter,
                &Sample::Meta { payload_len: 4 },
            );
        }
        assert_eq!(monitor.deferred.len(), 1);
        assert_eq!(monitor.deferred.get(&cid).map(|e| e.samples.len()), Some(3));

        // The request arrives; the whole deferred stream is then replayed.
        let request = RpcHeader {
            correlation_id: cid,
            service_id: 7,
            timestamp_ns: 1_000_000_000,
            ..RpcHeader::default()
        };
        monitor.on_request(CHANNEL, &request, &Sample::Meta { payload_len: 6 });
        monitor.resolve_deferred();

        let text = metrics.render_prometheus();
        assert!(
            text.contains("ice_rpc_inflight{channel=\"TestChannel\",service=\"7\"} 0"),
            "the terminal sample must close the call:\n{text}"
        );
        // The wire carries only the method id and this observer was given no
        // decoder to name it, so the label falls back to the hexadecimal id.
        assert!(
            text.contains(
                "ice_rpc_latency_seconds_count{channel=\"TestChannel\",service=\"7\",method=\"0x00000000\"} 1"
            ),
            "the first response must yield exactly one latency:\n{text}"
        );
        assert!(
            !text.contains("ice_rpc_orphan_responses_total{channel=\"TestChannel\"}"),
            "a matched deferred stream is not an orphan:\n{text}"
        );
    }

    #[test]
    fn a_monitor_reports_a_trace_file_it_cannot_open() {
        let config = Config {
            trace_sample_rate: 1,
            trace_file: Some(PathBuf::from("no-such-dir-ice-rpc/traces.ndjson")),
            ..Config::default()
        };
        let error = match Monitor::new(config, Arc::new(Metrics::new())) {
            Ok(_) => panic!("a monitor must not start without its trace file"),
            Err(error) => error,
        };
        assert!(error.contains("cannot open trace file"), "{error}");
    }

    #[test]
    fn tracing_can_go_to_stdout_or_to_memory() {
        // Tracing on, no file: the records stream to stdout.
        let to_stdout = Config {
            trace_sample_rate: 1,
            ..Config::default()
        };
        let monitor = Monitor::new(to_stdout, Arc::new(Metrics::new())).expect("built");
        assert_eq!(monitor.dropped_traces(), 0);
        assert!(monitor.recent_messages().is_none());

        // The live view cannot let the messages scroll: it keeps them in memory.
        let live = Config {
            console_live: true,
            trace_sample_rate: 1,
            ..Config::default()
        };
        let monitor = Monitor::new(live, Arc::new(Metrics::new())).expect("built");
        assert!(
            monitor.recent_messages().is_some(),
            "the live view keeps the last messages in memory"
        );
    }

    #[test]
    fn a_completed_call_is_written_to_the_trace_file() {
        let path = trace_path("trace");
        let _ = std::fs::remove_file(&path);

        let config = Config {
            trace_sample_rate: 1,
            trace_file: Some(path.clone()),
            ..Config::default()
        };
        let mut monitor = Monitor::new(config, Arc::new(Metrics::new())).expect("built");
        let request = header(0x11, EventKind::Request, 1_000_000_000);
        monitor.on_request(
            "C",
            &request,
            &Sample::Full {
                payload: b"go".to_vec(),
            },
        );
        let response = header(0x11, EventKind::Complete, 1_001_500_000);
        monitor.on_response(
            "C",
            &response,
            emitter(),
            &Sample::Full {
                payload: b"ok".to_vec(),
            },
        );
        // Dropping the monitor closes the sink and joins its writer, which flushes.
        drop(monitor);

        let content = read_and_remove(&path);
        assert!(content.contains("\"call_id\":"), "{content}");
        assert!(content.contains("\"event_kind\":\"complete\""), "{content}");
        assert!(content.contains("\"latency_us\":1500"), "{content}");
    }

    #[test]
    fn trace_sampling_skips_the_calls_between_the_samples() {
        let path = trace_path("sampling");
        let _ = std::fs::remove_file(&path);

        let config = Config {
            trace_sample_rate: 100,
            trace_file: Some(path.clone()),
            ..Config::default()
        };
        let mut monitor = Monitor::new(config, Arc::new(Metrics::new())).expect("built");
        let request = header(0x22, EventKind::Request, 10);
        monitor.on_request("C", &request, &Sample::Meta { payload_len: 1 });
        let response = header(0x22, EventKind::Complete, 20);
        monitor.on_response("C", &response, emitter(), &Sample::Meta { payload_len: 1 });
        drop(monitor);

        let content = read_and_remove(&path);
        assert!(
            content.is_empty(),
            "the first of a hundred calls must not be traced:\n{content}"
        );
    }

    #[test]
    fn detail_mode_decodes_both_messages_through_the_registry() {
        let path = trace_path("detail");
        let _ = std::fs::remove_file(&path);

        let mut decoders = Decoders::new();
        decoders.register(
            7,
            Arc::new(
                ClosureDecoder::new(
                    |_method, payload| Some(format!("req:{}", payload.len())),
                    |_method, payload| Some(format!("resp:{}", payload.len())),
                )
                .with_method("echo"),
            ),
        );
        let config = Config {
            decoders: Arc::new(decoders),
            mode: Mode::Detail,
            trace_sample_rate: 1,
            trace_file: Some(path.clone()),
            ..Config::default()
        };
        let metrics = Arc::new(Metrics::new());
        let mut monitor = Monitor::new(config, metrics.clone()).expect("built");

        // The wire carries only the method id, so the header must carry the id
        // the registry was told about.
        let mut request = header(0x66, EventKind::Request, 1_000);
        request.method_id = ice_rpc::gen::method_id_of("echo");
        monitor.on_request(
            "C",
            &request,
            &Sample::Full {
                payload: b"hi".to_vec(),
            },
        );
        let response = header(0x66, EventKind::Complete, 2_000);
        monitor.on_response(
            "C",
            &response,
            emitter(),
            &Sample::Full {
                payload: b"ok".to_vec(),
            },
        );
        drop(monitor);

        let content = read_and_remove(&path);
        assert!(content.contains("\"method\":\"echo\""), "{content}");
        assert!(content.contains("\"request\":\"req:2\""), "{content}");
        assert!(content.contains("\"response\":\"resp:2\""), "{content}");
        // The wire carries only the method id; the registry names it.
        assert!(
            metrics.render_prometheus().contains("method=\"echo\""),
            "the request series is labelled with the resolved method"
        );
    }

    #[test]
    fn detail_mode_without_a_decoder_marks_the_payload_opaque() {
        let path = trace_path("opaque");
        let _ = std::fs::remove_file(&path);

        let config = Config {
            mode: Mode::Detail,
            trace_sample_rate: 1,
            trace_file: Some(path.clone()),
            ..Config::default()
        };
        let mut monitor = Monitor::new(config, Arc::new(Metrics::new())).expect("built");
        let request = header(0x77, EventKind::Request, 1_000);
        monitor.on_request(
            "C",
            &request,
            &Sample::Full {
                payload: b"go".to_vec(),
            },
        );
        let response = header(0x77, EventKind::Complete, 2_000);
        monitor.on_response(
            "C",
            &response,
            emitter(),
            &Sample::Full {
                payload: b"ok".to_vec(),
            },
        );
        drop(monitor);

        let content = read_and_remove(&path);
        assert!(content.contains("no decoder"), "{content}");
    }

    #[test]
    fn a_transport_error_response_is_traced_with_its_own_kind() {
        let path = trace_path("rpcerror");
        let _ = std::fs::remove_file(&path);

        let config = Config {
            mode: Mode::Detail,
            trace_sample_rate: 1,
            trace_file: Some(path.clone()),
            ..Config::default()
        };
        let mut monitor = Monitor::new(config, Arc::new(Metrics::new())).expect("built");
        let request = header(0x88, EventKind::Request, 1_000);
        monitor.on_request("C", &request, &Sample::Meta { payload_len: 1 });
        // A rejection carries a bare `RpcError`, not the service's `WireEvent`.
        let response = header(0x88, EventKind::RpcError, 2_000);
        monitor.on_response("C", &response, emitter(), &Sample::Meta { payload_len: 1 });
        drop(monitor);

        let content = read_and_remove(&path);
        assert!(
            content.contains("\"event_kind\":\"rpc-error\""),
            "{content}"
        );
    }

    #[test]
    fn a_request_kind_observed_on_the_request_channel_is_accepted() {
        let metrics = Arc::new(Metrics::new());
        let mut monitor = Monitor::new(Config::default(), metrics.clone()).expect("built");
        let request = header(0x99, EventKind::Request, 1);
        monitor.on_request("C", &request, &Sample::Meta { payload_len: 1 });
        assert_eq!(monitor.correlate.len(), 1);
        assert!(metrics
            .render_prometheus()
            .contains("ice_rpc_requests_total{channel=\"C\""));
    }

    #[test]
    fn a_non_request_sample_on_the_request_channel_is_ignored() {
        let metrics = Arc::new(Metrics::new());
        let mut monitor = Monitor::new(Config::default(), metrics.clone()).expect("built");
        let bogus = header(0xAA, EventKind::Next, 1);
        monitor.on_request("C", &bogus, &Sample::Meta { payload_len: 1 });

        assert_eq!(monitor.correlate.len(), 0, "no call is tracked");
        // The `# HELP` line always names the metric: only a series proves a
        // request was actually counted.
        let text = metrics.render_prometheus();
        assert!(!text.contains("ice_rpc_requests_total{"), "{text}");
    }

    #[test]
    fn a_response_older_than_its_request_is_a_clock_skew() {
        let metrics = Arc::new(Metrics::new());
        let mut monitor = Monitor::new(Config::default(), metrics.clone()).expect("built");
        let request = header(0xBB, EventKind::Request, 5_000);
        monitor.on_request("C", &request, &Sample::Meta { payload_len: 1 });
        // A clock adjustment made the response look older than the request.
        let response = header(0xBB, EventKind::Next, 1_000);
        monitor.on_response("C", &response, emitter(), &Sample::Meta { payload_len: 1 });

        assert!(metrics
            .render_prometheus()
            .contains("ice_rpc_clock_skew_total 1"));
    }

    #[test]
    fn a_sequence_hole_is_counted_as_an_observer_gap() {
        let metrics = Arc::new(Metrics::new());
        let mut monitor = Monitor::new(Config::default(), metrics.clone()).expect("built");
        let mut sample = header(0xCC, EventKind::Next, 1);
        sample.seq = 10;
        monitor.process(
            "C",
            Direction::Response,
            sample,
            emitter(),
            Sample::Meta { payload_len: 4 },
        );
        // Four samples went missing between the two observations.
        sample.seq = 15;
        monitor.process(
            "C",
            Direction::Response,
            sample,
            emitter(),
            Sample::Meta { payload_len: 4 },
        );

        assert!(metrics
            .render_prometheus()
            .contains("ice_rpc_observer_gaps_total 4"));
    }

    #[test]
    fn a_deferred_response_without_its_request_becomes_an_orphan() {
        let metrics = Arc::new(Metrics::new());
        let mut monitor = Monitor::new(Config::default(), metrics.clone()).expect("built");
        let response = header(0xDD, EventKind::Complete, 1);
        monitor.defer_response("C", &response, emitter(), &Sample::Meta { payload_len: 1 });

        // Age the entry past the grace instead of sleeping through it.
        if let Some(entry) = monitor.deferred.get_mut(&[0xDD; CORRELATION_ID_LEN]) {
            entry.since = Instant::now() - ORPHAN_GRACE - Duration::from_millis(1);
        }
        monitor.resolve_deferred();

        assert!(monitor.deferred.is_empty());
        assert!(metrics
            .render_prometheus()
            .contains("ice_rpc_orphan_responses_total{channel=\"C\"} 1"));
    }

    #[test]
    fn resolving_an_empty_deferred_table_is_a_no_op() {
        let mut monitor = Monitor::new(Config::default(), Arc::new(Metrics::new())).expect("built");
        monitor.resolve_deferred();
        assert!(monitor.deferred.is_empty());
    }

    #[test]
    fn the_deferred_table_evicts_the_oldest_entry_when_full() {
        let metrics = Arc::new(Metrics::new());
        let mut monitor = Monitor::new(Config::default(), metrics.clone()).expect("built");

        for index in 0..MAX_DEFERRED as u64 {
            let mut cid = [0u8; CORRELATION_ID_LEN];
            cid[..8].copy_from_slice(&index.to_be_bytes());
            let response = RpcHeader {
                correlation_id: cid,
                service_id: 7,
                ..RpcHeader::default()
            };
            monitor.defer_response("C", &response, emitter(), &Sample::Meta { payload_len: 1 });
        }
        assert_eq!(monitor.deferred.len(), MAX_DEFERRED);

        // One more entry evicts the oldest, whose samples become orphans.
        let extra = header(0xFF, EventKind::Next, 1);
        monitor.defer_response("C", &extra, emitter(), &Sample::Meta { payload_len: 1 });

        assert_eq!(
            monitor.deferred.len(),
            MAX_DEFERRED,
            "the table stays bounded"
        );
        assert!(metrics
            .render_prometheus()
            .contains("ice_rpc_orphan_responses_total{channel=\"C\"} 1"));
    }

    #[test]
    fn a_runaway_response_stream_is_given_up_as_orphan() {
        let metrics = Arc::new(Metrics::new());
        let mut monitor = Monitor::new(Config::default(), metrics.clone()).expect("built");
        let cid = [0xEE; CORRELATION_ID_LEN];
        let response = header(0xEE, EventKind::Next, 1);
        for _ in 0..MAX_DEFERRED_SAMPLES {
            monitor.defer_response("C", &response, emitter(), &Sample::Meta { payload_len: 1 });
        }
        assert_eq!(
            monitor.deferred.get(&cid).map(|entry| entry.samples.len()),
            Some(MAX_DEFERRED_SAMPLES)
        );

        // The next sample overflows the entry: it is dropped and counted whole.
        monitor.defer_response("C", &response, emitter(), &Sample::Meta { payload_len: 1 });
        assert!(!monitor.deferred.contains_key(&cid));
        assert!(metrics.render_prometheus().contains(&format!(
            "ice_rpc_orphan_responses_total{{channel=\"C\"}} {MAX_DEFERRED_SAMPLES}"
        )));
    }

    #[test]
    fn a_call_that_outlives_its_ttl_is_counted_as_unmatched() {
        let metrics = Arc::new(Metrics::new());
        let config = Config {
            call_ttl: Duration::from_millis(1),
            ..Config::default()
        };
        let mut monitor = Monitor::new(config, metrics.clone()).expect("built");
        let request = header(0x12, EventKind::Request, 1);
        monitor.on_request("C", &request, &Sample::Meta { payload_len: 1 });

        std::thread::sleep(Duration::from_millis(10));
        monitor.sweep();

        assert_eq!(monitor.correlate.len(), 0);
        let text = metrics.render_prometheus();
        assert!(
            text.contains("ice_rpc_unmatched_requests_total{channel=\"C\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("ice_rpc_inflight{channel=\"C\",service=\"7\"} 0"),
            "the in-flight gauge is corrected on eviction:\n{text}"
        );
    }

    #[test]
    fn a_disappeared_emitter_counts_as_a_crash_once() {
        let metrics = Arc::new(Metrics::new());
        let mut monitor = Monitor::new(Config::default(), metrics.clone()).expect("built");
        // A pid that cannot be running: it is already gone on the first check.
        monitor.pids.insert(u32::MAX - 1, true);

        monitor.check_liveness();
        let text = metrics.render_prometheus();
        assert!(text.contains("ice_rpc_node_crashes_total 1"), "{text}");
        assert!(text.contains("ice_rpc_nodes_alive 0"), "{text}");

        // It stays dead: the crash must not be counted twice.
        monitor.check_liveness();
        assert!(metrics
            .render_prometheus()
            .contains("ice_rpc_node_crashes_total 1"));
    }

    #[test]
    fn a_known_channel_without_a_view_is_reported_unattached() {
        let metrics = Arc::new(Metrics::new());
        // A zero health interval keeps the inventory (and its scan) out of the way.
        let config = Config {
            health_interval: Duration::ZERO,
            channels: vec!["C".to_owned()],
            ..Config::default()
        };
        let mut monitor = Monitor::new(config, metrics.clone()).expect("built");
        monitor.refresh_channel_list();
        assert_eq!(monitor.known_channels, vec!["C"]);

        monitor.refresh_health();

        let text = metrics.render_prometheus();
        assert!(
            text.contains("ice_rpc_channel{channel=\"C\",direction=\"req\"} 0"),
            "{text}"
        );
        assert!(
            text.contains("ice_rpc_channel{channel=\"C\",direction=\"resp\"} 0"),
            "{text}"
        );

        let health = monitor.channel_health();
        assert_eq!(health.len(), 2);
        assert!(health.iter().all(|channel| !channel.attached));
    }

    #[test]
    fn an_explicit_channel_list_skips_discovery() {
        let config = Config {
            channels: vec!["A".to_owned(), "B".to_owned()],
            ..Config::default()
        };
        let mut monitor = Monitor::new(config, Arc::new(Metrics::new())).expect("built");
        monitor.refresh_channel_list();
        assert_eq!(monitor.known_channels, vec!["A", "B"]);
    }

    #[test]
    fn attaching_to_a_channel_that_does_not_exist_yet_retries_silently() {
        let mut monitor = Monitor::new(Config::default(), Arc::new(Metrics::new())).expect("built");
        monitor.known_channels = vec!["DefinitelyNotRunningService".to_owned()];

        monitor.attach_known_channels();

        assert!(monitor.views.is_empty(), "nothing to attach to yet");
    }

    #[test]
    fn draining_without_a_view_receives_nothing() {
        let mut monitor = Monitor::new(Config::default(), Arc::new(Metrics::new())).expect("built");
        assert!(!monitor.drain_all());
    }

    #[test]
    fn waiting_without_a_view_waits_for_the_next_attachment_retry() {
        let mut monitor = Monitor::new(Config::default(), Arc::new(Metrics::new())).expect("built");
        let started = Instant::now();
        monitor.wait_idle();
        assert!(
            started.elapsed() >= ATTACH_RETRY,
            "an observer with nothing to read waits for the retry"
        );
    }
}
