//! NDJSON trace stream correlated by `correlation_id`.
//!
//! Records are handed to a dedicated writer thread through a **bounded** queue:
//! a full queue drops records (and counts them) instead of blocking the
//! acquisition loop, so tracing can never make the observer fall behind. The
//! writer is line-buffered, so a consumer can `tail -f` the stream live.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::fs::File;
use std::io::{LineWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// Number of records kept by [`TraceSink::memory`].
pub const RECENT_KEEP: usize = 10;

/// Shared, bounded buffer of the most recent rendered records.
///
/// A live console view reads it to display the last messages inside its frame,
/// instead of letting the stream scroll.
pub type RecentBuffer = Arc<Mutex<VecDeque<String>>>;

use serde_json::{json, Map, Value};

/// Output format of the trace stream.
///
/// JSON (NDJSON) is the machine-readable default; [`TraceFormat::Human`] renders
/// one compact, prefixed line per record for a console observer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TraceFormat {
    /// One JSON object per line, correlated by `trace_id`.
    #[default]
    Json,
    /// One human-readable line per record, meant for a terminal.
    Human,
}

/// One completed call, ready to be serialised.
pub struct TraceRecord<'a> {
    /// `correlation_id` formatted as a UUID-like string: the id of **this call**.
    ///
    /// Named `call_id`, not `trace_id`: a call and a trace are different things,
    /// and the header carries both. One trace spans many calls, across processes.
    pub call_id: &'a str,
    /// Distributed trace id, lowercase hexadecimal; `None` when the caller
    /// propagated no trace.
    pub trace_id: Option<&'a str>,
    /// Span the caller parented this call on; `0` when there is no trace.
    pub parent_span_id: u64,
    /// Channel the call was observed on.
    pub channel: &'a str,
    /// Service id carried by the header.
    pub service_id: u32,
    /// Method carried by the request.
    pub method: &'a str,
    /// Terminal kind of the response (`complete` / `error`).
    pub event_kind: &'a str,
    /// Process that emitted the terminal response.
    pub emitter_pid: u32,
    /// Request payload length in bytes.
    pub request_bytes: usize,
    /// Terminal response payload length in bytes.
    pub response_bytes: usize,
    /// Exact latency in microseconds (`response.ts - request.ts`).
    pub latency_us: u64,
    /// Human-readable request (detail mode only).
    pub request_text: Option<&'a str>,
    /// Human-readable terminal response (detail mode only).
    pub response_text: Option<&'a str>,
}

impl TraceRecord<'_> {
    /// Renders the record in `format`, without the trailing newline.
    fn render(&self, format: TraceFormat) -> String {
        match format {
            TraceFormat::Json => self.to_json(),
            TraceFormat::Human => self.to_human(),
        }
    }

    /// One human-readable line, prefixed so it is greppable in a console stream.
    ///
    /// The payload fields are appended only in detail mode; in stats mode the
    /// line stays metadata-only, exactly like the JSON variant.
    fn to_human(&self) -> String {
        let mut line = format!(
            "[msg] cid={} channel={} service={} method={} kind={} latency_us={} \
             request_bytes={} response_bytes={} emitter_pid={}",
            self.call_id,
            self.channel,
            self.service_id,
            self.method,
            self.event_kind,
            self.latency_us,
            self.request_bytes,
            self.response_bytes,
            self.emitter_pid
        );
        if let Some(trace_id) = self.trace_id {
            let _ = write!(
                line,
                " trace={trace_id} parent_span={}",
                self.parent_span_id
            );
        }
        if let Some(text) = self.request_text {
            let _ = write!(line, " request={text}");
        }
        if let Some(text) = self.response_text {
            let _ = write!(line, " response={text}");
        }
        line
    }

    /// Serialises the record as a single JSON line (without the trailing newline).
    ///
    /// The payload fields are omitted entirely in stats mode, so the stream
    /// stays compact.
    fn to_json(&self) -> String {
        let mut map = Map::new();
        map.insert("call_id".to_owned(), json!(self.call_id));
        // `null` rather than absent: a consumer can then tell "no trace" from
        // "field the writer does not know about".
        map.insert("trace_id".to_owned(), json!(self.trace_id));
        map.insert("parent_span_id".to_owned(), json!(self.parent_span_id));
        map.insert("channel".to_owned(), json!(self.channel));
        map.insert("service_id".to_owned(), json!(self.service_id));
        map.insert("method".to_owned(), json!(self.method));
        map.insert("event_kind".to_owned(), json!(self.event_kind));
        map.insert("emitter_pid".to_owned(), json!(self.emitter_pid));
        map.insert("request_bytes".to_owned(), json!(self.request_bytes));
        map.insert("response_bytes".to_owned(), json!(self.response_bytes));
        map.insert("latency_us".to_owned(), json!(self.latency_us));
        if let Some(text) = self.request_text {
            map.insert("request".to_owned(), json!(text));
        }
        if let Some(text) = self.response_text {
            map.insert("response".to_owned(), json!(text));
        }
        Value::Object(map).to_string()
    }
}

/// Bounded, non-blocking trace sink.
pub struct TraceSink {
    /// `None` once the sink is being dropped, which ends the writer loop.
    sender: Option<std::sync::mpsc::SyncSender<String>>,
    dropped: Arc<AtomicU64>,
    handle: Option<JoinHandle<()>>,
    /// How each record is rendered before being handed to the writer thread.
    format: TraceFormat,
}

impl TraceSink {
    /// Writes the traces to a file, in the given format.
    ///
    /// # Errors
    /// Propagates the file-creation error.
    pub fn file(path: &Path, capacity: usize, format: TraceFormat) -> std::io::Result<Self> {
        let file = File::create(path)?;
        Ok(Self::new(Box::new(file), capacity, format))
    }

    /// Writes the traces to stdout, in the given format.
    pub fn stdout(capacity: usize, format: TraceFormat) -> Self {
        Self::new(Box::new(std::io::stdout()), capacity, format)
    }

    /// Collects the traces into a bounded in-memory ring buffer.
    ///
    /// Returns the sink plus the shared buffer, which a live console view reads
    /// to display the last messages inside its frame.
    pub fn memory(capacity: usize, format: TraceFormat) -> (Self, RecentBuffer) {
        let buffer: RecentBuffer = Arc::new(Mutex::new(VecDeque::with_capacity(RECENT_KEEP)));
        let writer_buffer = buffer.clone();
        let sink = Self::spawn(capacity, format, move |line| {
            if let Ok(mut guard) = writer_buffer.lock() {
                if guard.len() >= RECENT_KEEP {
                    guard.pop_front();
                }
                guard.push_back(line);
            }
        });
        (sink, buffer)
    }

    fn new(writer: Box<dyn Write + Send>, capacity: usize, format: TraceFormat) -> Self {
        // Line-buffered: every record becomes visible as soon as it is written,
        // which is what makes the stream tail-able.
        let mut writer = LineWriter::new(writer);
        Self::spawn(capacity, format, move |line| {
            // A sink failure must not stall the producer: keep draining.
            if writer.write_all(line.as_bytes()).is_ok() {
                let _ = writer.write_all(b"\n");
            }
        })
    }

    /// Spawns the writer thread applying `on_line` to every rendered record.
    fn spawn(
        capacity: usize,
        format: TraceFormat,
        mut on_line: impl FnMut(String) + Send + 'static,
    ) -> Self {
        let (sender, receiver) = sync_channel::<String>(capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        let handle = std::thread::spawn(move || {
            while let Ok(line) = receiver.recv() {
                on_line(line);
            }
        });
        Self {
            sender: Some(sender),
            dropped,
            handle: Some(handle),
            format,
        }
    }

    /// Enqueues one record; drops it when the queue is saturated or the writer
    /// is gone.
    pub fn emit(&self, record: &TraceRecord<'_>) {
        let Some(sender) = self.sender.as_ref() else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        match sender.try_send(record.render(self.format)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Number of records dropped because the sink could not keep up.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl Drop for TraceSink {
    fn drop(&mut self) {
        // Closing the channel *before* joining terminates the writer loop, which
        // then flushes. Joining while the sender is still alive would deadlock:
        // struct fields are only dropped after this body runs.
        drop(self.sender.take());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> TraceRecord<'static> {
        TraceRecord {
            call_id: "deadbeef-cafe-babe-0011-223344556677",
            trace_id: Some("00112233445566778899aabbccddeeff"),
            parent_span_id: 0,
            channel: "DatabaseService",
            service_id: 42,
            method: "get_user_age",
            event_kind: "complete",
            emitter_pid: 1234,
            request_bytes: 8,
            response_bytes: 4,
            latency_us: 137,
            request_text: None,
            response_text: None,
        }
    }

    #[test]
    fn a_stats_record_omits_the_message_fields() {
        let json = record().to_json();
        assert!(json.contains("\"call_id\":\"deadbeef-cafe-babe-0011-223344556677\""));
        assert!(json.contains("\"trace_id\":\"00112233445566778899aabbccddeeff\""));
        assert!(json.contains("\"parent_span_id\":0"));
        assert!(json.contains("\"latency_us\":137"));
        assert!(json.contains("\"method\":\"get_user_age\""));
        assert!(
            !json.contains("\"request\"") && !json.contains("\"response\""),
            "stats mode must stay compact: {json}"
        );
    }

    #[test]
    fn a_detail_record_carries_the_messages() {
        let mut record = record();
        record.request_text = Some("get_user_age(name=\"Alice\")");
        record.response_text = Some("30");
        let json = record.to_json();
        assert!(json.contains("\"request\":\"get_user_age(name=\\\"Alice\\\")\""));
        assert!(json.contains("\"response\":\"30\""));
    }

    #[test]
    fn a_human_record_is_a_single_prefixed_line() {
        let line = record().to_human();
        assert!(line.starts_with("[msg] cid=deadbeef"));
        assert!(line.contains("method=get_user_age"));
        assert!(line.contains("latency_us=137"));
        assert!(
            !line.contains('\n'),
            "a record must stay on one line: {line}"
        );
        // Stats mode: no message fields.
        assert!(!line.contains(" request="));
    }

    #[test]
    fn a_human_detail_record_appends_the_messages() {
        let mut record = record();
        record.request_text = Some("echo(hi)");
        record.response_text = Some("42");
        let line = record.to_human();
        assert!(line.contains(" request=echo(hi)"));
        assert!(line.contains(" response=42"));
    }

    #[test]
    fn render_dispatches_on_the_format() {
        let stats = record();
        assert!(stats.render(TraceFormat::Json).starts_with('{'));
        assert!(stats.render(TraceFormat::Human).starts_with("[msg]"));
    }

    #[test]
    fn the_memory_sink_keeps_only_the_last_records() {
        let (sink, recent) = TraceSink::memory(64, TraceFormat::Human);
        for index in 0..(RECENT_KEEP + 3) {
            let channel = format!("c{index}");
            let record = TraceRecord {
                call_id: "id",
                trace_id: None,
                parent_span_id: 0,
                channel: &channel,
                service_id: 1,
                method: "m",
                event_kind: "complete",
                emitter_pid: 1,
                request_bytes: 0,
                response_bytes: 0,
                latency_us: 0,
                request_text: None,
                response_text: None,
            };
            sink.emit(&record);
        }
        // Dropping the sink joins the writer thread: every record was consumed.
        drop(sink);

        let guard = recent.lock().expect("buffer");
        assert_eq!(guard.len(), RECENT_KEEP);
        assert!(
            guard.back().expect("non empty").contains("channel=c12"),
            "the last record must be kept: {:?}",
            guard.back()
        );
    }
}
