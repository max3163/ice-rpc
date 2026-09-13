//! NDJSON trace stream correlated by `correlation_id`.
//!
//! Records are handed to a dedicated writer thread through a **bounded** queue:
//! a full queue drops records (and counts them) instead of blocking the
//! acquisition loop, so tracing can never make the observer fall behind.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, TrySendError};
use std::sync::Arc;
use std::thread::JoinHandle;

use serde_json::json;

/// One completed call, ready to be serialised.
pub struct TraceRecord<'a> {
    /// `correlation_id` formatted as a UUID-like string (the trace id).
    pub trace_id: &'a str,
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
}

impl TraceRecord<'_> {
    /// Serialises the record as a single JSON line (without the trailing newline).
    fn to_json(&self) -> String {
        json!({
            "trace_id": self.trace_id,
            "channel": self.channel,
            "service_id": self.service_id,
            "method": self.method,
            "event_kind": self.event_kind,
            "emitter_pid": self.emitter_pid,
            "request_bytes": self.request_bytes,
            "response_bytes": self.response_bytes,
            "latency_us": self.latency_us,
        })
        .to_string()
    }
}

/// Bounded, non-blocking trace sink.
pub struct TraceSink {
    sender: std::sync::mpsc::SyncSender<String>,
    dropped: Arc<AtomicU64>,
    handle: Option<JoinHandle<()>>,
}

impl TraceSink {
    /// Writes the traces to a file.
    ///
    /// # Errors
    /// Propagates the file-creation error.
    pub fn file(path: &Path, capacity: usize) -> std::io::Result<Self> {
        let file = File::create(path)?;
        Ok(Self::new(Box::new(BufWriter::new(file)), capacity))
    }

    /// Writes the traces to stdout.
    pub fn stdout(capacity: usize) -> Self {
        Self::new(Box::new(BufWriter::new(std::io::stdout())), capacity)
    }

    fn new(writer: Box<dyn Write + Send>, capacity: usize) -> Self {
        let (sender, receiver) = sync_channel::<String>(capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        let handle = std::thread::spawn(move || {
            let mut writer = writer;
            while let Ok(line) = receiver.recv() {
                // A sink failure must not stall the producer: keep draining.
                if writer.write_all(line.as_bytes()).is_ok() {
                    let _ = writer.write_all(b"\n");
                }
            }
            let _ = writer.flush();
        });
        Self {
            sender,
            dropped,
            handle: Some(handle),
        }
    }

    /// Enqueues one record; drops it when the queue is saturated or the writer
    /// is gone.
    pub fn emit(&self, record: &TraceRecord<'_>) {
        match self.sender.try_send(record.to_json()) {
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
        // Dropping the sender ends the writer loop, which then flushes.
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_is_serialised_as_flat_json() {
        let record = TraceRecord {
            trace_id: "deadbeef-cafe-babe-0011-223344556677",
            channel: "DatabaseService",
            service_id: 42,
            method: "get_user_age",
            event_kind: "complete",
            emitter_pid: 1234,
            request_bytes: 8,
            response_bytes: 4,
            latency_us: 137,
        };
        let json = record.to_json();
        assert!(json.contains("\"trace_id\":\"deadbeef-cafe-babe-0011-223344556677\""));
        assert!(json.contains("\"latency_us\":137"));
        assert!(json.contains("\"method\":\"get_user_age\""));
    }
}
