//! ice-rpc HTTP Consumer — large payload transfer demonstration.
//!
//! Tests the HttpService with payloads from 1 KB to 100 MB
//! to demonstrate the zero-copy capabilities of iceoryx2.
//!
//! ## Getting started
//! ```bash
//! cargo run --example consumer-http-app
//! ```

mod shared;

use ice_rpc::{take_one_or_cancel, TakeOneError};
use shared::{HttpError, HttpRequestParams, HttpService, HttpServiceProxy};
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, BufReader};

fn generate_payload(size_bytes: usize, label: &str) -> Vec<u8> {
    let header = format!(
        "[ice-rpc-http-demo] payload={} label={} ",
        size_bytes, label
    );
    let mut payload = Vec::with_capacity(size_bytes);
    payload.extend_from_slice(header.as_bytes());

    let chunk_size: usize = 64 * 1024;
    let mut chunk = Vec::with_capacity(chunk_size);
    let mut byte: u8 = 0xAB;
    for _ in 0..chunk_size {
        chunk.push(byte);
        byte = byte.wrapping_mul(7).wrapping_add(3);
    }

    while payload.len() < size_bytes {
        let remaining = size_bytes - payload.len();
        if remaining >= chunk_size {
            payload.extend_from_slice(&chunk);
        } else {
            payload.extend_from_slice(&chunk[..remaining]);
        }
    }

    payload
}

fn fmt_latency(ms: f64) -> String {
    if ms < 5.0 {
        format!("\x1b[32m{:.3}ms\x1b[0m", ms)
    } else if ms < 50.0 {
        format!("\x1b[33m{:.3}ms\x1b[0m", ms)
    } else if ms < 500.0 {
        format!("\x1b[31m{:.3}ms\x1b[0m", ms)
    } else {
        format!("\x1b[1;31m{:.3}ms\x1b[0m", ms)
    }
}

fn fmt_throughput(bytes: usize, ms: f64) -> String {
    if ms <= 0.0 {
        return "N/A".to_string();
    }
    let bytes_per_sec = bytes as f64 / (ms / 1000.0);
    let mbps = bytes_per_sec / (1024.0 * 1024.0);
    format!("{:.2} MB/s", mbps)
}

fn fmt_bytes(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.3} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

async fn run_http_query(http: &HttpServiceProxy, label: &str, payload_size: usize) -> bool {
    let cancel = ice_rpc::global_cancel_token();

    let body = generate_payload(payload_size, label);
    let request = HttpRequestParams {
        method: "POST".to_string(),
        url: format!("http://ice-rpc-demo/{}", label),
        headers: vec![
            ("host".to_string(), "ice-rpc-demo.local".to_string()),
            (
                "content-type".to_string(),
                "application/octet-stream".to_string(),
            ),
            ("x-payload-size".to_string(), payload_size.to_string()),
            ("x-payload-label".to_string(), label.to_string()),
        ],
        body,
    };

    let req_size = request.body.len()
        + request.url.len()
        + request.method.len()
        + request
            .headers
            .iter()
            .map(|(k, v)| k.len() + v.len() + 2)
            .sum::<usize>();

    log::info!(
        "→ [{}] Sending {} (total req ~{})...",
        label,
        fmt_bytes(payload_size),
        fmt_bytes(req_size),
    );

    let t_send = Instant::now();

    let result = take_one_or_cancel!(http.send_request(request).await, cancel);

    match result {
        None => {
            log::info!("   (cancelled by Ctrl+C)");
            false
        }
        Some(Ok(response)) => {
            let elapsed_ms = t_send.elapsed().as_secs_f64() * 1000.0;
            let res_size = response.body.len()
                + response.status_text.len()
                + response
                    .headers
                    .iter()
                    .map(|(k, v)| k.len() + v.len() + 2)
                    .sum::<usize>();

            log::info!(
                "← [{}] Response {} {} (req {} → res {}) — [{}] — effective throughput {} (bidirectional total {})",
                label,
                response.status_code,
                response.status_text,
                fmt_bytes(req_size),
                fmt_bytes(res_size),
                fmt_latency(elapsed_ms),
                fmt_throughput(req_size + res_size, elapsed_ms),
                fmt_bytes(req_size + res_size),
            );

            if response.status_code != 200 {
                log::warn!(
                    "  ⚠ Unexpected status: {} {}",
                    response.status_code,
                    response.status_text
                );
            }

            true
        }
        Some(Err(TakeOneError::Service(HttpError::PayloadTooLarge {
            max_bytes,
            actual_bytes,
        }))) => {
            let elapsed_ms = t_send.elapsed().as_secs_f64() * 1000.0;
            log::error!(
                "  ✗ [{}] Payload too large: {} max, {} sent  [{}]",
                label,
                fmt_bytes(max_bytes as usize),
                fmt_bytes(actual_bytes as usize),
                fmt_latency(elapsed_ms),
            );
            true
        }
        Some(Err(TakeOneError::Service(e))) => {
            let elapsed_ms = t_send.elapsed().as_secs_f64() * 1000.0;
            log::error!(
                "  ✗ [{}] Business error: {:?}  [{}]",
                label,
                e,
                fmt_latency(elapsed_ms)
            );
            true
        }
        Some(Err(TakeOneError::Ipc(e))) => {
            let elapsed_ms = t_send.elapsed().as_secs_f64() * 1000.0;
            log::error!(
                "  ✗ [{}] IPC error: {}  [{}]",
                label,
                e,
                fmt_latency(elapsed_ms)
            );
            true
        }
        Some(Err(TakeOneError::Empty)) => {
            let elapsed_ms = t_send.elapsed().as_secs_f64() * 1000.0;
            log::warn!(
                "  ✗ [{}] No value received  [{}]",
                label,
                fmt_latency(elapsed_ms)
            );
            true
        }
    }
}

struct PayloadTest {
    label: &'static str,
    body_size: usize,
}

async fn run_all_http_tests(http: &HttpServiceProxy) -> bool {
    let tests = vec![
        PayloadTest {
            label: "1 KB",
            body_size: 1024,
        },
        PayloadTest {
            label: "1 MB",
            body_size: 1024 * 1024,
        },
        PayloadTest {
            label: "5 MB",
            body_size: 5 * 1024 * 1024,
        },
        PayloadTest {
            label: "10 MB",
            body_size: 10 * 1024 * 1024,
        },
        PayloadTest {
            label: "100 MB",
            body_size: 100 * 1024 * 1024,
        },
    ];

    log::info!("");
    log::info!("╔══════════════════════════════════════════════════════════════╗");
    log::info!("║       Large payload transfer tests via iceoryx2              ║");
    log::info!("╚══════════════════════════════════════════════════════════════╝");
    log::info!("");
    log::info!("Protocol: zero-copy shared memory (iceoryx2)");
    log::info!("Serialization: rkyv (zero-copy)");
    log::info!("Transport: {} requests/responses", tests.len());
    log::info!("");

    for test in &tests {
        log::info!("───────────────────────────────────────────────────────────────");
        if !run_http_query(http, test.label, test.body_size).await {
            return false;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    log::info!("───────────────────────────────────────────────────────────────");
    log::info!("");
    log::info!("✅ All transfer tests are complete.");
    log::info!(
        "Total transferred: {} outbound + {} return = {}",
        fmt_bytes(tests.iter().map(|t| t.body_size).sum::<usize>()),
        fmt_bytes(tests.iter().map(|t| t.body_size).sum::<usize>()),
        fmt_bytes(tests.iter().map(|t| t.body_size * 2).sum::<usize>()),
    );
    log::info!("");

    true
}

/// Reads a line from stdin asynchronously.
/// Returns `None` if stdin is closed or the cancellation token is triggered.
async fn read_line_or_cancel(
    reader: &mut (impl AsyncBufReadExt + Unpin),
    cancel: &ice_rpc::CancellationToken,
) -> Option<String> {
    let mut line = String::new();
    tokio::select! {
        _ = cancel.cancelled() => None,
        result = reader.read_line(&mut line) => match result {
            Ok(0) => None, // EOF
            Ok(_) => Some(line),
            Err(_) => None,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    log::info!("╔══════════════════════════════════════════════════════════════╗");
    log::info!("║    ice-rpc HTTP Consumer — Large payload demonstration       ║");
    log::info!("╚══════════════════════════════════════════════════════════════╝");
    log::info!("");

    // RAII guard: cancels the cancellation tokens on Drop (even on panic).
    // shutdown() must be called explicitly for a clean stop with
    // waiting for the IPC threads and releasing the iceoryx2 node.
    let shutdown_guard = ice_rpc::ShutdownGuard::new();

    // This process consumes HttpService via locator().get().
    ice_rpc::init_consumer();

    let cancel = ice_rpc::global_cancel_token();
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);

    let http_service = ice_rpc::locator()
        .get::<HttpServiceProxy>()
        .await
        .expect("HttpService unknown in the registry");

    log::info!("--- Initial execution ---");
    if !run_all_http_tests(&http_service).await {
        return shutdown(&shutdown_guard).await;
    }
    log::info!("--- End. [ENTER] to replay, [Ctrl+C] to quit. ---\n");

    loop {
        match read_line_or_cancel(&mut reader, cancel).await {
            None => break, // Ctrl+C or EOF
            Some(_) => {
                log::info!("\n--- Relaunching tests ---");
                if !run_all_http_tests(&http_service).await {
                    break;
                }
                log::info!("--- End. [ENTER] to replay, [Ctrl+C] to quit. ---\n");
            }
        }
    }

    shutdown(&shutdown_guard).await
}

/// Clean shutdown: ice-rpc shutdown via the RAII guard.
///
/// The guard guarantees that the tokens are cancelled even if this function
/// is not called (panic, early return…).
async fn shutdown(guard: &ice_rpc::ShutdownGuard) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("\nStopping HTTP consumer...");
    guard.shutdown().await;
    log::info!("HTTP consumer stopped.");
    Ok(())
}
