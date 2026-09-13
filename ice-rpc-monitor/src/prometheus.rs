//! Minimal Prometheus text-format HTTP endpoint.
//!
//! Deliberately dependency-free: a single blocking `TcpListener` thread serves
//! `GET /metrics`. The exposition payload is built from the shared
//! [`Metrics`] registry.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::thread::JoinHandle;

use crate::metrics::Metrics;

/// Starts the endpoint and returns its serving thread.
///
/// # Errors
/// Propagates the bind error (typically a port already in use).
pub fn serve(addr: SocketAddr, metrics: Arc<Metrics>) -> std::io::Result<JoinHandle<()>> {
    let listener = TcpListener::bind(addr)?;
    log::info!("[monitor] Prometheus endpoint on http://{addr}/metrics");

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    let mut buffer = [0u8; 1024];
                    let _ = stream.read(&mut buffer);
                    let path = request_path(&buffer);
                    let (status, body) = if path == "/metrics" {
                        ("200 OK", metrics.render_prometheus())
                    } else {
                        ("404 Not Found", String::from("not found\n"))
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\n\
                         Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
                         Content-Length: {}\r\n\
                         Connection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes());
                    let _ = stream.flush();
                }
                Err(e) => log::warn!("[monitor] accept failed: {e}"),
            }
        }
    });

    Ok(handle)
}

/// Extracts the request target from a raw HTTP request.
fn request_path(buffer: &[u8]) -> &str {
    std::str::from_utf8(buffer)
        .unwrap_or("")
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_request_target_is_extracted() {
        assert_eq!(
            request_path(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n"),
            "/metrics"
        );
        assert_eq!(request_path(b"GET / HTTP/1.1\r\n"), "/");
        assert_eq!(request_path(b""), "/");
    }
}
