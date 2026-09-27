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
    use std::io::{Read, Write};
    use std::net::TcpStream;

    use super::*;

    /// Sends one request and returns the whole response, headers included.
    fn http_get(addr: SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(addr).expect("connect");
        let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).expect("write");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read");
        response
    }

    #[test]
    fn the_request_target_is_extracted() {
        assert_eq!(
            request_path(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n"),
            "/metrics"
        );
        assert_eq!(request_path(b"GET / HTTP/1.1\r\n"), "/");
        assert_eq!(request_path(b""), "/");
    }

    #[test]
    fn binding_a_port_already_in_use_fails() {
        // Holding the listener is what makes the port unavailable.
        let taken = TcpListener::bind("127.0.0.1:0").expect("bind a probe port");
        let addr = taken.local_addr().expect("probe address");
        assert!(
            serve(addr, Arc::new(Metrics::new())).is_err(),
            "the endpoint must report a port it cannot bind"
        );
    }

    #[test]
    fn the_endpoint_serves_the_metrics_and_rejects_other_paths() {
        // Take an ephemeral port, then release it for the endpoint to bind.
        let probe = TcpListener::bind("127.0.0.1:0").expect("probe bind");
        let addr = probe.local_addr().expect("probe address");
        drop(probe);

        let metrics = Arc::new(Metrics::new());
        metrics.on_request("DatabaseService", 42, "get_user_age");
        let _server = serve(addr, metrics.clone()).expect("the endpoint starts");

        let found = http_get(addr, "/metrics");
        assert!(found.starts_with("HTTP/1.1 200 OK"), "{found}");
        assert!(found.contains("Content-Type: text/plain"), "{found}");
        assert!(
            found.contains(
                "ice_rpc_requests_total{channel=\"DatabaseService\",service=\"42\",method=\"get_user_age\"} 1"
            ),
            "the exposition is served:\n{found}"
        );

        let missing = http_get(addr, "/nope");
        assert!(missing.starts_with("HTTP/1.1 404 Not Found"), "{missing}");
        assert!(missing.contains("not found"), "{missing}");
    }
}
