//! End-to-end test of the Phoenix Channels WebSocket origin policy.
//!
//! `trillium-websockets` refuses a browser handshake whose `Origin` does not
//! name the request host. The gateway keeps that default and exposes two ways
//! to widen it: an exact `allowed_origins` allowlist and the
//! `allow_any_origin` opt-out. The test speaks the handshake directly, so no
//! service has to be exposed to observe the policy.

#![cfg(feature = "channels-tokio")]
#![allow(clippy::unwrap_used)] // tests may panic

use std::collections::HashMap;
use std::time::Duration;

use async_tungstenite::tungstenite::Error;
use async_tungstenite::tungstenite::client::IntoClientRequest;
use ice_rpc::ChannelsGatewayConfig;

type Socket = async_tungstenite::WebSocketStream<async_tungstenite::tokio::ConnectStream>;

/// Returns a port that was free a moment ago.
fn free_port() -> u16 {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("a free port");
    probe.local_addr().expect("the probe address").port()
}

/// Attempts the WebSocket handshake with an optional `Origin`, as a browser
/// would send it.
async fn connect_with_origin(port: u16, origin: Option<&str>) -> Result<Socket, Error> {
    let url = format!("ws://127.0.0.1:{port}/socket/websocket?vsn=2.0.0");
    let mut request = url
        .into_client_request()
        .expect("a valid handshake request");
    if let Some(origin) = origin {
        request
            .headers_mut()
            .insert("origin", origin.parse().expect("a valid Origin header"));
    }
    async_tungstenite::tokio::connect_async(request)
        .await
        .map(|(socket, _response)| socket)
}

/// Waits until the gateway on `port` answers a handshake.
async fn wait_for(port: u16) {
    for _ in 0..100 {
        if connect_with_origin(port, None).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the gateway did not start on port {port}");
}

/// Asserts that the handshake was refused with `403 Forbidden`.
fn assert_forbidden(result: Result<Socket, Error>) {
    match result {
        Err(Error::Http(response)) => assert_eq!(
            response.status(),
            403,
            "a refused origin is answered with 403 Forbidden"
        ),
        Err(other) => panic!("expected a 403 response, got {other}"),
        Ok(_) => panic!("a foreign origin must be refused at the handshake"),
    }
}

/// Starts the gateway with `config` and no exposed service.
async fn serve(port: u16, config: ChannelsGatewayConfig) {
    ice_rpc::start_channels_server_with_config(port, config, HashMap::new()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_websocket_origin_policy_is_same_origin_by_default_and_can_be_widened() {
    let same_origin_port = free_port();
    let same_origin = tokio::spawn(serve(same_origin_port, ChannelsGatewayConfig::new()));

    let any_port = free_port();
    let mut any_config = ChannelsGatewayConfig::new();
    any_config.allow_any_origin = true;
    let any = tokio::spawn(serve(any_port, any_config));

    let listed_port = free_port();
    let mut listed_config = ChannelsGatewayConfig::new();
    listed_config.allowed_origins = vec!["https://app.example.com".to_string()];
    let listed = tokio::spawn(serve(listed_port, listed_config));

    let result = tokio::time::timeout(Duration::from_secs(10), async {
        wait_for(same_origin_port).await;
        wait_for(any_port).await;
        wait_for(listed_port).await;

        // Default: a page served from another host is refused, while a
        // non-browser client that sends no Origin is accepted.
        assert_forbidden(
            connect_with_origin(same_origin_port, Some("https://app.example.com")).await,
        );
        connect_with_origin(same_origin_port, None)
            .await
            .expect("a client without an Origin is not a browser");

        // `allow_any_origin` disables the check entirely.
        connect_with_origin(any_port, Some("https://app.example.com"))
            .await
            .expect("allow_any_origin accepts any page");
        connect_with_origin(any_port, Some("http://evil.example.com"))
            .await
            .expect("allow_any_origin accepts a foreign page");

        // An allowlist admits exactly the listed origins and keeps refusing
        // the others.
        connect_with_origin(listed_port, Some("https://app.example.com"))
            .await
            .expect("the allowlisted origin is accepted");
        assert_forbidden(connect_with_origin(listed_port, Some("https://evil.example.com")).await);
        connect_with_origin(listed_port, None)
            .await
            .expect("a client without an Origin is still accepted");
    })
    .await;

    ice_rpc::global_cancel_token().cancel();
    let _ = tokio::join!(same_origin, any, listed);
    result.expect("the origin test completes before the timeout");
}
