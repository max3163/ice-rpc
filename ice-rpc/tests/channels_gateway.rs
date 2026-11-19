//! End-to-end test of the Phoenix Channels wire protocol.
//!
//! The client speaks the V2 array format directly: what a `phoenix` JavaScript
//! client sends and receives. The gateway is served by `trillium-tokio`, the
//! same adapter the library uses, and the test covers per-service joins, unary
//! calls, business errors, streaming, cancellation and heartbeat.

#![cfg(feature = "channels-tokio")]
#![allow(clippy::unwrap_used)] // tests may panic

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_tungstenite::tungstenite::Message;
use futures_util::StreamExt;
use ice_rpc::ChannelsGatewayConfig;
use ice_rpc::codegen::serde_json::{Value, json};
use ice_rpc::json::{
    JsonCallError, JsonEventSink, JsonInvoker, JsonInvokerFactory, JsonOutcome, JsonStreamEvent,
    ReadMode,
};

type Socket = async_tungstenite::WebSocketStream<async_tungstenite::tokio::ConnectStream>;

struct MockInvoker;

#[ice_rpc::codegen::async_trait::async_trait]
impl JsonInvoker for MockInvoker {
    fn service_name(&self) -> &'static str {
        "MockService"
    }

    async fn invoke_json(
        &self,
        method: &str,
        _args: Value,
        _read: ReadMode,
    ) -> Option<Result<JsonOutcome, JsonCallError>> {
        match method {
            "echo" => Some(Ok(JsonOutcome::Value(json!({ "ok": true })))),
            "nothing" => Some(Ok(JsonOutcome::Nothing)),
            "accepted" => Some(Ok(JsonOutcome::Accepted { delivered: true })),
            "fail" => Some(Err(JsonCallError::Business("no".to_string()))),
            _ => None,
        }
    }

    async fn invoke_json_stream(
        &self,
        method: &str,
        _args: Value,
        sink: JsonEventSink,
    ) -> Option<()> {
        match method {
            "values" => {
                if !sink.send(JsonStreamEvent::Next(json!(1))).await {
                    return Some(());
                }
                if !sink.send(JsonStreamEvent::Next(json!(2))).await {
                    return Some(());
                }
                let _ = sink.send(JsonStreamEvent::Complete).await;
                Some(())
            }
            "hang" => {
                // Runs until the gateway drops the sink: that is the
                // cancellation signal.
                loop {
                    if !sink.send(JsonStreamEvent::Next(json!(0))).await {
                        return Some(());
                    }
                    ice_rpc::rt::sleep(Duration::from_millis(10)).await;
                }
            }
            "notify" => {
                let _ = sink
                    .send(JsonStreamEvent::Accepted { delivered: true })
                    .await;
                Some(())
            }
            _ => None,
        }
    }
}

fn factory() -> Arc<dyn JsonInvoker> {
    Arc::new(MockInvoker)
}

fn factories() -> HashMap<&'static str, JsonInvokerFactory> {
    HashMap::from([
        ("MockService", factory as JsonInvokerFactory),
        ("SecondService", factory as JsonInvokerFactory),
    ])
}

async fn connect(port: u16) -> Socket {
    let url = format!("ws://127.0.0.1:{port}/socket/websocket?vsn=2.0.0");
    for _ in 0..100 {
        match async_tungstenite::tokio::connect_async(&url).await {
            Ok((socket, _response)) => return socket,
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    panic!("the gateway did not start");
}

async fn send(socket: &mut Socket, message: Value) {
    socket
        .send(Message::text(message.to_string()))
        .await
        .expect("the socket accepts the frame");
}

async fn recv(socket: &mut Socket) -> Value {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .expect("a frame arrives before the test timeout")
            .expect("the socket stays open")
            .expect("a valid frame");
        match message {
            Message::Text(_) => {
                return serde_json::from_str(message.to_text().expect("text frame"))
                    .expect("valid JSON");
            }
            Message::Close(_) => panic!("the gateway closed the socket"),
            _ => {}
        }
    }
}

async fn recv_until(socket: &mut Socket, mut predicate: impl FnMut(&Value) -> bool) -> Value {
    loop {
        let value = recv(socket).await;
        if predicate(&value) {
            return value;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_phoenix_channels_protocol_serves_calls_and_streams() {
    let port = {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("a free port");
        probe.local_addr().expect("the probe address").port()
    };

    let server = tokio::spawn(async move {
        ice_rpc::start_channels_server_with_config(port, ChannelsGatewayConfig::new(), factories())
            .await;
    });

    let result = tokio::time::timeout(Duration::from_secs(10), async {
        let mut socket = connect(port).await;

        // Each service is its own topic: joining it is what authorizes calls.
        send(
            &mut socket,
            json!(["1", "1", "MockService", "phx_join", {}]),
        )
        .await;
        let join = recv_until(&mut socket, |value| value[3] == "phx_reply").await;
        assert_eq!(join[0].as_str(), Some("1"));
        assert_eq!(join[1].as_str(), Some("1"));
        assert_eq!(join[2].as_str(), Some("MockService"));
        assert_eq!(join[4]["status"].as_str(), Some("ok"));
        assert_eq!(join[4]["response"]["protocol"].as_u64(), Some(1));
        assert_eq!(join[4]["response"]["service"].as_str(), Some("MockService"));

        // Unary call: the topic names the service, the push ref is the
        // correlation id, and the payload only carries the method.
        send(
            &mut socket,
            json!(["1", "2", "MockService", "call", { "method": "echo" }]),
        )
        .await;
        let call = recv_until(&mut socket, |value| {
            value[3] == "phx_reply" && value[1] == "2"
        })
        .await;
        assert_eq!(call[4]["status"].as_str(), Some("ok"));
        assert_eq!(call[4]["response"]["kind"].as_str(), Some("value"));
        assert_eq!(call[4]["response"]["data"]["ok"].as_bool(), Some(true));

        // Business error.
        send(
            &mut socket,
            json!(["1", "3", "MockService", "call", { "method": "fail" }]),
        )
        .await;
        let error = recv_until(&mut socket, |value| {
            value[3] == "phx_reply" && value[1] == "3"
        })
        .await;
        assert_eq!(error[4]["status"].as_str(), Some("error"));
        assert_eq!(error[4]["response"]["kind"].as_str(), Some("E_BUSINESS"));

        // One-way call.
        send(
            &mut socket,
            json!(["1", "8", "MockService", "call", {
                "method": "accepted",
            }]),
        )
        .await;
        let accepted = recv_until(&mut socket, |value| {
            value[3] == "phx_reply" && value[1] == "8"
        })
        .await;
        assert_eq!(accepted[4]["status"].as_str(), Some("ok"));
        assert_eq!(accepted[4]["response"]["kind"].as_str(), Some("accepted"));
        assert_eq!(accepted[4]["response"]["delivered"].as_bool(), Some(true));

        // Streaming: the push ref identifies the stream in the ack and in
        // every server push.
        let values_ref = "4";
        send(
            &mut socket,
            json!(["1", values_ref, "MockService", "stream", {
                "method": "values",
            }]),
        )
        .await;
        let ack = recv_until(&mut socket, |value| {
            value[3] == "phx_reply" && value[1] == values_ref
        })
        .await;
        assert_eq!(ack[4]["status"].as_str(), Some("ok"));
        assert_eq!(ack[4]["response"]["ref"].as_str(), Some(values_ref));

        let mut next_values = Vec::new();
        loop {
            let value = recv(&mut socket).await;
            match value[3].as_str() {
                Some("next") if value[4]["ref"] == values_ref => {
                    next_values.push(value[4]["data"].clone());
                }
                Some("complete") if value[4]["ref"] == values_ref => break,
                Some("error") if value[4]["ref"] == values_ref => {
                    panic!("unexpected stream error: {value}")
                }
                _ => {}
            }
        }
        assert_eq!(next_values, vec![json!(1), json!(2)]);

        // One-way stream: the ack, then the accepted event, both on the push
        // ref.
        let notify_ref = "9";
        send(
            &mut socket,
            json!(["1", notify_ref, "MockService", "stream", {
                "method": "notify",
            }]),
        )
        .await;
        let notify_ack = recv_until(&mut socket, |value| {
            value[3] == "phx_reply" && value[1] == notify_ref
        })
        .await;
        assert_eq!(notify_ack[4]["status"].as_str(), Some("ok"));
        let accepted_event = recv_until(&mut socket, |value| {
            value[3] == "accepted" && value[4]["ref"] == notify_ref
        })
        .await;
        assert_eq!(accepted_event[4]["delivered"].as_bool(), Some(true));

        // Cancellation. The cancel reply and the terminal push may interleave,
        // so both are collected until the pair is complete.
        let hang_ref = "5";
        send(
            &mut socket,
            json!(["1", hang_ref, "MockService", "stream", {
                "method": "hang",
            }]),
        )
        .await;
        let _ack = recv_until(&mut socket, |value| {
            value[3] == "phx_reply" && value[1] == hang_ref
        })
        .await;
        let _first_next = recv_until(&mut socket, |value| {
            value[3] == "next" && value[4]["ref"] == hang_ref
        })
        .await;

        send(
            &mut socket,
            json!(["1", "6", "MockService", "cancel", { "ref": hang_ref }]),
        )
        .await;

        let mut saw_cancel_reply = false;
        let mut saw_complete = false;
        while !(saw_cancel_reply && saw_complete) {
            let value = recv(&mut socket).await;
            match value[3].as_str() {
                Some("phx_reply") if value[1] == "6" => {
                    assert_eq!(value[4]["status"].as_str(), Some("ok"));
                    saw_cancel_reply = true;
                }
                Some("complete") if value[4]["ref"] == hang_ref => {
                    saw_complete = true;
                }
                _ => {}
            }
        }

        // A second service is a second topic on the same socket.
        send(
            &mut socket,
            json!(["20", "20", "SecondService", "phx_join", {}]),
        )
        .await;
        let second_join = recv_until(&mut socket, |value| {
            value[3] == "phx_reply" && value[1] == "20"
        })
        .await;
        assert_eq!(second_join[4]["status"].as_str(), Some("ok"));
        send(
            &mut socket,
            json!(["20", "21", "SecondService", "call", { "method": "echo" }]),
        )
        .await;
        let second_call = recv_until(&mut socket, |value| {
            value[3] == "phx_reply" && value[1] == "21"
        })
        .await;
        assert_eq!(second_call[4]["status"].as_str(), Some("ok"));

        // A call on a topic that was never joined is refused before any service
        // lookup, so the error names the join and not the service.
        send(
            &mut socket,
            json!(["30", "31", "UnknownService", "call", { "method": "echo" }]),
        )
        .await;
        let not_joined = recv_until(&mut socket, |value| {
            value[3] == "phx_reply" && value[1] == "31"
        })
        .await;
        assert_eq!(
            not_joined[4]["response"]["kind"].as_str(),
            Some("E_NOT_JOINED")
        );

        // Heartbeat.
        send(&mut socket, json!([null, "7", "phoenix", "heartbeat", {}])).await;
        let heartbeat = recv_until(&mut socket, |value| {
            value[2] == "phoenix" && value[1] == "7"
        })
        .await;
        assert_eq!(heartbeat[3].as_str(), Some("phx_reply"));
        assert_eq!(heartbeat[4]["status"].as_str(), Some("ok"));

        // An unknown service is refused at join time.
        send(
            &mut socket,
            json!(["10", "11", "UnknownService", "phx_join", {}]),
        )
        .await;
        let denied = recv_until(&mut socket, |value| {
            value[3] == "phx_reply" && value[1] == "11"
        })
        .await;
        assert_eq!(denied[0].as_str(), Some("10"));
        assert_eq!(denied[2].as_str(), Some("UnknownService"));
        assert_eq!(denied[4]["status"].as_str(), Some("error"));
        assert_eq!(
            denied[4]["response"]["kind"].as_str(),
            Some("E_UNKNOWN_SERVICE")
        );

        // The heartbeat topic is reserved and cannot be joined as a service.
        send(&mut socket, json!(["12", "13", "phoenix", "phx_join", {}])).await;
        let reserved = recv_until(&mut socket, |value| {
            value[3] == "phx_reply" && value[1] == "13"
        })
        .await;
        assert_eq!(reserved[4]["status"].as_str(), Some("error"));
        assert_eq!(
            reserved[4]["response"]["kind"].as_str(),
            Some("E_UNKNOWN_TOPIC")
        );
    })
    .await;

    ice_rpc::global_cancel_token().cancel();
    let _ = server.await;
    result.expect("the protocol test completes before the timeout");
}
