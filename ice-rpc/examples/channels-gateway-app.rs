//! Phoenix Channels gateway: a provider and the gateway in one process.
//!
//! Run it with the tokio runtime and the channels adapter:
//!
//! ```bash
//! cargo run -p ice-rpc --example channels-gateway-app \
//!     --features tokio,channels-tokio
//! ```
//!
//! Then, from a browser with the `phoenix` package:
//!
//! ```js
//! const socket = new Socket("/socket", { transport: WebSocket });
//! // Each service is its own topic: the topic is the service name.
//! const channel = socket.channel("GreeterService", {});
//! channel.join();
//!
//! // The push ref is the correlation id; a stream echoes it in every event.
//! channel.push("call", {
//!   method: "hello",
//!   args: "Alice",
//! }).receive("ok", (reply) => console.log(reply));
//!
//! channel.on("next", (event) => console.log(event.ref, event.data));
//! channel.push("stream", {
//!   method: "countdown",
//!   args: 5,
//! });
//! ```
//!
//! The REST gateway is mounted on the same port; `GET /GreeterService/hello?name=Alice`
//! still works.

#![allow(missing_docs)] // example target: documented by Readme.md, not part of a published API
#![allow(clippy::unwrap_used)] // tests/examples/benches may panic

use ice_rpc::{Observable, service};

/// The business error of the example service.
#[ice_rpc::payload(display)]
#[derive(Debug, Clone)]
pub enum GreeterError {
    /// The name was empty.
    EmptyName,
}

/// A service with a unary method and a multi-value stream.
#[service("GreeterService")]
pub trait GreeterService {
    /// Returns one greeting.
    async fn hello(&self, name: String) -> Observable<String, GreeterError>;

    /// Emits `from` values, then completes.
    async fn countdown(&self, from: u32) -> Observable<u32, GreeterError>;
}

/// The implementation served by this process.
struct Greeter;

#[ice_rpc::async_trait]
impl GreeterService for Greeter {
    async fn hello(&self, name: String) -> Observable<String, GreeterError> {
        if name.is_empty() {
            return ice_rpc::throw_error(GreeterError::EmptyName);
        }
        ice_rpc::of(format!("Hello {name}!"))
    }

    async fn countdown(&self, from: u32) -> Observable<u32, GreeterError> {
        let events = (0..from)
            .map(ice_rpc::Event::Next)
            .chain(std::iter::once(ice_rpc::Event::Complete));
        Observable::from_events(events)
    }
}

#[ice_rpc::main(tokio)]
async fn main() {
    // The provider blocks until shutdown, so it runs on its own task while the
    // gateway owns the main task.
    ice_rpc::rt::spawn(async {
        if let Err(error) = ice_rpc::run_provider!(GreeterServiceProxy::provide(Greeter)).await {
            log::error!("provider stopped: {error}");
        }
    });

    ice_rpc::start_channels_gateway!(8080, GreeterServiceProxy).await;
}
