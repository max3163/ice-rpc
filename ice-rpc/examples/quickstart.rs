//! ice-rpc quick start — one service, two roles, one file.
//!
//! Run the provider in one terminal and the consumer in another. Both must run
//! from the same directory, so they resolve the same iceoryx2 configuration:
//!
//! ```bash
//! # terminal 1 — the provider (blocks until Ctrl+C)
//! cargo run -p ice-rpc --example quickstart --features tokio -- provider
//!
//! # terminal 2 — the consumer (prints the greeting and exits)
//! cargo run -p ice-rpc --example quickstart --features tokio -- consumer
//! ```

#![allow(missing_docs)] // example target: documented by Readme.md, not part of a published API
#![allow(clippy::unwrap_used)] // tests/examples/benches may panic

use ice_rpc::{Observable, service};

/// The business error of [`GreeterService`].
///
/// `#[ice_rpc::payload]` supplies the rkyv derives (and `Debug`, which this type
/// derives itself) — no `rkyv` dependency of your own.
#[ice_rpc::payload]
#[derive(Debug, Clone)]
pub enum GreeterError {
    /// The name was empty.
    EmptyName,
}

impl std::fmt::Display for GreeterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GreeterError::EmptyName => f.write_str("the name must not be empty"),
        }
    }
}

/// A trivial service: greets a name.
///
/// The macro generates `GreeterServiceRequest`, `GreeterServiceClient`,
/// `GreeterServiceServer`, `GreeterServiceProxy` and `GreeterServiceMode`.
#[service("GreeterService")]
pub trait GreeterService {
    /// Returns a greeting for `name`.
    async fn hello(&self, name: String) -> Observable<String, GreeterError>;
}

/// The provider implementation.
struct Greeter;

#[ice_rpc::async_trait]
impl GreeterService for Greeter {
    async fn hello(&self, name: String) -> Observable<String, GreeterError> {
        if name.is_empty() {
            // A business error is an observable: it travels inside the flux.
            return ice_rpc::throw_error(GreeterError::EmptyName);
        }
        // One value, then completion — no channel, no task. A long-lived push
        // stream would use `ice_rpc::codegen::channel`.
        ice_rpc::of(format!("Hello {name}!"))
    }
}

/// Hosts the service and blocks until Ctrl+C.
async fn run_provider() {
    println!("[provider] GreeterService ready. Start the consumer in another terminal.");
    // `run_provider!` never returns before shutdown: it awaits Ctrl+C.
    if let Err(error) = ice_rpc::run_provider!(GreeterServiceProxy::provide(Greeter)).await {
        eprintln!("[provider] stopped: {error}");
    }
}

/// Calls the service: the proxy is built lazily from its type — no registry, no
/// explicit connection. The IPC link opens on the first call.
async fn run_consumer() {
    let proxy = ice_rpc::locator()
        .get::<GreeterServiceProxy>()
        .await
        .expect("GreeterService unknown");

    match proxy.hello("Alice".into()).await.first_value().await {
        Ok(greeting) => println!("[consumer] {greeting}"),
        Err(error) => eprintln!("[consumer] call failed: {error}"),
    }
}

#[ice_rpc::main(tokio)]
async fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("provider") | None => run_provider().await,
        Some("consumer") => run_consumer().await,
        Some(other) => eprintln!("usage: quickstart [provider|consumer] (got {other})"),
    }
}
