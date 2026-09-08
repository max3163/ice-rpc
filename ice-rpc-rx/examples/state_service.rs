//! StateService over ice-rpc, using `Subject` + `ShareReplay`.
//!
//! Two programs communicate over ice-rpc:
//!
//! - **program B (provider)** hosts a `StateService` that keeps an observable
//!   state and subscribes to it to be notified of every change;
//! - **program A (consumer)** subscribes to `get_state` **and** pushes new
//!   states with `set_state`, then receives its own notifications.
//!
//! Run the provider in one terminal:
//!
//! ```bash
//! cargo run -p ice-rpc-rx --example state_service --features tokio -- provider
//! ```
//!
//! And the consumer in another:
//!
//! ```bash
//! cargo run -p ice-rpc-rx --example state_service --features tokio -- consumer
//! ```

use ice_rpc::{service, Observable};
use ice_rpc_rx::{ShareReplay, Subject};
use rkyv::{Archive, Deserialize, Serialize};

/// Status of a service or component.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Archive,
    Serialize,
    Deserialize,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum Status {
    /// Nominal.
    Ok,
    /// Not operational.
    Nok,
    /// Not communicated / unknown.
    Nc,
}

/// RPC service exposing an observable state.
#[service("StateService")]
pub trait StateService {
    /// Subscribes to the state. The current value is replayed first.
    async fn get_state(&self) -> Observable<Status, String>;

    /// Pushes a new status to every subscriber.
    async fn set_state(&self, status: Status) -> Observable<(), String>;
}

/// Provider implementation backed by a `Subject` + `ShareReplay`.
struct StateServiceImpl {
    subject: Subject<Status, String>,
    state: ShareReplay<Status, String>,
}

impl StateServiceImpl {
    async fn new() -> Self {
        let subject = Subject::new();
        // Feed the ShareReplay with the Subject stream so that `get_state`
        // replays the last value to late subscribers.
        let state = ShareReplay::new(subject.subscribe().await);
        Self { subject, state }
    }
}

#[async_trait::async_trait]
impl StateService for StateServiceImpl {
    async fn get_state(&self) -> Observable<Status, String> {
        Ok(self.state.subscribe().await)
    }

    async fn set_state(&self, status: Status) -> Observable<(), String> {
        self.subject.next(status).await;
        // Acknowledge with a single terminal value.
        let (tx, rx) = ice_rpc::channel::<(), String>(1);
        let _ = tx.try_send_complete_with(());
        Ok(rx)
    }
}

async fn run_provider() {
    ice_rpc::init();
    let impl_ = StateServiceImpl::new().await;

    // The provider subscribes to its own state and is notified of changes.
    let rx = impl_.get_state().await.expect("subscribe failed");
    tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            if let ice_rpc::Event::Next(status) = event {
                println!("[provider] notified: state = {:?}", status);
            }
        }
    });

    println!("[provider] StateService ready. Start the consumer in another terminal.");
    let _ = ice_rpc::run_provider!(StateServiceProxy::provide(impl_)).await;
}

async fn run_consumer() {
    ice_rpc::init();
    let guard = ice_rpc::ShutdownGuard::new();

    let proxy = ice_rpc::locator()
        .get::<StateServiceProxy>()
        .await
        .expect("StateService unknown");

    // Program A subscribes to state notifications.
    let rx = proxy.get_state().await.expect("get_state failed");
    tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            if let ice_rpc::Event::Next(status) = event {
                println!("[consumer] notified: state = {:?}", status);
            }
        }
    });

    // Give the subscription a moment to be registered and replayed.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Program A sends new states and is notified of each change.
    for status in [Status::Ok, Status::Nok, Status::Nc, Status::Ok] {
        println!("[consumer] set_state({:?})", status);
        if let Ok(rx) = proxy.set_state(status).await {
            let _ = rx.first_value().await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    guard.shutdown().await;
}

#[tokio::main]
async fn main() {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "provider".to_string());

    match mode.as_str() {
        "provider" => run_provider().await,
        "consumer" => run_consumer().await,
        other => eprintln!("usage: state_service [provider|consumer] (got {other})"),
    }
}
