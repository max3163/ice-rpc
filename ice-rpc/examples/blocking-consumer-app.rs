//! ice-rpc **blocking** consumer: a plain `fn main`, no async runtime.
//!
//! `#[ice_rpc::main(rt-none)]` bootstraps ice-rpc around a **synchronous** main:
//! no `#[ice_rpc::main]` async wrapper, no executor, no reactor. The body runs
//! blocking — [`ServiceLocator::get_blocking`], `{method}_blocking`,
//! `first_value_blocking` — and the macro shuts ice-rpc down with
//! `ShutdownGuard::shutdown_blocking` on the way out.
//!
//! A provider must be running (`cargo run --example provider-app`).
//!
//! Usage:
//! - `cargo run --example blocking-consumer-app`

#![allow(clippy::unwrap_used)] // examples may panic

use common::DatabaseServiceProxy;
use ice_rpc::ObservableError;
use ice_rpc::ServiceLocator;

#[ice_rpc::main(rt-none)]
fn main() {
    // Same logging setup as the async examples.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    log::info!("=== BLOCKING CONSUMER STARTUP ===");

    // The lazy proxy, through the blocking twin of `get`.
    let proxy: std::sync::Arc<DatabaseServiceProxy> = ServiceLocator::global()
        .get_blocking::<DatabaseServiceProxy>()
        .expect("the locator always builds a consumer proxy");

    // `#[service(..., blocking)]` generated a synchronous twin of the method:
    // the call is published directly, with no future and no `block_on` at the
    // call site. Only `first_value_blocking` pulls the response back.
    let stream = proxy.get_user_age_blocking("Alice".into());
    match stream.first_value_blocking() {
        Ok(age) => log::info!("<- Alice is {age} years old"),
        Err(ObservableError::Business(e)) => log::warn!("<- business error: {e:?}"),
        Err(ObservableError::Technical(e)) => log::error!("<- IPC error: {e}"),
        Err(ObservableError::Empty) => log::warn!("<- no value received"),
    }

    // `#[ice_rpc::main(rt-none)]` runs `shutdown_blocking` after this body, so the
    // dispatch threads are joined and the iceoryx2 ports dropped before exit.
    log::info!("=== BLOCKING CONSUMER STOPPED ===");
}
