//! A provider wait must be interruptible.
//!
//! `native_call` publishes until a subscriber appears, which blocks for the
//! whole provider-wait deadline (30 s by default) when no provider is running;
//! Ctrl+C — and the end of `main` — must abort that wait.
//!
//! This test runs in its own binary: it cancels the process-wide shutdown
//! token, which would leak into every other test of the same process.

#![allow(clippy::unwrap_used)] // tests/examples/benches may panic
use std::time::{Duration, Instant};

use ice_rpc::{CancellationToken, RpcError};

/// Channel nobody serves: the call stays in the provider-wait loop.
const ABSENT_CHANNEL: &str = "IceRpcAbsentProvider";

#[test]
fn a_call_waiting_for_an_absent_provider_aborts_on_shutdown() {
    let cancel: &'static CancellationToken = ice_rpc::gen::global_cancel_token();
    let watcher = cancel.clone();
    std::thread::spawn(move || {
        // Long enough for the call to be well inside the wait loop, short enough
        // that a regression fails the deadline below instead of hanging the job.
        std::thread::sleep(Duration::from_millis(250));
        watcher.cancel();
    });

    let started = Instant::now();
    let result =
        ice_rpc::gen::native_call::<i32, String>(ABSENT_CHANNEL, 0xDEAD_BEEF, "ping", b"go");
    let elapsed = started.elapsed();

    // `Observable` is not `Debug`, so report the outcome explicitly.
    let outcome = match &result {
        Ok(_) => "Ok(stream)".to_string(),
        Err(error) => format!("Err({error})"),
    };
    assert!(
        matches!(result, Err(RpcError::Cancelled)),
        "the wait must report a cancellation, got {outcome}"
    );
    assert!(
        elapsed < Duration::from_secs(10),
        "the provider wait ignored the shutdown for {elapsed:?} (deadline is 30s)"
    );
}
