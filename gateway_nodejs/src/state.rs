//! Lifecycle state of the gateway.
//!
//! Replaces the two `OnceLock`s the N-API surface used to rely on
//! (`SHUTDOWN_GUARD` and the bridge singleton). A `OnceLock` can be filled
//! once and never emptied, so `init` after `shutdown` was impossible and every
//! illegal transition had to be swallowed into a `false` return value. Here one
//! cell owns the phase, the bridge and the shutdown guard, which makes the
//! transitions of [`Phase`] explicit and every illegal move reportable.
//!
//! ```text
//! Idle --registerService--> Configured --init--> Running --shutdown--> Idle
//! Idle -------------------init-----------------> Running
//! ```
//!
//! `init` reserves the transition with [`begin_start`] *before* creating the
//! shutdown guard. Dropping a `ShutdownGuard` cancels the ice-rpc tokens, so a
//! refused `init` must never build one: reservation is what keeps a rejected
//! double `init` from killing an already running gateway.

use crate::error::{GatewayError, GatewayErrorCode};
use crate::nodejs_bridge::NodeJsBridge;
use std::sync::{Arc, Mutex, MutexGuard};

/// Where the gateway stands in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Nothing done yet, or a previous `shutdown` completed.
    Idle,
    /// At least one provider registered; `init` has not run.
    Configured,
    /// `init` ran: the bridge is live and the shutdown guard is held.
    Running,
}

struct Inner {
    phase: Phase,
    bridge: Option<Arc<NodeJsBridge>>,
    guard: Option<ice_rpc::gen::ShutdownGuard>,
}

static STATE: Mutex<Inner> = Mutex::new(Inner {
    phase: Phase::Idle,
    bridge: None,
    guard: None,
});

/// Locks the state, recovering from a poisoned mutex instead of panicking.
///
/// The workspace builds with `panic = "abort"`, where a poisoned lock would
/// otherwise take the whole process down; recovering keeps the gateway
/// observable, which is what a diagnostics surface must be after a failure.
fn lock() -> MutexGuard<'static, Inner> {
    STATE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// The current lifecycle phase.
pub fn phase() -> Phase {
    lock().phase
}

/// Records a provider registration, refusing it once the gateway runs.
pub fn mark_configured() -> Result<(), GatewayError> {
    let mut inner = lock();
    match inner.phase {
        Phase::Running => Err(GatewayError::new(
            GatewayErrorCode::GatewayState,
            "registerService() must be called before init()",
        )),
        _ => {
            inner.phase = Phase::Configured;
            Ok(())
        }
    }
}

/// Reserves the transition to [`Phase::Running`].
///
/// # Errors
/// `E_GATEWAY_STATE` when the gateway already runs.
pub fn begin_start() -> Result<(), GatewayError> {
    let mut inner = lock();
    if inner.phase == Phase::Running {
        return Err(GatewayError::new(
            GatewayErrorCode::GatewayState,
            "init() has already been called; call shutdown() before initializing again",
        ));
    }
    inner.phase = Phase::Running;
    Ok(())
}

/// Hands the resources to a reserved start. Call only after [`begin_start`].
pub fn complete_start(bridge: Arc<NodeJsBridge>, guard: ice_rpc::gen::ShutdownGuard) {
    let mut inner = lock();
    inner.bridge = Some(bridge);
    inner.guard = Some(guard);
}

/// The bridge of a running gateway.
///
/// # Errors
/// `E_GATEWAY_STATE` when `init` has not run (or `shutdown` already did).
pub fn bridge() -> Result<Arc<NodeJsBridge>, GatewayError> {
    lock().bridge.clone().ok_or_else(|| {
        GatewayError::new(
            GatewayErrorCode::GatewayState,
            "the gateway is not initialized; call init() first",
        )
    })
}

/// Stops the gateway and returns to [`Phase::Idle`].
///
/// The guard is *taken out* rather than dropped under the lock: the caller
/// decides when the cancellation happens, so the lock is never held across an
/// await point and a failed shutdown can still be retried.
pub fn stop() -> Option<ice_rpc::gen::ShutdownGuard> {
    let mut inner = lock();
    let guard = inner.guard.take();
    inner.bridge = None;
    inner.phase = Phase::Idle;
    guard
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The whole lifecycle in one test on purpose.
    ///
    /// The state is process-global, so splitting it across tests would let
    /// them interleave and observe each other's transitions.
    #[test]
    fn the_lifecycle_is_a_single_ordered_sequence() {
        // Before `init`: no bridge, and that is reported, not panicked.
        match bridge() {
            Err(error) => assert_eq!(error.code(), GatewayErrorCode::GatewayState),
            Ok(_) => panic!("a bridge must not exist before init"),
        }
        assert_eq!(phase(), Phase::Idle);

        // A registration only moves to `Configured`, and is idempotent.
        mark_configured().expect("idle accepts a registration");
        assert_eq!(phase(), Phase::Configured);
        mark_configured().expect("configured still accepts a registration");

        // Reserving the start moves to `Running`; a second reservation fails,
        // which is what makes a double `init` reportable.
        begin_start().expect("configured can start");
        assert_eq!(phase(), Phase::Running);
        let error = begin_start().expect_err("a second init is refused");
        assert_eq!(error.code(), GatewayErrorCode::GatewayState);

        // A registration after `init` is refused too.
        let error = mark_configured().expect_err("no registration while running");
        assert_eq!(error.code(), GatewayErrorCode::GatewayState);

        // `stop` is the only way back, and it reports there was no guard.
        assert!(stop().is_none());
        assert_eq!(phase(), Phase::Idle);
    }
}
