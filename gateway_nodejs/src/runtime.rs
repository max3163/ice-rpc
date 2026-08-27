//! Runtime facade for the Node.js gateway.
//!
//! Delegates to [`ice_rpc::rt`] (runtime-agnostic). The gateway no longer
//! embeds a dedicated Tokio runtime: task spawning and blocking execution
//! are handled by the ice-rpc core facade.

/// Initializes the runtime facade.
///
/// Always succeeds: the agnostic executor starts lazily and needs no
/// explicit initialization. Kept for API compatibility with the call sites.
pub fn init_runtime() -> bool {
    true
}

/// Executes a future to completion on the current thread.
///
/// # Warning
/// Blocks the calling thread. Prefer [`spawn_task`] when possible.
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    ice_rpc::rt::block_on(future)
}

/// Spawns an async task on the runtime-agnostic global executor.
///
/// # Returns
/// `Ok(())` — the agnostic executor always accepts the task.
pub fn spawn_task<F>(future: F) -> Result<(), String>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    ice_rpc::rt::spawn(future);
    Ok(())
}

/// No-op shutdown.
///
/// The agnostic executor has no dedicated runtime to stop; the ice-rpc core
/// releases its IPC resources through [`ice_rpc::shutdown_and_release`].
pub fn shutdown_runtime() {}
