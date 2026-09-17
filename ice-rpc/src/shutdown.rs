//! Registry of blocking IPC threads for an orderly shutdown.
//!
//! All IPC threads (dispatch loop, NODE_REGISTRY listener, monitoring,
//! etc.) register their handle here. During shutdown,
//! [`ShutdownRegistry::join_all`] waits for all threads to finish before
//! dropping the iceoryx2 node.

use std::any::Any;
use std::sync::Mutex;

use crate::global::Locked;

/// Registry of blocking IPC thread handles.
///
/// Allows explicitly waiting for all IPC threads to finish before
/// releasing the iceoryx2 resources.
pub(crate) struct ShutdownRegistry {
    handles: Mutex<Vec<crate::rt::BlockingHandle>>,
}

impl ShutdownRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self {
            handles: Mutex::new(Vec::new()),
        }
    }

    /// Registers the handle of a blocking IPC thread.
    pub fn register(&self, handle: crate::rt::BlockingHandle) {
        crate::sync::lock(&self.handles).push(handle);
    }

    /// Waits for all registered threads to finish (async), then clears the registry.
    ///
    /// # Returns
    /// Number of threads awaited.
    pub async fn join_all(&self) -> usize {
        // The guard is dropped before the handles are awaited.
        let handles = {
            let mut guard = crate::sync::lock(&self.handles);
            std::mem::take(&mut *guard)
        };

        let count = handles.len();
        if count > 0 {
            log::info!(
                "[ShutdownRegistry] Waiting for {} IPC thread(s) to finish...",
                count
            );
            for handle in handles {
                handle.await;
            }
            log::info!("[ShutdownRegistry] All IPC threads terminated.");
        }
        count
    }
}

// ---------------------------------------------------------------------------
// Process-lifetime IPC resources
// ---------------------------------------------------------------------------

static IPC_CLEANUP_RESOURCES: Locked<Vec<Box<dyn Any + Send>>> = Locked::new();

/// Registers an iceoryx2 resource (port, writer, notifier, ...) that must be
/// dropped during shutdown, so that iceoryx2 cleans up its backing files.
pub fn register_ipc_cleanup(resource: Box<dyn Any + Send>) {
    IPC_CLEANUP_RESOURCES.with(|resources| resources.push(resource));
}

/// Drops all resources registered via [`register_ipc_cleanup`].
pub fn clear_ipc_cleanup() {
    let count = IPC_CLEANUP_RESOURCES.with(|resources| {
        let count = resources.len();
        resources.clear();
        count
    });

    if count > 0 {
        log::info!("[ice-rpc] dropped {count} registered IPC resource(s).");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_registry_join_returns_zero() {
        let registry = ShutdownRegistry::new();
        let count = pollster::block_on(registry.join_all());
        assert_eq!(count, 0);
    }

    #[test]
    fn register_and_join_single_handle() {
        let registry = ShutdownRegistry::new();
        let handle = crate::rt::spawn_blocking(|| {});
        registry.register(handle);
        let count = pollster::block_on(registry.join_all());
        assert_eq!(count, 1);
    }

    #[test]
    fn register_and_join_multiple_handles() {
        let registry = ShutdownRegistry::new();
        for _ in 0..3 {
            let handle = crate::rt::spawn_blocking(|| {});
            registry.register(handle);
        }
        let count = pollster::block_on(registry.join_all());
        assert_eq!(count, 3);
    }

    #[test]
    fn join_all_clears_registry() {
        let registry = ShutdownRegistry::new();
        let handle = crate::rt::spawn_blocking(|| {});
        registry.register(handle);
        pollster::block_on(registry.join_all());

        // A second join must return 0 (the registry was cleared).
        let count = pollster::block_on(registry.join_all());
        assert_eq!(count, 0);
    }

    #[test]
    fn handles_terminate_before_join_completes() {
        let registry = ShutdownRegistry::new();
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = crate::rt::spawn_blocking(move || {
            let _ = tx.send(());
        });
        registry.register(handle);
        // Wait for the thread to finish
        rx.recv().unwrap();
        let count = pollster::block_on(registry.join_all());
        assert_eq!(count, 1);
    }
}
