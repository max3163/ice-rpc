//! Registry of blocking IPC threads for an orderly shutdown.
//!
//! All IPC threads (dispatch loop, NODE_REGISTRY listener, monitoring,
//! etc.) register their handle here. During shutdown,
//! [`ShutdownRegistry::join_all`] waits for all threads to finish before
//! dropping the iceoryx2 node.

use std::sync::Mutex;

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
        if let Ok(mut handles) = self.handles.lock() {
            handles.push(handle);
        }
    }

    /// Waits for all registered threads to finish (async), then clears the registry.
    ///
    /// # Returns
    /// Number of threads awaited.
    pub async fn join_all(&self) -> usize {
        let handles = {
            let mut guard = match self.handles.lock() {
                Ok(g) => g,
                Err(_) => return 0,
            };
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
