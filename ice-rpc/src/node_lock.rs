//! Node crash monitoring through a cross-platform kernel mutex.
//!
//! # Principle
//!
//! The provider creates a named Mutex and keeps it held in a dedicated
//! thread. On process crash, the kernel kills all threads → the mutex
//! is abandoned → the client detects the absence of the mutex → crash confirmed.
//!
//! | OS      | Provider                      | Watcher                       |
//! |---------|-------------------------------|-------------------------------|
//! | Windows | `CreateMutexA` + dedicated thread | `OpenMutexA` existence check  |
//! | Linux   | `open` + `flock(LOCK_EX\|NB)` | `open` + `flock(LOCK_EX\|NB)` |
//! | macOS   | `open` + `flock(LOCK_EX\|NB)` | `open` + `flock(LOCK_EX\|NB)` |

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::reconnect::fire as fire_reconnect_callbacks;
use crate::types::NodeId;

/// Polling interval of the watcher (ms).
pub const LOCK_WATCHER_POLL_MS: u64 = 100;

/// Prefix of the mutex/lock file name.
pub(crate) const LOCK_NAME_PREFIX: &str = "ice_rpc_node_";

#[cfg(windows)]
mod platform {
    use std::ffi::CString;
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_ALREADY_EXISTS, HANDLE, WAIT_OBJECT_0,
    };
    use windows_sys::Win32::System::Threading::{CreateMutexA, WaitForSingleObject, INFINITE};
    use windows_sys::Win32::System::WindowsProgramming::OpenMutexA;

    const SYNCHRONIZE: u32 = 0x00100000;

    /// Creates a Win32 Mutex and keeps it held in a dedicated thread.
    pub fn acquire(name: &str) -> Result<(), String> {
        let (tx, rx) = std::sync::mpsc::channel::<Result<(), String>>();
        let name_owned = name.to_string();

        std::thread::Builder::new()
            .name(format!("node-lock-{}", name_owned))
            .spawn(move || {
                let cname = match CString::new(name_owned.as_str()) {
                    Ok(s) => s,
                    Err(e) => {
                        let _ = tx.send(Err(format!("CString: {}", e)));
                        return;
                    }
                };

                let handle: HANDLE =
                    unsafe { CreateMutexA(std::ptr::null(), 0, cname.as_ptr() as *const u8) };
                if handle.is_null() {
                    let _ = tx.send(Err(format!(
                        "[NodeLock] CreateMutexA('{}') failed",
                        name_owned
                    )));
                    return;
                }

                let last_err = unsafe { windows_sys::Win32::Foundation::GetLastError() };
                if last_err == ERROR_ALREADY_EXISTS {
                    unsafe {
                        CloseHandle(handle);
                    }
                    let _ = tx.send(Err(format!(
                        "[NodeLock] Mutex '{}' already created by another process",
                        name_owned
                    )));
                    return;
                }

                let rc = unsafe { WaitForSingleObject(handle, INFINITE) };
                if rc != WAIT_OBJECT_0 {
                    unsafe {
                        CloseHandle(handle);
                    }
                    let _ = tx.send(Err(format!(
                        "[NodeLock] WaitForSingleObject failed rc={}",
                        rc
                    )));
                    return;
                }

                let _ = tx.send(Ok(()));
                loop {
                    std::thread::sleep(std::time::Duration::from_secs(3600));
                }
            })
            .map_err(|e| format!("[NodeLock] spawn thread failed: {}", e))?;

        rx.recv()
            .map_err(|e| format!("[NodeLock] channel recv failed: {}", e))?
    }

    /// Checks whether the provider is still alive through the Win32 mutex.
    pub fn is_alive(name: &str) -> bool {
        let cname = match CString::new(name) {
            Ok(s) => s,
            Err(_) => return false,
        };
        let handle: HANDLE = unsafe { OpenMutexA(SYNCHRONIZE, 0, cname.as_ptr() as *const u8) };
        if handle.is_null() {
            return false;
        }
        unsafe {
            CloseHandle(handle);
        }
        true
    }

    /// Releases the mutex (called during a clean shutdown).
    pub fn release(_name: &str) {
        // The mutex is released automatically by the kernel at process end.
        // For a clean shutdown, the dedicated thread cannot be easily killed.
        // We let the kernel clean up.
    }
}

#[cfg(unix)]
mod platform {
    use std::fs::OpenOptions;
    use std::os::unix::io::IntoRawFd;

    /// Builds the lock file path.
    fn lock_path(name: &str) -> String {
        format!("/tmp/{}.lock", name)
    }

    /// Creates the lock file and acquires an exclusive flock.
    pub fn acquire(name: &str) -> Result<(), String> {
        let path = lock_path(name);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .map_err(|e| format!("[NodeLock] open('{}') failed: {}", path, e))?;

        let fd = file.into_raw_fd();
        let rc = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
        if rc == 0 {
            Ok(())
        } else {
            unsafe {
                libc::close(fd);
            }
            Err(format!("[NodeLock] flock('{}') failed: already held", path))
        }
    }

    /// Checks whether the flock is still held (provider alive).
    pub fn is_alive(name: &str) -> bool {
        let path = lock_path(name);
        let file = match OpenOptions::new().write(true).open(&path) {
            Ok(f) => f,
            Err(_) => return false,
        };
        let fd = file.into_raw_fd();
        let rc = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
        if rc == 0 {
            unsafe {
                libc::flock(fd, libc::LOCK_UN);
                libc::close(fd);
            }
            false
        } else {
            unsafe {
                libc::close(fd);
            }
            true
        }
    }

    /// Releases the flock (called during a clean shutdown).
    pub fn release(name: &str) {
        let path = lock_path(name);
        // Removes the lock file to release immediately.
        let _ = std::fs::remove_file(&path);
    }
}

static GLOBAL_LOCK_NAME: OnceLock<String> = OnceLock::new();

/// Acquires the kernel lock for this node.
///
/// Idempotent: if already acquired, returns the existing name.
///
/// # Returns
/// * `Ok(String)` — Name of the lock.
/// * `Err(String)` — Acquisition failure.
pub fn acquire_global_node_lock(node_id: NodeId) -> Result<String, String> {
    if let Some(name) = GLOBAL_LOCK_NAME.get() {
        return Ok(name.clone());
    }
    let lock_name = format!("{}{}", LOCK_NAME_PREFIX, node_id.0);
    platform::acquire(&lock_name)?;
    let _ = GLOBAL_LOCK_NAME.set(lock_name.clone());
    Ok(lock_name)
}

/// Explicitly releases the kernel lock.
///
/// Called during a clean shutdown so that `is_node_alive()`
/// returns `false` immediately (without waiting for process end).
pub fn release_global_node_lock() {
    if let Some(name) = GLOBAL_LOCK_NAME.get() {
        platform::release(name);
    }
}

/// Checks whether a provider node is still alive through its kernel lock.
pub fn is_node_alive(lock_name: &str) -> bool {
    platform::is_alive(lock_name)
}

/// Common monitoring loop (Tokio and std::thread).
fn watcher_loop(
    node_id: NodeId,
    lock_name: &str,
    running: &Arc<AtomicBool>,
    cancel: &crate::rt::CancellationToken,
) {
    let poll_interval = Duration::from_millis(LOCK_WATCHER_POLL_MS);

    loop {
        if cancel.is_cancelled() {
            break;
        }
        if !running.load(Ordering::Relaxed) {
            break;
        }

        if !is_node_alive(lock_name) {
            log::warn!("[NodeLockWatcher] CRASH DETECTED for Node {}", node_id);
            crate::locator::ServiceLocator::global()
                .node_discovery()
                .invalidate_node_services(node_id);
            fire_reconnect_callbacks(node_id.0);
            running.store(false, Ordering::Relaxed);
            break;
        }

        std::thread::sleep(poll_interval);
    }
}

/// Crash watcher for a remote provider node.
pub struct NodeLockWatcher {
    node_id: NodeId,
    lock_name: String,
    running: Arc<AtomicBool>,
}

impl NodeLockWatcher {
    /// Launches a watcher in a blocking thread (runtime-agnostic).
    pub fn spawn(node_id: NodeId, lock_name: String) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();
        let lock_name_clone = lock_name.clone();
        let cancel = crate::global_cancel_token().clone();

        let handle = crate::rt::spawn_blocking(move || {
            watcher_loop(node_id, &lock_name_clone, &running_clone, &cancel);
        });

        crate::locator::ServiceLocator::global().register_shutdown_handle(handle);
        Self {
            node_id,
            lock_name,
            running,
        }
    }

    /// Launches a watcher in a std thread (outside a Tokio runtime).
    pub fn spawn_std_thread(node_id: NodeId, lock_name: String) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();
        let lock_name_clone = lock_name.clone();
        let cancel = crate::global_cancel_token().clone();

        std::thread::spawn(move || {
            watcher_loop(node_id, &lock_name_clone, &running_clone, &cancel);
        });

        Self {
            node_id,
            lock_name,
            running,
        }
    }

    /// Stops the watcher.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Returns `true` if the watcher is active.
    #[inline]
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Returns the name of the watched lock.
    #[inline]
    pub fn lock_name(&self) -> &str {
        &self.lock_name
    }

    /// Returns the id of the watched node.
    #[inline]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
}

impl Drop for NodeLockWatcher {
    fn drop(&mut self) {
        self.stop();
    }
}

fn watcher_registry() -> &'static std::sync::Mutex<std::collections::HashMap<u32, NodeLockWatcher>>
{
    static REGISTRY: OnceLock<std::sync::Mutex<std::collections::HashMap<u32, NodeLockWatcher>>> =
        OnceLock::new();
    REGISTRY.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Registers a lock watcher for a remote node.
pub fn register_node_lock_watcher(node_id: NodeId, lock_name: String) {
    if node_id == NodeId::current() {
        return;
    }
    let mut registry = match watcher_registry().lock() {
        Ok(r) => r,
        Err(_) => return,
    };
    if let Some(existing) = registry.get(&node_id.0) {
        if existing.is_running() {
            return;
        }
    }
    // The runtime-agnostic blocking facade works with or without an active
    // async runtime, so a single path is sufficient.
    let watcher = NodeLockWatcher::spawn(node_id, lock_name);
    registry.insert(node_id.0, watcher);
}

/// Removes the watcher of a node.
pub fn unregister_node_lock_watcher(node_id: NodeId) {
    if let Ok(mut registry) = watcher_registry().lock() {
        if let Some(watcher) = registry.remove(&node_id.0) {
            watcher.stop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_name_format() {
        assert_eq!(
            format!("{}{}", LOCK_NAME_PREFIX, 12345u32),
            "ice_rpc_node_12345"
        );
    }

    #[test]
    fn is_alive_unknown_lock_returns_false() {
        assert!(!is_node_alive("ice_rpc_node_99999999_unknown"));
    }

    #[test]
    fn acquire_then_detected_alive() {
        // Use a unique lock name (PID + monotonic counter) so this test is
        // isolated from the global lock name shared by other tests running in
        // parallel. Another test may call release_global_node_lock() and remove
        // the lock file, which would otherwise race with this assertion.
        static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let unique = std::process::id() ^ (COUNTER.fetch_add(1, Ordering::SeqCst) << 16);
        let node_id = NodeId(unique);
        let lock_name = format!("{}{}", LOCK_NAME_PREFIX, node_id.0);

        platform::acquire(&lock_name)
            .unwrap_or_else(|e| panic!("failed to acquire node lock: {}", e));

        // Bounded wait: the kernel lock must become observable shortly after
        // acquisition. Retrying avoids flakiness on slow CI runners.
        let alive = (0..10).any(|_| {
            if is_node_alive(&lock_name) {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
            false
        });
        assert!(
            alive,
            "The node must be detected alive (lock '{}')",
            lock_name
        );

        platform::release(&lock_name);

        // On Unix, release() removes the lock file, so the node must become
        // undetectable. On Windows the kernel releases the mutex only at process
        // end (the holding thread keeps it), so this check is Unix-only.
        if cfg!(unix) {
            assert!(
                !is_node_alive(&lock_name),
                "The node must be detected dead after release (lock '{}')",
                lock_name
            );
        }
    }

    #[test]
    fn watcher_stop_flag() {
        let running = Arc::new(AtomicBool::new(true));
        assert!(running.load(Ordering::Relaxed));
        running.store(false, Ordering::Relaxed);
        assert!(!running.load(Ordering::Relaxed));
    }

    #[test]
    fn watcher_detects_missing_lock_and_stops() {
        let node_id = NodeId(0x0D1E_0002);
        let lock_name = format!("{}{}", LOCK_NAME_PREFIX, node_id.0);
        let watcher = NodeLockWatcher::spawn_std_thread(node_id, lock_name);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while watcher.is_running() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        assert!(
            !watcher.is_running(),
            "watcher must detect the missing lock and stop"
        );
    }
}
