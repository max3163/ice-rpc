//! Process-wide state: a single declaration site and a single locking policy.
//!
//! Every piece of state that lives for the whole process is declared in this
//! module and listed in the inventory below. Before it existed, each piece came
//! with its own private accessor (`fn response_handlers() -> &'static
//! Mutex<HashMap<..>>`) spread over half a dozen modules, which made both the
//! lifetime rules and the poisoning policy invisible from the call sites.
//!
//! # Inventory
//!
//! | State | Declaration | Key | Written by | Read by | Released |
//! |---|---|---|---|---|---|
//! | Cancel token of the IPC threads | `lib.rs`, [`Global`] | — | unchanged after creation | every dispatch loop, `wait_for_shutdown` | never (process lifetime) |
//! | Registry cancel token | `lib.rs`, [`Global`] | — | unchanged after creation | `publish_until_delivered` | never |
//! | iceoryx2 bootstrap (`Once`) | `lib.rs`, [`Global`] | — | first `init()` | every `init()` | never |
//! | Signal-handling flag | `lib.rs`, `AtomicBool` | — | `init` / `init_without_ctrl_c` | `waitset_signal_handling_mode` | not applicable |
//! | Shared iceoryx2 node | `transport::shared_node`, [`Global`] | — | first port creation | every port creation, observers | never |
//! | In-flight response handlers | `transport::client`, [`Registry`] | correlation id | `native_call`, before publishing | response dispatch thread | on the terminal event of the call |
//! | Consumer ports per channel | `transport::client`, [`Registry`] | channel name | `consumer_ports` | `native_call` | never (one set per channel) |
//! | Pending channels | `transport::server`, [`Registry`] | channel name | `register_native_service` | `start_registered_channels` | drained at the end of initialization |
//! | Channels sealed flag | `transport::server`, `AtomicBool` | — | `start_registered_channels` | `register_native_service` | not applicable |
//! | Watched PIDs (liveness) | `node_liveness`, [`Registry`] | pid | `register_node_liveness_watcher` | the shared poller | `unregister_…` or on confirmed death |
//! | Liveness poll interval | `node_liveness`, [`Global`] | — | first read of the env var | every poller tick | never |
//! | Liveness poller started | `node_liveness`, [`Global`] | — | first watcher registration | every registration | never |
//! | IPC cleanup resources | `shutdown`, [`Locked`] | — | `register_ipc_cleanup` | `clear_ipc_cleanup` | at shutdown |
//! | Service locator | `locator`, [`Global`] | — | first `locator()` call | everywhere | never |
//! | Node.js dispatch pointer | `nodejs_dispatch`, [`Global`] | — | gateway startup | ProviderNodeJs dispatch | never |
//! | Notification clock origin | `transport::notify`, [`Global`] | — | first notification | every coalescing check | never |
//!
//! Three raw styles remain on purpose, because they carry no locking policy:
//! `AtomicBool`/`AtomicU64` for flags and counters, [`Global`] for values that
//! are never mutated after creation, and [`Locked`] only where a collection is
//! really shared between threads.
//!
//! # Poisoning
//!
//! A poisoned lock is recovered by [`crate::sync::lock`], the single
//! implementation of the policy: the guarded values are plain collections
//! updated through short critical sections, so the original panic is what
//! matters, not a second one raised while unwrapping.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Mutex, OnceLock};

use crate::sync::lock;

/// A value initialized once and read for the rest of the process lifetime.
///
/// Use it for singletons that are never mutated after their creation, where a
/// mutex would only add an uncontended lock on every access.
pub struct Global<T>(OnceLock<T>);

impl<T> Global<T> {
    /// Creates an empty cell; the value is built by the first `get_or_init`.
    pub const fn new() -> Self {
        Self(OnceLock::new())
    }

    /// Returns the value, or `None` while it has not been initialized.
    pub fn get(&self) -> Option<&T> {
        self.0.get()
    }

    /// Returns the value, building it with `init` on the first call.
    ///
    /// `init` runs at most once, even under a race; the losers of the race get
    /// the winner's value.
    pub fn get_or_init(&self, init: impl FnOnce() -> T) -> &T {
        self.0.get_or_init(init)
    }
}

impl<T> Default for Global<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// A `T` shared between threads, created on first use, guarded by one mutex.
///
/// The single entry point is [`Locked::with`], so the lock can never be held
/// beyond the critical section and the poisoning policy stays in one place.
pub struct Locked<T>(OnceLock<Mutex<T>>);

impl<T: Default> Locked<T> {
    /// Creates the cell; the value is built by the first `with` call.
    pub const fn new() -> Self {
        Self(OnceLock::new())
    }

    /// Runs `f` on the protected value, creating it on first use.
    pub fn with<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        let mutex = self.0.get_or_init(|| Mutex::new(T::default()));
        let mut guard = lock(mutex);
        f(&mut guard)
    }
}

impl<T: Default> Default for Locked<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// A process-wide map, the shared counterpart of [`Locked`] for keyed values.
///
/// Each method takes the lock for the shortest time possible; the longer
/// critical sections (creating a channel's ports, for instance) go through
/// [`Registry::with`].
pub struct Registry<K, V>(Locked<HashMap<K, V>>);

impl<K: Eq + Hash, V> Registry<K, V> {
    /// Creates an empty registry.
    pub const fn new() -> Self {
        Self(Locked::new())
    }

    /// Inserts `value` under `key`, returning the value it replaced.
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.0.with(|map| map.insert(key, value))
    }

    /// Returns a copy of the value stored under `key`.
    pub fn get_cloned(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        self.0.with(|map| map.get(key).cloned())
    }

    /// Removes and returns the value stored under `key`.
    pub fn remove(&self, key: &K) -> Option<V> {
        self.0.with(|map| map.remove(key))
    }

    /// Empties the registry and returns its entries.
    ///
    /// The entries are collected *inside* the critical section and returned, so
    /// the caller walks them without holding the lock.
    pub fn drain(&self) -> Vec<(K, V)> {
        self.0.with(|map| map.drain().collect())
    }

    /// Runs `f` on the map under the lock, for the critical sections that must
    /// stay atomic (a lookup followed by an insert, for instance).
    pub fn with<R>(&self, f: impl FnOnce(&mut HashMap<K, V>) -> R) -> R {
        self.0.with(f)
    }
}

impl<K: Eq + Hash, V> Default for Registry<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_is_initialized_once() {
        static CELL: Global<String> = Global::new();
        assert_eq!(CELL.get(), None);

        let first = CELL.get_or_init(|| String::from("first"));
        assert_eq!(first, "first");

        // The second initializer must not run.
        let second = CELL.get_or_init(|| String::from("second"));
        assert_eq!(second, "first");
    }

    #[test]
    fn registry_insert_remove_and_drain() {
        let registry: Registry<&'static str, u32> = Registry::new();

        registry.insert("a", 1);
        registry.insert("b", 2);
        assert_eq!(registry.with(|map| map.len()), 2);
        assert_eq!(registry.get_cloned(&"a"), Some(1));

        assert_eq!(registry.remove(&"a"), Some(1));
        assert_eq!(registry.get_cloned(&"a"), None);

        let mut keys: Vec<&str> = registry.with(|map| map.keys().copied().collect());
        keys.sort_unstable();
        assert_eq!(keys, vec!["b"]);

        assert_eq!(registry.drain(), vec![("b", 2)]);
        assert!(registry.with(|map| map.is_empty()));
    }

    #[test]
    fn registry_recovers_from_a_poisoned_lock() {
        let registry: Registry<u32, u32> = Registry::new();
        registry.insert(1, 1);

        // Poison the lock from a panicking critical section.
        let _ = std::panic::catch_unwind(|| {
            registry.with(|map| {
                map.insert(2, 2);
                panic!("poison the lock");
            });
        });

        // The value written before the panic is still there and still usable.
        assert_eq!(registry.get_cloned(&2), Some(2));
        registry.insert(3, 3);
        assert_eq!(registry.with(|map| map.len()), 3);
    }

    #[test]
    fn locked_creates_its_value_on_first_use() {
        static LIST: Locked<Vec<u32>> = Locked::new();
        LIST.with(|values| values.push(1));
        LIST.with(|values| values.push(2));
        assert_eq!(LIST.with(|values| values.clone()), vec![1, 2]);
    }
}
