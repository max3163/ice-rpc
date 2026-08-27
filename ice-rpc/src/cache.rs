//! Local cache with TTL for RPC responses.
//!
//! Provides a thread-safe hash table associating a hash key
//! (derived from the RPC method arguments) with a value with a lifetime.
//! Cleanup is performed lazily during lookups.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Cache entry: a value with its expiration date.
#[derive(Clone)]
struct CacheEntry<V> {
    value: V,
    expires_at: Instant,
}

impl<V> CacheEntry<V> {
    fn is_expired(&self, now: Instant) -> bool {
        now >= self.expires_at
    }
}

/// Thread-safe local cache with TTL and lazy cleanup.
///
/// # Generic parameters
/// * `V` — Type of the stored value (e.g. `Vec<u8>` containing the rkyv bytes).
pub struct RpcCache<V> {
    inner: Mutex<HashMap<u64, CacheEntry<V>>>,
    ttl: Duration,
    /// Maximum number of entries before eviction of the oldest ones.
    max_entries: usize,
}

impl<V: Clone> RpcCache<V> {
    /// Creates a new cache with the specified TTL.
    ///
    /// # Arguments
    /// * `ttl` — Lifetime of the entries.
    pub fn new(ttl: Duration) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            ttl,
            max_entries: 1024,
        }
    }

    /// Creates a new cache with a TTL and a maximum number of entries.
    pub fn with_max_entries(ttl: Duration, max_entries: usize) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            ttl,
            max_entries,
        }
    }

    /// Inserts a value into the cache for the given key.
    ///
    /// If the cache is full, expired entries are cleaned up first.
    /// If still full, the oldest entries are removed.
    pub fn insert(&self, key: u64, value: V) {
        let mut guard = self.inner.lock().expect("RpcCache lock poisoning");
        let now = Instant::now();

        // Lazy cleanup: remove expired entries.
        guard.retain(|_, entry| !entry.is_expired(now));

        // If the cache is full, remove the oldest entries.
        while guard.len() >= self.max_entries {
            // Remove an arbitrary entry (the first in the iterator).
            if let Some(oldest_key) = guard.keys().next().copied() {
                guard.remove(&oldest_key);
            } else {
                break;
            }
        }

        guard.insert(
            key,
            CacheEntry {
                value,
                expires_at: now + self.ttl,
            },
        );
    }

    /// Looks up a value in the cache.
    ///
    /// # Returns
    /// * `Some(V)` if the key exists and is not expired.
    /// * `None` if absent or expired.
    pub fn get(&self, key: u64) -> Option<V> {
        let mut guard = self.inner.lock().expect("RpcCache lock poisoning");
        let now = Instant::now();

        match guard.get(&key) {
            Some(entry) if !entry.is_expired(now) => Some(entry.value.clone()),
            _ => {
                // Remove the expired entry if it exists.
                guard.remove(&key);
                None
            }
        }
    }

    /// Removes all expired entries.
    pub fn purge_expired(&self) {
        let mut guard = self.inner.lock().expect("RpcCache lock poisoning");
        let now = Instant::now();
        guard.retain(|_, entry| !entry.is_expired(now));
    }

    /// Completely empties the cache.
    pub fn clear(&self) {
        self.inner.lock().expect("RpcCache lock poisoning").clear();
    }

    /// Returns the number of entries currently in the cache.
    pub fn len(&self) -> usize {
        self.inner.lock().expect("RpcCache lock poisoning").len()
    }

    /// Returns `true` if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Computes a u64 hash for a byte slice.
///
/// Used to create the cache key from the serialized arguments (rkyv).
/// The hash is NOT cryptographic — it is designed to be fast.
#[inline]
pub fn hash_bytes(bytes: &[u8]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

/// Computes a u64 hash for a hashable value.
#[inline]
pub fn hash_key<K: Hash>(key: &K) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn insert_and_get() {
        let cache = RpcCache::new(Duration::from_secs(60));
        cache.insert(42, "hello".to_string());
        assert_eq!(cache.get(42), Some("hello".to_string()));
    }

    #[test]
    fn miss_on_unknown_key() {
        let cache: RpcCache<String> = RpcCache::new(Duration::from_secs(60));
        assert_eq!(cache.get(999), None);
    }

    #[test]
    fn miss_on_expired() {
        let cache = RpcCache::new(Duration::from_millis(1));
        cache.insert(1, "expired_value".to_string());
        std::thread::sleep(Duration::from_millis(10));
        assert_eq!(cache.get(1), None);
    }

    #[test]
    fn clear_removes_all() {
        let cache = RpcCache::new(Duration::from_secs(60));
        cache.insert(1, "a".to_string());
        cache.insert(2, "b".to_string());
        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn len_and_is_empty() {
        let cache = RpcCache::new(Duration::from_secs(60));
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        cache.insert(1, "x".to_string());
        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn hash_bytes_is_deterministic() {
        let h1 = hash_bytes(b"hello");
        let h2 = hash_bytes(b"hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_bytes_different_for_different_inputs() {
        let h1 = hash_bytes(b"hello");
        let h2 = hash_bytes(b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn max_entries_eviction() {
        let cache = RpcCache::with_max_entries(Duration::from_secs(60), 2);
        cache.insert(1, "a".to_string());
        cache.insert(2, "b".to_string());
        cache.insert(3, "c".to_string()); // should evict one entry
        let count = cache.len();
        assert!(count <= 2, "The cache must not exceed max_entries");
    }

    #[test]
    fn purge_expired_cleans_up() {
        let cache = RpcCache::new(Duration::from_millis(1));
        cache.insert(1, "x".to_string());
        cache.insert(2, "y".to_string());
        std::thread::sleep(Duration::from_millis(10));
        cache.purge_expired();
        assert!(cache.is_empty());
    }
}
