//! Locking helpers with a single, documented poisoning policy.
//!
//! A poisoned lock is recovered with [`std::sync::PoisonError::into_inner`] so
//! the protected data stays accessible: the guarded values are plain collections
//! updated through short critical sections, and the original panic is what
//! matters, not a second one.

use std::sync::{Mutex, MutexGuard, PoisonError};

/// Acquires a `Mutex`, recovering the guard if the lock was poisoned.
#[inline]
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutex_guard_is_recovered_after_poisoning() {
        let mutex = Mutex::new(1);
        let _ = std::panic::catch_unwind(|| {
            let mut guard = mutex.lock().unwrap();
            *guard = 2;
            panic!("poison the lock");
        });

        // The lock is poisoned, but the value is not torn: it stays accessible.
        assert_eq!(*lock(&mutex), 2);
    }
}
