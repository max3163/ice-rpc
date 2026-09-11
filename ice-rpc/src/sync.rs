//! Locking helpers with a single, documented poisoning policy.
//!
//! # Poisoning policy
//!
//! A poisoned lock means a thread panicked while holding it. Every value guarded
//! in this crate is a plain map, a set of shared pointers or a state machine,
//! always updated through short critical sections that cannot panic. A poisoned
//! lock is therefore not actionable: propagating a second panic — or a `Result`
//! that every caller would have to handle — would only hide the original
//! failure.
//!
//! The guard is recovered with [`std::sync::PoisonError::into_inner`], so the
//! protected data is still readable and writable. Using these helpers everywhere
//! keeps the call sites free of `.lock().expect("... poisoning")` noise and
//! makes the policy explicit, documented and tested in one place.

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
