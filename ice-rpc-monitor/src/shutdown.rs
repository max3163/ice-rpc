//! Process termination: one flag, set by a signal callback and read by the loops.
//!
//! iceoryx2 exposes [`SignalHandler::termination_requested`], but it **consumes**
//! the pending signal (`last_signal` swaps it for "none"), so a polling bridge
//! competes with every other reader for a single-shot value: whichever thread
//! reads first wins, and the process may never notice the Ctrl+C. Registering a
//! callback instead sets one monotonic flag, once, and keeps it set.
//!
//! On any registration failure the module falls back to the polling bridge, so a
//! process that cannot install the callback still shuts down the way it used to.

use std::sync::atomic::{AtomicBool, Ordering};

use iceoryx2_bb_posix::signal::{FetchableSignal, SignalGuard, SignalHandler};

/// Set once a termination signal (Ctrl+C) has been received.
///
/// Monotonic: nothing ever clears it, so a slow loop still sees it on its next
/// check however late that is.
static TERMINATED: AtomicBool = AtomicBool::new(false);

/// Whether a termination signal has been received.
pub fn requested() -> bool {
    TERMINATED.load(Ordering::Relaxed)
}

/// The process-wide flag, for loops that wait on it.
///
/// `'static`, so it can be shared with the acquisition and console threads
/// without an `Arc`.
pub fn flag() -> &'static AtomicBool {
    &TERMINATED
}

/// Sets the flag; idempotent.
pub fn request() {
    TERMINATED.store(true, Ordering::Relaxed);
}

/// Callback invoked by the iceoryx2 signal handler on Ctrl+C.
fn on_termination(_signal: FetchableSignal) {
    request();
}

/// Keeps the signal callbacks registered for as long as it is alive.
///
/// Dropping it unregisters them, so it must outlive the loops that read
/// [`flag`]. [`Termination::install`] never fails: when the callbacks cannot be
/// registered it starts the polling fallback instead.
pub struct Termination {
    _guards: Vec<SignalGuard>,
}

impl Termination {
    /// Installs the termination handling: a persistent callback, or the polling
    /// bridge as a fallback.
    pub fn install() -> Self {
        let mut guards = Vec::new();
        for signal in [FetchableSignal::Interrupt, FetchableSignal::Terminate] {
            match SignalHandler::register(signal, &on_termination) {
                Ok(guard) => guards.push(guard),
                Err(_) => {
                    log::warn!(
                        "[monitor] could not register the {signal:?} handler; \
                         falling back to polling termination_requested()"
                    );
                    spawn_polling_fallback();
                    return Self { _guards: guards };
                }
            }
        }
        Self { _guards: guards }
    }
}

/// Polls iceoryx2's consumed signal slot and mirrors it into the monotonic flag.
///
/// The historical mechanism, kept only as a fallback: it loses the signal to any
/// other reader, which is exactly why the callback is preferred.
fn spawn_polling_fallback() {
    std::thread::spawn(|| loop {
        if SignalHandler::termination_requested() {
            request();
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_flag_is_monotonic_and_starts_clear() {
        // A fresh process has not been asked to stop.
        assert!(!requested());
        request();
        assert!(requested(), "once set, the flag stays set");
        assert!(requested(), "reading it does not clear it");
    }
}
