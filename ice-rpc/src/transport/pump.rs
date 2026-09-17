//! The receive loop shared by the two dispatch threads.
//!
//! A dispatch thread (provider side of a channel, consumer side of a channel)
//! does exactly the same thing: spin briefly, block on a `WaitSet` attached to
//! its wake-up listener, and hand every sample to a callback. Keeping that
//! control flow in one place is what makes the arbitration between "poll",
//! "block" and "terminate" readable — and testable by reading, since both
//! threads now share the same code path.

use iceoryx2::prelude::*;
use iceoryx2_bb_posix::signal::SignalHandler;

use super::waitset::wait_for_wakeup;
use super::{Iox, IoxListener, IoxSubscriber, IDLE_SPINS, SIGNAL_CHECK_SAMPLES};
use crate::types::RpcHeader;

/// Drains `subscriber` until `stop` returns `true` or the process is asked to
/// terminate, calling `handle` with the header and payload of every sample.
///
/// `channel` and `direction` only label the logs (`"request"` on the provider
/// side, `"response"` on the consumer side), so one log line still identifies
/// the thread it came from.
pub(super) fn run_receive_loop(
    channel: &str,
    direction: &str,
    subscriber: &IoxSubscriber,
    listener: &IoxListener,
    stop: impl Fn() -> bool,
    mut handle: impl FnMut(&RpcHeader, &[u8]),
) {
    let Ok(waitset) = WaitSetBuilder::new()
        .signal_handling_mode(crate::waitset_signal_handling_mode())
        .create::<Iox>()
    else {
        log::error!("[transport] '{channel}': waitset creation failed");
        return;
    };

    // A plain notification attachment: the wake-up deadline is passed to
    // `wait_and_process_once_with_timeout`, so attaching it as a deadline would
    // make the guard fire on every expiry and defeat the idle path.
    let Ok(guard) = waitset.attach_notification(listener) else {
        log::error!("[transport] '{channel}': waitset attach failed");
        return;
    };

    let mut idle_spins: u32 = 0;
    let mut signal_ticks: u32 = 0;

    while !stop() {
        match subscriber.receive() {
            Ok(Some(sample)) => {
                idle_spins = 0;
                // A saturated channel never reaches the blocking path below,
                // where iceoryx2 reports the termination request.
                signal_ticks = signal_ticks.wrapping_add(1);
                if signal_ticks & (SIGNAL_CHECK_SAMPLES - 1) == 0
                    && SignalHandler::termination_requested()
                {
                    crate::request_shutdown();
                    break;
                }
                handle(sample.user_header(), &sample);
            }
            Ok(None) => {
                if idle_spins < IDLE_SPINS {
                    idle_spins += 1;
                    std::thread::yield_now();
                } else {
                    let notified = wait_for_wakeup(&waitset, &guard, listener);
                    // Only a notification means there is something to poll for;
                    // a bare deadline expiry keeps the thread blocked.
                    idle_spins = if notified { 0 } else { IDLE_SPINS };
                }
            }
            Err(e) => {
                log::warn!("[transport] '{channel}': {direction} receive error: {e:?}");
                idle_spins = 0;
                std::thread::yield_now();
            }
        }
    }
}
