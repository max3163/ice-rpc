//! Event-driven wait shared by both dispatch threads.

use iceoryx2::prelude::*;
use iceoryx2::waitset::WaitSetRunResult;

use super::{Iox, IoxListener, WAITSET_DEADLINE};

/// Blocks on `waitset` until the notifier fires or [`WAITSET_DEADLINE`] expires.
///
/// Returns `true` when the wake-up came from the attached notification, which
/// tells "data is probably available" from "still idle".
///
/// The notification is **consumed** here. It is level-triggered: as long as the
/// listener has a pending event its ready edge stays set, so the `WaitSet` would
/// return immediately on every iteration and the dispatch loop would busy-spin
/// (`yield_now` in a tight loop) instead of blocking. Draining the listener
/// clears the edge; the samples themselves are drained by the caller through the
/// subscriber.
pub(super) fn wait_for_wakeup(
    waitset: &WaitSet<Iox>,
    guard: &WaitSetGuard<'_, '_, Iox>,
    listener: &IoxListener,
) -> bool {
    let mut notified = false;
    let result = waitset.wait_and_process_once_with_timeout(
        |attachment_id| {
            if attachment_id.has_event_from(guard) {
                notified = true;
            }
            CallbackProgression::Continue
        },
        WAITSET_DEADLINE,
    );

    // Clear the readiness edge, whether or not this wait reported it: a stale
    // notification from an earlier iteration would keep the port ready forever.
    while matches!(listener.try_wait_one(), Ok(Some(_))) {}

    // iceoryx2 reports SIGINT/SIGTERM here instead of killing the process.
    if matches!(
        result,
        Ok(WaitSetRunResult::TerminationRequest) | Ok(WaitSetRunResult::Interrupt)
    ) {
        crate::request_shutdown();
        return true;
    }

    notified
}
