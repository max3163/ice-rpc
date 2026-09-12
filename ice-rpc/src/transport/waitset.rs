//! Event-driven wait shared by both dispatch threads.

use iceoryx2::prelude::*;
use iceoryx2::waitset::WaitSetRunResult;

use super::{Iox, WAITSET_DEADLINE};

/// Blocks on `waitset` until the notifier fires or [`WAITSET_DEADLINE`] expires.
///
/// Returns `true` when the wake-up came from the attached notification, which
/// tells "data is probably available" from "still idle".
pub(super) fn wait_for_wakeup(waitset: &WaitSet<Iox>, guard: &WaitSetGuard<'_, '_, Iox>) -> bool {
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

    // In `HandleTerminationRequests` mode iceoryx2 owns the SIGINT/SIGTERM
    // handler: the signal is reported here instead of killing the process, so the
    // framework cancels its tokens and the caller exits cleanly.
    if matches!(
        result,
        Ok(WaitSetRunResult::TerminationRequest) | Ok(WaitSetRunResult::Interrupt)
    ) {
        crate::request_shutdown();
        return true;
    }

    notified
}
