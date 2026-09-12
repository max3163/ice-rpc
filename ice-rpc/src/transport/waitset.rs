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
