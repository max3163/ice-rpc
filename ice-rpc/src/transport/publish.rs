//! Publication of one sample: the retrying write **both sides** go through.
//!
//! A request is published by the consumer and a response by the provider, and
//! both must offer the same guarantees: never copy a payload nobody can receive,
//! never wait past a deadline, and honour a shutdown request while waiting. Those
//! rules live here once, so the provider side no longer has to reach into the
//! consumer module for the primitive it publishes with.

use std::time::{Duration, Instant};

use iceoryx2::prelude::*;
use iceoryx2_bb_posix::signal::SignalHandler;

use super::{transport_error, IoxPubSub, IoxPublisher, PUBLISH_RETRY_SLEEP, PUBLISH_SPIN_ATTEMPTS};
use crate::types::{RpcError, RpcHeader};

/// Publishes `header ++ payload` on `publisher`, retrying until at least one
/// subscriber receives it or `timeout` elapses.
///
/// `service` is the pub/sub service `publisher` belongs to, used only to read
/// its **subscriber count**. With no subscriber connected — typically a call
/// made before the provider process is up — nothing is loaned and the payload is
/// never copied: writing a sample nobody can receive would copy the whole
/// payload again on every attempt, for up to `timeout` (30 s by default). The
/// wait is spent on the subscriber count instead, and the payload is copied
/// once, when a receiver exists.
pub(super) fn publish_until_delivered(
    publisher: &IoxPublisher,
    service: &IoxPubSub,
    header: RpcHeader,
    payload: &[u8],
    timeout: Duration,
) -> Result<(), RpcError> {
    let deadline = Instant::now() + timeout;
    let mut attempts: u32 = 0;

    loop {
        if shutdown_requested() {
            return Err(RpcError::Cancelled);
        }

        if service.dynamic_config().number_of_subscribers() == 0 {
            if Instant::now() >= deadline {
                return Err(RpcError::TransportError(
                    "no subscriber connected (is the provider running?)".to_string(),
                ));
            }
            backoff(&mut attempts)?;
            continue;
        }

        if try_publish(publisher, header, payload)? {
            return Ok(());
        }

        // A subscriber is connected but did not take the sample: its buffer is
        // full, which is backpressure rather than a missing peer.
        if Instant::now() >= deadline {
            return Err(RpcError::TransportError(
                "delivery refused: the subscriber buffer stayed full".to_string(),
            ));
        }
        backoff(&mut attempts)?;
    }
}

/// Whether the process was asked to stop.
fn shutdown_requested() -> bool {
    crate::global_cancel_token().is_cancelled() || crate::registry_cancel_token().is_cancelled()
}

/// Waits between two delivery attempts: a burst of yields, then short sleeps.
///
/// This call path owns no `WaitSet`, so the OS termination flag is sampled here:
/// it is what makes Ctrl+C honourable when this wait is the only running code.
fn backoff(attempts: &mut u32) -> Result<(), RpcError> {
    if *attempts < PUBLISH_SPIN_ATTEMPTS {
        *attempts += 1;
        std::thread::yield_now();
        return Ok(());
    }

    if SignalHandler::termination_requested() {
        crate::request_shutdown();
        return Err(RpcError::Cancelled);
    }
    std::thread::sleep(PUBLISH_RETRY_SLEEP);
    Ok(())
}

/// Loans one sample, writes `header ++ payload` into it and sends it.
///
/// Returns `true` when at least one subscriber received the sample. Also used by
/// the consumer for its fire-and-forget samples — a Cancel, whose loss is
/// acceptable — which is why it is visible to the sibling modules.
pub(super) fn try_publish(
    publisher: &IoxPublisher,
    header: RpcHeader,
    payload: &[u8],
) -> Result<bool, RpcError> {
    // A zero-length payload still needs a sample
    const EMPTY: [u8; 1] = [0];
    let payload = if payload.is_empty() {
        &EMPTY[..]
    } else {
        payload
    };

    let sample = publisher
        .loan_slice_uninit(payload.len())
        .map_err(|e| transport_error("loan sample", e))?;
    let mut sample = sample.write_from_slice(payload);
    *sample.user_header_mut() = header;
    let delivered = sample
        .send()
        .map_err(|e| transport_error("send sample", e))?;
    Ok(delivered > 0)
}

#[cfg(test)]
mod tests {
    use super::super::open::{open_service, OpenMode};
    use super::super::{shared_node, DEFAULT_MAX_SLICE_LEN, REQUEST_SUFFIX};
    use super::*;

    /// `try_publish` answers "did anyone take it?", and both answers matter:
    /// `false` is the ordinary "no subscriber yet" case that the retrying caller
    /// turns into a wait, never into a failure.
    ///
    /// The channel belongs to this test, so it appears and disappears with it.
    #[test]
    fn try_publish_reports_whether_anyone_received_the_sample() {
        let node = shared_node().expect("the process node");
        let channel = format!("UnitPublish{}", std::process::id());
        let service = open_service(&node, &channel, REQUEST_SUFFIX, OpenMode::CreateOrOpen)
            .expect("the channel is created");
        // The builder default is one byte, so the declared slice length has to be
        // requested explicitly — exactly what the consumer port does.
        let publisher = service
            .publisher_builder()
            .initial_max_slice_len(DEFAULT_MAX_SLICE_LEN)
            .create()
            .expect("a publisher");
        let header = RpcHeader::default();

        // Nobody is attached: the sample is not delivered, which is not an error.
        assert!(!try_publish(&publisher, header, b"data").expect("publish a sample"));

        // A zero-length payload still travels as a sample: it is the placeholder
        // byte that makes this branch different from the one above.
        assert!(!try_publish(&publisher, header, b"").expect("publish an empty sample"));

        // A subscriber takes it, so the same call now reports a delivery.
        let _subscriber = service.subscriber_builder().create().expect("a subscriber");
        assert!(try_publish(&publisher, header, b"data").expect("publish a sample"));
    }
}
