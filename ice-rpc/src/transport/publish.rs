//! Publication of one sample: the retrying write **both sides** go through.
//!
//! A request is published by the consumer and a response by the provider, and
//! both must offer the same guarantees: never copy a payload nobody can receive,
//! never wait past a deadline, and honour a shutdown request while waiting. Those
//! rules live here once, so the provider side no longer has to reach into the
//! consumer module for the primitive it publishes with.

use std::time::{Duration, Instant};

use iceoryx2::port::LoanError;
use iceoryx2::prelude::*;
use iceoryx2_bb_posix::signal::SignalHandler;

use super::{IoxPubSub, IoxPublisher, PUBLISH_RETRY_SLEEP, PUBLISH_SPIN_ATTEMPTS, transport_error};
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
///
/// A publisher loan limit is transient: like a full subscriber buffer, it is
/// retried with [`backoff`] until the deadline. Memory stays bounded by the
/// configured limit; if the limit stays exhausted for the whole deadline, the
/// call fails with a [`RpcError::TransportError`] that names it.
pub(super) fn publish_until_delivered(
    publisher: &IoxPublisher,
    service: &IoxPubSub,
    header: RpcHeader,
    payload: &[u8],
    timeout: Duration,
) -> Result<(), RpcError> {
    publish_until_delivered_with(
        publisher,
        service,
        header,
        payload,
        timeout,
        shutdown_requested,
    )
}

/// Retry loop behind [`publish_until_delivered`], with an injectable shutdown
/// probe so a unit test does not observe the process-wide token cancelled by
/// another test.
fn publish_until_delivered_with(
    publisher: &IoxPublisher,
    service: &IoxPubSub,
    header: RpcHeader,
    payload: &[u8],
    timeout: Duration,
    is_shutdown: impl Fn() -> bool,
) -> Result<(), RpcError> {
    let deadline = Instant::now() + timeout;
    let mut attempts: u32 = 0;

    loop {
        if is_shutdown() {
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

        match try_publish_attempt(publisher, header, payload)? {
            PublishAttempt::Delivered(true) => return Ok(()),
            // Subscriber connected but buffer full: backpressure, not a missing peer.
            PublishAttempt::Delivered(false) => {
                if Instant::now() >= deadline {
                    return Err(RpcError::TransportError(
                        "delivery refused: the subscriber buffer stayed full".to_string(),
                    ));
                }
            }
            // Every loan slot is in flight: wait for one to be released, unless the
            // deadline has already passed.
            PublishAttempt::LoanLimitReached => {
                if Instant::now() >= deadline {
                    return Err(loan_limit_error(publisher));
                }
            }
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

/// Publishes one sample **once**, and never waits for anything.
///
/// The one-way counterpart of [`publish_until_delivered`]: no retry, no spin, no
/// deadline, and no [`RpcError::Cancelled`]. It answers the only question a
/// fire-and-forget call can answer — *did any subscriber take it?* — and treats
/// a "no" as an ordinary outcome, not a failure: the caller's contract is best
/// effort.
///
/// The subscriber count is read first, for the same reason the retrying path
/// reads it before every attempt: a sample nobody can receive is not worth
/// copying. That read is one relaxed atomic load on shared memory (a few
/// nanoseconds), so the short-circuit costs nothing.
pub(super) fn publish_one_way(
    publisher: &IoxPublisher,
    service: &IoxPubSub,
    header: RpcHeader,
    payload: &[u8],
) -> Result<bool, RpcError> {
    if service.dynamic_config().number_of_subscribers() == 0 {
        return Ok(false);
    }
    try_publish(publisher, header, payload)
}

/// The outcome of one publication attempt.
enum PublishAttempt {
    /// `true` when at least one subscriber received the sample.
    Delivered(bool),
    /// Every loan slot was in flight; a later attempt may succeed.
    LoanLimitReached,
}

/// Builds the error a persistent loan-limit exhaustion reports.
fn loan_limit_error(publisher: &IoxPublisher) -> RpcError {
    RpcError::TransportError(format!(
        "publisher loan limit reached ({} samples in flight); raise the \
         channel's max_loaned_samples or reduce concurrency",
        publisher.max_loaned_samples()
    ))
}

/// Loans one sample, writes `header ++ payload` into it and sends it.
///
/// Returns `true` when at least one subscriber received the sample. This is the
/// **one-shot** form used by one-way calls and Cancel samples: it surfaces a
/// loan-limit exhaustion immediately instead of waiting. Normal calls go through
/// [`publish_until_delivered`], which retries that transient condition with
/// backpressure until its deadline.
pub(super) fn try_publish(
    publisher: &IoxPublisher,
    header: RpcHeader,
    payload: &[u8],
) -> Result<bool, RpcError> {
    match try_publish_attempt(publisher, header, payload)? {
        PublishAttempt::Delivered(delivered) => Ok(delivered),
        PublishAttempt::LoanLimitReached => Err(loan_limit_error(publisher)),
    }
}

/// One loan/write/send attempt, without deciding how to handle a loan limit.
fn try_publish_attempt(
    publisher: &IoxPublisher,
    header: RpcHeader,
    payload: &[u8],
) -> Result<PublishAttempt, RpcError> {
    // A zero-length payload still needs a sample
    const EMPTY: [u8; 1] = [0];
    let payload = if payload.is_empty() {
        &EMPTY[..]
    } else {
        payload
    };

    let sample = match publisher.loan_slice_uninit(payload.len()) {
        Ok(sample) => sample,
        Err(LoanError::ExceedsMaxLoans) => return Ok(PublishAttempt::LoanLimitReached),
        Err(e) => return Err(transport_error("loan sample", e)),
    };
    let mut sample = sample.write_from_slice(payload);
    *sample.user_header_mut() = header;
    let delivered = sample
        .send()
        .map_err(|e| transport_error("send sample", e))?;
    Ok(PublishAttempt::Delivered(delivered > 0))
}

#[cfg(test)]
mod tests {
    use super::super::open::{OpenMode, open_service};
    use super::super::{DEFAULT_MAX_SLICE_LEN, REQUEST_SUFFIX, shared_node};
    use super::*;

    /// The one-shot path used by one-way calls and Cancel samples reports a loan
    /// limit immediately instead of waiting for a slot.
    #[test]
    fn try_publish_reports_the_loan_limit_as_a_transport_error() {
        let node = shared_node().expect("the process node");
        let channel = format!("UnitLoanLimit{}", std::process::id());
        let service = open_service(&node, &channel, REQUEST_SUFFIX, OpenMode::CreateOrOpen)
            .expect("the channel is created");
        let publisher = service
            .publisher_builder()
            .initial_max_slice_len(DEFAULT_MAX_SLICE_LEN)
            .max_loaned_samples(1)
            .create()
            .expect("a publisher");

        // Hold the only slot, so the next loan cannot be served.
        let _held = publisher
            .loan_slice_uninit(1)
            .expect("the first loan fits the limit");

        let err = try_publish(&publisher, RpcHeader::default(), b"data")
            .expect_err("the second loan must exceed the configured limit");
        match err {
            RpcError::TransportError(message) => {
                assert!(message.contains("loan limit"), "{message}");
                assert!(message.contains('1'), "{message}");
            }
            other => panic!("expected a transport error, got {other:?}"),
        }
    }

    /// A normal call retries a transient loan limit: once the holder releases
    /// its slot, the publication succeeds without raising the limit.
    #[test]
    fn publish_until_delivered_retries_a_transient_loan_limit() {
        let node = shared_node().expect("the process node");
        let channel = format!("UnitLoanRetry{}", std::process::id());
        let service = open_service(&node, &channel, REQUEST_SUFFIX, OpenMode::CreateOrOpen)
            .expect("the channel is created");
        // A subscriber keeps the loop from waiting for a peer.
        let _subscriber = service.subscriber_builder().create().expect("a subscriber");
        let publisher = std::sync::Arc::new(
            service
                .publisher_builder()
                .initial_max_slice_len(DEFAULT_MAX_SLICE_LEN)
                .max_loaned_samples(1)
                .create()
                .expect("a publisher"),
        );

        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let holder_publisher = std::sync::Arc::clone(&publisher);
        let holder = std::thread::spawn(move || {
            let _held = holder_publisher
                .loan_slice_uninit(1)
                .expect("the holder takes the only slot");
            ready_tx.send(()).expect("signal the loan");
            std::thread::sleep(Duration::from_millis(50));
        });
        ready_rx.recv().expect("the holder loaned its sample");

        let result = publish_until_delivered_with(
            publisher.as_ref(),
            &service,
            RpcHeader::default(),
            b"data",
            Duration::from_secs(1),
            || false,
        );
        holder.join().expect("the holder thread must finish");
        assert!(
            result.is_ok(),
            "the retry must outlive the holder: {result:?}"
        );
    }

    /// If the limit stays exhausted for the whole deadline, the call reports it
    /// instead of waiting forever.
    #[test]
    fn publish_until_delivered_fails_on_a_persistent_loan_limit() {
        let node = shared_node().expect("the process node");
        let channel = format!("UnitLoanPersistent{}", std::process::id());
        let service = open_service(&node, &channel, REQUEST_SUFFIX, OpenMode::CreateOrOpen)
            .expect("the channel is created");
        let _subscriber = service.subscriber_builder().create().expect("a subscriber");
        let publisher = std::sync::Arc::new(
            service
                .publisher_builder()
                .initial_max_slice_len(DEFAULT_MAX_SLICE_LEN)
                .max_loaned_samples(1)
                .create()
                .expect("a publisher"),
        );

        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let holder_publisher = std::sync::Arc::clone(&publisher);
        let holder = std::thread::spawn(move || {
            let _held = holder_publisher
                .loan_slice_uninit(1)
                .expect("the holder takes the only slot");
            ready_tx.send(()).expect("signal the loan");
            std::thread::sleep(Duration::from_millis(200));
        });
        ready_rx.recv().expect("the holder loaned its sample");

        let err = publish_until_delivered_with(
            publisher.as_ref(),
            &service,
            RpcHeader::default(),
            b"data",
            Duration::from_millis(20),
            || false,
        )
        .expect_err("the slot stays held past the deadline");
        holder.join().expect("the holder thread must finish");
        match err {
            RpcError::TransportError(message) => {
                assert!(message.contains("loan limit"), "{message}");
                assert!(message.contains('1'), "{message}");
            }
            other => panic!("expected a transport error, got {other:?}"),
        }
    }

    #[test]
    fn try_publish_reports_whether_anyone_received_the_sample() {
        let node = shared_node().expect("the process node");
        let channel = format!("UnitPublish{}", std::process::id());
        let service = open_service(&node, &channel, REQUEST_SUFFIX, OpenMode::CreateOrOpen)
            .expect("the channel is created");
        // The builder default is one byte: the slice length must be requested.
        let publisher = service
            .publisher_builder()
            .initial_max_slice_len(DEFAULT_MAX_SLICE_LEN)
            .create()
            .expect("a publisher");
        let header = RpcHeader::default();

        // Nobody is attached: the sample is not delivered, which is not an error.
        assert!(!try_publish(&publisher, header, b"data").expect("publish a sample"));

        // A zero-length payload still travels as a placeholder byte.
        assert!(!try_publish(&publisher, header, b"").expect("publish an empty sample"));

        // A subscriber takes it, so the same call now reports a delivery.
        let _subscriber = service.subscriber_builder().create().expect("a subscriber");
        assert!(try_publish(&publisher, header, b"data").expect("publish a sample"));
    }
}
