//! Tests for the conditional operators.

use crate::transform::test_support::{drain, is_technical, local};
use crate::Event;

#[test]
fn take_until_token_emits_cancelled_when_the_token_fires() {
    let token = crate::CancellationToken::new();
    token.cancel();
    let stream = local([1, 2, 3]).take_until_token(&token);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 1);
    assert!(is_technical(&events[0]));
}

#[test]
fn take_until_token_forwards_values_when_not_cancelled() {
    let token = crate::CancellationToken::new();
    let stream = local([1, 2, 3]).take_until_token(&token);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 4);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(&events[1], Event::Next(v) if *v == 2));
    assert!(matches!(&events[2], Event::Next(v) if *v == 3));
    assert!(matches!(&events[3], Event::Complete));
}

/// The RxJS contract: the stop is a **completion**, not a failure.
#[test]
fn take_until_stops_on_the_notifiers_first_value() {
    let (stop_tx, stop_rx) = crate::channel::<(), String>(1);
    stop_tx.try_send_next(()).expect("the channel has room");
    drop(stop_tx);

    let events = pollster::block_on(drain(local([1, 2, 3]).take_until(stop_rx)));
    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], Event::Complete));
}

/// The point of completing instead of failing: a value already emitted is
/// followed by the stop, so `collect` keeps the prefix instead of discarding it.
#[test]
fn take_until_keeps_the_prefix_already_emitted() {
    use std::task::{Context, Poll, Waker};

    let (tx, rx) = crate::channel::<i32, String>(8);
    let (stop_tx, stop_rx) = crate::channel::<(), String>(1);
    let mut stream = Box::pin(rx.take_until(stop_rx));
    let mut cx = Context::from_waker(Waker::noop());

    pollster::block_on(tx.send_next(1)).unwrap();
    assert!(matches!(
        futures_lite::Stream::poll_next(stream.as_mut(), &mut cx),
        Poll::Ready(Some(Event::Next(1)))
    ));

    // The notifier fires only now: what was read before stays on the stream.
    stop_tx.try_send_next(()).expect("the channel has room");
    assert!(matches!(
        futures_lite::Stream::poll_next(stream.as_mut(), &mut cx),
        Poll::Ready(Some(Event::Complete))
    ));

    drop(tx);
    drop(stop_tx);
}

/// A notifier that ends without emitting never stops the source (RxJS).
#[test]
fn take_until_ignores_a_notifier_that_completes_without_a_value() {
    let (stop_tx, stop_rx) = crate::channel::<(), String>(1);
    drop(stop_tx);

    let events = pollster::block_on(drain(local([1, 2, 3]).take_until(stop_rx)));
    assert_eq!(events.len(), 4);
    assert!(matches!(&events[3], Event::Complete));
}

#[test]
fn take_until_forwards_a_notifier_error() {
    let (stop_tx, stop_rx) = crate::channel::<(), String>(1);
    stop_tx
        .try_send_error("stop failed".to_string())
        .expect("the channel has room");
    drop(stop_tx);

    let events = pollster::block_on(drain(local([1, 2, 3]).take_until(stop_rx)));
    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0],
        Event::Error(crate::ObservableError::Business(e)) if e == "stop failed"
    ));
}

/// The notifier may be a **future** — an `async fn` call — thanks to
/// `ObservableInput`; its first value stops the source as usual.
#[test]
fn take_until_accepts_a_future_notifier() {
    let events = pollster::block_on(drain(
        local([1, 2, 3]).take_until(async { crate::of::<(), String>(()) }),
    ));

    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], Event::Complete));
}

/// The future form and the `Observable` form are the same operator underneath.
#[test]
fn take_until_future_notifier_matches_the_observable_form() {
    let via_future = pollster::block_on(drain(
        local([1, 2, 3]).take_until(async { crate::of::<(), String>(()) }),
    ));
    let via_observable = pollster::block_on(drain(
        local([1, 2, 3]).take_until(crate::of::<(), String>(())),
    ));

    assert_eq!(via_future.len(), via_observable.len());
    assert!(matches!(&via_future[0], Event::Complete));
    assert!(matches!(&via_observable[0], Event::Complete));
}

/// Laziness: the future body runs at the first poll, never when the pipeline is
/// merely built.
#[test]
fn take_until_does_not_run_the_future_before_a_poll() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_in = Arc::clone(&calls);
    let stream = local([1]).take_until(async move {
        calls_in.fetch_add(1, Ordering::SeqCst);
        crate::of::<(), String>(())
    });

    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "nothing runs before a poll"
    );

    let events = pollster::block_on(drain(stream));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(matches!(&events[0], Event::Complete));
}
