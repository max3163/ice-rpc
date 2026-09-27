//! Tests for the filtering operators.

use crate::transform::test_support::{drain, local, single};
use crate::{Event, ObservableError};

#[test]
fn filter_normalizes_complete_with_as_value() {
    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let stream = rx.filter(|v| *v % 2 == 1);

    pollster::block_on(tx.send_next(1)).unwrap();
    pollster::block_on(tx.send_next(2)).unwrap();
    pollster::block_on(tx.send_complete_with(9)).unwrap();
    drop(tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 3);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(&events[1], Event::Next(v) if *v == 9));
    assert!(matches!(&events[2], Event::Complete));
}

#[test]
fn take_zero_completes_without_forwarding() {
    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let stream = rx.take(0);

    pollster::block_on(tx.send_next(1)).unwrap();
    drop(tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], Event::Complete));
}

#[test]
fn take_forwards_source_terminal_before_limit() {
    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let stream = rx.take(5);

    pollster::block_on(tx.send_next(1)).unwrap();
    pollster::block_on(tx.send_next(2)).unwrap();
    pollster::block_on(tx.send_error("boom".to_string())).unwrap();
    drop(tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 3);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(&events[1], Event::Next(v) if *v == 2));
    assert!(matches!(
        &events[2],
        Event::Error(ObservableError::Business(e)) if e.as_str() == "boom"
    ));
}

/// Regression for the liveness defect: `take(n)` must complete on its own,
/// without waiting for an event the source may never send. The channel stays
/// open after its single value, which is exactly what used to park `take`
/// on the source for ever.
#[test]
fn take_completes_without_waiting_for_a_further_event() {
    use std::task::{Context, Poll, Waker};

    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    tx.try_send_next(1).expect("the channel has room");

    let mut stream = std::pin::pin!(rx.take(1));
    let mut cx = Context::from_waker(Waker::noop());

    assert!(matches!(
        futures_lite::Stream::poll_next(stream.as_mut(), &mut cx),
        Poll::Ready(Some(Event::Next(1)))
    ));
    // No further event is pushed and the sender is still alive: the completion
    // must be the one `take` synthesizes.
    assert!(matches!(
        futures_lite::Stream::poll_next(stream.as_mut(), &mut cx),
        Poll::Ready(Some(Event::Complete))
    ));

    drop(tx);
}

/// Same scenario through the public `recv()` API: one value, an **open** source,
/// then the completion. Bounded by a timeout so a regression can never hang the
/// suite.
#[test]
fn take_one_delivers_complete_through_recv() {
    use std::time::Duration;

    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    tx.try_send_next(1).expect("the channel has room");

    let mut stream = rx.take(1);
    // The sender stays alive: only `take` itself can end the stream.
    let first = pollster::block_on(crate::rt::timeout(Duration::from_secs(2), stream.recv()))
        .expect("a single value must not wait for ever");
    assert_eq!(first.expect("the channel is alive"), Event::Next(1));

    let second = pollster::block_on(crate::rt::timeout(Duration::from_secs(2), stream.recv()))
        .expect("the completion must not wait for ever");
    assert_eq!(second.expect("the channel is alive"), Event::Complete);

    // Really over: the source is never polled again.
    assert!(pollster::block_on(stream.recv()).is_err());

    drop(tx);
}

/// `take(0)` completes at once, without reading the source: a stream that never
/// produces anything still ends.
#[test]
fn take_zero_completes_without_polling_the_source() {
    use std::task::{Context, Poll, Waker};

    // No event is ever sent, and the sender stays open.
    let (_tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let mut stream = std::pin::pin!(rx.take(0));
    let mut cx = Context::from_waker(Waker::noop());

    assert!(matches!(
        futures_lite::Stream::poll_next(stream.as_mut(), &mut cx),
        Poll::Ready(Some(Event::Complete))
    ));
}

/// Symmetry check: before the bound, a source that closes after its only value
/// still ends the stream on a `Complete` of `take`'s own making.
#[test]
fn take_one_completes_after_its_single_value() {
    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    tx.try_send_next(1).expect("the channel has room");
    drop(tx);

    let events = pollster::block_on(drain(rx.take(1)));
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(&events[1], Event::Complete));
}

/// The bound reached on a live stream: two values, then the completion, with no
/// third event and no closed sender.
#[test]
fn take_two_completes_after_two_values_on_a_live_stream() {
    use std::task::{Context, Poll, Waker};

    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    tx.try_send_next(1).expect("the channel has room");
    tx.try_send_next(2).expect("the channel has room");

    let mut stream = std::pin::pin!(rx.take(2));
    let mut cx = Context::from_waker(Waker::noop());

    for expected in [1, 2] {
        assert!(matches!(
            futures_lite::Stream::poll_next(stream.as_mut(), &mut cx),
            Poll::Ready(Some(Event::Next(v))) if v == expected
        ));
    }
    assert!(matches!(
        futures_lite::Stream::poll_next(stream.as_mut(), &mut cx),
        Poll::Ready(Some(Event::Complete))
    ));

    drop(tx);
}

#[test]
fn first_emits_only_first_value() {
    let stream = local([1, 2, 3]).first();

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(&events[1], Event::Complete));
}

#[test]
fn first_with_emits_first_matching_value() {
    let stream = local([1, 2, 3]).first_with(|v| *v >= 2);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], Event::Next(v) if *v == 2));
    assert!(matches!(&events[1], Event::Complete));
}

#[test]
fn first_forwards_error_before_any_value() {
    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let stream = rx.first();

    pollster::block_on(tx.send_error("boom".to_string())).unwrap();
    drop(tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0],
        Event::Error(ObservableError::Business(e)) if e.as_str() == "boom"
    ));
}

#[test]
fn first_completes_empty_when_no_value() {
    let stream = local(std::iter::empty::<i32>()).first();

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], Event::Complete));
}

#[test]
fn first_with_completes_empty_when_no_match() {
    let stream = local([1, 2, 3]).first_with(|v| *v > 10);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], Event::Complete));
}

#[test]
fn first_with_matches_single_of_value() {
    let stream = single(7).first_with(|v| *v > 5);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], Event::Next(v) if *v == 7));
    assert!(matches!(&events[1], Event::Complete));
}

#[test]
fn skip_drops_leading_values() {
    let stream = local([1, 2, 3, 4]).skip(2);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 3);
    assert!(matches!(&events[0], Event::Next(v) if *v == 3));
    assert!(matches!(&events[1], Event::Next(v) if *v == 4));
    assert!(matches!(&events[2], Event::Complete));
}

/// Consecutive duplicates are dropped, a value repeated **later** is not: that
/// is the difference with `distinct`, which remembers the whole history.
#[test]
fn distinct_until_changed_drops_consecutive_duplicates_only() {
    let stream = local([1, 1, 2, 2, 1, 1]).distinct_until_changed();

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 4);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(&events[1], Event::Next(v) if *v == 2));
    assert!(matches!(&events[2], Event::Next(v) if *v == 1));
    assert!(matches!(&events[3], Event::Complete));
}

/// A dropped duplicate must not cut the stream short: the source's terminal
/// still arrives, even when the last value before it was a repeat.

#[test]
fn distinct_until_changed_forwards_the_terminal_after_a_duplicate() {
    let stream = local([1, 1]).distinct_until_changed();

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(&events[1], Event::Complete));
}
