//! Tests for the combining operators.

use crate::transform::test_support::{drain, local, single};
use crate::{Event, ObservableError};

#[test]
fn start_with_prefixes_initial_value() {
    let stream = single(1).start_with(0);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 3);
    assert!(matches!(&events[0], Event::Next(v) if *v == 0));
    assert!(matches!(&events[1], Event::Next(v) if *v == 1));
    assert!(matches!(&events[2], Event::Complete));
}

/// The two sources are interleaved, and a source that is always ready cannot
/// starve the other one: the round-robin is what makes this order predictable.
#[test]
fn merge_interleaves_the_two_sources_fairly() {
    let stream = local([1, 2]).merge(local([10, 20]));

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 5);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(&events[1], Event::Next(v) if *v == 10));
    assert!(matches!(&events[2], Event::Next(v) if *v == 2));
    assert!(matches!(&events[3], Event::Next(v) if *v == 20));
    assert!(matches!(&events[4], Event::Complete));
}

/// The `Complete` of one source does not end the merge: the other one keeps
/// producing, and the merged stream emits exactly **one** `Complete`, when the
/// last of the two is over.
#[test]
fn merge_waits_for_the_last_source_before_completing() {
    let (left_tx, left_rx) = crate::channel::<i32, String>(4);
    let (right_tx, right_rx) = crate::channel::<i32, String>(4);
    let stream = left_rx.merge(right_rx);

    // The left source is over before the right one has delivered anything.
    pollster::block_on(left_tx.send_complete()).expect("room for the terminal");
    pollster::block_on(right_tx.send_next(7)).expect("room for the value");
    pollster::block_on(right_tx.send_complete()).expect("room for the terminal");
    drop((left_tx, right_tx));

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], Event::Next(v) if *v == 7));
    assert!(matches!(&events[1], Event::Complete));
}

/// An error from either source ends the merge at once, values before it
/// included.
#[test]
fn merge_ends_on_an_error_from_either_source() {
    let (left_tx, left_rx) = crate::channel::<i32, String>(4);
    let (right_tx, right_rx) = crate::channel::<i32, String>(4);
    let stream = left_rx.merge(right_rx);

    pollster::block_on(right_tx.send_next(7)).expect("room for the value");
    pollster::block_on(right_tx.send_error("boom".to_string())).expect("room for the error");

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], Event::Next(v) if *v == 7));
    assert!(matches!(&events[1], Event::Error(ObservableError::Business(e)) if e == "boom"));

    drop(left_tx);
}

/// A source with nothing to emit must not hold the merge open.
#[test]
fn merge_forwards_the_other_source_when_one_is_empty() {
    let stream = local(std::iter::empty::<i32>()).merge(single(5));

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], Event::Next(v) if *v == 5));
    assert!(matches!(&events[1], Event::Complete));
}
