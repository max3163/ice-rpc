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
