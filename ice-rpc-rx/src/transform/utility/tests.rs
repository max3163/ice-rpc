//! Tests for the utility operators.

use crate::transform::test_support::{drain, next_event};
use crate::{Event, ObservableError};

#[test]
fn finalize_runs_on_complete() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let finalized = Arc::new(AtomicBool::new(false));
    let flag = finalized.clone();
    let stream = rx.finalize(move || flag.store(true, Ordering::SeqCst));

    pollster::block_on(tx.send_next(1)).unwrap();
    pollster::block_on(tx.send_complete()).unwrap();
    drop(tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(&events[1], Event::Complete));
    assert!(finalized.load(Ordering::SeqCst));
}

#[test]
fn finalize_runs_on_source_channel_close() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let finalized = Arc::new(AtomicBool::new(false));
    let flag = finalized.clone();
    let stream = rx.finalize(move || flag.store(true, Ordering::SeqCst));

    pollster::block_on(tx.send_next(1)).unwrap();
    drop(tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(finalized.load(Ordering::SeqCst));
}

#[test]
fn finalize_runs_on_error() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let finalized = Arc::new(AtomicBool::new(false));
    let flag = finalized.clone();
    let stream = rx.finalize(move || flag.store(true, Ordering::SeqCst));

    pollster::block_on(tx.send_error("boom".to_string())).unwrap();
    drop(tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0],
        Event::Error(ObservableError::Business(e)) if e.as_str() == "boom"
    ));
    assert!(finalized.load(Ordering::SeqCst));
}

#[test]
fn tap_runs_side_effect() {
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;

    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let seen = Arc::new(AtomicI32::new(0));
    let flag = seen.clone();
    let stream = rx.tap(move |_| {
        flag.fetch_add(1, Ordering::SeqCst);
    });

    pollster::block_on(tx.send_next(1)).unwrap();
    pollster::block_on(tx.send_next(2)).unwrap();
    pollster::block_on(tx.send_complete()).unwrap();
    drop(tx);

    pollster::block_on(drain(stream));
    assert_eq!(seen.load(Ordering::SeqCst), 2);
}

#[test]
fn tap_does_not_touch_terminal_events() {
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;

    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let seen = Arc::new(AtomicI32::new(0));
    let flag = seen.clone();
    let stream = rx.tap(move |_| {
        flag.fetch_add(1, Ordering::SeqCst);
    });

    pollster::block_on(tx.send_next(1)).unwrap();
    pollster::block_on(tx.send_error("boom".to_string())).unwrap();
    drop(tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(
        &events[1],
        Event::Error(ObservableError::Business(e)) if e.as_str() == "boom"
    ));
    assert_eq!(seen.load(Ordering::SeqCst), 1);
}

#[test]
fn delay_postpones_events() {
    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let stream = rx.delay(std::time::Duration::from_millis(20));

    pollster::block_on(tx.send_next(1)).unwrap();
    drop(tx);

    let mut stream = Box::pin(stream);
    let start = std::time::Instant::now();
    // `delay` sleeps through `rt::sleep`, which needs a runtime under the
    // `tokio` facade: `test_block_on` supplies one for the whole poll.
    let event = crate::rt::test_block_on(next_event(&mut stream));
    let elapsed = start.elapsed();

    assert!(matches!(event, Some(Event::Next(1))));
    assert!(elapsed >= std::time::Duration::from_millis(15));
}

#[test]
fn delay_forwards_terminal_events() {
    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let stream = rx.delay(std::time::Duration::from_millis(20));

    pollster::block_on(tx.send_complete()).unwrap();
    drop(tx);

    let mut stream = Box::pin(stream);
    let start = std::time::Instant::now();
    let event = crate::rt::test_block_on(next_event(&mut stream));
    let elapsed = start.elapsed();

    assert!(matches!(event, Some(Event::Complete)));
    assert!(elapsed >= std::time::Duration::from_millis(15));
}

#[test]
fn timeout_emits_technical_error_on_silence() {
    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let stream = rx.timeout(std::time::Duration::from_millis(20));

    let mut stream = Box::pin(stream);
    let event = crate::rt::test_block_on(next_event(&mut stream));
    assert!(matches!(
        event,
        Some(Event::Error(ObservableError::Technical(_)))
    ));

    drop(tx);
}

#[test]
fn timeout_forwards_values_before_deadline() {
    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let stream = rx.timeout(std::time::Duration::from_millis(200));

    pollster::block_on(tx.send_next(1)).unwrap();
    pollster::block_on(tx.send_complete()).unwrap();
    drop(tx);

    let events = crate::rt::test_block_on(drain(stream));
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(&events[1], Event::Complete));
}
