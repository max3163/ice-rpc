//! Tests for the error-handling operators.

use crate::transform::test_support::{drain, is_technical};
use crate::{Event, ObservableError};

#[test]
fn catch_error_replaces_error_with_fallback() {
    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let stream = rx.catch_error(|_| -1);

    pollster::block_on(tx.send_next(1)).unwrap();
    pollster::block_on(tx.send_error("boom".to_string())).unwrap();
    drop(tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 3);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(&events[1], Event::Next(v) if *v == -1));
    assert!(matches!(&events[2], Event::Complete));
}

#[test]
fn catch_error_forwards_technical_error_unchanged() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let called = Arc::new(AtomicBool::new(false));
    let flag = called.clone();
    let stream = rx.catch_error(move |_| {
        flag.store(true, Ordering::SeqCst);
        -1
    });

    pollster::block_on(tx.send_event(Event::Error(ObservableError::Technical(
        crate::RpcError::Timeout,
    ))))
    .unwrap();
    drop(tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 1);
    assert!(is_technical(&events[0]));
    assert!(!called.load(Ordering::SeqCst));
}

#[test]
fn catch_error_passthrough_when_no_error() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let called = Arc::new(AtomicBool::new(false));
    let flag = called.clone();
    let stream = rx.catch_error(move |_| {
        flag.store(true, Ordering::SeqCst);
        -1
    });

    pollster::block_on(tx.send_next(1)).unwrap();
    pollster::block_on(tx.send_complete()).unwrap();
    drop(tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(&events[1], Event::Complete));
    assert!(!called.load(Ordering::SeqCst));
}
