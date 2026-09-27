//! Tests for the utility operators.

use crate::transform::test_support::{drain, next_event};
use crate::{Event, ObservableError};

#[test]
fn finalize_runs_on_complete() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let (tx, rx) = crate::channel::<i32, String>(8);
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

    let (tx, rx) = crate::channel::<i32, String>(8);
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

    let (tx, rx) = crate::channel::<i32, String>(8);
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

    let (tx, rx) = crate::channel::<i32, String>(8);
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

    let (tx, rx) = crate::channel::<i32, String>(8);
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
    let (tx, rx) = crate::channel::<i32, String>(8);
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
    let (tx, rx) = crate::channel::<i32, String>(8);
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

/// A burst must cost **one** delay, not one per event: the old implementation
/// paced the queue instead of shifting it.
#[test]
fn delay_shifts_a_burst_instead_of_spreading_it() {
    use std::time::{Duration, Instant};

    let stream = crate::from::<i32, String, _>([1, 2, 3]).delay(Duration::from_millis(40));

    let start = Instant::now();
    let values = crate::rt::test_block_on(stream.collect()).expect("the stream completes cleanly");
    let elapsed = start.elapsed();

    assert_eq!(values, vec![1, 2, 3]);
    assert!(
        elapsed < Duration::from_millis(110),
        "a burst of three must cost one delay, not three: {elapsed:?}"
    );
}

/// The source's own spacing must survive: two arrivals 20 ms apart come out
/// 20 ms apart, not `duration` apart. The old implementation stretched them to
/// the delay, behaving as a pacer.
#[test]
fn delay_preserves_the_source_spacing() {
    use std::time::{Duration, Instant};

    let (tx, rx) = crate::channel::<i32, String>(8);
    let mut stream = Box::pin(rx.delay(Duration::from_millis(100)));

    // The second value arrives 20 ms after the first, well inside the delay
    // window, so the shift and the pacing give different answers.
    let producer = tx.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        let _ = pollster::block_on(producer.send_next(2));
    });
    pollster::block_on(tx.send_next(1)).unwrap();

    let outcome = crate::rt::test_block_on(async {
        let first = next_event(&mut stream).await;
        let start = Instant::now();
        let second = next_event(&mut stream).await;
        (first, second, start.elapsed())
    });

    assert!(matches!(outcome.0, Some(Event::Next(1))));
    assert!(matches!(outcome.1, Some(Event::Next(2))));
    assert!(
        outcome.2 < Duration::from_millis(60),
        "the source's 20 ms spacing must survive the delay, got {:?}",
        outcome.2
    );
    drop(tx);
}

#[test]
fn timeout_emits_technical_error_on_silence() {
    let (tx, rx) = crate::channel::<i32, String>(8);
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
    let (tx, rx) = crate::channel::<i32, String>(8);
    let stream = rx.timeout(std::time::Duration::from_millis(200));

    pollster::block_on(tx.send_next(1)).unwrap();
    pollster::block_on(tx.send_complete()).unwrap();
    drop(tx);

    let events = crate::rt::test_block_on(drain(stream));
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(&events[1], Event::Complete));
}
