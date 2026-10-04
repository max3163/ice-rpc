//! Tests for the transforming operators.

use crate::transform::test_support::{drain, is_technical, local, next_event};
use crate::{Event, ObservableError};

#[test]
fn map_maps_normalized_single_value() {
    let (tx, rx) = crate::channel::<i32, String>(8);
    let stream = rx.map(|v| v * 2);

    pollster::block_on(tx.send_complete_with(5)).unwrap();
    drop(tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], Event::Next(v) if *v == 10));
    assert!(matches!(&events[1], Event::Complete));
}

#[test]
fn map_forwards_terminal_events_unchanged() {
    let (tx, rx) = crate::channel::<i32, String>(8);
    let stream = rx.map(|v| v * 2);

    pollster::block_on(tx.send_error("boom".to_string())).unwrap();
    pollster::block_on(tx.send_event(Event::Error(ObservableError::Technical(
        crate::RpcError::Timeout,
    ))))
    .unwrap();
    drop(tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 2);
    assert!(matches!(
        &events[0],
        Event::Error(ObservableError::Business(e)) if e.as_str() == "boom"
    ));
    assert!(is_technical(&events[1]));
}

#[test]
fn map_err_transforms_error_type() {
    let (tx, rx) = crate::channel::<i32, String>(8);
    let stream = rx.map_err(|e| e.len());

    pollster::block_on(tx.send_next(1)).unwrap();
    pollster::block_on(tx.send_error("boom".to_string())).unwrap();
    drop(tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(
        &events[1],
        Event::Error(ObservableError::Business(n)) if *n == 4
    ));
}

#[test]
fn scan_emits_running_accumulator() {
    let stream = local([1, 2, 3]).scan(0, |acc, v| acc + v);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 4);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(&events[1], Event::Next(v) if *v == 3));
    assert!(matches!(&events[2], Event::Next(v) if *v == 6));
    assert!(matches!(&events[3], Event::Complete));
}

#[test]
fn switch_map_switches_to_latest_projection_and_cancels_previous() {
    use std::sync::{Arc, Mutex};

    let (source_tx, source_rx) = crate::channel::<i32, String>(8);
    let senders: Arc<Mutex<Vec<crate::Sender<i32, String>>>> = Arc::new(Mutex::new(Vec::new()));

    let senders_for_task = senders.clone();
    let stream = source_rx.switch_map(move |_| {
        let (tx, rx) = crate::channel::<i32, String>(8);
        senders_for_task.lock().unwrap().push(tx);
        rx
    });

    pollster::block_on(source_tx.send_next(1)).unwrap();
    pollster::block_on(source_tx.send_next(2)).unwrap();

    let mut stream = Box::pin(stream);
    // A single poll drives the lazy combinator: it consumes both source
    // values and subscribes twice.
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let _ = futures_lite::Stream::poll_next(stream.as_mut(), &mut cx);
    assert_eq!(senders.lock().unwrap().len(), 2);

    let projected2_tx = senders.lock().unwrap()[1].clone();
    pollster::block_on(projected2_tx.send_next(20)).unwrap();

    assert!(matches!(
        pollster::block_on(next_event(&mut stream)),
        Some(Event::Next(v)) if v == 20
    ));

    let projected1_tx = senders.lock().unwrap()[0].clone();
    assert!(pollster::block_on(projected1_tx.send_next(10)).is_err());

    pollster::block_on(source_tx.send_complete()).unwrap();

    // RxJS: the source's completion waits for the projected stream in flight,
    // so the pipeline is not over while `projected2` is still open.
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(matches!(
        futures_lite::Stream::poll_next(stream.as_mut(), &mut cx),
        std::task::Poll::Pending
    ));

    let projected2_tx = senders.lock().unwrap()[1].clone();
    pollster::block_on(projected2_tx.send_complete()).unwrap();
    drop(projected2_tx);
    drop(source_tx);

    assert!(matches!(
        pollster::block_on(next_event(&mut stream)),
        Some(Event::Complete)
    ));
}

/// Regression for P0-2: the last projected stream must still deliver after the
/// source has completed — RxJS awaits it instead of dropping it.
#[test]
fn switch_map_waits_for_the_in_flight_projection_before_completing() {
    use std::cell::RefCell;

    let (source_tx, source_rx) = crate::channel::<i32, String>(8);
    let (projected_tx, projected_rx) = crate::channel::<i32, String>(8);

    let projected = RefCell::new(Some(projected_rx));
    let mut stream = Box::pin(source_rx.switch_map(move |_| {
        projected
            .borrow_mut()
            .take()
            .expect("the projected channel is subscribed once")
    }));

    pollster::block_on(source_tx.send_next(1)).unwrap();
    // The source ends while the projected stream is still in flight.
    pollster::block_on(source_tx.send_complete()).unwrap();

    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(matches!(
        futures_lite::Stream::poll_next(stream.as_mut(), &mut cx),
        std::task::Poll::Pending
    ));

    // The last projected stream still delivers: nothing is lost.
    pollster::block_on(projected_tx.send_next(10)).unwrap();
    assert!(matches!(
        pollster::block_on(next_event(&mut stream)),
        Some(Event::Next(v)) if v == 10
    ));

    pollster::block_on(projected_tx.send_complete()).unwrap();
    drop(projected_tx);
    drop(source_tx);

    assert!(matches!(
        pollster::block_on(next_event(&mut stream)),
        Some(Event::Complete)
    ));
}

#[test]
fn switch_map_forwards_projected_error() {
    use std::cell::RefCell;

    let (source_tx, source_rx) = crate::channel::<i32, String>(8);
    let (projected_tx, projected_rx) = crate::channel::<i32, String>(8);

    // Each source value subscribes to a fresh projected stream: hand it out once.
    let projected = RefCell::new(Some(projected_rx));
    let stream = source_rx.switch_map(move |_| {
        projected
            .borrow_mut()
            .take()
            .expect("the projected channel is subscribed once")
    });

    pollster::block_on(source_tx.send_next(1)).unwrap();
    pollster::block_on(projected_tx.send_error("boom".to_string())).unwrap();
    drop(source_tx);
    drop(projected_tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0],
        Event::Error(ObservableError::Business(e)) if e.as_str() == "boom"
    ));
}

#[test]
fn switch_map_ignores_projected_complete() {
    use std::cell::RefCell;

    let (source_tx, source_rx) = crate::channel::<i32, String>(8);
    let (projected1_tx, projected1_rx) = crate::channel::<i32, String>(8);
    let (projected2_tx, projected2_rx) = crate::channel::<i32, String>(8);

    let projected1 = RefCell::new(Some(projected1_rx));
    let projected2 = RefCell::new(Some(projected2_rx));
    let stream = source_rx.switch_map(move |v| {
        let slot = if v == 1 { &projected1 } else { &projected2 };
        slot.borrow_mut()
            .take()
            .expect("each projected channel is subscribed once")
    });

    pollster::block_on(source_tx.send_next(1)).unwrap();
    pollster::block_on(projected1_tx.send_complete()).unwrap();
    pollster::block_on(source_tx.send_next(2)).unwrap();
    pollster::block_on(projected2_tx.send_next(20)).unwrap();
    pollster::block_on(source_tx.send_complete()).unwrap();
    drop(source_tx);
    drop(projected1_tx);
    drop(projected2_tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], Event::Next(v) if *v == 20));
    assert!(matches!(&events[1], Event::Complete));
}

/// A projection may return a **future** — the shape of an `async fn` client
/// call — thanks to `ObservableInput`; here it produces a value directly.
#[test]
fn switch_map_projects_to_a_future() {
    let values = pollster::block_on(
        local([1, 2])
            .switch_map(|v| async move { crate::of::<i32, String>(v * 10) })
            .collect(),
    )
    .expect("the stream completes cleanly");

    assert_eq!(values, vec![10, 20]);
}

/// A future projection that streams several values: `switch_map` waits for the
/// last projected stream, so every value travels.
#[test]
fn switch_map_future_may_stream_several_values() {
    let values = pollster::block_on(
        local([1, 2])
            .switch_map(|v| async move { crate::from::<i32, String, _>([v, v * 10]) })
            .collect(),
    )
    .expect("the stream completes cleanly");

    assert_eq!(values, vec![1, 10, 2, 20]);
}

/// The future form and the explicit `defer` form are the same operator
/// underneath: identical output.
#[test]
fn switch_map_future_matches_the_defer_form() {
    let via_future = pollster::block_on(
        local([1, 2, 3])
            .switch_map(|v| async move { crate::of::<i32, String>(v + 1) })
            .collect(),
    )
    .expect("clean");

    let via_defer = pollster::block_on(
        local([1, 2, 3])
            .switch_map(|v| crate::defer(move || async move { crate::of::<i32, String>(v + 1) }))
            .collect(),
    )
    .expect("clean");

    assert_eq!(via_future, via_defer);
}

/// Laziness: the projection runs at the first poll, never when the pipeline is
/// merely built — the promise `defer` makes, kept through `switch_map`.
#[test]
fn switch_map_future_is_not_projected_before_a_poll() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_in = Arc::clone(&calls);
    let stream = local([1]).switch_map(move |v| {
        calls_in.fetch_add(1, Ordering::SeqCst);
        async move { crate::of::<i32, String>(v * 10) }
    });

    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "nothing runs before a poll"
    );

    let events = pollster::block_on(drain(stream));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], Event::Next(v) if *v == 10));
    assert!(matches!(&events[1], Event::Complete));
}
