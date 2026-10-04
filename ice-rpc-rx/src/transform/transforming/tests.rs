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
fn switch_map_switches_to_latest_inner_and_cancels_previous() {
    use std::sync::{Arc, Mutex};

    let (outer_tx, outer_rx) = crate::channel::<i32, String>(8);
    let senders: Arc<Mutex<Vec<crate::Sender<i32, String>>>> = Arc::new(Mutex::new(Vec::new()));

    let senders_for_task = senders.clone();
    let stream = outer_rx.switch_map(move |_| {
        let (tx, rx) = crate::channel::<i32, String>(8);
        senders_for_task.lock().unwrap().push(tx);
        rx
    });

    pollster::block_on(outer_tx.send_next(1)).unwrap();
    pollster::block_on(outer_tx.send_next(2)).unwrap();

    let mut stream = Box::pin(stream);
    // A single poll drives the lazy combinator: it consumes both source
    // values and subscribes twice.
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let _ = futures_lite::Stream::poll_next(stream.as_mut(), &mut cx);
    assert_eq!(senders.lock().unwrap().len(), 2);

    let inner2_tx = senders.lock().unwrap()[1].clone();
    pollster::block_on(inner2_tx.send_next(20)).unwrap();

    assert!(matches!(
        pollster::block_on(next_event(&mut stream)),
        Some(Event::Next(v)) if v == 20
    ));

    let inner1_tx = senders.lock().unwrap()[0].clone();
    assert!(pollster::block_on(inner1_tx.send_next(10)).is_err());

    pollster::block_on(outer_tx.send_complete()).unwrap();

    // RxJS: the outer's completion waits for the inner in flight, so the
    // pipeline is not over while `inner2` is still open.
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(matches!(
        futures_lite::Stream::poll_next(stream.as_mut(), &mut cx),
        std::task::Poll::Pending
    ));

    let inner2_tx = senders.lock().unwrap()[1].clone();
    pollster::block_on(inner2_tx.send_complete()).unwrap();
    drop(inner2_tx);
    drop(outer_tx);

    assert!(matches!(
        pollster::block_on(next_event(&mut stream)),
        Some(Event::Complete)
    ));
}

/// Regression for P0-2: the last inner must still deliver after the outer has
/// completed — RxJS awaits it instead of dropping it.
#[test]
fn switch_map_waits_for_the_in_flight_inner_before_completing() {
    use std::cell::RefCell;

    let (outer_tx, outer_rx) = crate::channel::<i32, String>(8);
    let (inner_tx, inner_rx) = crate::channel::<i32, String>(8);

    let inner = RefCell::new(Some(inner_rx));
    let mut stream = Box::pin(outer_rx.switch_map(move |_| {
        inner
            .borrow_mut()
            .take()
            .expect("the inner channel is subscribed once")
    }));

    pollster::block_on(outer_tx.send_next(1)).unwrap();
    // The outer ends while the inner is still in flight.
    pollster::block_on(outer_tx.send_complete()).unwrap();

    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(matches!(
        futures_lite::Stream::poll_next(stream.as_mut(), &mut cx),
        std::task::Poll::Pending
    ));

    // The last inner still delivers: nothing is lost.
    pollster::block_on(inner_tx.send_next(10)).unwrap();
    assert!(matches!(
        pollster::block_on(next_event(&mut stream)),
        Some(Event::Next(v)) if v == 10
    ));

    pollster::block_on(inner_tx.send_complete()).unwrap();
    drop(inner_tx);
    drop(outer_tx);

    assert!(matches!(
        pollster::block_on(next_event(&mut stream)),
        Some(Event::Complete)
    ));
}

#[test]
fn switch_map_forwards_inner_error() {
    use std::cell::RefCell;

    let (outer_tx, outer_rx) = crate::channel::<i32, String>(8);
    let (inner_tx, inner_rx) = crate::channel::<i32, String>(8);

    // Each source value subscribes to a fresh inner stream: hand it out once.
    let inner = RefCell::new(Some(inner_rx));
    let stream = outer_rx.switch_map(move |_| {
        inner
            .borrow_mut()
            .take()
            .expect("the inner channel is subscribed once")
    });

    pollster::block_on(outer_tx.send_next(1)).unwrap();
    pollster::block_on(inner_tx.send_error("boom".to_string())).unwrap();
    drop(outer_tx);
    drop(inner_tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0],
        Event::Error(ObservableError::Business(e)) if e.as_str() == "boom"
    ));
}

#[test]
fn switch_map_ignores_inner_complete() {
    use std::cell::RefCell;

    let (outer_tx, outer_rx) = crate::channel::<i32, String>(8);
    let (inner1_tx, inner1_rx) = crate::channel::<i32, String>(8);
    let (inner2_tx, inner2_rx) = crate::channel::<i32, String>(8);

    let inner1 = RefCell::new(Some(inner1_rx));
    let inner2 = RefCell::new(Some(inner2_rx));
    let stream = outer_rx.switch_map(move |v| {
        let slot = if v == 1 { &inner1 } else { &inner2 };
        slot.borrow_mut()
            .take()
            .expect("each inner channel is subscribed once")
    });

    pollster::block_on(outer_tx.send_next(1)).unwrap();
    pollster::block_on(inner1_tx.send_complete()).unwrap();
    pollster::block_on(outer_tx.send_next(2)).unwrap();
    pollster::block_on(inner2_tx.send_next(20)).unwrap();
    pollster::block_on(outer_tx.send_complete()).unwrap();
    drop(outer_tx);
    drop(inner1_tx);
    drop(inner2_tx);

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
/// last inner, so every value travels.
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
