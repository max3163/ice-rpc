//! Tests for the reactive operators.

use std::pin::Pin;

use super::RxStreamExt;
use ice_rpc::{Event, ObservableError};

/// Builds a local `Observable<T, String>` from an iterator (test helper).
fn local<T>(values: impl IntoIterator<Item = T>) -> ice_rpc::Observable<T, String> {
    crate::from::<T, String, _>(values)
}

/// Builds a local single-value `Observable<T, String>` (test helper).
fn single<T>(value: T) -> ice_rpc::Observable<T, String> {
    crate::of::<T, String>(value)
}

/// Drains a poll-based stream to completion.
async fn drain<S, T, E>(stream: S) -> Vec<Event<T, E>>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
{
    let mut stream = Box::pin(stream);
    let mut out = Vec::new();
    while let Some(event) =
        futures_lite::future::poll_fn(|cx| futures_lite::Stream::poll_next(stream.as_mut(), cx))
            .await
    {
        out.push(event);
    }
    out
}

/// Reads the next event from a boxed, pinned stream.
async fn next_event<S, T, E>(stream: &mut Pin<Box<S>>) -> Option<Event<T, E>>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
{
    futures_lite::future::poll_fn(|cx| futures_lite::Stream::poll_next(stream.as_mut(), cx)).await
}

/// Returns `true` when the event is a terminal technical error.
fn is_technical<T, E>(event: &Event<T, E>) -> bool {
    matches!(event, Event::Error(ObservableError::Technical(_)))
}

#[test]
fn map_filter_take_pipeline() {
    let (tx, rx) = ice_rpc::gen::channel::<i32, String>(6);
    pollster::block_on(tx.send_next(1)).unwrap();
    pollster::block_on(tx.send_next(2)).unwrap();
    pollster::block_on(tx.send_next(3)).unwrap();
    pollster::block_on(tx.send_next(4)).unwrap();
    pollster::block_on(tx.send_next(5)).unwrap();
    pollster::block_on(tx.send_complete()).unwrap();
    drop(tx);

    let stream = rx.filter(|v| *v % 2 == 1).map(|v| v * 10).take(3);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 4);
    assert!(matches!(&events[0], Event::Next(v) if *v == 10));
    assert!(matches!(&events[1], Event::Next(v) if *v == 30));
    assert!(matches!(&events[2], Event::Next(v) if *v == 50));
    assert!(matches!(&events[3], Event::Complete));
}

#[test]
fn finalize_runs_on_complete() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let (tx, rx) = ice_rpc::gen::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
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

    let (tx, rx) = ice_rpc::gen::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
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

    let (tx, rx) = ice_rpc::gen::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
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

    let (tx, rx) = ice_rpc::gen::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
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

    let (tx, rx) = ice_rpc::gen::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
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
    let (tx, rx) = ice_rpc::gen::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let stream = rx.delay(std::time::Duration::from_millis(20));

    pollster::block_on(tx.send_next(1)).unwrap();
    drop(tx);

    let mut stream = Box::pin(stream);
    let start = std::time::Instant::now();
    let event = pollster::block_on(next_event(&mut stream));
    let elapsed = start.elapsed();

    assert!(matches!(event, Some(Event::Next(1))));
    assert!(elapsed >= std::time::Duration::from_millis(15));
}

#[test]
fn delay_forwards_terminal_events() {
    let (tx, rx) = ice_rpc::gen::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let stream = rx.delay(std::time::Duration::from_millis(20));

    pollster::block_on(tx.send_complete()).unwrap();
    drop(tx);

    let mut stream = Box::pin(stream);
    let start = std::time::Instant::now();
    let event = pollster::block_on(next_event(&mut stream));
    let elapsed = start.elapsed();

    assert!(matches!(event, Some(Event::Complete)));
    assert!(elapsed >= std::time::Duration::from_millis(15));
}

#[test]
fn catch_error_replaces_error_with_fallback() {
    let (tx, rx) = ice_rpc::gen::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
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

    let (tx, rx) = ice_rpc::gen::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let called = Arc::new(AtomicBool::new(false));
    let flag = called.clone();
    let stream = rx.catch_error(move |_| {
        flag.store(true, Ordering::SeqCst);
        -1
    });

    pollster::block_on(tx.send_event(Event::Error(ObservableError::Technical(
        ice_rpc::RpcError::Timeout,
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

    let (tx, rx) = ice_rpc::gen::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
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

#[test]
fn map_maps_normalized_single_value() {
    let (tx, rx) = ice_rpc::gen::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
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
    let (tx, rx) = ice_rpc::gen::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let stream = rx.map(|v| v * 2);

    pollster::block_on(tx.send_error("boom".to_string())).unwrap();
    pollster::block_on(tx.send_event(Event::Error(ObservableError::Technical(
        ice_rpc::RpcError::Timeout,
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
fn filter_normalizes_complete_with_as_value() {
    let (tx, rx) = ice_rpc::gen::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
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
    let (tx, rx) = ice_rpc::gen::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let stream = rx.take(0);

    pollster::block_on(tx.send_next(1)).unwrap();
    drop(tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], Event::Complete));
}

#[test]
fn take_forwards_source_terminal_before_limit() {
    let (tx, rx) = ice_rpc::gen::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
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
    let (tx, rx) = ice_rpc::gen::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
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
fn map_err_transforms_error_type() {
    let (tx, rx) = ice_rpc::gen::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
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
fn start_with_prefixes_initial_value() {
    let stream = single(1).start_with(0);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 3);
    assert!(matches!(&events[0], Event::Next(v) if *v == 0));
    assert!(matches!(&events[1], Event::Next(v) if *v == 1));
    assert!(matches!(&events[2], Event::Complete));
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

#[test]
fn timeout_emits_technical_error_on_silence() {
    let (tx, rx) = ice_rpc::gen::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let stream = rx.timeout(std::time::Duration::from_millis(20));

    let mut stream = Box::pin(stream);
    let event = pollster::block_on(next_event(&mut stream));
    assert!(matches!(
        event,
        Some(Event::Error(ObservableError::Technical(_)))
    ));

    drop(tx);
}

#[test]
fn timeout_forwards_values_before_deadline() {
    let (tx, rx) = ice_rpc::gen::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let stream = rx.timeout(std::time::Duration::from_millis(200));

    pollster::block_on(tx.send_next(1)).unwrap();
    pollster::block_on(tx.send_complete()).unwrap();
    drop(tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(&events[1], Event::Complete));
}

#[test]
fn switch_map_switches_to_latest_inner_and_cancels_previous() {
    use std::sync::{Arc, Mutex};

    let (outer_tx, outer_rx) = ice_rpc::gen::channel::<i32, String>(8);
    let senders: Arc<Mutex<Vec<ice_rpc::gen::Sender<i32, String>>>> =
        Arc::new(Mutex::new(Vec::new()));

    let senders_for_task = senders.clone();
    let stream = outer_rx.switch_map(move |_| {
        let (tx, rx) = ice_rpc::gen::channel::<i32, String>(8);
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
    drop(outer_tx);

    assert!(matches!(
        pollster::block_on(next_event(&mut stream)),
        Some(Event::Complete)
    ));
}

#[test]
fn switch_map_forwards_inner_error() {
    let (outer_tx, outer_rx) = ice_rpc::gen::channel::<i32, String>(8);
    let (inner_tx, inner_rx) = ice_rpc::gen::channel::<i32, String>(8);

    let stream = outer_rx.switch_map(move |_| {
        inner_rx
            .try_clone()
            .expect("a channel-backed observable is clonable")
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
    let (outer_tx, outer_rx) = ice_rpc::gen::channel::<i32, String>(8);
    let (inner1_tx, inner1_rx) = ice_rpc::gen::channel::<i32, String>(8);
    let (inner2_tx, inner2_rx) = ice_rpc::gen::channel::<i32, String>(8);

    let stream = outer_rx.switch_map(move |v| {
        if v == 1 {
            inner1_rx
                .try_clone()
                .expect("a channel-backed observable is clonable")
        } else {
            inner2_rx
                .try_clone()
                .expect("a channel-backed observable is clonable")
        }
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

#[test]
fn take_until_emits_cancelled_when_token_fires() {
    let token = ice_rpc::CancellationToken::new();
    token.cancel();
    let stream = local([1, 2, 3]).take_until(&token);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 1);
    assert!(is_technical(&events[0]));
}

#[test]
fn take_until_forwards_values_when_not_cancelled() {
    let token = ice_rpc::CancellationToken::new();
    let stream = local([1, 2, 3]).take_until(&token);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 4);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(&events[1], Event::Next(v) if *v == 2));
    assert!(matches!(&events[2], Event::Next(v) if *v == 3));
    assert!(matches!(&events[3], Event::Complete));
}
