//! Tests for the error-handling operators.

use crate::transform::test_support::{drain, is_technical};
use crate::{Event, ObservableError};

/// A recovery is a whole stream: its values *and* its terminal replace the
/// failed source.
#[test]
fn catch_error_switches_to_the_recovery_stream() {
    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let stream = rx.catch_error(|_| crate::from([10, 20]));

    pollster::block_on(tx.send_next(1)).unwrap();
    pollster::block_on(tx.send_error("boom".to_string())).unwrap();
    drop(tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 4);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(&events[1], Event::Next(v) if *v == 10));
    assert!(matches!(&events[2], Event::Next(v) if *v == 20));
    assert!(matches!(&events[3], Event::Complete));
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
        crate::of(-1)
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
        crate::of(-1)
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

/// The recovery may carry an error of its own: RxJS spells this `throwError` in
/// the selector.
#[test]
fn catch_error_can_rethrow() {
    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let stream = rx.catch_error(|_| crate::throw_error("recovered".to_string()));

    pollster::block_on(tx.send_error("boom".to_string())).unwrap();
    drop(tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0],
        Event::Error(ObservableError::Business(e)) if e == "recovered"
    ));
}

/// RxJS: the selector runs **once** — an error raised by the recovery is not
/// caught a second time.
#[test]
fn catch_error_does_not_catch_the_recovery_error() {
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;

    let (tx, rx) = crate::channel::<i32, String>(crate::MULTICAST_CHANNEL_CAPACITY);
    let calls = Arc::new(AtomicI32::new(0));
    let counter = calls.clone();
    let stream = rx.catch_error(move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
        crate::throw_error("still broken".to_string())
    });

    pollster::block_on(tx.send_error("boom".to_string())).unwrap();
    drop(tx);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0],
        Event::Error(ObservableError::Business(e)) if e == "still broken"
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1, "the selector runs once");
}

/// A source that closes without a terminal ends the stream for good: the old
/// code left its `done` flag unset and polled the dead source again.
#[test]
fn catch_error_does_not_repoll_a_closed_source() {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll};

    struct Closing {
        polls: Arc<AtomicI32>,
    }

    impl futures_lite::Stream for Closing {
        type Item = Event<i32, String>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(None)
        }
    }

    let polls = Arc::new(AtomicI32::new(0));
    let stream = crate::Observable::<i32, String>::from_stream(Closing {
        polls: polls.clone(),
    })
    .catch_error(|_| crate::of(0));

    let mut stream = Box::pin(stream);
    let mut cx = Context::from_waker(std::task::Waker::noop());

    assert!(matches!(
        futures_lite::Stream::poll_next(stream.as_mut(), &mut cx),
        Poll::Ready(None)
    ));
    assert!(matches!(
        futures_lite::Stream::poll_next(stream.as_mut(), &mut cx),
        Poll::Ready(None)
    ));
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "a closed source must not be polled twice"
    );
}
