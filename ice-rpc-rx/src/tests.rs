//! Tests of the reactive layer: the event vocabulary, the `Observable` stream,
//! the operators and the terminals.

use super::*;

// ── Event ───────────────────────────────────────────────────────────

#[test]
fn event_is_terminal_flags() {
    assert!(!Event::<i32, String>::Next(1).is_terminal());
    assert!(Event::<i32, String>::Complete.is_terminal());
    assert!(Event::<i32, String>::Error(ObservableError::Business("e".into())).is_terminal());
    assert!(
        Event::<i32, String>::Error(ObservableError::Technical(RpcError::Timeout)).is_terminal()
    );
}

#[test]
fn observable_error_predicates() {
    let business: ObservableError<String> = ObservableError::Business("boom".into());
    assert_eq!(business.as_business(), Some(&"boom".to_string()));
    assert!(business.as_technical().is_none());

    let technical: ObservableError<String> = ObservableError::Technical(RpcError::Timeout);
    assert!(technical.as_business().is_none());
    assert!(technical.as_technical().is_some());
}

#[test]
fn observable_error_display() {
    let business: ObservableError<String> = ObservableError::Business("boom".into());
    assert_eq!(business.to_string(), "boom");

    let technical: ObservableError<String> = ObservableError::Technical(RpcError::Timeout);
    assert!(technical.to_string().contains("timeout"));

    let empty: ObservableError<String> = ObservableError::Empty;
    assert_eq!(empty.to_string(), "stream ended without a value");
    assert!(empty.as_business().is_none());
    assert!(empty.as_technical().is_none());
    assert!(!empty.is_business());
    assert!(!empty.is_technical());
}

// ── RpcError ────────────────────────────────────────────────────────

#[test]
fn rpc_error_is_retryable_classification() {
    assert!(RpcError::TransportError("boom".into()).is_retryable());
    assert!(RpcError::Timeout.is_retryable());
    assert!(!RpcError::SerializationError.is_retryable());
    assert!(!RpcError::Cancelled.is_retryable());
    assert!(!RpcError::Internal("boom".into()).is_retryable());
}

#[test]
fn rpc_error_display_variants() {
    assert!(RpcError::SerializationError
        .to_string()
        .contains("serialization"));
    assert!(RpcError::Timeout.to_string().contains("timeout"));
    assert!(RpcError::Cancelled.to_string().contains("cancelled"));
    assert!(RpcError::TransportError("boom".into())
        .to_string()
        .contains("boom"));
}

// ── Observable ──────────────────────────────────────────────────────

#[test]
fn stream_yields_a_single_value_then_completion() {
    let (tx, mut rx) = channel::<i32, String>(4);
    tx.try_send_complete_with(42).unwrap();
    drop(tx);

    assert_eq!(pollster::block_on(rx.recv()).unwrap(), Event::Next(42));
    assert_eq!(pollster::block_on(rx.recv()).unwrap(), Event::Complete);
}

#[test]
fn from_events_is_channel_free_and_replays_in_order() {
    let mut stream =
        Observable::<i32, String>::from_events([Event::Next(1), Event::Next(2), Event::Complete]);
    assert_eq!(pollster::block_on(stream.recv()).unwrap(), Event::Next(1));
    assert_eq!(pollster::block_on(stream.recv()).unwrap(), Event::Next(2));
    assert_eq!(pollster::block_on(stream.recv()).unwrap(), Event::Complete);
}

#[test]
fn from_technical_error_emits_single_terminal_error() {
    let mut stream = Observable::<i32, String>::from_technical_error(RpcError::Timeout);
    match pollster::block_on(stream.recv()).unwrap() {
        Event::Error(ObservableError::Technical(RpcError::Timeout)) => {}
        other => panic!("expected a technical error, got {other:?}"),
    }
}

#[test]
fn channel_close_without_terminal_is_reported_as_closed() {
    let mut stream = Observable::<i32, String>::from_events([Event::Next(1)]);
    assert_eq!(pollster::block_on(stream.recv()).unwrap(), Event::Next(1));
    assert!(pollster::block_on(stream.recv()).is_err());
}

#[test]
fn next_maps_the_event_vocabulary_and_keeps_none_for_a_clean_end() {
    let mut stream = Observable::<i32, String>::from_events([Event::Next(1), Event::Complete]);
    assert_eq!(pollster::block_on(stream.next()), Some(Ok(1)));
    // `Complete` was delivered, so `None` means "the producer finished".
    assert_eq!(pollster::block_on(stream.next()), None);

    let mut failing = Observable::<i32, String>::from_events([Event::Error(
        ObservableError::Business("boom".into()),
    )]);
    assert!(matches!(
        pollster::block_on(failing.next()),
        Some(Err(ObservableError::Business(_)))
    ));
    assert_eq!(pollster::block_on(failing.next()), None);
}

#[test]
fn next_turns_an_abrupt_close_into_a_technical_error() {
    // The producer disappears without completing: `next` reports it.
    let (tx, mut stream) = channel::<i32, String>(4);
    tx.try_send_next(1).unwrap();
    drop(tx);

    assert_eq!(pollster::block_on(stream.next()), Some(Ok(1)));
    match pollster::block_on(stream.next()) {
        Some(Err(ObservableError::Technical(RpcError::TransportError(message)))) => {
            assert!(
                message.contains("before completion"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected an abrupt-close technical error, got {other:?}"),
    }
    // The failure is reported once, then the stream reads as finished.
    assert_eq!(pollster::block_on(stream.next()), None);
}

#[test]
fn a_relayed_terminal_error_reaches_the_consumer_unchanged() {
    let (tx, mut stream) = channel::<i32, String>(4);
    tx.try_send_event(Event::Error(ObservableError::Business("boom".into())))
        .unwrap();
    drop(tx);
    assert_eq!(
        pollster::block_on(stream.recv()).unwrap(),
        Event::Error(ObservableError::Business("boom".to_string()))
    );
}

// ── terminal combinators ────────────────────────────────────────────

#[test]
fn first_event_returns_first_value() {
    let stream = Observable::<i32, String>::from_events([Event::Next(5), Event::Complete]);
    assert_eq!(pollster::block_on(first_event(stream)).unwrap(), 5);
}

#[test]
fn first_event_reports_business_error() {
    let stream = Observable::<i32, String>::from_events([Event::Error(ObservableError::Business(
        "boom".into(),
    ))]);
    assert!(matches!(
        pollster::block_on(first_event(stream)),
        Err(ObservableError::Business(_))
    ));
}

#[test]
fn collect_values_gathers_values_and_reports_business_error() {
    let stream =
        Observable::<i32, String>::from_events([Event::Next(1), Event::Next(2), Event::Complete]);
    assert_eq!(
        pollster::block_on(collect_values(stream)).unwrap(),
        vec![1, 2]
    );

    let failing = Observable::<i32, String>::from_events([Event::Error(
        ObservableError::Business("boom".into()),
    )]);
    assert!(pollster::block_on(collect_values(failing)).is_err());
}

use super::{from, of, throw_error};
use crate::{Event, ObservableError};
use std::convert::Infallible;

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

#[test]
fn from_emits_values_then_complete() {
    let events: Vec<Event<i32, Infallible>> = pollster::block_on(drain(from([1, 2, 3])));
    assert_eq!(events.len(), 4);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(&events[1], Event::Next(v) if *v == 2));
    assert!(matches!(&events[2], Event::Next(v) if *v == 3));
    assert!(matches!(&events[3], Event::Complete));
}

#[test]
fn of_emits_next_then_complete() {
    let events: Vec<Event<i32, Infallible>> = pollster::block_on(drain(of(42)));
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], Event::Next(v) if *v == 42));
    assert!(matches!(&events[1], Event::Complete));
}

#[test]
fn collect_gathers_all_values() {
    let stream: crate::Observable<i32, Infallible> = from([1, 2, 3]);
    let values = pollster::block_on(stream.collect()).unwrap();
    assert_eq!(values, vec![1, 2, 3]);
}

#[test]
fn of_returns_a_channel_free_observable() {
    let stream: crate::Observable<i32, Infallible> = of(7);
    let values = pollster::block_on(stream.collect()).unwrap();
    assert_eq!(values, vec![7]);
}

#[test]
fn a_pipeline_stays_one_observable_type() {
    // An operator returns the same `Observable` type as its source, so a
    // pipeline can be returned by a service method as-is: no `into_observable`.
    let stream: crate::Observable<i32, Infallible> = from([1, 2, 3]).map(|v| v * 2);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 4);
    assert!(matches!(&events[0], Event::Next(v) if *v == 2));
    assert!(matches!(&events[1], Event::Next(v) if *v == 4));
    assert!(matches!(&events[2], Event::Next(v) if *v == 6));
    assert!(matches!(&events[3], Event::Complete));
}

/// Terminal consumption on a plain [`crate::Observable`].
fn native_first(events: Vec<Event<i32, String>>) -> Result<i32, crate::ObservableError<String>> {
    pollster::block_on(crate::Observable::<i32, String>::from_events(events).first_value())
}

/// Same input, consumed at the end of an operator pipeline.
fn pipeline_first(events: Vec<Event<i32, String>>) -> Result<i32, crate::ObservableError<String>> {
    pollster::block_on(
        crate::Observable::<i32, String>::from_events(events)
            .map(|v| v)
            .first_value(),
    )
}

/// Same pair, for `collect`.
fn native_collect(
    events: Vec<Event<i32, String>>,
) -> Result<Vec<i32>, crate::ObservableError<String>> {
    pollster::block_on(crate::Observable::<i32, String>::from_events(events).collect())
}

fn pipeline_collect(
    events: Vec<Event<i32, String>>,
) -> Result<Vec<i32>, crate::ObservableError<String>> {
    pollster::block_on(
        crate::Observable::<i32, String>::from_events(events)
            .map(|v| v)
            .collect(),
    )
}

/// Every outcome, fed to both terminal surfaces, must be identical.
fn terminal_cases() -> Vec<Vec<Event<i32, String>>> {
    vec![
        // Value then `Complete`.
        vec![Event::Next(5), Event::Complete],
        // Values then `Complete`.
        vec![Event::Next(1), Event::Next(2), Event::Complete],
        // Business error, before and after a value.
        vec![Event::Error(ObservableError::Business("boom".into()))],
        vec![
            Event::Next(1),
            Event::Error(ObservableError::Business("boom".into())),
        ],
        // Technical error.
        vec![Event::Error(ObservableError::Technical(
            crate::RpcError::Timeout,
        ))],
        // Empty.
        vec![Event::Complete],
        vec![],
    ]
}

#[test]
fn terminal_first_value_surfaces_agree_on_every_outcome() {
    for case in terminal_cases() {
        let native = native_first(case.clone());
        let pipeline = pipeline_first(case.clone());
        assert_eq!(
            format!("{native:?}"),
            format!("{pipeline:?}"),
            "first_value diverged on {case:?}"
        );
    }

    assert_eq!(
        native_first(vec![Event::Next(5), Event::Complete]).unwrap(),
        5
    );
    assert!(matches!(
        pipeline_first(vec![Event::Complete]),
        Err(crate::ObservableError::Empty)
    ));
}

#[test]
fn terminal_collect_surfaces_agree_on_every_outcome() {
    for case in terminal_cases() {
        let native = native_collect(case.clone());
        let pipeline = pipeline_collect(case.clone());
        assert_eq!(
            format!("{native:?}"),
            format!("{pipeline:?}"),
            "collect diverged on {case:?}"
        );
    }

    assert_eq!(
        native_collect(vec![Event::Next(1), Event::Next(2), Event::Complete]).unwrap(),
        vec![1, 2]
    );
    assert!(matches!(
        pipeline_collect(vec![Event::Next(1)]),
        Ok(values) if values == vec![1]
    ));
}

#[test]
fn throw_error_emits_business_error() {
    let events: Vec<Event<i32, String>> =
        pollster::block_on(drain(throw_error::<i32, String>("boom".into())));
    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0],
        Event::Error(ObservableError::Business(e)) if e == "boom"
    ));
}
