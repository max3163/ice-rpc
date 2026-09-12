//! Reactive layer of ice-rpc: operators, constructors and multicast
//! primitives, inspired by RxJS.
//!
//! The operators are **inherent methods** on [`crate::Observable`]: no extension
//! trait to import, one single stream type from the first operator to the last.
//!
//! ```rust,ignore
//! // `stream` is the native type returned by an ice-rpc service.
//! let stream: crate::Observable<i32, String> = proxy.foo().await;
//!
//! let odds = stream.filter(|v| *v % 2 == 1).map(|v| v * 10).take(5);
//! let first = odds.first_value().await?;
//! ```
//!
//! - [`crate::Observable`] — the operators (`map`, `filter`, `map_err`, `scan`,
//!   `switch_map`, `take`, `skip`, `first`, `first_with`, `start_with`, `tap`,
//!   `finalize`, `delay`, `timeout`, `catch_error`, `take_until`) and the
//!   terminals (`first_value`, `collect`, `for_each`, `subscribe`,
//!   `subscribe_with`, `next`, `recv`). Every operator is a pull-based
//!   combinator: no intermediate channel, no spawned task.
//! - [`Subject`] — the push side: a multi-producer / multi-consumer multicast
//!   source. [`Subject::new`] multicasts to the current subscribers,
//!   [`Subject::replay`] also replays the last `n` values (and the terminal
//!   state) to late subscribers.
//! - [`Observable::subscribe`] / [`Observable::subscribe_all`] — push-based
//!   consumption through RxJS-style callbacks. [`Subscription`] is the
//!   cancellation handle.
//! - [`from`], [`of`], [`throw_error`] — channel-free local constructors.
//!
//! ## Normalization
//!
//! ice-rpc can transport a single response as an internal `CompleteWith`
//! sample. [`crate::Observable::recv`] normalizes it into `Next` followed by
//! `Complete`, so consuming code always observes a uniform stream of `Next`
//! values followed by a terminal event (`Complete` or `Error`).
//!
//! ## Timeouts
//!
//! `discovery_timeout` (a **service-level** attribute) bounds the node discovery
//! performed before the call is sent; [`crate::Observable::timeout`] is a
//! per-event **silence watchdog** on an active stream, reset after every event.

mod creation;
mod subject;
mod subscribe;
mod transform;

pub use creation::{from, of, throw_error};
pub use subject::Subject;
pub use subscribe::Subscription;

/// Default capacity of the channels created by the multicast primitive.
///
/// A bounded channel provides backpressure: a producer waits when the queue is
/// full, which keeps memory usage bounded.
pub(crate) const MULTICAST_CHANNEL_CAPACITY: usize = 8;

#[cfg(test)]
mod tests {
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

    #[test]
    fn pipeline_single_value_keeps_one_wire_sample() {
        let mut stream: crate::Observable<i32, Infallible> = from([42]).map(|v| v);

        assert!(matches!(
            pollster::block_on(stream.recv_wire()),
            Ok(crate::gen::WireEvent::CompleteWith(42))
        ));
        assert!(pollster::block_on(stream.recv_wire()).is_err());
    }

    /// Terminal consumption on a plain [`crate::Observable`].
    fn native_first(
        events: Vec<Event<i32, String>>,
    ) -> Result<i32, crate::ObservableError<String>> {
        pollster::block_on(crate::Observable::<i32, String>::from_events(events).first_value())
    }

    /// Same input, consumed at the end of an operator pipeline.
    fn pipeline_first(
        events: Vec<Event<i32, String>>,
    ) -> Result<i32, crate::ObservableError<String>> {
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
}
