//! Helpers shared by the operator tests.
//!
//! Declared once, so `transform/<category>/tests.rs` never duplicates them:
//! they build local, channel-free observables and drive a poll-based stream to
//! completion. `#[cfg(test)]` at the declaration, so nothing here reaches a
//! normal build.

use std::pin::Pin;

use crate::{Event, ObservableError};

/// Builds a local `Observable<T, String>` from an iterator (test helper).
pub fn local<T>(values: impl IntoIterator<Item = T>) -> crate::Observable<T, String> {
    crate::from::<T, String, _>(values)
}

/// Builds a local single-value `Observable<T, String>` (test helper).
pub fn single<T>(value: T) -> crate::Observable<T, String> {
    crate::of::<T, String>(value)
}

/// Drains a poll-based stream to completion.
pub async fn drain<S, T, E>(stream: S) -> Vec<Event<T, E>>
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
pub async fn next_event<S, T, E>(stream: &mut Pin<Box<S>>) -> Option<Event<T, E>>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
{
    futures_lite::future::poll_fn(|cx| futures_lite::Stream::poll_next(stream.as_mut(), cx)).await
}

/// Returns `true` when the event is a terminal technical error.
pub fn is_technical<T, E>(event: &Event<T, E>) -> bool {
    matches!(event, Event::Error(ObservableError::Technical(_)))
}
