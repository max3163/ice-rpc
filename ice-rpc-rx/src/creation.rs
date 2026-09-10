//! Stream creation helpers.
//!
//! [`from`] and [`of`] build local observables, mirroring the RxJS constructors
//! of the same name. They are backed by [`ice_rpc::Observable::from_events`], so
//! they allocate no channel and spawn no task: the events are produced lazily
//! by the consumer's own polling.

use ice_rpc::{Event, Observable, ObservableError};

/// Creates an observable from an iterator, emitting each value as `Next` then
/// `Complete`.
///
/// Equivalent to RxJS `from`.
///
/// # Example
/// ```rust,ignore
/// use ice_rpc_rx::from;
///
/// let stream: ice_rpc::Observable<i32, String> = from([1, 2, 3]);
/// ```
pub fn from<T, E, I>(iter: I) -> Observable<T, E>
where
    I: IntoIterator<Item = T>,
{
    let mut events: Vec<Event<T, E>> = iter.into_iter().map(Event::Next).collect();
    events.push(Event::Complete);
    ice_rpc::Observable::from_events(events)
}

/// Creates a single-value observable.
///
/// Consumers observe the value as `Next` followed by `Complete`. Equivalent to
/// RxJS `of`.
///
/// # Example
/// ```rust,ignore
/// use ice_rpc_rx::of;
///
/// let stream: ice_rpc::Observable<i32, String> = of(42);
/// ```
pub fn of<T, E>(value: T) -> Observable<T, E> {
    ice_rpc::Observable::from_events([Event::Next(value), Event::Complete])
}

/// Creates an observable that only emits a business error.
///
/// Equivalent to RxJS `throwError`: the stream terminates on
/// [`Event::Error`] with the business variant of
/// [`ObservableError`](ice_rpc::ObservableError). This is the counterpart of
/// [`of`] for single-response services that must fail on the business channel.
///
/// # Example
/// ```rust,ignore
/// use ice_rpc_rx::throw_error;
///
/// let stream: ice_rpc::Observable<i32, MyError> = throw_error(MyError::NotFound);
/// ```
pub fn throw_error<T, E>(error: E) -> Observable<T, E> {
    ice_rpc::Observable::from_events([Event::Error(ObservableError::Business(error))])
}
