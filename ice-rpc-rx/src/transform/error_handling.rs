//! Error Handling Operators.
//!
//! ReactiveX category: [`catch_error`](crate::Observable::catch_error) (Catch).
//!
//! The poll-based combinator and the `Observable` method that exposes it both
//! live in this file.

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::Event;
use crate::Observable;

pin_project_lite::pin_project! {
    /// See [`Observable::catch_error`](crate::Observable::catch_error).
    pub struct CatchError<S, F, T, E> {
        #[pin]
        stream: S,
        f: Option<F>,
        completed: bool,
        done: bool,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, F, T, E> CatchError<S, F, T, E> {
    pub(super) fn new(stream: S, f: F) -> Self {
        Self {
            stream,
            f: Some(f),
            completed: false,
            done: false,
            _marker: PhantomData,
        }
    }
}

impl<S, F, T, E> futures_lite::Stream for CatchError<S, F, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    F: FnOnce(E) -> T,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.done {
            return Poll::Ready(None);
        }
        if *this.completed {
            *this.done = true;
            return Poll::Ready(Some(Event::Complete));
        }
        match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
            Poll::Ready(Some(Event::Next(v))) => Poll::Ready(Some(Event::Next(v))),
            // Only a business error can be caught; a technical error is fatal.
            Poll::Ready(Some(Event::Error(crate::ObservableError::Business(e)))) => {
                match this.f.take() {
                    Some(f) => {
                        *this.completed = true;
                        Poll::Ready(Some(Event::Next(f(e))))
                    }
                    None => {
                        *this.done = true;
                        Poll::Ready(Some(Event::Complete))
                    }
                }
            }
            Poll::Ready(Some(other)) => {
                *this.done = true;
                Poll::Ready(Some(other))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T, E> Observable<T, E> {
    /// Replaces a **business** error with a fallback value, then completes
    /// (RxJS `catchError`).
    ///
    /// On `ObservableError::Business(e)`, `f(e)` is emitted as one `Next` value
    /// followed by `Complete`, so the caller observes a normal stream that
    /// recovered. A technical error is **not** caught: it is forwarded and
    /// terminates the stream, because a service implementation cannot recover
    /// from a transport, discovery or protocol failure.
    ///
    /// # Example
    /// ```rust
    /// use ice_rpc_rx::{rt::block_on, throw_error};
    ///
    /// let recovered = block_on(
    ///     throw_error::<i32, String>("missing".to_string())
    ///         .catch_error(|_e| -1)
    ///         .collect(),
    /// )
    /// .expect("the failure becomes a value, then Complete");
    /// assert_eq!(recovered, vec![-1]);
    /// ```
    pub fn catch_error<F>(self, f: F) -> Observable<T, E>
    where
        F: FnOnce(E) -> T + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(CatchError::new(self, f))
    }
}

#[cfg(test)]
mod tests;
