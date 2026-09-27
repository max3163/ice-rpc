//! Error Handling Operators.
//!
//! ReactiveX category: [`catch_error`](crate::Observable::catch_error) (Catch).
//!
//! The poll-based combinator and the `Observable` method that exposes it both
//! live in this file.

use std::pin::Pin;
use std::task::{Context, Poll};

use crate::Event;
use crate::Observable;

pin_project_lite::pin_project! {
    /// See [`Observable::catch_error`](crate::Observable::catch_error).
    pub struct CatchError<S, F, T, E> {
        #[pin]
        stream: S,
        // The selector, consumed by the first business error (`FnOnce`).
        f: Option<F>,
        // The recovery stream: it replaces the failed source entirely, values
        // and terminal included.
        #[pin]
        recovery: Option<Observable<T, E>>,
        // Set once our own terminal has been emitted.
        finished: bool,
    }
}

impl<S, F, T, E> CatchError<S, F, T, E> {
    pub(super) fn new(stream: S, f: F) -> Self {
        Self {
            stream,
            f: Some(f),
            recovery: None,
            finished: false,
        }
    }
}

impl<S, F, T, E> futures_lite::Stream for CatchError<S, F, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    F: FnOnce(E) -> Observable<T, E>,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if *this.finished {
                return Poll::Ready(None);
            }

            // Recovering: the recovery stream is the only source left, and its
            // terminal is ours.
            if this.recovery.is_some() {
                let polled = {
                    let recovery = this
                        .recovery
                        .as_mut()
                        .as_pin_mut()
                        .expect("recovery stream");
                    futures_lite::Stream::poll_next(recovery, cx)
                };
                return match polled {
                    Poll::Ready(Some(event)) => {
                        if event.is_terminal() {
                            *this.finished = true;
                        }
                        Poll::Ready(Some(event))
                    }
                    Poll::Ready(None) => {
                        *this.finished = true;
                        Poll::Ready(None)
                    }
                    Poll::Pending => Poll::Pending,
                };
            }

            match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
                Poll::Ready(Some(Event::Next(v))) => return Poll::Ready(Some(Event::Next(v))),
                // Only a business error is recoverable; a technical error and
                // `Empty` are fatal, so they travel untouched.
                Poll::Ready(Some(Event::Error(crate::ObservableError::Business(e)))) => {
                    // The selector runs exactly once: an error raised by the
                    // recovery is not caught again, as in RxJS.
                    let f = this
                        .f
                        .take()
                        .expect("the selector runs when the first business error arrives");
                    this.recovery.set(Some(f(e)));
                }
                Poll::Ready(Some(Event::Error(other))) => {
                    *this.finished = true;
                    return Poll::Ready(Some(Event::Error(other)));
                }
                Poll::Ready(Some(Event::Complete)) => {
                    *this.finished = true;
                    return Poll::Ready(Some(Event::Complete));
                }
                // An abrupt close ends the stream for good: `finished` is set, so
                // a later poll never touches the dead source again.
                Poll::Ready(None) => {
                    *this.finished = true;
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<T, E> Observable<T, E> {
    /// Replaces a **business** error with a fallback **stream** (RxJS
    /// `catchError`).
    ///
    /// On `ObservableError::Business(e)`, `f(e)` is subscribed in place of the
    /// failed source: its values **and** its terminal become the output. That is
    /// what makes an asynchronous recovery possible — a call to a replica, a
    /// delayed retry — and a rethrow possible too, through
    /// [`throw_error`](crate::throw_error).
    ///
    /// A technical error is **not** caught: it is forwarded and terminates the
    /// stream, because a service implementation cannot recover from a transport,
    /// discovery or protocol failure. `f` runs **at most once**: an error raised
    /// by the recovery is not caught again, exactly as in RxJS.
    ///
    /// # Example
    /// ```rust
    /// use ice_rpc_rx::{of, rt::block_on, throw_error};
    ///
    /// let recovered = block_on(
    ///     throw_error::<i32, String>("missing".to_string())
    ///         .catch_error(|_e| of(-1))
    ///         .collect(),
    /// )
    /// .expect("the failure becomes a value, then Complete");
    /// assert_eq!(recovered, vec![-1]);
    /// ```
    ///
    /// A recovery is a stream: it may emit several values, and it may fail in
    /// turn — its error then travels downstream, uncaught.
    /// ```rust
    /// use ice_rpc_rx::{from, rt::block_on, throw_error};
    ///
    /// let values = block_on(
    ///     throw_error::<i32, String>("missing".to_string())
    ///         .catch_error(|_e| from([1, 2]))
    ///         .collect(),
    /// )
    /// .expect("the recovery completes cleanly");
    /// assert_eq!(values, vec![1, 2]);
    /// ```
    pub fn catch_error<F>(self, f: F) -> Observable<T, E>
    where
        F: FnOnce(E) -> Observable<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        Observable::from_stream(CatchError::new(self, f))
    }
}

#[cfg(test)]
mod tests;
