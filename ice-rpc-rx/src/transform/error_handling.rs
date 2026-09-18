//! Error Handling Operators.
//!
//! ReactiveX category: [`catch_error`](crate::Observable::catch_error) (Catch).

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::Event;

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
