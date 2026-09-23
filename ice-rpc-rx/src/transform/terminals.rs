//! Terminal consumption of an Observable.
//!
//! These methods **end** the chain instead of wrapping it: `for_each`,
//! `subscribe` and `subscribe_all` drain the pipeline rather than returning
//! another `Observable`. They live next to the operators because they are called
//! the same way — as inherent methods — and because `subscribe` spawns a task,
//! which is the one place the execution facade is required.

use crate::subscribe::{spawn_push, ObserverFns};
use crate::{Observable, ObservableError, Subscription};

impl<T, E> Observable<T, E> {
    /// Consumes the stream value by value (RxJS `forEach`).
    ///
    /// Fully pull-based: no task is spawned and nothing is buffered. Returns
    /// `Ok(())` on a normal end and the terminal error (business or technical)
    /// otherwise. Use it for its side effects when the values themselves are not
    /// needed; [`collect`](Self::collect) is the variant that keeps them.
    ///
    /// # Example
    /// ```rust
    /// use ice_rpc_rx::{from, rt::block_on};
    ///
    /// let mut seen = vec![];
    /// block_on(from::<i32, String, _>([1, 2, 3]).for_each(|v| seen.push(v)))
    ///     .expect("the stream completes cleanly");
    /// assert_eq!(seen, vec![1, 2, 3]);
    /// ```
    pub async fn for_each<F>(mut self, mut f: F) -> Result<(), ObservableError<E>>
    where
        F: FnMut(T),
    {
        while let Some(event) = self.next().await {
            match event {
                Ok(value) => f(value),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Subscribes with a value-only callback (RxJS `subscribe(next)`).
    ///
    /// **One** task pulls the stream and calls `on_next` for every value; errors
    /// and completion are ignored — use
    /// [`subscribe_all`](Self::subscribe_all) to observe them. This is the only
    /// operator that spawns, so it needs the execution facade.
    ///
    /// Dropping the returned [`Subscription`](crate::Subscription) cancels the
    /// task silently, as does
    /// [`unsubscribe`](crate::Subscription::unsubscribe). Await
    /// [`Subscription::closed`](crate::Subscription::closed) to know when the
    /// stream ended on its own.
    ///
    /// # Example
    /// ```rust,no_run
    /// use ice_rpc_rx::{from, rt::block_on};
    ///
    /// let sub = from::<i32, String, _>([1, 2, 3]).subscribe(|v| println!("next {v}"));
    /// // `sub.unsubscribe();` cancels before the end.
    /// block_on(sub.closed());
    /// ```
    pub fn subscribe<F>(self, on_next: F) -> Subscription
    where
        F: FnMut(T) + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.subscribe_all(on_next, |_| {}, || {})
    }

    /// Subscribes with the three RxJS callbacks: `on_next`, `on_error`
    /// (business **or** technical) and `on_complete`.
    ///
    /// The counterpart of `subscribe({ next, error, complete })` in RxJS: exactly
    /// one of `on_error` / `on_complete` runs, and neither runs when the
    /// [`Subscription`](crate::Subscription) is dropped or
    /// [`unsubscribe`](crate::Subscription::unsubscribe)d — a cancellation is
    /// silent by design, which is what makes a dropped handle a clean "stop
    /// listening". Needs the execution facade (it spawns one task).
    ///
    /// # Example
    /// ```rust,no_run
    /// use ice_rpc_rx::{from, rt::block_on, ObservableError};
    ///
    /// let sub = from::<i32, String, _>([1, 2, 3]).subscribe_all(
    ///     |v| println!("next {v}"),
    ///     |e: ObservableError<String>| eprintln!("error {e}"),
    ///     || println!("complete"),
    /// );
    /// block_on(sub.closed());
    /// ```
    pub fn subscribe_all<N, Er, C>(self, on_next: N, on_error: Er, on_complete: C) -> Subscription
    where
        N: FnMut(T) + Send + 'static,
        Er: FnMut(ObservableError<E>) + Send + 'static,
        C: FnMut() + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let cancel = crate::CancellationToken::new();
        spawn_push(
            self,
            ObserverFns::new(on_next, on_error, on_complete),
            cancel.clone(),
        );
        Subscription::new(cancel)
    }
}
