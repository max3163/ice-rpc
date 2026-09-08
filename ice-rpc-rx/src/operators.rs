//! Composable operators for [`ice_rpc::Stream`].
//!
//! The [`RxStreamExt`] trait extends the native [`ice_rpc::Stream`] type with
//! several classic operators:
//!
//! - [`map`](RxStreamExt::map) — transforms every `Next` value;
//! - [`filter`](RxStreamExt::filter) — keeps only the `Next` values matching a
//!   predicate;
//! - [`take`](RxStreamExt::take) — emits at most `n` `Next` values and then
//!   completes;
//! - [`first`](RxStreamExt::first) — emits only the first `Next` value;
//! - [`first_with`](RxStreamExt::first_with) — emits the first `Next` value
//!   matching a predicate;
//! - [`finalize`](RxStreamExt::finalize) — runs a callback once the stream
//!   terminates;
//! - [`tap`](RxStreamExt::tap) — runs a side effect on each value without
//!   altering it;
//! - [`delay`](RxStreamExt::delay) — delays every event by a duration;
//! - [`catch_error`](RxStreamExt::catch_error) — replaces an `Error` with a
//!   fallback value and completes.
//!
//! Every operator returns the native [`ice_rpc::Stream`] type, so they compose
//! without any wrapper or conversion.
//!
//! # Example
//!
//! ```rust,ignore
//! use ice_rpc_rx::RxStreamExt;
//!
//! let stream: ice_rpc::Stream<i32, String> = proxy.list().await?;
//! let top = stream.filter(|v| *v > 0).map(|v| v * 2).take(3);
//! ```

use ice_rpc::{Event, Stream};

/// Extension trait adding reactive operators to the native [`ice_rpc::Stream`].
pub trait RxStreamExt<T, E>: Sized {
    /// Transforms every `Next` value with the mapping function `f`.
    ///
    /// `CompleteWith` is normalized to a mapped value as well, so consumers
    /// always observe a uniform `Next` stream. Terminal events (`Complete`,
    /// `Error`, `RpcError`) are forwarded unchanged.
    ///
    /// # Arguments
    /// * `f` - Synchronous function applied to each emitted value.
    ///
    /// # Returns
    /// A new [`Stream`] whose values are of type `U`.
    ///
    /// # Example
    /// ```rust,ignore
    /// use ice_rpc_rx::RxStreamExt;
    ///
    /// let stream: ice_rpc::Stream<i32, String> = proxy.numbers().await?;
    /// let doubled = stream.map(|v| v * 2);
    /// ```
    fn map<U, F>(self, f: F) -> Stream<U, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        U: Send + 'static,
        F: Fn(T) -> U + Send + 'static;

    /// Keeps only the `Next` values for which the predicate `f` returns `true`.
    ///
    /// Filtered values are simply dropped. Terminal events (`Complete`,
    /// `Error`, `RpcError`, `CompleteWith`) are forwarded unchanged.
    ///
    /// # Arguments
    /// * `f` - Predicate applied to each value by reference.
    ///
    /// # Returns
    /// A new [`Stream`] of the same type `(T, E)`.
    ///
    /// # Example
    /// ```rust,ignore
    /// use ice_rpc_rx::RxStreamExt;
    ///
    /// let stream: ice_rpc::Stream<i32, String> = proxy.numbers().await?;
    /// let positives = stream.filter(|v| *v > 0);
    /// ```
    fn filter<F>(self, f: F) -> Stream<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: Fn(&T) -> bool + Send + 'static;

    /// Emits at most `n` `Next` values, then forces a `Complete`.
    ///
    /// If the source terminates before `n` values are emitted, the source
    /// terminal event is forwarded as-is.
    ///
    /// # Arguments
    /// * `n` - Maximum number of values to forward.
    ///
    /// # Returns
    /// A new [`Stream`] of the same type `(T, E)`.
    ///
    /// # Example
    /// ```rust,ignore
    /// use ice_rpc_rx::RxStreamExt;
    ///
    /// let stream: ice_rpc::Stream<i32, String> = proxy.numbers().await?;
    /// let first_three = stream.take(3);
    /// ```
    fn take(self, n: usize) -> Stream<T, E>
    where
        T: Send + 'static,
        E: Send + 'static;

    /// Emits only the first `Next` value, then forces a `Complete`.
    ///
    /// Equivalent to [`first_with`](RxStreamExt::first_with) with an
    /// always-true predicate. Terminal events are forwarded unchanged; if the
    /// source completes without emitting a value, a bare `Complete` is emitted.
    ///
    /// # Returns
    /// A new [`Stream`] of the same type `(T, E)`.
    ///
    /// # Example
    /// ```rust,ignore
    /// use ice_rpc_rx::RxStreamExt;
    ///
    /// let stream: ice_rpc::Stream<i32, String> = proxy.numbers().await?;
    /// let first = stream.first();
    /// ```
    fn first(self) -> Stream<T, E>
    where
        T: Send + 'static,
        E: Send + 'static;

    /// Emits the first `Next` value for which `predicate` returns `true`, then
    /// forces a `Complete`.
    ///
    /// Non-matching values are dropped. If the source terminates without a
    /// matching value, a bare `Complete` is emitted: the equivalent RxJS
    /// `EmptyError` is not representable while [`crate::RxError`] remains
    /// uninhabited.
    ///
    /// # Arguments
    /// * `predicate` - Predicate applied to each value by reference.
    ///
    /// # Returns
    /// A new [`Stream`] of the same type `(T, E)`.
    ///
    /// # Example
    /// ```rust,ignore
    /// use ice_rpc_rx::RxStreamExt;
    ///
    /// let stream: ice_rpc::Stream<i32, String> = proxy.numbers().await?;
    /// let first_positive = stream.first_with(|v| *v > 0);
    /// ```
    fn first_with<F>(self, predicate: F) -> Stream<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnMut(&T) -> bool + Send + 'static;

    /// Runs `f` exactly once when the stream terminates.
    ///
    /// The callback runs on `Complete`, `CompleteWith`, `Error`, `RpcError`,
    /// or when the channel is closed (consumer dropped). It is the equivalent
    /// of RxJS `finalize`.
    ///
    /// # Arguments
    /// * `f` - Callback executed once at termination.
    ///
    /// # Returns
    /// A new [`Stream`] of the same type `(T, E)`.
    ///
    /// # Example
    /// ```rust,ignore
    /// use ice_rpc_rx::RxStreamExt;
    ///
    /// let stream: ice_rpc::Stream<i32, String> = proxy.numbers().await?;
    /// let stream = stream.finalize(|| println!("done"));
    /// ```
    fn finalize<F>(self, f: F) -> Stream<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce() + Send + 'static;

    /// Runs a side effect for every `Next` value without altering the stream.
    ///
    /// The callback is invoked by mutable reference, so it can maintain state.
    /// Terminal events are forwarded unchanged.
    ///
    /// # Arguments
    /// * `f` - Side effect applied to each value by reference.
    ///
    /// # Returns
    /// A new [`Stream`] of the same type `(T, E)`.
    ///
    /// # Example
    /// ```rust,ignore
    /// use ice_rpc_rx::RxStreamExt;
    ///
    /// let stream: ice_rpc::Stream<i32, String> = proxy.numbers().await?;
    /// let logged = stream.tap(|v| println!("value: {v}"));
    /// ```
    fn tap<F>(self, f: F) -> Stream<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnMut(&T) + Send + 'static;

    /// Delays every event by `duration` before forwarding it.
    ///
    /// # Arguments
    /// * `duration` - Delay applied before each event is emitted.
    ///
    /// # Returns
    /// A new [`Stream`] of the same type `(T, E)`.
    ///
    /// # Example
    /// ```rust,ignore
    /// use std::time::Duration;
    /// use ice_rpc_rx::RxStreamExt;
    ///
    /// let stream: ice_rpc::Stream<i32, String> = proxy.numbers().await?;
    /// let delayed = stream.delay(Duration::from_millis(50));
    /// ```
    fn delay(self, duration: std::time::Duration) -> Stream<T, E>
    where
        T: Send + 'static,
        E: Send + 'static;

    /// Replaces an `Error` with a fallback value and completes.
    ///
    /// When the source emits `Error(e)`, this operator emits `Next(f(e))`
    /// followed by `Complete`, then stops. All other events are forwarded
    /// unchanged.
    ///
    /// # Arguments
    /// * `f` - Handler producing the recovery value from the error.
    ///
    /// # Returns
    /// A new [`Stream`] of the same type `(T, E)`.
    ///
    /// # Example
    /// ```rust,ignore
    /// use ice_rpc_rx::RxStreamExt;
    ///
    /// let stream: ice_rpc::Stream<i32, String> = proxy.numbers().await?;
    /// let safe = stream.catch_error(|e| {
    ///     eprintln!("error: {e}");
    ///     -1
    /// });
    /// ```
    fn catch_error<F>(self, f: F) -> Stream<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(E) -> T + Send + 'static;
}

impl<T, E> RxStreamExt<T, E> for Stream<T, E> {
    /// See [`RxStreamExt::map`].
    fn map<U, F>(self, f: F) -> Stream<U, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        U: Send + 'static,
        F: Fn(T) -> U + Send + 'static,
    {
        // Bridge the source into a fresh channel: the operator returns the same
        // native `Stream` type, so it composes with the other operators.
        let (tx, rx) = ice_rpc::channel::<U, E>(crate::OPERATOR_CHANNEL_CAPACITY);
        ice_rpc::rt::spawn(async move {
            while let Ok(event) = self.recv().await {
                // Only `Next` values are mapped; terminal events and
                // `CompleteWith` are forwarded unchanged.
                let mapped = match event {
                    Event::Next(v) => Event::Next(f(v)),
                    Event::Complete => Event::Complete,
                    Event::CompleteWith(v) => Event::CompleteWith(f(v)),
                    Event::Error(e) => Event::Error(e),
                    Event::RpcError(e) => Event::RpcError(e),
                };
                if tx.send(mapped).await.is_err() {
                    return;
                }
            }
        });
        rx
    }

    /// See [`RxStreamExt::filter`].
    fn filter<F>(self, f: F) -> Stream<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: Fn(&T) -> bool + Send + 'static,
    {
        let (tx, rx) = ice_rpc::channel::<T, E>(crate::OPERATOR_CHANNEL_CAPACITY);
        ice_rpc::rt::spawn(async move {
            while let Ok(event) = self.recv().await {
                match event {
                    // Only `Next` values are filtered; terminal events are
                    // forwarded as-is.
                    Event::Next(v) => {
                        // The predicate decides whether to forward; stop as
                        // soon as the consumer is gone.
                        if f(&v) && tx.send(Event::Next(v)).await.is_err() {
                            return;
                        }
                    }
                    other => {
                        if tx.send(other).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });
        rx
    }

    /// See [`RxStreamExt::take`].
    fn take(self, n: usize) -> Stream<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        let (tx, rx) = ice_rpc::channel::<T, E>(crate::OPERATOR_CHANNEL_CAPACITY);
        ice_rpc::rt::spawn(async move {
            // Number of `Next` values still allowed before completing.
            let mut remaining = n;
            while let Ok(event) = self.recv().await {
                match event {
                    Event::Next(v) => {
                        // Budget exhausted: force a `Complete` and stop.
                        if remaining == 0 {
                            let _ = tx.send(Event::Complete).await;
                            return;
                        }
                        remaining -= 1;
                        if tx.send(Event::Next(v)).await.is_err() {
                            return;
                        }
                        // The last allowed value was sent: complete now.
                        if remaining == 0 {
                            let _ = tx.send(Event::Complete).await;
                            return;
                        }
                    }
                    other => {
                        if tx.send(other).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });
        rx
    }

    /// See [`RxStreamExt::first`].
    fn first(self) -> Stream<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        // `first` is `first_with` with an always-true predicate: the first
        // value observed is the one emitted.
        self.first_with(|_| true)
    }

    /// See [`RxStreamExt::first_with`].
    fn first_with<F>(self, mut predicate: F) -> Stream<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnMut(&T) -> bool + Send + 'static,
    {
        let (tx, rx) = ice_rpc::channel::<T, E>(crate::OPERATOR_CHANNEL_CAPACITY);
        ice_rpc::rt::spawn(async move {
            while let Ok(event) = self.recv().await {
                match event {
                    Event::Next(v) => {
                        // First matching value: emit it, then complete.
                        if predicate(&v) {
                            let _ = tx.send(Event::Next(v)).await;
                            let _ = tx.send(Event::Complete).await;
                            return;
                        }
                        // Non-matching value: drop it and keep scanning.
                    }
                    Event::CompleteWith(v) => {
                        // Terminal single value: emit it only if it matches.
                        if predicate(&v) {
                            let _ = tx.send(Event::Next(v)).await;
                        }
                        let _ = tx.send(Event::Complete).await;
                        return;
                    }
                    Event::Complete => {
                        // Source completed before any value: complete empty.
                        let _ = tx.send(Event::Complete).await;
                        return;
                    }
                    Event::Error(e) => {
                        let _ = tx.send(Event::Error(e)).await;
                        return;
                    }
                    Event::RpcError(e) => {
                        let _ = tx.send(Event::RpcError(e)).await;
                        return;
                    }
                }
            }
        });
        rx
    }

    /// See [`RxStreamExt::finalize`].
    fn finalize<F>(self, f: F) -> Stream<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce() + Send + 'static,
    {
        let (tx, rx) = ice_rpc::channel::<T, E>(crate::OPERATOR_CHANNEL_CAPACITY);
        ice_rpc::rt::spawn(async move {
            // `FnOnce` is stored in an `Option` so it can be taken exactly once.
            let mut f = Some(f);
            while let Ok(event) = self.recv().await {
                // A terminal event completes the stream; `CompleteWith` is also
                // terminal because it carries the single, final value.
                let terminal = matches!(
                    &event,
                    Event::Complete | Event::CompleteWith(_) | Event::Error(_) | Event::RpcError(_)
                );
                if tx.send(event).await.is_err() {
                    // The consumer is gone: still run finalize, then stop.
                    if let Some(cb) = f.take() {
                        cb();
                    }
                    return;
                }
                if terminal {
                    if let Some(cb) = f.take() {
                        cb();
                    }
                    return;
                }
            }
            // The source channel closed without an explicit terminal event.
            if let Some(cb) = f.take() {
                cb();
            }
        });
        rx
    }

    /// See [`RxStreamExt::tap`].
    fn tap<F>(self, mut f: F) -> Stream<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnMut(&T) + Send + 'static,
    {
        let (tx, rx) = ice_rpc::channel::<T, E>(crate::OPERATOR_CHANNEL_CAPACITY);
        ice_rpc::rt::spawn(async move {
            while let Ok(event) = self.recv().await {
                // Apply the side effect on `Next` values only, then forward the
                // event unchanged.
                if let Event::Next(v) = &event {
                    f(v);
                }
                if tx.send(event).await.is_err() {
                    return;
                }
            }
        });
        rx
    }

    /// See [`RxStreamExt::delay`].
    fn delay(self, duration: std::time::Duration) -> Stream<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        let (tx, rx) = ice_rpc::channel::<T, E>(crate::OPERATOR_CHANNEL_CAPACITY);
        ice_rpc::rt::spawn(async move {
            while let Ok(event) = self.recv().await {
                // Delay every event (values and terminal events) before
                // forwarding.
                ice_rpc::rt::sleep(duration).await;
                if tx.send(event).await.is_err() {
                    return;
                }
            }
        });
        rx
    }

    /// See [`RxStreamExt::catch_error`].
    fn catch_error<F>(self, f: F) -> Stream<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(E) -> T + Send + 'static,
    {
        let (tx, rx) = ice_rpc::channel::<T, E>(crate::OPERATOR_CHANNEL_CAPACITY);
        ice_rpc::rt::spawn(async move {
            let mut f = Some(f);
            while let Ok(event) = self.recv().await {
                match event {
                    Event::Error(e) => {
                        // Recover once: emit the fallback value, then complete.
                        if let Some(handler) = f.take() {
                            let _ = tx.send(Event::Next(handler(e))).await;
                        }
                        let _ = tx.send(Event::Complete).await;
                        return;
                    }
                    other => {
                        if tx.send(other).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });
        rx
    }
}

#[cfg(test)]
mod tests {
    use super::RxStreamExt;
    use crate::{from, of};
    use ice_rpc::Event;

    #[test]
    fn map_filter_take_pipeline() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(6);
        pollster::block_on(tx.send(Event::Next(1))).unwrap();
        pollster::block_on(tx.send(Event::Next(2))).unwrap();
        pollster::block_on(tx.send(Event::Next(3))).unwrap();
        pollster::block_on(tx.send(Event::Next(4))).unwrap();
        pollster::block_on(tx.send(Event::Next(5))).unwrap();
        pollster::block_on(tx.send(Event::Complete)).unwrap();
        drop(tx);

        let stream = rx.filter(|v| *v % 2 == 1).map(|v| v * 10).take(3);

        let collected = pollster::block_on(async {
            let mut out = Vec::new();
            while let Ok(ev) = stream.recv().await {
                match ev {
                    Event::Next(v) => out.push(v),
                    Event::Complete => break,
                    other => panic!("unexpected event: {:?}", other),
                }
            }
            out
        });
        assert_eq!(collected, vec![10, 30, 50]);
    }

    #[test]
    fn finalize_runs_on_complete() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let finalized = Arc::new(AtomicBool::new(false));
        let flag = finalized.clone();
        let stream = rx.finalize(move || flag.store(true, Ordering::SeqCst));

        pollster::block_on(tx.send(Event::Next(1))).unwrap();
        pollster::block_on(tx.send(Event::Complete)).unwrap();
        drop(tx);

        let collected = pollster::block_on(async {
            let mut out = Vec::new();
            while let Ok(ev) = stream.recv().await {
                match ev {
                    Event::Next(v) => out.push(v),
                    Event::Complete => break,
                    other => panic!("unexpected event: {:?}", other),
                }
            }
            out
        });
        assert_eq!(collected, vec![1]);
        // Give the background task a moment to run finalize after Complete.
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(finalized.load(Ordering::SeqCst));
    }

    #[test]
    fn tap_runs_side_effect() {
        use std::sync::atomic::{AtomicI32, Ordering};
        use std::sync::Arc;

        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let seen = Arc::new(AtomicI32::new(0));
        let flag = seen.clone();
        let stream = rx.tap(move |_| {
            flag.fetch_add(1, Ordering::SeqCst);
        });

        pollster::block_on(tx.send(Event::Next(1))).unwrap();
        pollster::block_on(tx.send(Event::Next(2))).unwrap();
        pollster::block_on(tx.send(Event::Complete)).unwrap();
        drop(tx);

        pollster::block_on(async { while stream.recv().await.is_ok() {} });
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(seen.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn delay_postpones_events() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.delay(std::time::Duration::from_millis(20));

        pollster::block_on(tx.send(Event::Next(1))).unwrap();
        drop(tx);

        let start = std::time::Instant::now();
        let event = pollster::block_on(stream.recv());
        let elapsed = start.elapsed();

        assert!(matches!(event, Ok(Event::Next(1))));
        assert!(elapsed >= std::time::Duration::from_millis(15));
    }

    #[test]
    fn catch_error_replaces_error_with_fallback() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.catch_error(|_| -1);

        pollster::block_on(tx.send(Event::Next(1))).unwrap();
        pollster::block_on(tx.send(Event::Error("boom".to_string()))).unwrap();
        drop(tx);

        let events = pollster::block_on(async {
            let mut out = Vec::new();
            while let Ok(ev) = stream.recv().await {
                match ev {
                    Event::Next(v) => out.push(v),
                    Event::Complete => break,
                    other => panic!("unexpected event: {:?}", other),
                }
            }
            out
        });
        assert_eq!(events, vec![1, -1]);
    }

    /// Collects every event until the operator closes its output channel.
    async fn drain<T, E>(stream: &ice_rpc::Stream<T, E>) -> Vec<Event<T, E>> {
        let mut out = Vec::new();
        while let Ok(ev) = stream.recv().await {
            out.push(ev);
        }
        out
    }

    #[test]
    fn map_maps_complete_with_value() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.map(|v| v * 2);

        pollster::block_on(tx.send(Event::CompleteWith(5))).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(&stream));
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Event::CompleteWith(v) if *v == 10));
    }

    #[test]
    fn map_forwards_terminal_events_unchanged() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.map(|v| v * 2);

        pollster::block_on(tx.send(Event::Error("boom".to_string()))).unwrap();
        pollster::block_on(tx.send(Event::RpcError(ice_rpc::RpcError::Timeout))).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(&stream));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Error(e) if e.as_str() == "boom"));
        assert!(matches!(&events[1], Event::RpcError(_)));
    }

    #[test]
    fn filter_forwards_terminal_events_unchanged() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.filter(|v| *v % 2 == 1);

        pollster::block_on(tx.send(Event::Next(1))).unwrap();
        pollster::block_on(tx.send(Event::Next(2))).unwrap();
        pollster::block_on(tx.send(Event::CompleteWith(9))).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(&stream));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        assert!(matches!(&events[1], Event::CompleteWith(v) if *v == 9));
    }

    #[test]
    fn take_zero_completes_without_forwarding() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.take(0);

        pollster::block_on(tx.send(Event::Next(1))).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(&stream));
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Event::Complete));
    }

    #[test]
    fn take_forwards_source_terminal_before_limit() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.take(5);

        pollster::block_on(tx.send(Event::Next(1))).unwrap();
        pollster::block_on(tx.send(Event::Next(2))).unwrap();
        pollster::block_on(tx.send(Event::Error("boom".to_string()))).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(&stream));
        assert_eq!(events.len(), 3);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        assert!(matches!(&events[1], Event::Next(v) if *v == 2));
        assert!(matches!(&events[2], Event::Error(e) if e.as_str() == "boom"));
    }

    #[test]
    fn finalize_runs_on_source_channel_close() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let finalized = Arc::new(AtomicBool::new(false));
        let flag = finalized.clone();
        let stream = rx.finalize(move || flag.store(true, Ordering::SeqCst));

        pollster::block_on(tx.send(Event::Next(1))).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(&stream));
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(finalized.load(Ordering::SeqCst));
    }

    #[test]
    fn finalize_runs_on_error() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let finalized = Arc::new(AtomicBool::new(false));
        let flag = finalized.clone();
        let stream = rx.finalize(move || flag.store(true, Ordering::SeqCst));

        pollster::block_on(tx.send(Event::Error("boom".to_string()))).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(&stream));
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Event::Error(e) if e.as_str() == "boom"));
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(finalized.load(Ordering::SeqCst));
    }

    #[test]
    fn tap_does_not_touch_terminal_events() {
        use std::sync::atomic::{AtomicI32, Ordering};
        use std::sync::Arc;

        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let seen = Arc::new(AtomicI32::new(0));
        let flag = seen.clone();
        let stream = rx.tap(move |_| {
            flag.fetch_add(1, Ordering::SeqCst);
        });

        pollster::block_on(tx.send(Event::Next(1))).unwrap();
        pollster::block_on(tx.send(Event::Error("boom".to_string()))).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(&stream));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        assert!(matches!(&events[1], Event::Error(e) if e.as_str() == "boom"));
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn delay_forwards_terminal_events() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.delay(std::time::Duration::from_millis(20));

        pollster::block_on(tx.send(Event::Complete)).unwrap();
        drop(tx);

        let start = std::time::Instant::now();
        let event = pollster::block_on(stream.recv());
        let elapsed = start.elapsed();

        assert!(matches!(event, Ok(Event::Complete)));
        assert!(elapsed >= std::time::Duration::from_millis(15));
    }

    #[test]
    fn catch_error_forwards_rpc_error_unchanged() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let called = Arc::new(AtomicBool::new(false));
        let flag = called.clone();
        let stream = rx.catch_error(move |_| {
            flag.store(true, Ordering::SeqCst);
            -1
        });

        pollster::block_on(tx.send(Event::RpcError(ice_rpc::RpcError::Timeout))).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(&stream));
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Event::RpcError(_)));
        assert!(!called.load(Ordering::SeqCst));
    }

    #[test]
    fn catch_error_passthrough_when_no_error() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let called = Arc::new(AtomicBool::new(false));
        let flag = called.clone();
        let stream = rx.catch_error(move |_| {
            flag.store(true, Ordering::SeqCst);
            -1
        });

        pollster::block_on(tx.send(Event::Next(1))).unwrap();
        pollster::block_on(tx.send(Event::Complete)).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(&stream));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        assert!(matches!(&events[1], Event::Complete));
        assert!(!called.load(Ordering::SeqCst));
    }

    #[test]
    fn first_emits_only_first_value() {
        let stream = from([1, 2, 3]).first();

        let events = pollster::block_on(drain(&stream));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        assert!(matches!(&events[1], Event::Complete));
    }

    #[test]
    fn first_with_emits_first_matching_value() {
        let stream = from([1, 2, 3]).first_with(|v| *v >= 2);

        let events = pollster::block_on(drain(&stream));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 2));
        assert!(matches!(&events[1], Event::Complete));
    }

    #[test]
    fn first_forwards_error_before_any_value() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.first();

        pollster::block_on(tx.send(Event::Error("boom".to_string()))).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(&stream));
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Event::Error(e) if e.as_str() == "boom"));
    }

    #[test]
    fn first_completes_empty_when_no_value() {
        let stream = from(std::iter::empty::<i32>()).first();

        let events = pollster::block_on(drain(&stream));
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Event::Complete));
    }

    #[test]
    fn first_with_completes_empty_when_no_match() {
        let stream = from([1, 2, 3]).first_with(|v| *v > 10);

        let events = pollster::block_on(drain(&stream));
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Event::Complete));
    }

    #[test]
    fn first_with_matches_single_of_value() {
        let stream = of(7).first_with(|v| *v > 5);

        let events = pollster::block_on(drain(&stream));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 7));
        assert!(matches!(&events[1], Event::Complete));
    }
}
