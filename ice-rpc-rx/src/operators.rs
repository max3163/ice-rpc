//! Composable operators for [`ice_rpc::Stream`].
//!
//! The [`RxStreamExt`] trait extends the native [`ice_rpc::Stream`] type with
//! three classic operators:
//!
//! - [`map`](RxStreamExt::map) — transforms every `Next` value;
//! - [`filter`](RxStreamExt::filter) — keeps only the `Next` values matching a
//!   predicate;
//! - [`take`](RxStreamExt::take) — emits at most `n` `Next` values and then
//!   completes.
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
    /// Transforms every `Next` value with `f`.
    ///
    /// Terminal events (`Complete`, `Error`, `RpcError`) are passed through
    /// unchanged. For robustness, a `CompleteWith` value is also mapped.
    fn map<U, F>(self, f: F) -> Stream<U, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        U: Send + 'static,
        F: Fn(T) -> U + Send + 'static;

    /// Keeps only the `Next` values for which `f` returns `true`.
    ///
    /// Terminal events are passed through unchanged.
    fn filter<F>(self, f: F) -> Stream<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: Fn(&T) -> bool + Send + 'static;

    /// Emits at most `n` `Next` values, then forces a `Complete`.
    ///
    /// If the source completes before `n` values are emitted, the source
    /// terminal event is forwarded as-is.
    fn take(self, n: usize) -> Stream<T, E>
    where
        T: Send + 'static,
        E: Send + 'static;
}

impl<T, E> RxStreamExt<T, E> for Stream<T, E> {
    fn map<U, F>(self, f: F) -> Stream<U, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        U: Send + 'static,
        F: Fn(T) -> U + Send + 'static,
    {
        let (tx, rx) = ice_rpc::channel::<U, E>(8);
        ice_rpc::rt::spawn(async move {
            while let Ok(event) = self.recv().await {
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

    fn filter<F>(self, f: F) -> Stream<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: Fn(&T) -> bool + Send + 'static,
    {
        let (tx, rx) = ice_rpc::channel::<T, E>(8);
        ice_rpc::rt::spawn(async move {
            while let Ok(event) = self.recv().await {
                match event {
                    Event::Next(v) => {
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

    fn take(self, n: usize) -> Stream<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        let (tx, rx) = ice_rpc::channel::<T, E>(8);
        ice_rpc::rt::spawn(async move {
            let mut remaining = n;
            while let Ok(event) = self.recv().await {
                match event {
                    Event::Next(v) => {
                        if remaining == 0 {
                            let _ = tx.send(Event::Complete).await;
                            return;
                        }
                        remaining -= 1;
                        if tx.send(Event::Next(v)).await.is_err() {
                            return;
                        }
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
}

#[cfg(test)]
mod tests {
    use super::RxStreamExt;
    use ice_rpc::Event;

    #[test]
    fn map_filter_take_pipeline() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(8);
        pollster::block_on(tx.send(Event::Next(1))).unwrap();
        pollster::block_on(tx.send(Event::Next(2))).unwrap();
        pollster::block_on(tx.send(Event::Next(3))).unwrap();
        pollster::block_on(tx.send(Event::Complete)).unwrap();
        drop(tx);

        let stream = rx.filter(|v| *v % 2 == 1).map(|v| v * 10).take(2);

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
        assert_eq!(collected, vec![10, 30]);
    }
}
