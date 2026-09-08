//! Stream combination and resilience operators.
//!
//! Free functions that combine several streams ([`merge`]) or re-invoke an
//! underlying call on failure ([`retry`], [`retry_with`], [`retry_with_delay`]).

use ice_rpc::{Event, Stream};

/// Merges multiple streams into one, forwarding events from all of them.
///
/// The returned stream closes once every source stream is consumed. Ordering
/// between sources is not deterministic.
pub fn merge<T, E>(streams: Vec<Stream<T, E>>) -> Stream<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
{
    let (tx, rx) = ice_rpc::channel::<T, E>(crate::OPERATOR_CHANNEL_CAPACITY);
    for stream in streams {
        let tx = tx.clone();
        ice_rpc::rt::spawn(async move {
            while let Ok(event) = stream.recv().await {
                if tx.send_event(event).await.is_err() {
                    return;
                }
            }
        });
    }
    // The channel closes once every spawned task has dropped its sender clone.
    drop(tx);
    rx
}

/// Retries the source factory on a business `Error`, up to `retries` times.
///
/// The factory is re-invoked on each retry, so it typically wraps a proxy call.
pub fn retry<T, E, Fut, F>(factory: F, retries: usize) -> Stream<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ice_rpc::Observable<T, E>> + Send,
{
    retry_impl(factory, retries, |_| true, None)
}

/// Retries the source factory only when `should_retry` accepts the error.
pub fn retry_with<T, E, Fut, F, P>(factory: F, retries: usize, should_retry: P) -> Stream<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ice_rpc::Observable<T, E>> + Send,
    P: Fn(&E) -> bool + Send + 'static,
{
    retry_impl(factory, retries, should_retry, None)
}

/// Retries the source factory, sleeping `delay` between attempts.
pub fn retry_with_delay<T, E, Fut, F>(
    factory: F,
    retries: usize,
    delay: std::time::Duration,
) -> Stream<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ice_rpc::Observable<T, E>> + Send,
{
    retry_impl(factory, retries, |_| true, Some(delay))
}

/// Shared implementation of the retry operators.
fn retry_impl<T, E, Fut, F, P>(
    mut factory: F,
    retries: usize,
    should_retry: P,
    delay: Option<std::time::Duration>,
) -> Stream<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ice_rpc::Observable<T, E>> + Send,
    P: Fn(&E) -> bool + Send + 'static,
{
    let (tx, rx) = ice_rpc::channel::<T, E>(crate::OPERATOR_CHANNEL_CAPACITY);
    ice_rpc::rt::spawn(async move {
        let mut remaining = retries;
        loop {
            let stream = match factory().await {
                Err(e) => {
                    // Technical call failure: forward and stop.
                    let _ = tx.send_event(Event::RpcError(e)).await;
                    return;
                }
                Ok(s) => s,
            };

            let mut retried = false;
            while let Ok(event) = stream.recv().await {
                match event {
                    Event::Error(e) => {
                        if remaining > 0 && should_retry(&e) {
                            remaining -= 1;
                            retried = true;
                        } else {
                            let _ = tx.send_error(e).await;
                        }
                        break;
                    }
                    other => {
                        let terminal = matches!(&other, Event::Complete | Event::RpcError(_));
                        if tx.send_event(other).await.is_err() {
                            return;
                        }
                        if terminal {
                            return;
                        }
                    }
                }
            }

            if !retried {
                // Stream ended (or the error was forwarded) without a retry.
                return;
            }
            if let Some(d) = delay {
                ice_rpc::rt::sleep(d).await;
            }
        }
    });
    rx
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::of;
    use ice_rpc::Event;

    #[test]
    fn merge_combines_streams() {
        let stream = merge(vec![of(1), of(2)]);
        let mut values = Vec::new();
        let mut completed = 0;
        while let Ok(ev) = pollster::block_on(stream.recv()) {
            match ev {
                Event::Next(v) => values.push(v),
                Event::Complete => completed += 1,
                other => panic!("unexpected event: {:?}", other),
            }
        }
        values.sort_unstable();
        assert_eq!(values, vec![1, 2]);
        assert_eq!(completed, 2);
    }

    #[test]
    fn retry_recovers_after_business_error() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();
        let factory = move || {
            let a = attempts_clone.clone();
            async move {
                let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                let (tx, rx) = ice_rpc::channel::<i32, String>(2);
                if n < 3 {
                    let _ = tx.try_send_error("boom".to_string());
                } else {
                    let _ = tx.try_send_next(42);
                    let _ = tx.try_send_complete();
                }
                Ok(rx)
            }
        };

        let stream = retry(factory, 2);
        let events = pollster::block_on(async {
            let mut out = Vec::new();
            while let Ok(ev) = stream.recv().await {
                out.push(ev);
            }
            out
        });
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 42));
        assert!(matches!(&events[1], Event::Complete));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn retry_with_respects_predicate() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();
        let factory = move || {
            let a = attempts_clone.clone();
            async move {
                let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                let (tx, rx) = ice_rpc::channel::<i32, String>(1);
                if n == 1 {
                    let _ = tx.try_send_error("retryable".to_string());
                } else {
                    let _ = tx.try_send_error("fatal".to_string());
                }
                Ok(rx)
            }
        };

        // Only "retryable" errors trigger a retry; "fatal" is forwarded.
        let stream = retry_with(factory, 3, |e| e == "retryable");
        let events = pollster::block_on(async {
            let mut out = Vec::new();
            while let Ok(ev) = stream.recv().await {
                out.push(ev);
            }
            out
        });
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Event::Error(e) if e == "fatal"));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
