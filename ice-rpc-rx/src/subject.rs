//! [`Subject`]: a multi-producer / multi-consumer multicast primitive.
//!
//! A subject is the local equivalent of an RxJS `Subject`. Producers push
//! values with [`next`](Subject::next), [`complete`](Subject::complete) and
//! [`error`](Subject::error); each subscriber receives every event emitted
//! after it subscribed.
//!
//! # Example
//!
//! ```rust,ignore
//! use ice_rpc_rx::Subject;
//!
//! let subject = Subject::<i32, String>::new();
//! let rx = subject.subscribe().await;
//!
//! subject.next(42).await;
//! subject.complete().await;
//! ```

use ice_rpc::{Event, Sender, Stream};

/// A multi-producer / multi-consumer multicast source.
pub struct Subject<T, E> {
    subscribers: std::sync::Arc<ice_rpc::async_lock::Mutex<Vec<Sender<T, E>>>>,
}

impl<T, E> Subject<T, E> {
    /// Creates an empty subject.
    pub fn new() -> Self {
        Self {
            subscribers: std::sync::Arc::new(ice_rpc::async_lock::Mutex::new(Vec::new())),
        }
    }

    /// Subscribes to this subject.
    ///
    /// The returned [`Stream`] only receives events emitted **after** this
    /// call; it does not replay past values.
    pub async fn subscribe(&self) -> Stream<T, E> {
        let (tx, rx) = ice_rpc::channel::<T, E>(8);
        self.subscribers.lock().await.push(tx);
        rx
    }

    /// Emits a value to all current subscribers.
    pub async fn next(&self, value: T)
    where
        T: Clone,
    {
        let mut subs = self.subscribers.lock().await;
        let mut dead = Vec::new();
        for (i, tx) in subs.iter().enumerate() {
            if tx.send(Event::Next(value.clone())).await.is_err() {
                dead.push(i);
            }
        }
        for i in dead.into_iter().rev() {
            subs.remove(i);
        }
    }

    /// Completes the stream for all current subscribers.
    pub async fn complete(&self) {
        let mut subs = self.subscribers.lock().await;
        let mut dead = Vec::new();
        for (i, tx) in subs.iter().enumerate() {
            if tx.send(Event::Complete).await.is_err() {
                dead.push(i);
            }
        }
        for i in dead.into_iter().rev() {
            subs.remove(i);
        }
    }

    /// Emits a business error to all current subscribers.
    pub async fn error(&self, err: E)
    where
        E: Clone,
    {
        let mut subs = self.subscribers.lock().await;
        let mut dead = Vec::new();
        for (i, tx) in subs.iter().enumerate() {
            if tx.send(Event::Error(err.clone())).await.is_err() {
                dead.push(i);
            }
        }
        for i in dead.into_iter().rev() {
            subs.remove(i);
        }
    }
}

impl<T, E> Default for Subject<T, E> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::Subject;
    use ice_rpc::Event;

    #[test]
    fn subject_multicasts_to_subscribers() {
        let subject = Subject::<i32, String>::new();
        let rx1 = pollster::block_on(subject.subscribe());
        let rx2 = pollster::block_on(subject.subscribe());
        pollster::block_on(subject.next(7));

        for rx in [rx1, rx2] {
            match pollster::block_on(rx.recv()).unwrap() {
                Event::Next(v) => assert_eq!(v, 7),
                other => panic!("expected Next, got {:?}", other),
            }
        }
    }
}
