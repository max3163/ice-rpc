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

use ice_rpc::{Observable, Sender};

/// A multi-producer / multi-consumer multicast source.
pub struct Subject<T, E> {
    subscribers: std::sync::Arc<ice_rpc::async_lock::Mutex<Vec<Sender<T, E>>>>,
}

impl<T, E> Subject<T, E> {
    /// Creates a new empty [`Subject`].
    ///
    /// # Returns
    /// A [`Subject`] with no subscribers.
    ///
    /// # Example
    /// ```rust,ignore
    /// use ice_rpc_rx::Subject;
    ///
    /// let subject = Subject::<i32, String>::new();
    /// ```
    pub fn new() -> Self {
        Self {
            subscribers: std::sync::Arc::new(ice_rpc::async_lock::Mutex::new(Vec::new())),
        }
    }

    /// Subscribes to this subject.
    ///
    /// The returned [`Observable`] only receives events emitted after this call;
    /// past values are not replayed.
    ///
    /// # Returns
    /// An [`Observable`] receiving future events from this subject.
    ///
    /// # Example
    /// ```rust,ignore
    /// let rx = subject.subscribe().await;
    /// ```
    pub async fn subscribe(&self) -> Observable<T, E> {
        let (tx, rx) = ice_rpc::channel::<T, E>(crate::MULTICAST_CHANNEL_CAPACITY);
        self.subscribers.lock().await.push(tx);
        rx
    }

    /// Emits a value to all current subscribers.
    ///
    /// Subscribers whose channel is closed are removed during this call.
    ///
    /// # Arguments
    /// * `value` - The value to broadcast.
    ///
    /// # Example
    /// ```rust,ignore
    /// subject.next(42).await;
    /// ```
    pub async fn next(&self, value: T)
    where
        T: Clone,
    {
        let mut subs = self.subscribers.lock().await;
        // Subscribers whose channel is closed are pruned lazily here, so the
        // list never grows with dead receivers.
        let mut dead = Vec::new();
        for (i, tx) in subs.iter().enumerate() {
            if tx.send_next(value.clone()).await.is_err() {
                dead.push(i);
            }
        }
        // Remove from the end so earlier indices stay valid.
        for i in dead.into_iter().rev() {
            subs.remove(i);
        }
    }

    /// Completes the stream for all current subscribers.
    ///
    /// Dead subscribers are pruned after the broadcast.
    ///
    /// # Example
    /// ```rust,ignore
    /// subject.complete().await;
    /// ```
    pub async fn complete(&self) {
        let mut subs = self.subscribers.lock().await;
        // Prune dead receivers whose channel already closed.
        let mut dead = Vec::new();
        for (i, tx) in subs.iter().enumerate() {
            if tx.send_complete().await.is_err() {
                dead.push(i);
            }
        }
        for i in dead.into_iter().rev() {
            subs.remove(i);
        }
    }

    /// Emits a business error to all current subscribers.
    ///
    /// Dead subscribers are pruned after the broadcast.
    ///
    /// # Arguments
    /// * `err` - The error to broadcast.
    ///
    /// # Example
    /// ```rust,ignore
    /// subject.error("boom".to_string()).await;
    /// ```
    pub async fn error(&self, err: E)
    where
        E: Clone,
    {
        let mut subs = self.subscribers.lock().await;
        // Prune dead receivers whose channel already closed.
        let mut dead = Vec::new();
        for (i, tx) in subs.iter().enumerate() {
            if tx.send_error(err.clone()).await.is_err() {
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
    use ice_rpc::{Event, ObservableError};

    #[test]
    fn subject_multicasts_to_subscribers() {
        let subject = Subject::<i32, String>::new();
        let rx1 = pollster::block_on(subject.subscribe());
        let rx2 = pollster::block_on(subject.subscribe());
        pollster::block_on(subject.next(7));

        for mut rx in [rx1, rx2] {
            match pollster::block_on(rx.recv()).unwrap() {
                Event::Next(v) => assert_eq!(v, 7),
                other => panic!("expected Next, got {:?}", other),
            }
        }
    }

    #[test]
    fn subject_completes_subscribers() {
        let subject = Subject::<i32, String>::new();
        let mut rx = pollster::block_on(subject.subscribe());
        pollster::block_on(subject.complete());

        match pollster::block_on(rx.recv()).unwrap() {
            Event::Complete => {}
            other => panic!("expected Complete, got {:?}", other),
        }
    }

    #[test]
    fn subject_errors_subscribers() {
        let subject = Subject::<i32, String>::new();
        let mut rx = pollster::block_on(subject.subscribe());
        pollster::block_on(subject.error("boom".to_string()));

        match pollster::block_on(rx.recv()).unwrap() {
            Event::Error(ObservableError::Business(e)) => assert_eq!(e, "boom"),
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[test]
    fn subject_prunes_dead_subscribers() {
        let subject = Subject::<i32, String>::new();
        {
            // Dropping the receiver closes the channel, turning the sender dead.
            let _rx = pollster::block_on(subject.subscribe());
        }

        // Broadcasting to a dead subscriber prunes it without panicking, and
        // later subscribers still receive events.
        pollster::block_on(subject.next(1));
        pollster::block_on(subject.complete());

        let mut rx = pollster::block_on(subject.subscribe());
        pollster::block_on(subject.next(2));
        match pollster::block_on(rx.recv()).unwrap() {
            Event::Next(v) => assert_eq!(v, 2),
            other => panic!("expected Next, got {:?}", other),
        }
    }
}
