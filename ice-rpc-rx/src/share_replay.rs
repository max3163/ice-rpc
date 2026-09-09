//! [`ShareReplay`]: multicast with replay of the last value.
//!
//! A share-replay is the local equivalent of RxJS `shareReplay(1)`. It
//! multicasts a source stream to all subscribers and replays the **last**
//! emitted value to any subscriber that arrives after it.
//!
//! # Example
//!
//! ```rust,ignore
//! use ice_rpc_rx::ShareReplay;
//!
//! let shared = ShareReplay::new(source_stream);
//! let rx = shared.subscribe().await; // replays the last value, if any
//! ```

use ice_rpc::{Event, Sender, Stream};

/// Multicast source that replays the last value to late subscribers.
pub struct ShareReplay<T, E> {
    state: std::sync::Arc<ice_rpc::async_lock::Mutex<ShareState<T, E>>>,
}

struct ShareState<T, E> {
    last: Option<T>,
    completed: bool,
    error: Option<E>,
    subscribers: Vec<Sender<T, E>>,
}

impl<T, E> ShareReplay<T, E> {
    /// Creates a [`ShareReplay`] from a source stream.
    ///
    /// The source is consumed on a background task as soon as this function is
    /// called. The last emitted `Next` value is kept in shared state and
    /// replayed to every new subscriber.
    ///
    /// # Arguments
    /// * `source` - The stream to multicast.
    ///
    /// # Returns
    /// A [`ShareReplay`] handle used to create subscriptions.
    ///
    /// # Example
    /// ```rust,ignore
    /// use ice_rpc_rx::ShareReplay;
    ///
    /// let shared = ShareReplay::new(source_stream);
    /// ```
    pub fn new<S>(source: S) -> Self
    where
        S: futures_lite::Stream<Item = Event<T, E>> + Send + 'static,
        T: Clone + Send + 'static,
        E: Clone + Send + 'static,
    {
        // Shared state is guarded by a mutex because the background source task
        // and `subscribe` both mutate it concurrently.
        let state = std::sync::Arc::new(ice_rpc::async_lock::Mutex::new(ShareState {
            last: None,
            completed: false,
            error: None,
            subscribers: Vec::new(),
        }));
        let state_clone = state.clone();
        ice_rpc::rt::spawn(async move {
            let mut source = Box::pin(source);
            while let Some(event) = futures_lite::future::poll_fn(|cx| {
                futures_lite::Stream::poll_next(source.as_mut(), cx)
            })
            .await
            {
                let mut st = state_clone.lock().await;
                match event {
                    Event::Next(v) => {
                        // Keep the latest value and multicast it to every
                        // current subscriber.
                        st.last = Some(v.clone());
                        for tx in st.subscribers.iter() {
                            let _ = tx.send_next(v.clone()).await;
                        }
                    }
                    Event::Complete => {
                        st.completed = true;
                        for tx in st.subscribers.iter() {
                            let _ = tx.send_complete().await;
                        }
                        break;
                    }
                    Event::Error(e) => {
                        st.error = Some(e.clone());
                        for tx in st.subscribers.iter() {
                            let _ = tx.send_error(e.clone()).await;
                        }
                        break;
                    }
                    Event::RpcError(e) => {
                        for tx in st.subscribers.iter() {
                            let _ = tx.send_event(Event::RpcError(e.clone())).await;
                        }
                        break;
                    }
                }
            }
        });
        Self { state }
    }

    /// Subscribes to the shared stream.
    ///
    /// The returned [`Stream`] first replays the last value (if any) and the
    /// current terminal state, then receives every subsequent live event.
    ///
    /// # Returns
    /// A [`Stream`] observing the replayed snapshot followed by the live
    /// events.
    ///
    /// # Example
    /// ```rust,ignore
    /// let rx = shared.subscribe().await;
    /// while let Ok(event) = rx.recv().await {
    ///     // handle the event
    /// }
    /// ```
    pub async fn subscribe(&self) -> Stream<T, E>
    where
        T: Clone,
        E: Clone,
    {
        let (tx, rx) = ice_rpc::channel::<T, E>(crate::OPERATOR_CHANNEL_CAPACITY);
        {
            let mut state = self.state.lock().await;
            // Replay the snapshot first: last value, then terminal state.
            // The subscriber is registered afterwards to receive live events.
            if let Some(last) = &state.last {
                let _ = tx.send_next(last.clone()).await;
            }
            if let Some(err) = &state.error {
                let _ = tx.send_error(err.clone()).await;
            }
            if state.completed {
                let _ = tx.send_complete().await;
            }
            state.subscribers.push(tx);
        }
        rx
    }
}

#[cfg(test)]
mod tests {
    use super::ShareReplay;
    use ice_rpc::Event;

    #[test]
    fn share_replay_replays_last_value() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        pollster::block_on(tx.send_next(42)).unwrap();
        drop(tx);

        let shared = ShareReplay::new(rx);
        // Wait for the source task to consume the value.
        std::thread::sleep(std::time::Duration::from_millis(50));

        let rx = pollster::block_on(shared.subscribe());
        match pollster::block_on(rx.recv()).unwrap() {
            Event::Next(v) => assert_eq!(v, 42),
            other => panic!("expected replayed Next, got {:?}", other),
        }
    }

    #[test]
    fn share_replay_replays_only_last_value() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        pollster::block_on(tx.send_next(1)).unwrap();
        pollster::block_on(tx.send_next(2)).unwrap();
        drop(tx);

        let shared = ShareReplay::new(rx);
        std::thread::sleep(std::time::Duration::from_millis(50));

        let rx = pollster::block_on(shared.subscribe());
        match pollster::block_on(rx.recv()).unwrap() {
            Event::Next(v) => assert_eq!(v, 2),
            other => panic!("expected replayed Next(2), got {:?}", other),
        }
    }

    #[test]
    fn share_replay_replays_complete_state() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        pollster::block_on(tx.send_complete()).unwrap();
        drop(tx);

        let shared = ShareReplay::new(rx);
        std::thread::sleep(std::time::Duration::from_millis(50));

        let rx = pollster::block_on(shared.subscribe());
        match pollster::block_on(rx.recv()).unwrap() {
            Event::Complete => {}
            other => panic!("expected Complete, got {:?}", other),
        }
    }

    #[test]
    fn share_replay_replays_error_state() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        pollster::block_on(tx.send_error("boom".to_string())).unwrap();
        drop(tx);

        let shared = ShareReplay::new(rx);
        std::thread::sleep(std::time::Duration::from_millis(50));

        let rx = pollster::block_on(shared.subscribe());
        match pollster::block_on(rx.recv()).unwrap() {
            Event::Error(e) => assert_eq!(e, "boom"),
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[test]
    fn share_replay_replays_complete_with_as_value_then_complete() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        pollster::block_on(tx.send_complete_with(7)).unwrap();
        drop(tx);

        let shared = ShareReplay::new(rx);
        std::thread::sleep(std::time::Duration::from_millis(50));

        let rx = pollster::block_on(shared.subscribe());
        match pollster::block_on(rx.recv()).unwrap() {
            Event::Next(v) => assert_eq!(v, 7),
            other => panic!("expected replayed Next(7), got {:?}", other),
        }
        match pollster::block_on(rx.recv()).unwrap() {
            Event::Complete => {}
            other => panic!("expected Complete, got {:?}", other),
        }
    }

    #[test]
    fn share_replay_multicasts_live_events() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let shared = ShareReplay::new(rx);
        let rx1 = pollster::block_on(shared.subscribe());
        let rx2 = pollster::block_on(shared.subscribe());

        pollster::block_on(tx.send_next(11)).unwrap();
        pollster::block_on(tx.send_complete()).unwrap();
        drop(tx);

        for rx in [rx1, rx2] {
            match pollster::block_on(rx.recv()).unwrap() {
                Event::Next(v) => assert_eq!(v, 11),
                other => panic!("expected Next(11), got {:?}", other),
            }
            match pollster::block_on(rx.recv()).unwrap() {
                Event::Complete => {}
                other => panic!("expected Complete, got {:?}", other),
            }
        }
    }
}
