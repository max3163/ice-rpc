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
    /// Creates a share-replay from a source stream.
    ///
    /// The source is consumed on a background task; the last `Next` value is
    /// kept and replayed to new subscribers.
    pub fn new(source: Stream<T, E>) -> Self
    where
        T: Clone + Send + 'static,
        E: Clone + Send + 'static,
    {
        let state = std::sync::Arc::new(ice_rpc::async_lock::Mutex::new(ShareState {
            last: None,
            completed: false,
            error: None,
            subscribers: Vec::new(),
        }));
        let state_clone = state.clone();
        ice_rpc::rt::spawn(async move {
            while let Ok(event) = source.recv().await {
                let mut st = state_clone.lock().await;
                match event {
                    Event::Next(v) => {
                        st.last = Some(v.clone());
                        for tx in st.subscribers.iter() {
                            let _ = tx.send(Event::Next(v.clone())).await;
                        }
                    }
                    Event::Complete => {
                        st.completed = true;
                        for tx in st.subscribers.iter() {
                            let _ = tx.send(Event::Complete).await;
                        }
                        break;
                    }
                    Event::CompleteWith(v) => {
                        st.last = Some(v.clone());
                        for tx in st.subscribers.iter() {
                            let _ = tx.send(Event::Next(v.clone())).await;
                        }
                        st.completed = true;
                        for tx in st.subscribers.iter() {
                            let _ = tx.send(Event::Complete).await;
                        }
                        break;
                    }
                    Event::Error(e) => {
                        st.error = Some(e.clone());
                        for tx in st.subscribers.iter() {
                            let _ = tx.send(Event::Error(e.clone())).await;
                        }
                        break;
                    }
                    Event::RpcError(e) => {
                        for tx in st.subscribers.iter() {
                            let _ = tx.send(Event::RpcError(e.clone())).await;
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
    /// If a value was emitted before this call, it is replayed first, followed
    /// by the current terminal state (if any) and then the live stream.
    pub async fn subscribe(&self) -> Stream<T, E>
    where
        T: Clone,
        E: Clone,
    {
        let (tx, rx) = ice_rpc::channel::<T, E>(8);
        {
            let mut state = self.state.lock().await;
            if let Some(last) = &state.last {
                let _ = tx.send(Event::Next(last.clone())).await;
            }
            if let Some(err) = &state.error {
                let _ = tx.send(Event::Error(err.clone())).await;
            }
            if state.completed {
                let _ = tx.send(Event::Complete).await;
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
        let (tx, rx) = ice_rpc::channel::<i32, String>(8);
        pollster::block_on(tx.send(Event::Next(42))).unwrap();
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
}
