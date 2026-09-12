//! [`Subject`]: the single multicast primitive (push side of the stream API).
//!
//! A subject is the local equivalent of an RxJS `Subject`: producers push with
//! [`Subject::next`], [`Subject::error`] and [`Subject::complete`], and each
//! subscriber receives the events emitted after it subscribed. With
//! [`Subject::replay`], the last `n` values **and** the terminal state are also
//! handed to late subscribers, which covers the `shareReplay(1)` use case
//! without a second type.
//!
//! # Example
//!
//! ```rust,ignore
//! use crate::Subject;
//!
//! let subject = Subject::<i32, String>::new();
//! let rx = subject.subscribe().await;
//!
//! subject.next(42).await;
//! subject.complete().await;
//! ```

use std::collections::VecDeque;

use crate::gen::Sender;
use crate::{Event, Observable, ObservableError};

/// A multi-producer / multi-consumer multicast source.
///
/// `Subject::new()` multicasts only to the subscribers present at emission time
/// (RxJS `Subject`); `Subject::replay(n)` keeps the last `n` values and the
/// terminal state and replays them to late subscribers (`ReplaySubject(n)`).
///
/// Once terminated every later event is **ignored**, and a new subscriber
/// immediately observes the replayed terminal state.
pub struct Subject<T, E> {
    state: std::sync::Arc<crate::gen::async_lock::Mutex<State<T, E>>>,
}

struct State<T, E> {
    /// Last values handed to late subscribers (`0` = no replay).
    replay: VecDeque<T>,
    /// Maximum size of [`State::replay`].
    capacity: usize,
    completed: bool,
    error: Option<ObservableError<E>>,
    subscribers: Vec<Sender<T, E>>,
}

impl<T, E> State<T, E> {
    /// Returns `true` once `complete` or `error` was broadcast.
    fn is_terminated(&self) -> bool {
        self.completed || self.error.is_some()
    }

    /// Drops the subscribers whose channel is already closed, so the list never
    /// grows with dead receivers.
    fn prune(&mut self, dead: Vec<usize>) {
        // Remove from the end so earlier indices stay valid.
        for index in dead.into_iter().rev() {
            self.subscribers.remove(index);
        }
    }
}

impl<T, E> Subject<T, E> {
    /// Creates a subject without replay.
    ///
    /// # Example
    /// ```rust,ignore
    /// let subject = Subject::<i32, String>::new();
    /// ```
    pub fn new() -> Self {
        Self::with_replay(0)
    }

    /// Creates a subject that replays the last `replay` values (and the
    /// terminal state) to every late subscriber.
    ///
    /// `Subject::replay(1)` is the `shareReplay(1)` idiom: a subscriber arriving
    /// after the last update immediately receives it instead of waiting for the
    /// next one.
    ///
    /// # Example
    /// ```rust,ignore
    /// let subject = Subject::<Status, String>::replay(1);
    /// ```
    pub fn replay(replay: usize) -> Self {
        Self::with_replay(replay)
    }

    fn with_replay(capacity: usize) -> Self {
        Self {
            state: std::sync::Arc::new(crate::gen::async_lock::Mutex::new(State {
                replay: VecDeque::with_capacity(capacity),
                capacity,
                completed: false,
                error: None,
                subscribers: Vec::new(),
            })),
        }
    }

    /// Subscribes to this subject.
    ///
    /// The returned [`Observable`] first replays what the subject kept
    /// (the last values, then `Complete` or `Error` if it is terminated), then
    /// receives the live events.
    ///
    /// # Example
    /// ```rust,ignore
    /// let rx = subject.subscribe().await;
    /// ```
    pub async fn subscribe(&self) -> Observable<T, E>
    where
        T: Clone,
        E: Clone,
    {
        let (tx, rx) = crate::gen::channel::<T, E>(crate::rx::MULTICAST_CHANNEL_CAPACITY);
        {
            let mut state = self.state.lock().await;
            // Replay the snapshot first, then register the subscriber so that no
            // live event can slip in-between.
            for value in &state.replay {
                let _ = tx.send_next(value.clone()).await;
            }
            if let Some(error) = &state.error {
                let _ = tx.send_event(Event::Error(error.clone())).await;
            }
            if state.completed {
                let _ = tx.send_complete().await;
            }
            // A terminated subject never emits again: registering would only
            // leave a sender that is never used.
            if !state.is_terminated() {
                state.subscribers.push(tx);
            }
        }
        rx
    }

    /// Emits a value to all current subscribers.
    ///
    /// Ignored once the subject is terminated.
    ///
    /// # Example
    /// ```rust,ignore
    /// subject.next(42).await;
    /// ```
    pub async fn next(&self, value: T)
    where
        T: Clone,
    {
        let mut state = self.state.lock().await;
        if state.is_terminated() {
            return;
        }
        if state.capacity > 0 {
            if state.replay.len() == state.capacity {
                state.replay.pop_front();
            }
            state.replay.push_back(value.clone());
        }
        let mut dead = Vec::new();
        for (index, tx) in state.subscribers.iter().enumerate() {
            if tx.send_next(value.clone()).await.is_err() {
                dead.push(index);
            }
        }
        state.prune(dead);
    }

    /// Ends the stream for all current (and future) subscribers.
    ///
    /// # Example
    /// ```rust,ignore
    /// subject.complete().await;
    /// ```
    pub async fn complete(&self) {
        let mut state = self.state.lock().await;
        if state.is_terminated() {
            return;
        }
        state.completed = true;
        let mut dead = Vec::new();
        for (index, tx) in state.subscribers.iter().enumerate() {
            if tx.send_complete().await.is_err() {
                dead.push(index);
            }
        }
        state.prune(dead);
    }

    /// Emits a terminal **business** error to all current (and future)
    /// subscribers.
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
        let mut state = self.state.lock().await;
        if state.is_terminated() {
            return;
        }
        let error = ObservableError::Business(err.clone());
        state.error = Some(error.clone());
        let mut dead = Vec::new();
        for (index, tx) in state.subscribers.iter().enumerate() {
            if tx.send_event(Event::Error(error.clone())).await.is_err() {
                dead.push(index);
            }
        }
        state.prune(dead);
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
    use crate::{Event, ObservableError};

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

        let mut rx = pollster::block_on(subject.subscribe());
        pollster::block_on(subject.next(2));
        match pollster::block_on(rx.recv()).unwrap() {
            Event::Next(v) => assert_eq!(v, 2),
            other => panic!("expected Next, got {:?}", other),
        }
    }

    #[test]
    fn subject_ignores_events_after_termination() {
        let subject = Subject::<i32, String>::new();
        let mut rx = pollster::block_on(subject.subscribe());
        pollster::block_on(subject.complete());
        // RxJS semantics: a terminated subject is inert.
        pollster::block_on(subject.next(1));
        pollster::block_on(subject.error("late".to_string()));
        pollster::block_on(subject.complete());

        // The subscriber sees the terminal event, and nothing before it.
        assert!(matches!(
            pollster::block_on(rx.recv()).unwrap(),
            Event::Complete
        ));
        // Dropping the subject closes the channel: the stream is really over
        // (a `recv` would otherwise park on the sender still held by the
        // subscribers list).
        drop(subject);
        assert!(pollster::block_on(rx.recv()).is_err());
    }

    #[test]
    fn a_subscriber_arriving_after_the_end_observes_the_end() {
        let subject = Subject::<i32, String>::new();
        pollster::block_on(subject.complete());

        let mut late = pollster::block_on(subject.subscribe());
        assert!(matches!(
            pollster::block_on(late.recv()).unwrap(),
            Event::Complete
        ));
        // A terminated subject does not register subscribers, so the channel is
        // already closed behind the replayed terminal event.
        assert!(pollster::block_on(late.recv()).is_err());
    }

    #[test]
    fn subject_does_not_replay_without_a_replay_size() {
        let subject = Subject::<i32, String>::new();
        pollster::block_on(subject.next(1));

        let mut rx = pollster::block_on(subject.subscribe());
        pollster::block_on(subject.next(2));
        match pollster::block_on(rx.recv()).unwrap() {
            Event::Next(v) => assert_eq!(v, 2, "only the live value is delivered"),
            other => panic!("expected Next, got {:?}", other),
        }
    }

    #[test]
    fn subject_replay_hands_the_last_values_to_late_subscribers() {
        let subject = Subject::<i32, String>::replay(2);
        for value in [1, 2, 3] {
            pollster::block_on(subject.next(value));
        }

        let mut rx = pollster::block_on(subject.subscribe());
        for expected in [2, 3] {
            match pollster::block_on(rx.recv()).unwrap() {
                Event::Next(v) => assert_eq!(v, expected),
                other => panic!("expected Next({expected}), got {:?}", other),
            }
        }

        // The replayed snapshot is followed by the live events.
        pollster::block_on(subject.next(4));
        match pollster::block_on(rx.recv()).unwrap() {
            Event::Next(v) => assert_eq!(v, 4),
            other => panic!("expected Next(4), got {:?}", other),
        }
    }

    #[test]
    fn subject_replay_hands_the_terminal_state_to_late_subscribers() {
        let subject = Subject::<i32, String>::replay(1);
        pollster::block_on(subject.next(42));
        pollster::block_on(subject.complete());

        let mut rx = pollster::block_on(subject.subscribe());
        match pollster::block_on(rx.recv()).unwrap() {
            Event::Next(v) => assert_eq!(v, 42),
            other => panic!("expected the replayed Next(42), got {:?}", other),
        }
        assert!(matches!(
            pollster::block_on(rx.recv()).unwrap(),
            Event::Complete
        ));
    }
}
