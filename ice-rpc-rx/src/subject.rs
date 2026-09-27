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

use crate::Sender;
use crate::{Event, Observable, ObservableError};

/// A multi-producer / multi-consumer multicast source.
///
/// `Subject::new()` multicasts only to the subscribers present at emission time
/// (RxJS `Subject`); `Subject::replay(n)` keeps the last `n` values and the
/// terminal state and replays them to late subscribers (`ReplaySubject(n)`).
///
/// Once terminated every later event is **ignored**, and a new subscriber
/// immediately observes the replayed terminal state.
///
/// # Concurrency
///
/// Two locks, and the state lock is **never** held across an `await`:
///
/// - `state` is held only for short, non-`await`ing critical sections
///   (snapshot, replay bookkeeping, registration);
/// - `emit` serializes **every** broadcast and every subscription, so a
///   subscriber observes the events in exactly the order the subject emitted
///   them. It is the lock held while the sends are awaited — and those awaits
///   never wait on a subscriber, since each queue is unbounded.
///
/// Each subscriber gets an **unbounded** queue, as in RxJS: an emission never
/// waits for anyone, so a subscriber that stops reading stalls neither the
/// other subscribers, nor `complete`/`error`, nor a new subscription. The price
/// is memory — that subscriber's queue grows until it reads again — which is the
/// trade-off a hot multicast always makes.
///
/// # Sharing
///
/// `Subject` is deliberately **not** `Clone`: a cloned handle would silently
/// share the replay buffer and the terminal state. A subject shared across
/// tasks is held as `Arc<Subject<T, E>>`, which makes that sharing explicit.
pub struct Subject<T, E> {
    /// Short-lived critical sections only; never held across an `await`.
    state: std::sync::Arc<async_lock::Mutex<State<T, E>>>,
    /// Serializes broadcasts and subscriptions; held across the sends.
    emit: std::sync::Arc<async_lock::Mutex<()>>,
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
    ///
    /// Keyed on `Sender::is_closed` rather than on the index of a failed send:
    /// the sends happen outside the lock, so the list may have changed by then.
    fn retain_live(&mut self) {
        self.subscribers.retain(|tx| !tx.is_closed());
    }

    /// The event a late subscriber observes after the replayed values.
    fn terminal_event(&self) -> Option<Event<T, E>>
    where
        E: Clone,
    {
        if let Some(error) = &self.error {
            Some(Event::Error(error.clone()))
        } else if self.completed {
            Some(Event::Complete)
        } else {
            None
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
            state: std::sync::Arc::new(async_lock::Mutex::new(State {
                replay: VecDeque::with_capacity(capacity),
                capacity,
                completed: false,
                error: None,
                subscribers: Vec::new(),
            })),
            emit: std::sync::Arc::new(async_lock::Mutex::new(())),
        }
    }

    /// Subscribes to this subject.
    ///
    /// The returned [`Observable`] first replays what the subject kept
    /// (the last values, then `Complete` or `Error` if it is terminated), then
    /// receives the live events.
    ///
    /// The subscriber's queue is **unbounded**: a consumer that stops reading
    /// accumulates its own backlog instead of blocking the subject, exactly as
    /// an RxJS `Subject` behaves.
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
        // Held for the whole call: no broadcast can slip between the snapshot
        // below and the registration at the end, so a late subscriber never
        // misses the value the replay promised it.
        let _emit = self.emit.lock().await;

        // Snapshot under a short lock — no `await` while `state` is held.
        let (replayed, terminal) = {
            let state = self.state.lock().await;
            (state.replay.clone(), state.terminal_event())
        };

        // An unbounded queue: the replay is pushed before the consumer starts
        // polling, and a live emission must never wait for a subscriber — a
        // bounded channel used to deadlock here on `replay(n)` for `n` above its
        // capacity, and used to stall the whole subject on a slow subscriber.
        let (tx, rx) = crate::unbounded_channel::<T, E>();
        for value in replayed {
            let _ = tx.send_next(value).await;
        }
        if let Some(event) = terminal {
            let _ = tx.send_event(event).await;
        }

        {
            let mut state = self.state.lock().await;
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
        let _emit = self.emit.lock().await;
        let subscribers = {
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
            state.retain_live();
            state.subscribers.clone()
        };
        // The sends happen outside `state`: a slow subscriber no longer blocks
        // `subscribe`, `complete` or any other state access.
        for tx in &subscribers {
            let _ = tx.send_next(value.clone()).await;
        }
    }

    /// Ends the stream for all current (and future) subscribers.
    ///
    /// # Example
    /// ```rust,ignore
    /// subject.complete().await;
    /// ```
    pub async fn complete(&self) {
        let _emit = self.emit.lock().await;
        let subscribers = {
            let mut state = self.state.lock().await;
            if state.is_terminated() {
                return;
            }
            state.completed = true;
            state.retain_live();
            state.subscribers.clone()
        };
        for tx in &subscribers {
            let _ = tx.send_complete().await;
        }
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
        let _emit = self.emit.lock().await;
        let (error, subscribers) = {
            let mut state = self.state.lock().await;
            if state.is_terminated() {
                return;
            }
            let error = ObservableError::Business(err);
            state.error = Some(error.clone());
            state.retain_live();
            (error, state.subscribers.clone())
        };
        for tx in &subscribers {
            let _ = tx.send_event(Event::Error(error.clone())).await;
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

    /// The replay is pushed before the consumer starts polling: a replay larger
    /// than the multicast capacity used to fill the channel and deadlock
    /// `subscribe`, which held the state lock while waiting for room nobody
    /// would free.
    #[test]
    fn a_replay_larger_than_the_multicast_capacity_does_not_deadlock() {
        let subject = Subject::<u32, String>::replay(20);
        for value in 0..20u32 {
            pollster::block_on(subject.next(value));
        }

        // The whole replay must fit in the channel: subscribing cannot suspend
        // waiting for room nobody drains yet. `poll_once` yields `None` when the
        // future is not ready on its first poll — the old deadlock, asserted
        // without any timer, so it holds under the `tokio` facade too.
        let mut rx = pollster::block_on(futures_lite::future::poll_once(subject.subscribe()))
            .expect("subscribe must complete on its first poll");

        for expected in 0..20u32 {
            assert_eq!(
                pollster::block_on(rx.recv()).unwrap(),
                Event::Next(expected)
            );
        }
    }

    /// A subscriber that never reads cannot stall the subject: emissions never
    /// park, and the others keep receiving. Under the old bounded channel this
    /// failed at the 9th value, where the send waited for a queue nobody was
    /// draining.
    #[test]
    fn a_slow_subscriber_cannot_stall_the_subject() {
        let subject = Subject::<u32, String>::new();
        // Never drained: its queue grows, nothing else notices.
        let _stuck = pollster::block_on(subject.subscribe());
        let mut live = pollster::block_on(subject.subscribe());

        for value in 0..64u32 {
            let sent = pollster::block_on(futures_lite::future::poll_once(subject.next(value)));
            assert!(sent.is_some(), "emission {value} parked on a subscriber");
        }

        // The live subscriber received every value, in order.
        for expected in 0..64u32 {
            match pollster::block_on(live.recv()).unwrap() {
                Event::Next(v) => assert_eq!(v, expected),
                other => panic!("expected Next({expected}), got {other:?}"),
            }
        }
    }
}
