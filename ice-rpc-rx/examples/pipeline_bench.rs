//! Quick timing of a `map → filter → take` pipeline: pull-based combinators vs
//! the pre-refactor channel/task implementation.
//!
//! Run with:
//! ```bash
//! cargo run --release -p ice-rpc-rx --example pipeline_bench --features tokio
//! ```
//!
//! # How to read the numbers
//!
//! Each measurement is repeated `REPS` times and the **minimum** is reported:
//! the whole workload only lasts a few milliseconds, so on a non-realtime OS a
//! single scheduling hiccup would otherwise dominate the mean and make one run
//! look 5-10x slower than the next. `med`/`max` are printed to expose that
//! spread.
//!
//! A `pure Vec<Event>` row is printed as well: it is the floor of this harness,
//! i.e. the cost of iterating the very same item type with **no operator, no
//! channel and no lock**. The poll pipeline is expected to sit just above it —
//! that difference is the only meaningful "operator overhead" figure here.

use std::hint::black_box;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use ice_rpc::Event;
use ice_rpc_rx::RxStreamExt;
use std::convert::Infallible;

const N: i64 = 100_000;
const ITERS: usize = 5;
const REPS: usize = 15;

/// Drains any poll-based stream with a no-op waker.
fn drain<S: futures_lite::Stream>(stream: S) {
    let mut stream = Box::pin(stream);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut count = 0u64;
    while let Poll::Ready(Some(_)) = futures_lite::Stream::poll_next(stream.as_mut(), &mut cx) {
        count += 1;
    }
    black_box(count);
}

// ── poll: pull-based combinators ─────────────────────────────────────

fn time_poll() -> f64 {
    let start = Instant::now();
    for _ in 0..ITERS {
        // The business-error type is irrelevant here: the source never errors.
        let source: ice_rpc::Observable<i64, Infallible> = ice_rpc_rx::from(0..N);
        let pipeline = source
            .map(|v| v * 2)
            .filter(|v| v % 4 == 0)
            .take(N as usize);
        drain(pipeline);
    }
    start.elapsed().as_nanos() as f64 / (ITERS as f64 * N as f64)
}

// ── reference: pure Vec of the same item type ────────────────────────

/// Pure stream over a `Vec<Event>`: no operator, no channel, no lock. This is
/// the harness floor.
struct PureVecStream {
    values: std::vec::IntoIter<Event<i64, Infallible>>,
    done: bool,
}

// All fields are pointer-based: the stream never pins its contents.
impl Unpin for PureVecStream {}

impl futures_lite::Stream for PureVecStream {
    type Item = Event<i64, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.values.next() {
            Some(v) => Poll::Ready(Some(v)),
            None => {
                self.done = true;
                Poll::Ready(None)
            }
        }
    }
}

fn time_pure() -> f64 {
    let start = Instant::now();
    for _ in 0..ITERS {
        let mut events: Vec<Event<i64, Infallible>> = (0..N).map(Event::Next).collect();
        events.push(Event::Complete);
        drain(PureVecStream {
            values: events.into_iter(),
            done: false,
        });
    }
    start.elapsed().as_nanos() as f64 / (ITERS as f64 * N as f64)
}

// ── channel: the pre-refactor model (1 channel + 1 task per operator) ─

async fn drain_channel(mut rx: ice_rpc::Observable<i64, String>) {
    let mut count = 0u64;
    while rx.recv().await.is_ok() {
        count += 1;
    }
    black_box(count);
}

async fn time_channel() -> f64 {
    let start = Instant::now();
    for _ in 0..ITERS {
        let (s0, mut r0) = ice_rpc::gen::channel::<i64, String>(8);
        ice_rpc::rt::spawn(async move {
            for i in 0..N {
                if s0.send_next(i).await.is_err() {
                    return;
                }
            }
            let _ = s0.send_complete().await;
        });

        let (s1, mut r1) = ice_rpc::gen::channel::<i64, String>(8);
        ice_rpc::rt::spawn(async move {
            while let Ok(ev) = r0.recv().await {
                match ev {
                    Event::Next(v) => {
                        if s1.send_next(v * 2).await.is_err() {
                            return;
                        }
                    }
                    other => {
                        if s1.send_event(other).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        let (s2, mut r2) = ice_rpc::gen::channel::<i64, String>(8);
        ice_rpc::rt::spawn(async move {
            while let Ok(ev) = r1.recv().await {
                match ev {
                    Event::Next(v) => {
                        if v % 4 == 0 && s2.send_next(v).await.is_err() {
                            return;
                        }
                    }
                    other => {
                        if s2.send_event(other).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        let (s3, r3) = ice_rpc::gen::channel::<i64, String>(8);
        ice_rpc::rt::spawn(async move {
            let mut remaining = N as usize;
            while let Ok(ev) = r2.recv().await {
                match ev {
                    Event::Next(v) => {
                        if remaining == 0 {
                            let _ = s3.send_complete().await;
                            return;
                        }
                        remaining -= 1;
                        if s3.send_next(v).await.is_err() {
                            return;
                        }
                        if remaining == 0 {
                            let _ = s3.send_complete().await;
                            return;
                        }
                    }
                    other => {
                        if s3.send_event(other).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        drain_channel(r3).await;
    }
    start.elapsed().as_nanos() as f64 / (ITERS as f64 * N as f64)
}

// ── reporting ────────────────────────────────────────────────────────

/// Prints min/median/max for a set of samples and returns the minimum.
fn report(label: &str, mut samples: Vec<f64>) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).expect("no NaN sample"));
    let min = samples[0];
    let med = samples[samples.len() / 2];
    let max = samples[samples.len() - 1];
    println!("{label:<34}{min:>8.1} min {med:>8.1} med {max:>8.1} max");
    min
}

#[tokio::main]
async fn main() {
    if cfg!(debug_assertions) {
        eprintln!(
            "WARNING: DEBUG build — the figures below are ~10x too high.\n\
             Re-run with:\n\
             \x20 cargo run --release -p ice-rpc-rx --example pipeline_bench --features tokio\n"
        );
    }

    // Warm-up: page faults, allocator first-touch, CPU frequency ramp.
    let _ = time_pure();
    let _ = time_poll();
    let _ = time_channel().await;

    println!(
        "{:<34}{:>8}     {:>8}     {:>8}",
        "ns/event", "min", "med", "max"
    );

    let mut pure = Vec::with_capacity(REPS);
    let mut poll = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        pure.push(time_pure());
        poll.push(time_poll());
    }
    let pure_min = report("pure Vec<Event> (harness floor)", pure);
    let poll_min = report("poll (map→filter→take)", poll);

    let mut chan = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        chan.push(time_channel().await);
    }
    let chan_min = report("channel (map→filter→take)", chan);

    println!();
    println!(
        "operator overhead over the harness floor: {:.1} ns/event",
        poll_min - pure_min
    );
    println!(
        "ratio: poll is {:.1}x faster than the channel model",
        chan_min / poll_min
    );
}
