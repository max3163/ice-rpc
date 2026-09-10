//! Hot-path diagnostic probe — regression harness for the local (channel-free)
//! stream sources (`StreamInner::Buffered`) and for the single-response paths
//! used by a provider (`of` + `recv_wire`) and a client (`recv`).
//!
//! It answers three questions:
//! 1. how much does the *harness itself* cost (draining `Vec<u64>`) ?
//! 2. how much does a *pure* stream of `Event` cost, with no `Arc` and no lock ?
//! 3. how much do the framework paths add on top of (2) ?
//!
//! Run (release is mandatory — the numbers are meaningless in debug):
//! ```bash
//! cargo run --release -p ice-rpc-rx --example perf_probe
//! ```

use std::hint::black_box;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use ice_rpc::Event;
use ice_rpc_rx::{RxError, RxStreamExt};

const N: i64 = 200_000;
/// Repetitions per measurement: the **minimum** is reported, because on a
/// non-realtime OS a single scheduling hiccup (a few ms) would otherwise
/// dominate a measurement that only lasts a few ms in total.
const REPS: usize = 15;

/// A pure stream over a Vec: the shape of the pre-Option-B `From<T>` / `Of<T>`
/// combinators (no Arc, no Mutex, no VecDeque).
struct PureVecStream<T> {
    values: std::vec::IntoIter<T>,
    done: bool,
}

// All fields are pointer-based: the stream never pins its contents.
impl<T> Unpin for PureVecStream<T> {}

impl<T> futures_lite::Stream for PureVecStream<T> {
    type Item = T;

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

/// Polls a future exactly once, with a no-op waker (no runtime, no thread park).
fn poll_once<F: std::future::Future>(fut: F) -> Option<F::Output> {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(&waker);
    let mut fut = std::pin::pin!(fut);
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

/// Drains any poll-based stream with a no-op waker; returns the poll count.
fn drain<S: futures_lite::Stream>(stream: S) -> u64 {
    let mut stream = Box::pin(stream);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(&waker);
    let mut n = 0u64;
    while let Poll::Ready(Some(_)) = futures_lite::Stream::poll_next(stream.as_mut(), &mut cx) {
        n += 1;
    }
    black_box(n)
}

/// Runs `f` `REPS` times and prints the best (min) and median ns/event.
fn bench(label: &str, mut f: impl FnMut() -> u64) {
    let mut samples: Vec<f64> = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let start = std::time::Instant::now();
        let ops = f();
        samples.push(start.elapsed().as_nanos() as f64 / ops as f64);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let min = samples[0];
    let median = samples[REPS / 2];
    let max = samples[REPS - 1];
    println!("{label:<46}{min:>8.2} min {median:>8.2} med {max:>8.2} max");
}

fn sizes() {
    use std::mem::size_of;
    println!("--- sizes ---");
    println!("size_of::<u64>()                   = {}", size_of::<u64>());
    println!(
        "size_of::<RpcError>()              = {}",
        size_of::<ice_rpc::RpcError>()
    );
    println!(
        "size_of::<ObservableError<RxError>>() = {}",
        size_of::<ice_rpc::ObservableError<RxError>>()
    );
    println!(
        "size_of::<ObservableError<String>>()  = {}",
        size_of::<ice_rpc::ObservableError<String>>()
    );
    println!(
        "size_of::<Event<i64, RxError>>()   = {}",
        size_of::<Event<i64, RxError>>()
    );
    println!(
        "size_of::<Event<i64, String>>()    = {}",
        size_of::<Event<i64, String>>()
    );
    println!();
}

fn main() {
    println!("N = {N} events/iteration, {REPS} repetitions, release profile");
    println!();
    sizes();

    println!("--- harness floor (ns/event) ---");

    bench("H. drain Vec<u64>          (harness floor)", || {
        let values: Vec<u64> = (0..N as u64).collect();
        drain(PureVecStream {
            values: values.into_iter(),
            done: false,
        })
    });

    bench("C2. drain Vec<Event<i64,RxError>> (no lock)", || {
        let mut events: Vec<Event<i64, RxError>> = (0..N).map(Event::Next).collect();
        events.push(Event::Complete);
        drain(PureVecStream {
            values: events.into_iter(),
            done: false,
        })
    });

    println!();
    println!("--- framework local source (ns/event) ---");

    bench("A2. ice_rpc_rx::from  E=RxError (Buffered)", || {
        drain(ice_rpc_rx::from::<i64, RxError, _>(0..N))
    });

    bench("B2. from + map/filter/take  E=RxError", || {
        let s = ice_rpc_rx::from::<i64, RxError, _>(0..N);
        drain(s.map(|v| v * 2).filter(|v| v % 4 == 0).take(N as usize))
    });

    println!();
    println!("--- single response (ns/op) ---");

    bench("E. from_events + recv_wire (provider)", || {
        let mut ops = 0u64;
        for i in 0..N {
            let mut s =
                ice_rpc::Stream::<i64, String>::from_events([Event::Next(i), Event::Complete]);
            black_box(poll_once(s.recv_wire()));
            ops += 1;
        }
        ops
    });

    bench("D. channel(1)+try_send_complete_with+wire", || {
        let mut ops = 0u64;
        for i in 0..N {
            let (tx, mut rx) = ice_rpc::channel::<i64, String>(1);
            let _ = tx.try_send_complete_with(i);
            black_box(poll_once(rx.recv_wire()));
            ops += 1;
        }
        ops
    });
}
