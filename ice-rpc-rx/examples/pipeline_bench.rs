//! Quick timing of a `map → filter → take` pipeline: pull-based combinators vs
//! the pre-refactor channel/task implementation.
//!
//! Run with:
//! ```bash
//! cargo run --release -p ice-rpc-rx --example pipeline_bench --features tokio
//! ```
//! Absolute numbers are only meaningful in `--release`.

use std::hint::black_box;
use std::task::Poll;
use std::time::Instant;

use ice_rpc::Event;
use ice_rpc_rx::RxStreamExt;

const N: i64 = 100_000;
const ITERS: usize = 5;

fn drain_poll(pipeline: impl futures_lite::Stream<Item = Event<i64, ice_rpc_rx::RxError>>) {
    let mut stream = Box::pin(pipeline);
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(&waker);
    let mut count = 0u64;
    while let Poll::Ready(Some(_)) = futures_lite::Stream::poll_next(stream.as_mut(), &mut cx) {
        count += 1;
    }
    black_box(count);
}

fn time_poll() -> f64 {
    let start = Instant::now();
    for _ in 0..ITERS {
        let pipeline = ice_rpc_rx::from(0..N)
            .map(|v| v * 2)
            .filter(|v| v % 4 == 0)
            .take(N as usize);
        drain_poll(pipeline);
    }
    start.elapsed().as_nanos() as f64 / (ITERS as f64 * N as f64)
}

async fn drain_channel(rx: ice_rpc::Stream<i64, String>) {
    let mut count = 0u64;
    while rx.recv().await.is_ok() {
        count += 1;
    }
    black_box(count);
}

async fn time_channel() -> f64 {
    let start = Instant::now();
    for _ in 0..ITERS {
        let (s0, r0) = ice_rpc::channel::<i64, String>(8);
        ice_rpc::rt::spawn(async move {
            for i in 0..N {
                if s0.send_next(i).await.is_err() {
                    return;
                }
            }
            let _ = s0.send_complete().await;
        });

        let (s1, r1) = ice_rpc::channel::<i64, String>(8);
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

        let (s2, r2) = ice_rpc::channel::<i64, String>(8);
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

        let (s3, r3) = ice_rpc::channel::<i64, String>(8);
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

#[tokio::main]
async fn main() {
    eprintln!("warming up…");
    let _ = time_poll();
    eprintln!("warm-up poll done");

    let _ = time_channel().await;
    eprintln!("warm-up channel done");

    eprintln!("measuring poll pipeline…");
    let poll_ns = time_poll();
    eprintln!("measuring channel pipeline…");
    let channel_ns = time_channel().await;

    println!("poll    (map→filter→take): {:.1} ns/event", poll_ns);
    println!("channel (map→filter→take): {:.1} ns/event", channel_ns);
    println!("ratio: poll is {:.1}x faster", channel_ns / poll_ns);
}
