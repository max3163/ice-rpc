//! End-to-end provider ↔ consumer round-trip through the generated code and the
//! native iceoryx2 request/response transport (single process).

#![allow(missing_docs)]
#![allow(clippy::unwrap_used)]

use ice_rpc::{service, Event, Observable};

#[service("RoundtripService")]
#[async_trait::async_trait]
pub trait RoundtripService: Send + Sync + 'static {
    async fn echo(&self, value: i32) -> Observable<i32, String>;
}

struct Impl;

#[async_trait::async_trait]
impl RoundtripService for Impl {
    async fn echo(&self, value: i32) -> Observable<i32, String> {
        Observable::from_events([Event::Next(value + 1), Event::Complete])
    }
}

#[test]
fn provider_consumer_roundtrip() {
    let _guard = ice_rpc::gen::init_without_ctrl_c();

    let locator = ice_rpc::locator();
    let provider = RoundtripServiceProxy::provide(Impl);
    ice_rpc::rt::block_on(async {
        locator.register(provider).await;
        locator.initialize_all().await.expect("initialize_all");
    });

    // Let the native service thread create the iceoryx2 service.
    std::thread::sleep(std::time::Duration::from_millis(500));

    let consumer = RoundtripServiceProxy::consume();

    // Warm-up: creates the cached client (one-off cost) and connects it.
    let stream = ice_rpc::rt::block_on(consumer.echo(0));
    let values = ice_rpc::rt::block_on(stream.collect()).expect("collect");
    assert_eq!(values, vec![1]);

    // Steady state: 200 sequential round-trips through the cached client.
    const ITERS: usize = 200;
    let t0 = std::time::Instant::now();
    for i in 0..ITERS {
        let stream = ice_rpc::rt::block_on(consumer.echo(i as i32));
        let values = ice_rpc::rt::block_on(stream.collect()).expect("collect");
        assert_eq!(values, vec![i as i32 + 1]);
    }
    let elapsed = t0.elapsed();
    let per_call = elapsed.as_secs_f64() * 1000.0 / ITERS as f64;
    eprintln!(
        "[roundtrip] {ITERS} calls in {:?} -> {:.3} ms/call ({:.0} req/s)",
        elapsed,
        per_call,
        ITERS as f64 / elapsed.as_secs_f64()
    );

    // Concurrency probe: 8 threads, 100 calls each. Reports the first error.
    let consumer = std::sync::Arc::new(consumer);
    let mut handles = Vec::new();
    for w in 0..8usize {
        let consumer = consumer.clone();
        handles.push(std::thread::spawn(move || {
            let mut errs = 0usize;
            for i in 0..100i32 {
                let stream = ice_rpc::rt::block_on(consumer.echo(i));
                match ice_rpc::rt::block_on(stream.collect()) {
                    Ok(v) if v == vec![i + 1] => {}
                    Ok(other) => {
                        if errs == 0 {
                            eprintln!("[conc {w}] unexpected {other:?}");
                        }
                        errs += 1;
                    }
                    Err(e) => {
                        if errs == 0 {
                            eprintln!("[conc {w}] error: {e}");
                        }
                        errs += 1;
                    }
                }
            }
            errs
        }));
    }
    let total_errs: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    eprintln!("[roundtrip] concurrent errors: {total_errs}/800");
}
