//! Two services sharing **one** channel (`group`) must behave exactly like two
//! services on their own channels: the routing relies on the service id carried
//! by the zero-copy header.
//!
//! Dedicated binary: the channel registry is sealed once `initialize_all` has
//! run, so only a fresh process guarantees that both services are grouped.

#![allow(missing_docs)]
#![allow(clippy::unwrap_used)]

use ice_rpc::{service, Event, Observable};

/// Boots ice-rpc once for the whole test binary.
fn init_global() {
    static GUARD: std::sync::OnceLock<ice_rpc::gen::ShutdownGuard> = std::sync::OnceLock::new();
    GUARD.get_or_init(ice_rpc::gen::init_without_ctrl_c);
}

#[service("GroupAlpha", group = "sharedchannel")]
#[async_trait::async_trait]
pub trait GroupAlpha: Send + Sync + 'static {
    async fn alpha(&self, value: i32) -> Observable<i32, String>;
}

struct AlphaImpl;

#[async_trait::async_trait]
impl GroupAlpha for AlphaImpl {
    async fn alpha(&self, value: i32) -> Observable<i32, String> {
        Observable::from_events([Event::Next(value + 100), Event::Complete])
    }
}

#[service("GroupBeta", group = "sharedchannel")]
#[async_trait::async_trait]
pub trait GroupBeta: Send + Sync + 'static {
    async fn beta(&self, value: i32) -> Observable<i32, String>;
}

struct BetaImpl;

#[async_trait::async_trait]
impl GroupBeta for BetaImpl {
    async fn beta(&self, value: i32) -> Observable<i32, String> {
        Observable::from_events([Event::Next(value * 3), Event::Complete])
    }
}

#[test]
fn two_services_share_one_channel() {
    init_global();

    let locator = ice_rpc::locator();
    ice_rpc::rt::block_on(async {
        locator.register(GroupAlphaProxy::provide(AlphaImpl)).await;
        locator.register(GroupBetaProxy::provide(BetaImpl)).await;
        locator.initialize_all().await.expect("initialize_all");
    });

    // Let the single channel thread create the iceoryx2 services.
    std::thread::sleep(std::time::Duration::from_millis(500));

    let alpha = GroupAlphaProxy::consume();
    let beta = GroupBetaProxy::consume();

    // Sequential interleaving: each service must receive only its own requests,
    // although both share the request and response channels.
    for i in 0..20i32 {
        let stream = ice_rpc::rt::block_on(alpha.alpha(i));
        let values = ice_rpc::rt::block_on(stream.collect()).expect("alpha collect");
        assert_eq!(values, vec![i + 100], "alpha answered the wrong value");

        let stream = ice_rpc::rt::block_on(beta.beta(i));
        let values = ice_rpc::rt::block_on(stream.collect()).expect("beta collect");
        assert_eq!(values, vec![i * 3], "beta answered the wrong value");
    }

    // Concurrent, interleaved calls of both services on the one channel.
    let alpha = std::sync::Arc::new(alpha);
    let beta = std::sync::Arc::new(beta);
    let mut handles = Vec::new();
    for w in 0..8usize {
        let alpha = alpha.clone();
        let beta = beta.clone();
        handles.push(std::thread::spawn(move || {
            let mut errs = 0usize;
            for i in 0..50i32 {
                let stream = ice_rpc::rt::block_on(alpha.alpha(i));
                if ice_rpc::rt::block_on(stream.collect()).ok() != Some(vec![i + 100]) {
                    if errs == 0 {
                        eprintln!("[shared {w}] alpha({i}) failed");
                    }
                    errs += 1;
                }

                let stream = ice_rpc::rt::block_on(beta.beta(i));
                if ice_rpc::rt::block_on(stream.collect()).ok() != Some(vec![i * 3]) {
                    if errs == 0 {
                        eprintln!("[shared {w}] beta({i}) failed");
                    }
                    errs += 1;
                }
            }
            errs
        }));
    }
    let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    eprintln!("[channel-group] concurrent errors: {total}/800");
    assert_eq!(total, 0, "shared-channel calls must not fail");
}
