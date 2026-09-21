//! The same **direct** (in-process) call, with the `tracing` feature off.
//!
//! Without a collector there is nothing to analyse, so `local_call_scoped`
//! forwards the future untouched: no correlation id, no clock read, no
//! thread-local, no allocation. The observable contract is that the callee sees
//! **whatever ambient context** the caller had — its own when the caller is a
//! handler, none when there is none — and never a context minted for it.
//!
//! The feature-on counterpart is `local_call_span.rs`, and the allocation side is
//! counted by `local_call_allocations.rs` (3,00 per call, the same as before).
//!
//! See `plans/spans-appels-internes-provider.md`.

#![cfg(not(feature = "tracing"))]
#![allow(missing_docs)] // test target: documented by the plan, not part of a published API
#![allow(clippy::unwrap_used)] // tests may panic

use std::sync::Mutex;

use ice_rpc::gen::RpcHeader;
use ice_rpc::rt::block_on;
use ice_rpc::{service, CallContext, Observable};

/// The leaf the caller delegates to.
#[service("DirectPlainLeaf")]
#[async_trait::async_trait]
pub trait DirectPlainLeaf: Send + Sync + 'static {
    /// Returns its argument, incremented.
    async fn reserve(&self, value: i32) -> Observable<i32, String>;
}

/// What the leaf read about the call it was serving.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Seen {
    method: Option<&'static str>,
    service_id: Option<u32>,
}

/// Last observation of the leaf, for the test to assert on.
static SEEN: Mutex<Option<Seen>> = Mutex::new(None);

/// Leaf implementation: records the ambient context it was given.
struct Leaf;

#[async_trait::async_trait]
impl DirectPlainLeaf for Leaf {
    async fn reserve(&self, value: i32) -> Observable<i32, String> {
        let ctx = CallContext::current();
        *SEEN.lock().unwrap() = Some(Seen {
            method: ctx.map(|ctx| ctx.method()),
            service_id: ctx.map(|ctx| ctx.service_id()),
        });
        ice_rpc::of(value + 1)
    }
}

/// Reads back what the leaf last observed.
fn seen() -> Seen {
    SEEN.lock().unwrap().expect("the leaf ran")
}

/// Nothing is built for a call that nobody collects: the callee inherits **at
/// most** the caller's ambient context, and never one minted for it.
///
/// One `#[test]` for both cases on purpose: they share the observation slot, and
/// libtest runs the tests of a binary in parallel.
#[test]
fn a_direct_call_installs_nothing_when_nothing_is_collected() {
    let leaf = DirectPlainLeafProxy::provide(Leaf);

    // Outside any call: no context at all.
    assert_eq!(
        CallContext::current(),
        None,
        "the test starts outside any call"
    );
    let stream = block_on(leaf.reserve(41));
    let value = block_on(stream.first_value()).expect("the leaf answers");
    assert_eq!(value, 42, "the delegation still works");
    assert_eq!(
        seen(),
        Seen {
            method: None,
            service_id: None
        },
        "no context is minted for a call that nobody collects"
    );

    // Inside a caller's call: the caller's context, and only that one.
    let parent = CallContext::new(
        &RpcHeader::request("place_order", 1, 1),
        "OrderService",
        "place_order",
    );
    let value = {
        let _scope = parent.enter_ambient();
        let stream = block_on(leaf.reserve(41));
        block_on(stream.first_value()).expect("the leaf answers")
    };
    assert_eq!(value, 42);
    assert_eq!(
        seen(),
        Seen {
            method: Some("place_order"),
            service_id: Some(1)
        },
        "the callee sees the caller's context, and only that one"
    );
}
