//! Publish/subscribe transport: one request channel and one response channel per
//! **channel** (a `group` of services), correlated by the request id carried in
//! the zero-copy header.
//!
//! Every sample carries a [`RpcHeader`] in iceoryx2's `user_header` and the rkyv
//! bytes as payload. A subscribe port cannot be attached to a `WaitSet`, so each
//! side also owns an event service used as a wake-up signal.

use std::sync::Arc;

use iceoryx2::prelude::*;
use iceoryx2::service::ipc_threadsafe;

use crate::global::Global;
use crate::types::{RpcError, RpcHeader};

mod bridge;
mod client;
mod monitor;
mod notify;
mod pump;
mod server;
mod tuning;
mod waitset;

pub use bridge::{observable_to_responses, CollectEmitter, ResponseEmitter, ServiceDispatcher};
pub use client::native_call;
pub use monitor::{discover_channels, Direction, DirectionView, Emitter};
pub use server::{register_native_service, spawn_native_service, start_registered_channels};

// The transport reads its tunables through these names, so `tuning.rs` stays the
// only place where a value is defined and documented.
use tuning::{
    CONSUMER_WAIT_TIMEOUT, IDLE_SPINS, MAX_LOANED_SAMPLES, MAX_NODES, MAX_PUBLISHERS,
    MAX_SLICE_LEN, MAX_SUBSCRIBERS, PAYLOAD_ALIGNMENT, PROVIDER_WAIT_DEFAULT, PUBLISH_RETRY_SLEEP,
    PUBLISH_SPIN_ATTEMPTS, REQUEST_NOTIFY_SUFFIX, REQUEST_SUFFIX, RESPONSE_NOTIFY_SUFFIX,
    RESPONSE_SUFFIX, SIGNAL_CHECK_SAMPLES, SUBSCRIBER_BUFFER, WAITSET_DEADLINE,
};

/// Concrete iceoryx2 service flavour used by the transport.
type Iox = ipc_threadsafe::Service;
type IoxNode = iceoryx2::node::Node<Iox>;
type IoxPubSub =
    iceoryx2::service::port_factory::publish_subscribe::PortFactory<Iox, [u8], RpcHeader>;
type IoxEvent = iceoryx2::service::port_factory::event::PortFactory<Iox>;
type IoxPublisher = iceoryx2::port::publisher::Publisher<Iox, [u8], RpcHeader>;
type IoxSubscriber = iceoryx2::port::subscriber::Subscriber<Iox, [u8], RpcHeader>;
type IoxListener = iceoryx2::port::listener::Listener<Iox>;
type IoxNotifier = iceoryx2::port::notifier::Notifier<Iox>;

/// Wraps a transport error with its context.
pub(super) fn transport_error(context: &str, err: impl std::fmt::Debug) -> RpcError {
    RpcError::TransportError(format!("{context}: {err:?}"))
}

/// Returns the **process-wide** iceoryx2 node, created on first use.
pub(super) fn shared_node() -> Result<Arc<IoxNode>, RpcError> {
    static NODE: Global<Result<Arc<IoxNode>, String>> = Global::new();
    NODE.get_or_init(|| {
        NodeBuilder::new()
            .create::<Iox>()
            .map(Arc::new)
            .map_err(|e| format!("{e:?}"))
    })
    .clone()
    .map_err(|e| RpcError::TransportError(format!("node creation: {e}")))
}

/// Decodes a rkyv payload, copying it into an aligned buffer when needed.
///
/// The sample payload is aligned by construction ([`PAYLOAD_ALIGNMENT`]), but the
/// copy keeps the decoder correct even if that assumption is ever relaxed.
pub fn decode_aligned<T>(bytes: &[u8]) -> Result<T, rkyv::rancor::Error>
where
    T: rkyv::Archive,
    <T as rkyv::Archive>::Archived:
        rkyv::Deserialize<T, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
    for<'a> <T as rkyv::Archive>::Archived:
        rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
    aligned.extend_from_slice(bytes);
    rkyv::from_bytes::<T, rkyv::rancor::Error>(&aligned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
    struct Sample {
        id: u32,
        count: u64,
    }

    #[test]
    fn decode_aligned_tolerates_an_unaligned_payload() {
        let value = Sample {
            id: 7,
            count: 0x0102_0304_0506_0708,
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&value).unwrap();

        // Simulate a payload starting at an odd offset.
        let mut shifted = vec![0u8];
        shifted.extend_from_slice(&bytes);

        let decoded = decode_aligned::<Sample>(&shifted[1..]).expect("aligned decode");
        assert_eq!(decoded, value);
    }
}
