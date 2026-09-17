//! Publish/subscribe transport: one request channel and one response channel per
//! **channel** (a `group` of services), correlated by the request id carried in
//! the zero-copy header.
//!
//! Every sample carries an `RpcHeader` in iceoryx2's `user_header` and the rkyv
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
mod open;
mod pump;
mod server;
mod tuning;
mod waitset;

pub use bridge::{observable_to_responses, CollectEmitter, ResponseEmitter, ServiceDispatcher};
pub use client::{native_call, serialize_and_call};
pub use monitor::{discover_channels, Direction, DirectionView, Emitter};
pub use server::{register_native_service, spawn_native_service, start_registered_channels};

// The transport reads its tunables through these names, so `tuning.rs` stays the
// only place where a value is defined and documented.
use tuning::{
    CONSUMER_WAIT_TIMEOUT, IDLE_SPINS, MAX_LOANED_SAMPLES, MAX_NODES, MAX_PUBLISHERS,
    MAX_SLICE_LEN, MAX_SUBSCRIBERS, OPEN_RETRY_ATTEMPTS, OPEN_RETRY_SLEEP, PAYLOAD_ALIGNMENT,
    PROVIDER_WAIT_DEFAULT, PUBLISH_RETRY_SLEEP, PUBLISH_SPIN_ATTEMPTS, REQUEST_NOTIFY_SUFFIX,
    REQUEST_SUFFIX, RESPONSE_NOTIFY_SUFFIX, RESPONSE_SUFFIX, SIGNAL_CHECK_SAMPLES,
    SUBSCRIBER_BUFFER, WAITSET_DEADLINE,
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

/// Drops the per-channel port caches this process still holds.
///
/// Called by [`shutdown_and_release`](crate::shutdown_and_release) once the
/// dispatch threads are joined. A dispatch thread owns its ports, so joining it
/// releases them — but the cache of consumed channels lives in a `static`, and
/// Rust never drops a `static`. Without this call, a consumer that created a
/// service (it opened it before any provider existed) would leave that service on
/// the bus after exiting.
pub fn release_process_ports() -> usize {
    client::release_consumer_ports()
}

/// Reaps the resources iceoryx2 left behind by processes that are gone.
pub fn cleanup_dead_nodes() -> u64 {
    let config = crate::config::build_iceoryx2_config();
    let state = iceoryx2::node::Node::<Iox>::try_cleanup_dead_nodes(&config);

    if state.failed_cleanups > 0 {
        log::warn!(
            "[ice-rpc] {} dead node(s) could not be reaped, {} reaped \
             (insufficient permissions, or another process is on it)",
            state.failed_cleanups,
            state.cleanups
        );
    }

    state.cleanups
}

/// Decodes a rkyv payload, in place when the payload is already aligned.
///
/// A sample payload is aligned by construction — `PAYLOAD_ALIGNMENT` is the
/// alignment `rkyv::to_bytes` produces — so the common path needs neither a
/// second buffer nor a copy. `benches/hot_path.rs` measures 45.6 ns per call
/// against 1.2 ns when the copy is skipped.
///
/// The copy stays for the callers that cannot promise the alignment (a payload
/// built by hand, a buffer read outside the transport):
/// [`rkyv::from_bytes`] rejects a slice whose address does not satisfy the
/// alignment of the root type, so the two paths are not interchangeable.
pub fn decode_aligned<T>(bytes: &[u8]) -> Result<T, rkyv::rancor::Error>
where
    T: rkyv::Archive,
    <T as rkyv::Archive>::Archived:
        rkyv::Deserialize<T, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
    for<'a> <T as rkyv::Archive>::Archived:
        rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    if is_sample_aligned(bytes) {
        return rkyv::from_bytes::<T, rkyv::rancor::Error>(bytes);
    }

    // The alignment of the buffer follows the one the transport requests.
    let mut aligned = rkyv::util::AlignedVec::<{ PAYLOAD_ALIGNMENT }>::with_capacity(bytes.len());
    aligned.extend_from_slice(bytes);
    rkyv::from_bytes::<T, rkyv::rancor::Error>(&aligned)
}

/// Whether `bytes` starts on the alignment the transport guarantees a sample.
fn is_sample_aligned(bytes: &[u8]) -> bool {
    bytes.as_ptr() as usize % PAYLOAD_ALIGNMENT == 0
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

    /// The fast path is the one a real sample takes, so it must decode the same
    /// value as the copying one.
    #[test]
    fn decode_aligned_reads_an_aligned_payload_in_place() {
        let value = Sample {
            id: 11,
            count: 0x0a0b_0c0d_0e0f_1011,
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&value).unwrap();

        assert!(
            is_sample_aligned(&bytes),
            "an encoded payload must be as aligned as a sample"
        );
        let decoded = decode_aligned::<Sample>(&bytes).expect("in-place decode");
        assert_eq!(decoded, value);
    }
}
