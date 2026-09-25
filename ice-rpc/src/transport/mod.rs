//! Publish/subscribe transport: one request channel and one response channel per
//! **channel** (a `group` of services), correlated by the request id carried in
//! the zero-copy header.
//!
//! Every sample carries an `RpcHeader` in iceoryx2's `user_header` and the rkyv
//! bytes as payload. A subscribe port cannot be attached to a `WaitSet`, so each
//! side also owns an event service used as a wake-up signal.

use std::collections::HashMap;
use std::sync::Arc;

use iceoryx2::prelude::*;
use iceoryx2::service::ipc_threadsafe;

use crate::global::{Global, Locked};
use crate::types::{RpcError, RpcHeader};

mod bridge;
mod client;
mod monitor;
mod notify;
mod open;
mod publish;
mod pump;
mod server;
mod tuning;
mod waitset;

pub use bridge::{
    emit_rpc_error, observable_to_responses, BoxResponseFuture, CollectEmitter, OwnedEmitter,
    ResponseEmitter, ServiceDispatcher,
};
pub use client::{native_call, serialize_and_call};
pub use monitor::{discover_channels, Direction, DirectionView, Emitter};
pub use server::{register_native_service, spawn_native_service, start_registered_channels};

// The transport reads its tunables through these names, so `tuning.rs` stays the
// only place where a value is defined and documented.
use tuning::{
    CONSUMER_WAIT_TIMEOUT, IDLE_SPINS, OPEN_RETRY_ATTEMPTS, OPEN_RETRY_SLEEP, PAYLOAD_ALIGNMENT,
    PROVIDER_WAIT_DEFAULT, PUBLISH_RETRY_SLEEP, PUBLISH_SPIN_ATTEMPTS, REQUEST_NOTIFY_SUFFIX,
    REQUEST_SCRATCH_CAPACITY, REQUEST_SUFFIX, RESPONSE_NOTIFY_SUFFIX, RESPONSE_SUFFIX,
    SIGNAL_CHECK_SAMPLES, WAITSET_DEADLINE,
};

// The generated code names this default when `#[service]` omits `max_slice_len`,
// and the direct entry points (`native_call`, `spawn_native_service`) fall back to
// it. Re-exported so the value is defined in `tuning.rs` alone.
pub use tuning::DEFAULT_MAX_SLICE_LEN;

/// Declared initial slice length of the channels this process opens, by channel.
fn max_slice_len_registry() -> &'static Locked<HashMap<String, usize>> {
    static REGISTRY: Locked<HashMap<String, usize>> = Locked::new();
    &REGISTRY
}

/// Declares the initial slice length of `channel`'s publishers.
///
/// The generated provider and consumer call this once, before the channel's ports
/// are opened — the value is a property of the **channel**, not of a call, so it
/// does not travel through the call path. The first declaration wins; a later,
/// different one is logged and ignored.
pub fn declare_channel_max_slice_len(channel: &str, max_slice_len: usize) {
    max_slice_len_registry().with(|registry| {
        if let Some(&declared) = registry.get(channel) {
            if declared != max_slice_len {
                log::warn!(
                    "[transport] channel '{channel}': max_slice_len {max_slice_len} ignored, \
                     already declared as {declared}"
                );
            }
            return;
        }
        registry.insert(channel.to_owned(), max_slice_len);
    });
}

/// The initial slice length of `channel`'s publishers, read when its ports open.
pub(super) fn channel_max_slice_len(channel: &str) -> usize {
    max_slice_len_registry()
        .with(|registry| registry.get(channel).copied())
        .unwrap_or(DEFAULT_MAX_SLICE_LEN)
}

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
    let mut config = crate::config::build_iceoryx2_config();
    config.global.node.cleanup_dead_nodes_on_creation = false;

    let Ok(node) = iceoryx2::node::NodeBuilder::new()
        .config(&config)
        .create::<Iox>()
    else {
        return 0;
    };
    let state = node.try_cleanup_dead_nodes();

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

/// Reaps the `iox2_*.shm_state` markers iceoryx2 left behind on this machine.
///
/// `iceoryx2-pal-posix` emulates `shm_open` with memory-mapped files and keeps
/// one `<segment>.shm_state` marker per segment. The marker is deleted by
/// `shm_unlink` when a process releases its last reference, which is why a
/// process that is **killed** rather than exiting leaves it behind: the segment
/// is gone, the marker stays. [`cleanup_dead_nodes`] cannot reach those markers —
/// it removes the *service and port tags* of a dead node, never the dynamic
/// storage of an event service — and nothing ages them out, so they accumulate,
/// one per event service and per killed run.
///
/// This sweep is the missing half. `SharedMemory::list()` enumerates the markers
/// and `SharedMemory::does_exist()` answers, for each name, whether its segment is
/// still mapped. On Windows that call **is** the test: `shm_open` unlinks a name
/// it cannot open, which is exactly the orphan case — "the segment is gone, the
/// marker stays". Elsewhere it is a harmless no-op.
///
/// The primitive is iceoryx2's own, so it carries the assumption iceoryx2 makes
/// elsewhere: the state belongs to the current user, and a mapping owned by
/// another account may fail to open and lose its marker with it. That is why the
/// sweep runs at provider startup rather than in a shared or privileged context,
/// and why it is a one-shot before anything is created — the only race left is
/// another process that has written its marker but not yet its mapping.
///
/// # Returns
/// Number of markers removed.
pub fn sweep_orphan_shm_markers() -> usize {
    use iceoryx2_bb_posix::shared_memory::SharedMemory;

    let names = SharedMemory::list();
    let before = names.len();

    for name in &names {
        // The answer is deliberately ignored: the observable effect wanted here
        // is the unlink of an orphan, which `does_exist` performs itself.
        let _ = SharedMemory::does_exist(name);
    }

    before.saturating_sub(SharedMemory::list().len())
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
    (bytes.as_ptr() as usize).is_multiple_of(PAYLOAD_ALIGNMENT)
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
