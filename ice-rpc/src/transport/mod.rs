//! Publish/subscribe transport: one request channel and one response channel per
//! **channel** (a `group` of services), correlated by the request id carried in
//! the zero-copy header.
//!
//! A service joins a channel through the `group` parameter of `#[service]`
//! (default: its name). Services sharing a channel share its pub/sub services and
//! its dispatch thread; the provider routes with the `service_id` of the header,
//! which every process derives from the service name without discovery.
//!
//! Every sample carries a [`RpcHeader`] in iceoryx2's `user_header` (no
//! serialization) and the rkyv bytes as payload. A subscribe port cannot be
//! attached to a `WaitSet`, so each side also owns an event service used as a
//! wake-up signal.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use iceoryx2::prelude::*;
use iceoryx2::service::ipc_threadsafe;

use crate::types::{RpcError, RpcHeader};

mod bridge;
mod client;
mod notify;
mod server;
mod waitset;

pub use bridge::{observable_to_responses, ResponseIter, ServiceDispatcher};
pub use client::native_call;
pub use server::{register_native_service, spawn_native_service, start_registered_channels};

/// Suffixes of the iceoryx2 services backing one channel.
const REQUEST_SUFFIX: &str = "_req";
const RESPONSE_SUFFIX: &str = "_resp";
const REQUEST_NOTIFY_SUFFIX: &str = "_req_notify";
const RESPONSE_NOTIFY_SUFFIX: &str = "_resp_notify";

/// Samples a subscriber can buffer before backpressure is reported.
const SUBSCRIBER_BUFFER: usize = 1024;

/// Publishers accepted on one channel: one per process that sends on it.
const MAX_PUBLISHERS: usize = 16;

/// Subscribers accepted on one channel: one per process and per channel.
const MAX_SUBSCRIBERS: usize = 16;

/// Processes that can open the same channel at once.
const MAX_NODES: usize = 32;

/// Samples a publisher can keep loaned at once.
///
/// It sizes the publisher's data segment (`max_loaned_samples × sample`), i.e.
/// most of the memory a channel reserves.
const MAX_LOANED_SAMPLES: usize = 1024;

/// Initial slice length of a sample; large payloads grow the segment on demand.
const MAX_SLICE_LEN: usize = 256;

/// Payload alignment requested from iceoryx2.
const PAYLOAD_ALIGNMENT: usize = 16;

/// How long a call waits for the provider to be connected before failing.
///
/// Overridable with `ICE_RPC_PROVIDER_WAIT_MS`.
const PROVIDER_WAIT_DEFAULT: Duration = Duration::from_secs(30);

/// How long a response waits for the consumer to be connected.
const CONSUMER_WAIT_TIMEOUT: Duration = Duration::from_millis(500);

/// Sleep between two delivery attempts, once the spin budget is exhausted.
const PUBLISH_RETRY_SLEEP: Duration = Duration::from_millis(1);

/// Consecutive delivery attempts spent yielding before the retry loop sleeps.
///
/// A full channel is the normal case of a burst (the receiver frees a slot in
/// microseconds) while a sleep costs at least the system timer.
const PUBLISH_SPIN_ATTEMPTS: u32 = 4_096;

/// Upper bound on how long a dispatch thread blocks before it drains again.
///
/// The wait itself is event-driven; this deadline is the safety net that bounds
/// the cost of a missed notification.
const WAITSET_DEADLINE: Duration = Duration::from_millis(1);

/// Processed samples between two termination checks on the busy path.
///
/// `SignalHandler::termination_requested()` takes a process-wide mutex, so
/// sampling it keeps Ctrl+C responsive under load without serializing the
/// dispatch threads.
const SIGNAL_CHECK_SAMPLES: u32 = 256;

/// Consecutive empty polls spent spinning before a thread blocks on its
/// `WaitSet`.
const IDLE_SPINS: u32 = 2_000;

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
    static NODE: OnceLock<Result<Arc<IoxNode>, String>> = OnceLock::new();
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
