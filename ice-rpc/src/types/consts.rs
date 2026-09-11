//! Tuning constants: timeouts, buffer sizes, name lengths.
//!
//! `METHOD_NAME_LEN` and `SERVICE_NAME_LEN` are the maximum **byte lengths**
//! accepted by `#[service]`; they are duplicated as private constants in
//! `ice-rpc-macros` (which rejects any longer name at compile time). Both
//! values equal the `StaticString` capacity used by the wire types, so a name
//! accepted by the macro always fits without truncation — in the `RpcHeader`
//! **and** in the discovery blackboard key (see the `REGISTRY_*` constants
//! below, which must stay equal to `SERVICE_NAME_LEN`).

/// Maximum byte length of a method name in the RPC header (**inclusive**).
///
/// Must match the private `METHOD_NAME_LEN` constant in `ice-rpc-macros` and
/// the `StaticString` capacity used in `RpcHeader`.
pub const METHOD_NAME_LEN: usize = 64;

/// Maximum byte length of a service name in the RPC header (**inclusive**).
///
/// Must match the private `SERVICE_NAME_LEN` constant in `ice-rpc-macros` and
/// the `StaticString` capacity used in `RpcHeader`.
pub const SERVICE_NAME_LEN: usize = 64;

/// Version of the ice-rpc wire protocol carried in [`RpcHeader`].
///
/// Bumped whenever the layout of [`RpcHeader`] or the request/response
/// encoding changes. A peer with a different value is rejected before any
/// deserialization takes place.
pub const PROTOCOL_VERSION: u16 = 1;

/// Threshold above which a payload is published on the `_large` topic.
pub const LARGE_PAYLOAD_THRESHOLD: usize = 1024;

/// Default deadline (seconds) for the **discovery** phase of an RPC call.
///
/// It bounds the provider lookup performed by `ClientCore::resolve_target`
/// before the first call. Overridable per service with
/// `#[service(..., discovery_timeout = "5s")]`. It does **not** bound the
/// response wait: the provider may still answer arbitrarily late — use the
/// `timeout` operator on the consumer side for that.
pub const RPC_CALL_TIMEOUT_SECS: u64 = 30;

/// Initial slice size for iceoryx2 publishers (`_default` topics).
///
/// This is the fallback used when the `#[service]` macro does not override it
/// through `default_size_message`.
pub const PUBLISHER_DEFAULT_MAX_SLICE_LEN: usize = 256;
/// Initial slice size for iceoryx2 publishers (`_large` topics).
pub const PUBLISHER_LARGE_MAX_SLICE_LEN: usize = 4096;

/// WaitSet timeout (ms) for the hub dispatch loop.
pub const WAITSET_TIMEOUT_US: u64 = 500;
/// Poll interval (ms) while locating a service on the client side.
pub const SERVER_READY_POLL_MS: u64 = 100;
/// Retry interval (ms) for service initialization.
pub const INIT_RETRY_INTERVAL_MS: u64 = 1_000;

/// Global timeout (seconds) for [`initialize_all`](crate::ServiceLocator::initialize_all).
pub const INITIALIZE_ALL_TIMEOUT_SECS: u16 = 30;

/// Maximum number of readers on a Blackboard.
pub const BLACKBOARD_MAX_READERS: usize = 64;

/// Subscriber buffer size for the `_default` topics.
pub const DEFAULT_TOPIC_BUFFER_SIZE: usize = 4096;

/// Subscriber buffer size for the `_large` topics.
///
pub const LARGE_TOPIC_BUFFER_SIZE: usize = 4;
