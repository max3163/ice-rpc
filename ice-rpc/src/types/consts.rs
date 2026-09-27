//! Name-length limit shared with `ice-rpc-macros`.
//!
//! `SERVICE_NAME_LEN` is the maximum **byte length** accepted by `#[service]`
//! for a service or channel name, duplicated as a private constant in
//! `ice-rpc-macros` (which rejects any longer name at compile time).

/// Maximum byte length of a service or channel name (**inclusive**).
///
/// Must match the private `SERVICE_NAME_LEN` constant in `ice-rpc-macros`.
///
/// This is **not** a wire-header limit: the header carries
/// [`service_id_of`](crate::types::service_id_of), a fixed 4-byte hash, so the
/// name itself never travels and has no length constraint on the bus.
///
/// The limit exists because the name **is** the iceoryx2 service name: the
/// transport builds `{channel}_req`, `{channel}_resp` and their `_notify`
/// variants and hands them to `ServiceName::new` (see `transport/open.rs`).
/// iceoryx2 caps a `ServiceName` at 255 bytes; 64 is a conservative policy that
/// leaves room for every suffix and keeps names readable. Enforcing it in
/// `#[service]` turns a name that would fail `ServiceName::new` at runtime into
/// a **compile-time** error.
pub const SERVICE_NAME_LEN: usize = 64;

/// Version of the ice-rpc wire protocol carried in [`RpcHeader`].
///
/// A peer with a different value is reported before its request is dispatched.
/// Bumped to `2` when the method name left the header: `method_name` (a 32-byte
/// `StaticString`) became a 4-byte `method_id`, and the `span_id` and
/// `traceparent_version` fields were added for W3C conformance — the header
/// shrank from 120 to 80 bytes.
///
/// [`RpcHeader`]: crate::types::RpcHeader
pub const PROTOCOL_VERSION: u16 = 2;
