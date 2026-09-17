//! Name-length limits shared with `ice-rpc-macros`.
//!
//! `METHOD_NAME_LEN` and `SERVICE_NAME_LEN` are the maximum **byte lengths**
//! accepted by `#[service]`, duplicated as private constants in `ice-rpc-macros`
//! (which rejects any longer name at compile time). Both equal the
//! `StaticString` capacity used by the generated wire types.

/// Maximum byte length of a method name (**inclusive**).
///
/// Must match the private `METHOD_NAME_LEN` constant in `ice-rpc-macros`.
pub const METHOD_NAME_LEN: usize = 64;

/// Maximum byte length of a service name (**inclusive**).
///
/// Must match the private `SERVICE_NAME_LEN` constant in `ice-rpc-macros`.
pub const SERVICE_NAME_LEN: usize = 64;

/// Version of the ice-rpc wire protocol carried in [`RpcHeader`].
///
/// A peer with a different value is reported before its request is dispatched.
/// It is meant to be bumped whenever the framing or the header layout changes
/// **after publication**: while the library is pre-release with no deployed
/// client, the layout may still evolve with the value left at `1`.
///
/// [`RpcHeader`]: crate::types::RpcHeader
pub const PROTOCOL_VERSION: u16 = 1;
