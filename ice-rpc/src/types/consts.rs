//! Name-length limit shared with `ice-rpc-macros`.
//!
//! `SERVICE_NAME_LEN` is the maximum **byte length** accepted by `#[service]`
//! for a service or channel name, duplicated as a private constant in
//! `ice-rpc-macros` (which rejects any longer name at compile time).
//!
//! A channel named through a constant path (`group = common::SHARED_CHANNEL`) is
//! unknown to the macro, so the generated code re-checks it during constant
//! evaluation with [`const_ensure_channel_name`], which applies the very same
//! rules.

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
/// Bumped to `3` when the correlation id shrank from 16 to 8 bytes (a `u32`
/// process id followed by a `u32` counter): the header shrank from 80 to 72
/// bytes, which leaves room for future metadata inside iceoryx2's fixed
/// user-header budget.
///
/// [`RpcHeader`]: crate::types::RpcHeader
pub const PROTOCOL_VERSION: u16 = 3;

/// Compile-time check of a channel name, for the names the macro cannot read.
///
/// `#[service]` validates a `group = "..."` literal while it expands. A channel
/// named through a constant path — `group = common::SHARED_CHANNEL` — is only
/// known once that constant is evaluated, so the generated code calls this
/// function inside a `const` item: an invalid shared name fails the build instead
/// of failing `iceoryx2::ServiceName::new` at runtime.
///
/// The rules are the ones [`SERVICE_NAME_LEN`] documents for a literal: at most
/// that many bytes, ASCII alphanumerics, `'_'` and `'-'`, and a first character
/// that is alphanumeric.
///
/// This is macro plumbing, not a public API: it exists so the expansion stays a
/// single call instead of repeating the rules in generated code.
///
/// # Panics
/// Panics during constant evaluation when `name` breaks one of those rules, which
/// is what turns the violation into a compile-time error.
#[doc(hidden)]
pub const fn const_ensure_channel_name(name: &str) {
    let bytes = name.as_bytes();
    if bytes.len() > SERVICE_NAME_LEN {
        panic!(
            "ice-rpc: a channel name must be at most 64 bytes long \
             (SERVICE_NAME_LEN)"
        );
    }

    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if !(byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-') {
            panic!(
                "ice-rpc: a channel name may only contain ASCII alphanumerics, \
                 '_' and '-'"
            );
        }
        index += 1;
    }

    if !bytes.is_empty() && !bytes[0].is_ascii_alphanumeric() {
        panic!("ice-rpc: a channel name must start with an ASCII letter or a digit");
    }
}
