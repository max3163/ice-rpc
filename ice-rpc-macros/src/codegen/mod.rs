//! Codegen: sub-modules specialized by role (client, server, proxy, etc.).

pub mod client;
// Compiled whatever the build asks for: a generator is called or not according to
// `Features`, never according to `cfg!` — see `crate::features`.
#[cfg_attr(not(feature = "monitoring"), allow(dead_code))]
pub mod decoder;
pub mod helpers;
pub mod http;
pub mod lifecycle;
pub mod nodejs;
pub mod proxy;
pub mod server;
