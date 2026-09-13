//! Codegen: sub-modules specialized by role (client, server, proxy, etc.).

pub mod client;
#[cfg(feature = "monitoring")]
pub mod decoder;
pub mod helpers;
pub mod http;
pub mod lifecycle;
pub mod nodejs;
pub mod proxy;
pub mod server;
