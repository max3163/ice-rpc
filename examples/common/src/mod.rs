//! Shared definitions of ice-rpc services (usage example).
//!
//! This crate is **not shipped**: it illustrates how to define services
//! with the `#[service]` macro. Each annotated trait automatically generates
//! its Proxy, Client, Server and lifecycle implementations.
//!
//! | Sub-module   | Contents                                                       |
//! |--------------|----------------------------------------------------------------|
//! | [`config`]   | `ConfigService` + `ConfigError`                                |
//! | [`context`]  | `ContextService` + `ContextError` + `ContextEntry`             |
//! | [`database`] | `DatabaseService` + `DatabaseError` + `PersonneQuery`/`PersonneInfo` |
//! | [`http`]     | `HttpService` + `HttpRequestParams`/`HttpResponseParams` + `HttpError` |
//!
//! ## Lazy consumption
//!
//! No registry is required: [`ice_rpc::ServiceLocator::get`] instantiates
//! a Consumer proxy on demand from its type:
//!
//! ```rust,ignore
//! let proxy = ice_rpc::locator()
//!     .get::<ContextServiceProxy>()
//!     .await
//!     .expect("unknown service");
//! let val = take_one!(proxy.get("my.key".into()))?;
//! ```
//!

pub mod config;
pub mod context;
pub mod database;
pub mod http;

pub use config::*;
pub use context::*;
pub use database::*;
pub use http::*;
