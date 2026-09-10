//! Shared definitions of ice-rpc services (usage example).
//!
//! This crate is **not shipped**: it illustrates how to define services
//! with the `#[service]` macro. Each annotated trait automatically generates
//! its Proxy, Client, Server and lifecycle implementations.
//!
//! | Sub-module       | Contents                                                       |
//! |------------------|----------------------------------------------------------------|
//! | [`config`]       | `ConfigService` + `ConfigError`                                |
//! | [`context`]      | `ContextService` + `ContextError` + `ContextEntry`             |
//! | [`database`]     | `DatabaseService` + `DatabaseError` + `PersonneQuery`/`PersonneInfo` |
//! | [`http`]         | `HttpService` + `HttpRequestParams`/`HttpResponseParams` + `HttpError` |
//! | [`notification`] | `NotificationService` (multi-value stream for `subscribe`)     |
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
//! if let Ok(val) = proxy.get("my.key".into()).await?.first_value().await {
//!     // use `val` here
//! }
//! ```
//!

#![allow(missing_docs)]
// example crate: rkyv's Archive derive emits an undocumented Archived* struct per service that no consumer-side attribute can reach
#![cfg_attr(test, allow(clippy::unwrap_used))] // test code may panic; production libs keep the deny, see [workspace.lints]
pub mod config;
pub mod context;
pub mod database;
pub mod http;
pub mod notification;

pub use config::*;
pub use context::*;
pub use database::*;
pub use http::*;
pub use notification::*;

/// Inventory of the services this crate exposes as **Node.js providers**.
///
/// # Why this lives here
///
/// `gateway_nodejs` must register one `#[service]` proxy per service it
/// advertises. Keeping that list inside the gateway — far from the
/// declarations — let it drift silently: `NotificationService` was declared
/// here with `#[service]` but was missing from the gateway list, so the Node.js
/// process could never expose it as a provider. The list therefore lives next
/// to the declarations, and the
/// [`nodejs_provider_inventory_is_exhaustive`](self) test fails as soon as a
/// declared service is missing from it.
///
/// # Usage
///
/// `with_nodejs_providers!(my_macro, extra_args…)` expands to
/// `my_macro!(extra_args…, Proxy1, Proxy2, …)`.
///
/// See `gateway_nodejs::services::register_service` (registration) for the only
/// production consumer.
#[macro_export]
macro_rules! with_nodejs_providers {
    ($callback:ident $(, $arg:expr)* $(,)?) => {
        $callback!(
            $($arg,)*
            $crate::ConfigServiceProxy,
            $crate::ContextServiceProxy,
            $crate::DatabaseServiceProxy,
            $crate::HttpServiceProxy,
            $crate::NotificationServiceProxy,
        )
    };
}

#[cfg(test)]
mod nodejs_provider_inventory {
    /// The sources holding the `#[service]` declarations of this crate.
    const SOURCES: [&str; 5] = [
        include_str!("config.rs"),
        include_str!("context.rs"),
        include_str!("database.rs"),
        include_str!("http.rs"),
        include_str!("notification.rs"),
    ];

    /// Guard for M14: every `#[service("Name")]` of this crate must be listed in
    /// [`with_nodejs_providers!`](crate::with_nodejs_providers).
    ///
    /// Without it, a new service compiles and runs but is silently absent from
    /// the Node.js surface — exactly the failure `NotificationService` hit.
    #[test]
    fn nodejs_provider_inventory_is_exhaustive() {
        // 1. Logical names declared in the sources.
        let mut declared: Vec<String> = Vec::new();
        for src in SOURCES {
            let attributes = src.matches("#[service(").count();
            let names = service_names(src);
            assert_eq!(
                attributes,
                names.len(),
                "this guard only understands the explicit `#[service(\"Name\")]` form: \
                 found {attributes} attribute(s) but parsed {} name(s)",
                names.len()
            );
            declared.extend(names);
        }
        declared.sort_unstable();
        declared.dedup();

        // 2. Logical names advertised by the inventory, read from the macro
        //    itself — not from a text copy that could drift too.
        macro_rules! collect_names {
            ($($proxy:ty),* $(,)?) => {
                {
                    let mut names: Vec<String> =
                        vec![$(<$proxy>::SERVICE_NAME.to_string()),*];
                    names.sort_unstable();
                    names.dedup();
                    names
                }
            };
        }
        let listed = crate::with_nodejs_providers!(collect_names);

        assert_eq!(
            declared, listed,
            "the Node.js provider inventory must list every `#[service]` of this crate"
        );
    }

    /// Extracts the logical name of each explicit `#[service("Name")]`.
    fn service_names(src: &str) -> Vec<String> {
        let mut names = Vec::new();
        let mut rest = src;
        while let Some(start) = rest.find("#[service(") {
            rest = &rest[start + "#[service(".len()..];
            let Some(open) = rest.find('"') else { continue };
            let after_open = &rest[open + 1..];
            let Some(close) = after_open.find('"') else {
                continue;
            };
            names.push(after_open[..close].to_string());
            rest = &after_open[close..];
        }
        names
    }

    /// The inventory must not be empty, so a broken macro fails loudly here
    /// rather than silently disabling every provider.
    #[test]
    fn inventory_is_not_empty() {
        macro_rules! count {
            ($($proxy:ty),* $(,)?) => {
                [$(stringify!($proxy)),*].len()
            };
        }
        assert!(crate::with_nodejs_providers!(count) >= SOURCES.len());
    }
}
