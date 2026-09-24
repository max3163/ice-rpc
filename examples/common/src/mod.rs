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
//! ## Declarations only
//!
//! This crate declares services; it keeps no list of them. The three consumers
//! read the declarations instead:
//!
//! | Consumer | How it finds them |
//! |---|---|
//! | a provider or a consumer of one service | `ServiceLocator::get::<{Service}Proxy>()`, from the type it needs |
//! | an observer (`monitoring`) | [`Decoders::linked`], which reads the link-time slice every `#[service]` submits into |
//! | the Node.js gateway (`napi`) | its **own** list: it maintains a chosen subset, and the compiler checks it against the generated proxies |
//!
//! The first two cost nothing to maintain. The third is a list on purpose — a
//! gateway that advertises five of the fifty services a large `common` may hold
//! should say which five, in its own code, rather than depend on this crate
//! agreeing with it.
//!
//! ## Features
//!
//! Each one only forwards to the `ice-rpc` feature that switches the generated
//! code on; this crate exports nothing for them:
//!
//! - `napi` → `ice-rpc/json`: the rkyv ↔ JSON converters and the `ProviderJson` mode;
//! - `http` → `ice-rpc/http`: the `JsonInvoker` view the REST gateway dispatches to;
//! - `monitoring` → `ice-rpc/monitoring`: the `{Service}Decoder`s and the
//!   link-time registration an observer reads back with `Decoders::linked()`.

#![allow(missing_docs)]
// example crate: rkyv's Archive derive emits an undocumented Archived* struct per service
#![cfg_attr(test, allow(clippy::unwrap_used))] // test code may panic
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

/// The decoders this crate generates are **submitted at link time**.
///
/// Nothing here lists them: with the `monitoring` feature, each `#[service]`
/// expansion appends one entry to `ice_rpc::monitor::DECODERS`, and
/// `Decoders::linked()` reads the slice back. An observer therefore renders
/// exactly the services linked into its binary — including the ones a
/// hand-written list had forgotten, which is the defect this replaced.
///
/// The one rule: a binary that never references this crate does not link it, and
/// the slice is then empty. Writing `use common as _;` (or naming any type of
/// this crate) is what anchors it.
#[cfg(all(test, feature = "monitoring"))]
mod linked_decoders {
    use ice_rpc::gen::{rkyv, service_id_of, WireEvent};
    use ice_rpc::monitor::{Decoders, DECODERS};

    /// Every `#[service]` of this crate must be in the slice — that is the guard
    /// the hand-written inventory used to be, now checked against the mechanism.
    #[test]
    fn the_linked_registry_covers_the_declared_services() {
        let mut names: Vec<&str> = DECODERS
            .iter()
            .map(|registration| registration.service_name)
            .collect();
        names.sort_unstable();
        assert_eq!(
            names,
            [
                "ConfigService",
                "ContextService",
                "DatabaseService",
                "HttpService",
                "NotificationService",
            ],
            "every `#[service]` of this crate must submit a decoder"
        );
        assert_eq!(Decoders::linked().len(), names.len());
    }

    /// The linked decoders render the real wire encoding through `Display`.
    #[test]
    fn decoders_render_the_common_types() {
        fn encode<T>(value: &T) -> Vec<u8>
        where
            T: for<'a> rkyv::Serialize<
                rkyv::rancor::Strategy<
                    rkyv::ser::Serializer<
                        rkyv::util::AlignedVec,
                        rkyv::ser::allocator::ArenaHandle<'a>,
                        rkyv::ser::sharing::Share,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
        {
            rkyv::to_bytes::<rkyv::rancor::Error>(value)
                .expect("encode")
                .to_vec()
        }

        let decoders = Decoders::linked();
        let database = service_id_of("DatabaseService");
        let request = encode(&crate::DatabaseServiceRequest::GetUserAge {
            name: "Alice".into(),
        });
        assert_eq!(
            decoders
                .request(database, "get_user_age", &request)
                .as_deref(),
            Some("get_user_age(name=Alice)")
        );

        let response = encode(&WireEvent::<i32, crate::DatabaseError>::Next(30));
        assert_eq!(
            decoders
                .response(database, "get_user_age", &response)
                .as_deref(),
            Some("30")
        );

        // A unit success type (`Observable<(), String>`) has its own decoder.
        let notification = service_id_of("NotificationService");
        let unit = encode(&WireEvent::<(), String>::Complete);
        assert_eq!(
            decoders.response(notification, "ping", &unit).as_deref(),
            Some("complete")
        );
    }
}
