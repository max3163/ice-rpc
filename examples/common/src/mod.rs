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
//! | [`workload`]     | `WorkloadService` (benchmark payload shapes)                   |
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
pub mod workload;

pub use config::*;
pub use context::*;
pub use database::*;
pub use http::*;
pub use notification::*;
pub use workload::*;

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
                "MaskedService",
                "NotificationService",
                "WorkloadService",
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

/// The masking proof: a service, its payloads and its provider `impl` all
/// compile in a crate that declares **no** `rkyv`, `log` or `async-trait`
/// dependency of its own (see this crate's `Cargo.toml`).
///
/// Nothing here names any of the three: `#[ice_rpc::payload]` supplies the rkyv
/// derives and redirects their expansion through `ice_rpc::gen::rkyv`,
/// `#[ice_rpc::async_trait]` annotates the `impl`, and `#[service]` reaches the
/// rest — including the `log` events and the generated `impl` attributes — the
/// same way.
#[cfg(test)]
mod usage_without_direct_dependencies {
    use ice_rpc::gen::{rkyv, WireEvent};
    use ice_rpc::{service, Observable};

    /// A payload used both as a request argument and as a response value.
    ///
    /// The `serde` derives are the ones the `json` / `http` features require on
    /// a service type — see the rest of this crate. They are unrelated to the
    /// masking: `#[ice_rpc::payload]` only supplies the rkyv derives.
    #[ice_rpc::payload]
    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    pub struct MaskedPayload {
        /// A text field.
        pub name: String,
        /// A count the handler increments.
        pub count: u32,
    }

    /// A service whose request and response exercise the generated derives.
    #[service("MaskedService")]
    pub trait MaskedService {
        /// Echoes the payload, incremented so the round-trip is observable.
        async fn echo(&self, payload: MaskedPayload) -> Observable<MaskedPayload, String>;
    }

    /// The provider implementation, annotated through the re-export.
    struct MaskedImpl;

    #[ice_rpc::async_trait]
    impl MaskedService for MaskedImpl {
        async fn echo(&self, mut payload: MaskedPayload) -> Observable<MaskedPayload, String> {
            payload.count += 1;
            ice_rpc::of(payload)
        }
    }

    #[test]
    fn the_generated_request_round_trips_through_the_rkyv_re_export() {
        let request = MaskedServiceRequest::Echo {
            payload: MaskedPayload {
                name: "a".into(),
                count: 1,
            },
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&request).expect("encode request");
        let decoded = rkyv::from_bytes::<MaskedServiceRequest, rkyv::rancor::Error>(&bytes)
            .expect("decode request");
        match decoded {
            MaskedServiceRequest::Echo { payload } => assert_eq!(
                payload,
                MaskedPayload {
                    name: "a".into(),
                    count: 1,
                }
            ),
        }
    }

    #[test]
    fn the_payload_carried_by_a_response_decodes_back() {
        let event = WireEvent::<MaskedPayload, String>::Next(MaskedPayload {
            name: "b".into(),
            count: 2,
        });
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&event).expect("encode response");
        let decoded =
            rkyv::from_bytes::<WireEvent<MaskedPayload, String>, rkyv::rancor::Error>(&bytes)
                .expect("decode response");
        match decoded {
            WireEvent::Next(value) => assert_eq!(
                value,
                MaskedPayload {
                    name: "b".into(),
                    count: 2,
                }
            ),
            other => panic!("expected a Next event, got {other:?}"),
        }
    }

    #[test]
    fn the_masked_proxy_keeps_the_declared_name_and_can_be_built() {
        assert_eq!(MaskedServiceProxy::SERVICE_NAME, "MaskedService");
        let _provider = MaskedImpl;
    }
}
