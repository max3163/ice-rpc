//! ice-rpc — High-performance RPC framework over iceoryx2 shared memory.
//!
//! # Overview
//!
//! ice-rpc is a client/server RPC framework using iceoryx2 shared memory
//! as transport. From a simple Rust trait annotated with `#[service]`,
//! the procedural macro automatically generates the entire IPC code:
//! client, server, proxy and lifecycle.
//!
//! ## Quick start
//!
//! ### 1. Define a service
//!
//! ```rust,ignore
//! use ice_rpc::{service, Observable};
//! use rkyv::{Archive, Deserialize, Serialize};
//!
//! #[derive(Debug, Archive, Deserialize, Serialize)]
//! pub enum MyError {
//!     NotFound,
//! }
//!
//! #[service("MyService")]
//! pub trait MyService: Send + Sync + 'static {
//!     async fn hello(&self, name: String) -> Observable<String, MyError>;
//! }
//! ```
//!
//! The `#[service("MyService")]` macro automatically generates:
//! - `MyServiceRequest` — rkyv enum for serialization
//! - `MyServiceClient` — IPC client with automatic reconnection
//! - `MyServiceServer` — IPC server with dispatch loop
//! - `MyServiceProxy` — unified entry point (3 modes)
//!
//! ### 2. Start a Provider
//!
//! ```rust,ignore
//! struct MyServiceImpl;
//!
//! #[async_trait::async_trait]
//! impl MyService for MyServiceImpl {
//!     async fn hello(&self, name: String) -> Observable<String, MyError> {
//!         let (tx, rx) = ice_rpc::channel::<String, MyError>(2);
//!         ice_rpc::rt::spawn(async move {
//!             let _ = tx.send_complete_with(format!("Hello {} !", name)).await;
//!         });
//!         rx // no Result: a service returns the observable itself
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // `run_provider!` performs the full bootstrap (init + shutdown).
//!     ice_rpc::run_provider!(
//!         MyServiceProxy::provide(MyServiceImpl),
//!     ).await
//! }
//! ```
//!
//! If an implementation calls `locator().get()` (cross-service dependency):
//!
//! ```rust,ignore
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // This provider also consumes services via locator().get().
//!     ice_rpc::run_provider!(
//!         ServiceAProxy::provide_with_init(ServiceAImpl::new()),
//!         ServiceBProxy::provide_with_init(ServiceBImpl::new()), // depends on ServiceA
//!     ).await
//! }
//! ```
//!
//! ### 3. Call from a Consumer
//!
//! ```rust,ignore
//! #[ice_rpc::main(tokio)]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Proxy instantiated lazily, IPC connection on the first RPC call.
//!     let proxy = ice_rpc::locator()
//!         .get::<MyServiceProxy>().await
//!         .expect("MyService unknown");
//!
//!     let response = proxy.hello("Alice".into()).await.first_value().await?;
//!     log::info!("Response: {}", response);
//!
//!     Ok(()) // `#[ice_rpc::main]` shuts ice-rpc down on exit
//! }
//! ```
//!
//! # Architecture
//!
//! ## Workspace crates
//!
//! | Crate             | Role                                               |
//! |-------------------|----------------------------------------------------|
//! | `ice-rpc`         | Core framework (types, discovery, hub)             |
//! | `ice-rpc-macros`  | Procedural macros (`#[service]`)                   |
//! | `common`          | Example services (not shipped)                     |
//! | `gateway_nodejs`  | Node.js bridge (N-API) for the services            |
//!
//! ## Key concepts
//!
//! - **Service** : Rust trait annotated with `#[service("Name")]` defining RPC methods
//! - **Node** : process hosting one or more services, identified by its PID
//! - **Observable** : the composable stream returned by services; emits
//!   `Next` / `Complete` / `Error(ObservableError)` where the error is either
//!   business (`E`) or technical (`RpcError`)
//! - **NodeHub** : central communication hub managing the IPC publishers/subscribers
//! - **ServiceLocator** : service registry with dependency resolution and topological sort
//! - **NodeDiscovery** : local service→NodeId cache, initial discovery + Event-based updates
//! - **Proxy** : unified entry point supporting 3 modes (Provider / Consumer / ProviderNodeJs)
//!
//! ## Main modules
//!
//! | Module | Role |
//! |--------|------|
//! | `types` | Public Rx types (`Event`, `Observable`, `ObservableError`, `RpcError`, `StreamError`) and the wire types re-exported through `gen` |
//! | `hub` | `NodeHub` : centralized dispatch loop, publishers, response handlers |
//! | `locator` | `ServiceLocator` : registration, lifecycle, Kahn topological sort |
//! | `node_discovery` | `NodeDiscovery` : local cache, service→NodeId resolution |
//! | `blackboard` | Discovery registry: 1 Blackboard per node (`ice_rpc_node_{pid}`), key = service name |
//! | `registry_notify` | Event notifications: carries the NodeId via `EventId` |
//! | `registry_listener` | WaitSet loop: receives the Events, updates the cache, cleans dead nodes |
//! | `node_supervisor` | Node supervisor: broadcasts node death to subscribed clients |
//! | `node_liveness` | Crash detection through iceoryx2's native node monitoring |
//! | `client_core` | Shared consumer state: ConnectionState machine, `resolve_target`, reconnection callback, client bootstrap |
//! | `gen` | Internal contract for the generated code and `ice-rpc-rx`: wire types, provider primitives, constants, plumbing, dependency re-exports (doc-hidden) |
//! | `macros` | `try_or_log!` — internal helper (not exported) |

// The entry-point macros are the primary public API.
#![cfg_attr(test, allow(clippy::unwrap_used))] // test code may panic; production libs keep the deny, see [workspace.lints]
pub use ice_rpc_macros::{main, service};

// Dependency re-exports (`rkyv`, `serde_json`, `base64`, `async_channel`, …)
// now live in `ice_rpc::gen`: the generated code is their only consumer.

mod blackboard;
mod client_core;
mod config;
mod hub;
mod locator;
mod macros;
mod node_discovery;
mod node_liveness;
mod node_supervisor;
mod reconnect_manager;
mod registry_listener;
mod registry_notify;
pub mod rt;
mod service_traits;
mod shutdown;
mod sync;
mod types;

/// Internal facade for the code generated by `#[service]`.
#[doc(hidden)]
pub mod gen;

/// Node.js bridge: dynamic dispatch for the ProviderNodeJs mode.
#[doc(hidden)]
pub mod nodejs_dispatch;

/// Native iceoryx2 request/response transport.
#[doc(hidden)]
pub mod reqres;

#[cfg(feature = "http")]
mod http_gateway;

// ── Public API: service traits ─────────────────────────────────────
// Only `ServiceInit` is implemented by the developer (dependency declaration
// and the `on_init` hook). `ServiceConsumer`, `ServiceLifecycle`,
// `ServiceNamed` and `HttpCallable` are implemented by the generated code and
// remain reachable through `ice_rpc::gen`.
pub use service_traits::ServiceInit;

// ── Public API: Rx vocabulary used in service signatures ────────────
// Everything wire-level (`RpcHeader`, `WireEvent`, `EventKind`, `Sender`,
// `channel`, `NodeId`, correlation ids, tuning constants, `setup_iceoryx2_*`)
// lives in `ice_rpc::gen`, alongside the plumbing invoked by the macros.
pub use types::{Event, Observable, ObservableError, RpcError, StreamError};

// ── Crate-internal aliases ──────────────────────────────────────────
// The wire types and tuning constants are public (doc-hidden) through
// `ice_rpc::gen`; the crate itself keeps private aliases so that internal
// modules can keep referring to them as `crate::X`.
use types::{
    NodeId, BLACKBOARD_MAX_READERS, DEFAULT_TOPIC_BUFFER_SIZE, INITIALIZE_ALL_TIMEOUT_SECS,
    INIT_RETRY_INTERVAL_MS, LARGE_TOPIC_BUFFER_SIZE, PUBLISHER_DEFAULT_MAX_SLICE_LEN,
    PUBLISHER_LARGE_MAX_SLICE_LEN, SERVER_READY_POLL_MS, WAITSET_TIMEOUT_US,
};

// ── Public API: locator ─────────────────────────────────────────────
pub use locator::ServiceLocator;

use std::sync::OnceLock;

pub use crate::rt::CancellationToken;

/// Global cancellation token for the WaitSet loops (dispatch loop).
///
/// Triggered by the native iceoryx2 signal handling
/// (`WaitSetRunResult::Interrupt` for SIGINT/Ctrl+C,
/// `WaitSetRunResult::TerminationRequest` for SIGTERM), by a programmatic
/// [`request_shutdown`], or by the dispatch loop on fatal error.
#[doc(hidden)]
pub fn global_cancel_token() -> &'static CancellationToken {
    static TOKEN: OnceLock<CancellationToken> = OnceLock::new();
    TOKEN.get_or_init(CancellationToken::new)
}

/// Cancellation token for the NODE_REGISTRY listener.
///
/// Is NOT triggered by the dispatch loop on fatal error, only by a real
/// termination signal (SIGINT/SIGTERM) or a programmatic [`request_shutdown`].
/// This allows the listener to survive the provider death and to receive the
/// restart announcements.
#[doc(hidden)]
pub fn registry_cancel_token() -> &'static CancellationToken {
    static TOKEN: OnceLock<CancellationToken> = OnceLock::new();
    TOKEN.get_or_init(CancellationToken::new)
}

/// Cancels the IPC threads and waits for their termination in a single call.
///
/// # Example
/// ```rust,ignore
/// // Prefer the guard returned by `init()` (RAII):
/// let guard = ice_rpc::gen::init();
/// // ... usage ...
/// guard.shutdown().await; // clean shutdown waiting for the IPC threads
/// ```
#[doc(hidden)]
pub async fn shutdown_and_release() {
    global_cancel_token().cancel();
    registry_cancel_token().cancel();
    ServiceLocator::global().release_node().await;
}

/// Cancels **both** cancellation tokens to propagate a termination request.
///
/// Called by the `WaitSet` loops when iceoryx2 reports
/// [`WaitSetRunResult::Interrupt`](iceoryx2::waitset::WaitSetRunResult::Interrupt)
/// (SIGINT, i.e. Ctrl+C) or
/// [`TerminationRequest`](iceoryx2::waitset::WaitSetRunResult::TerminationRequest)
/// (SIGTERM). The dispatch loop on a fatal error must NOT use this helper: it
/// only cancels [`global_cancel_token`], so that the registry listener survives
/// the provider death.
pub(crate) fn request_shutdown() {
    log::info!("Termination signal received, shutting down...");
    global_cancel_token().cancel();
    registry_cancel_token().cancel();
}

/// RAII guard for the automatic shutdown of an ice-rpc process.
///
/// On `Drop` (process end or panic), cancels the cancellation tokens.
/// For a clean shutdown (waiting for the IPC threads), call
/// [`ShutdownGuard::shutdown`] before the end of `main`.
///
/// # Example
/// ```rust,ignore
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let guard = ice_rpc::gen::init(); // configures iceoryx2 + enables signal handling
///     // ... use ice-rpc ...
///     guard.shutdown().await;
///     Ok(())
/// }
/// ```
#[doc(hidden)]
pub struct ShutdownGuard {
    done: std::sync::atomic::AtomicBool,
}

impl ShutdownGuard {
    /// Creates a new shutdown guard.
    pub fn new() -> Self {
        Self {
            done: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Clean shutdown: cancels the tokens, waits for the IPC threads, releases the node.
    ///
    /// Idempotent. After the first call, the subsequent ones have no effect.
    pub async fn shutdown(&self) {
        if self.done.swap(true, std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        shutdown_and_release().await;
    }
}

impl Default for ShutdownGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        if !self.done.load(std::sync::atomic::Ordering::Relaxed) {
            // Fallback: cancel the tokens without waiting (the process is ending).
            global_cancel_token().cancel();
            registry_cancel_token().cancel();
        }
    }
}

/// Returns the global [`ServiceLocator`].
///
/// Short alias of [`ServiceLocator::global()`] to reduce verbosity
/// in applications.
#[inline]
pub fn locator() -> &'static ServiceLocator {
    ServiceLocator::global()
}

/// Waits for the stop signal (SIGINT/SIGTERM or programmatic cancellation).
///
/// Syntactic sugar over `global_cancel_token().cancelled().await`.
/// Used at the end of `main` to block until shutdown.
///
/// # Signal handling
///
/// The Ctrl+C/SIGTERM detection relies on the native iceoryx2 `WaitSet`: a
/// process must have started at least one `WaitSet` loop (the dispatch loop
/// and/or the registry listener) for the signal to be caught. Otherwise the
/// operating system's default disposition applies (hard termination). The
/// framework starts those loops on the first proxy use.
#[doc(hidden)]
pub async fn wait_for_shutdown() {
    global_cancel_token().cancelled().await;
}

// ────────────────────────────────────────────────────────────────────
// Initialization functions
// ────────────────────────────────────────────────────────────────────

/// Whether the iceoryx2 `WaitSet` loops must handle termination signals.
///
/// `true` by default (set by [`init`]); [`init_without_ctrl_c`] clears it so the
/// host keeps full control of the process signals.
static SIGNAL_HANDLING_ENABLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);

/// Performs the one-time process bootstrap.
///
/// The iceoryx2 global configuration is applied exactly once. Calling this
/// several times is safe and has no effect after the first call — which is what
/// allows [`run_provider!`] and `#[ice_rpc::main]` to bootstrap on their own.
///
/// The termination signals (SIGINT/SIGTERM) are no longer handled by a
/// dedicated handler: they are reported by the native iceoryx2 `WaitSet` (see
/// [`waitset_signal_handling_mode`]). [`init`] and [`init_without_ctrl_c`] only
/// toggle [`SIGNAL_HANDLING_ENABLED`].
fn ensure_initialized() {
    static CONFIG: std::sync::Once = std::sync::Once::new();
    CONFIG.call_once(config::setup_iceoryx2_global_config);
}

/// Pure resolver for the `WaitSet` signal handling mode.
///
/// Extracted from [`waitset_signal_handling_mode`] so the mapping stays
/// unit-testable without touching the process-wide flag.
fn resolve_signal_handling_mode(enabled: bool) -> iceoryx2::prelude::SignalHandlingMode {
    if enabled {
        iceoryx2::prelude::SignalHandlingMode::HandleTerminationRequests
    } else {
        iceoryx2::prelude::SignalHandlingMode::Disabled
    }
}

/// Returns the [`SignalHandlingMode`](iceoryx2::prelude::SignalHandlingMode)
/// that the `WaitSet` loops must use.
///
/// With [`init`], the default `HandleTerminationRequests` mode lets iceoryx2
/// install its native SIGINT/SIGTERM handler, so a Ctrl+C makes the blocking
/// wait return
/// [`WaitSetRunResult::Interrupt`](iceoryx2::waitset::WaitSetRunResult::Interrupt)
/// immediately, instead of relying on the polling timeout.
/// [`init_without_ctrl_c`] selects `Disabled`.
pub(crate) fn waitset_signal_handling_mode() -> iceoryx2::prelude::SignalHandlingMode {
    resolve_signal_handling_mode(SIGNAL_HANDLING_ENABLED.load(std::sync::atomic::Ordering::Relaxed))
}

/// Initializes the framework and returns the RAII shutdown guard.
///
/// Configures iceoryx2, enables the native signal handling of the `WaitSet`
/// loops (SIGINT/SIGTERM) and returns the [`ShutdownGuard`] owning the process
/// lifetime. Suitable for consumers, providers and provider+consumer processes
/// alike: no service registry is needed, proxies are instantiated on demand
/// from their type.
///
/// The returned guard **must be kept alive**: dropping it cancels the global
/// tokens. Call [`ShutdownGuard::shutdown`] for a clean stop (waiting for the
/// IPC threads and releasing the iceoryx2 node).
///
/// # Example
/// ```rust,ignore
/// let guard = ice_rpc::gen::init();
/// let proxy = ice_rpc::locator().get::<MyServiceProxy>().await.unwrap();
/// // ... use ice-rpc ...
/// guard.shutdown().await;
/// ```
#[doc(hidden)]
#[must_use = "the guard cancels the ice-rpc tokens when dropped; bind it for the process lifetime"]
pub fn init() -> ShutdownGuard {
    ensure_initialized();
    SIGNAL_HANDLING_ENABLED.store(true, std::sync::atomic::Ordering::Relaxed);
    ShutdownGuard::new()
}

/// Initializes the framework **without** signal handling and returns the RAII
/// shutdown guard (Node.js gateway, tests…).
///
/// Variant of [`init`] for contexts that must manage shutdown manually (N-API
/// thread, tests, embedded executors). The `WaitSet` loops are created with
/// [`SignalHandlingMode::Disabled`](iceoryx2::prelude::SignalHandlingMode), so
/// iceoryx2 registers no SIGINT/SIGTERM handler and the host keeps full control
/// of the process signals. The returned guard should be **stored** for the host
/// lifetime, otherwise dropping it cancels the tokens.
///
/// # Example
/// ```rust,ignore
/// let _guard = ice_rpc::gen::init_without_ctrl_c();
/// ```
#[doc(hidden)]
#[must_use = "the guard cancels the ice-rpc tokens when dropped; bind it for the host lifetime"]
pub fn init_without_ctrl_c() -> ShutdownGuard {
    ensure_initialized();
    SIGNAL_HANDLING_ENABLED.store(false, std::sync::atomic::Ordering::Relaxed);
    ShutdownGuard::new()
}

/// Starts the HTTP REST gateway with the given service mapping.
///
/// Requires the `http` feature in `Cargo.toml`:
/// ```toml
/// ice-rpc = { features = ["http"] }
/// ```
///
/// Prefer the [`start_http_gateway!`] macro which builds the mapping
/// automatically from the list of exposed proxies.
#[cfg(feature = "http")]
#[doc(hidden)]
pub async fn start_http_server(
    port: u16,
    factories: std::collections::HashMap<
        &'static str,
        fn() -> std::sync::Arc<dyn service_traits::HttpCallable>,
    >,
) {
    http_gateway::start_http_server(port, factories).await;
}

/// Starts the HTTP REST gateway exposing the listed services.
///
/// Builds locally the `name → factory` mapping from the provided proxy
/// types, then starts the HTTP server. No build.rs nor global
/// registry is needed: only the services exposed by this process
/// are listed here.
///
/// # Example
/// ```rust,ignore
/// #[ice_rpc::main(tokio)]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     ice_rpc::start_http_gateway!(8080, DatabaseServiceProxy, ConfigServiceProxy).await;
///     Ok(())
/// }
/// ```
#[cfg(feature = "http")]
#[macro_export]
macro_rules! start_http_gateway {
    ($port:expr, $($proxy:ty),+ $(,)?) => {{
        let mut __map: std::collections::HashMap<
            &'static str,
            fn() -> std::sync::Arc<dyn ice_rpc::gen::HttpCallable>,
        > = std::collections::HashMap::new();
        $(
            __map.insert(
                <$proxy>::SERVICE_NAME,
                || <$proxy>::consume() as std::sync::Arc<dyn ice_rpc::gen::HttpCallable>,
            );
        )+
        ice_rpc::start_http_server($port, __map)
    }};
}

/// Object trait allowing [`run_provider!`] to accept heterogeneous
/// proxies in a `Vec` without knowing their concrete type.
///
/// Implemented automatically for any type satisfying the constraints
/// of [`ServiceLocator::register`]. **Do not implement manually.**
#[doc(hidden)]
#[async_trait::async_trait]
pub trait _ProviderService: Send + Sync + 'static {
    async fn register_into(&self, locator: &'static ServiceLocator);
}

#[async_trait::async_trait]
impl<T> _ProviderService for std::sync::Arc<T>
where
    T: crate::service_traits::ServiceLifecycle
        + crate::service_traits::ServiceNamed
        + crate::service_traits::ServiceInit
        + std::any::Any
        + Send
        + Sync
        + 'static,
{
    async fn register_into(&self, locator: &'static ServiceLocator) {
        locator.register(self.clone()).await;
    }
}

/// Registers and initializes a list of Provider services, waits for Ctrl+C,
/// then performs the clean shutdown.
///
/// Internal function called by the [`run_provider!`] macro.
/// Prefer the macro for direct usage.
#[doc(hidden)]
pub async fn run_provider_inner(
    services: Vec<Box<dyn _ProviderService>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Full bootstrap: `run_provider!` is self-contained, so a provider `main`
    // does not need a preliminary `init()`. Idempotent, so a process that
    // already called `init()` is unaffected. Signal handling is enabled
    // explicitly so a prior `init_without_ctrl_c()` cannot disable it.
    ensure_initialized();
    SIGNAL_HANDLING_ENABLED.store(true, std::sync::atomic::Ordering::Relaxed);

    // RAII guard: cancels the tokens on panic before the final shutdown_and_release.
    // The explicit shutdown() at the end of the function ensures a clean stop waiting
    // for the IPC threads and releasing the iceoryx2 node.
    let guard = ShutdownGuard::new();

    let loc = ServiceLocator::global();
    for svc in services {
        svc.register_into(loc).await;
    }
    loc.initialize_all().await?;
    log::info!("All services are ready. Press Ctrl+C to stop.");
    wait_for_shutdown().await;
    log::info!("Stopping provider...");
    guard.shutdown().await;
    Ok(())
}

/// Starts the provider with the given services.
///
/// Registers each service, initializes everything in topological order,
/// blocks until Ctrl+C, then performs a clean shutdown.
///
/// Returns a `Future` — must be `.await`ed in an async context.
///
/// # Example — Pure provider
/// ```rust,ignore
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     env_logger::init();
///     // `run_provider!` bootstraps ice-rpc and shuts it down on exit.
///     ice_rpc::run_provider!(
///         ConfigServiceProxy::provide_with_init(ConfigServiceImpl::new("config.toml")),
///         DatabaseServiceProxy::provide_with_init(DatabaseServiceImpl::new()),
///     ).await
/// }
/// ```
///
/// # Example — Provider that also consumes external services
/// ```rust,ignore
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     env_logger::init();
///     // DatabaseServiceImpl calls ConfigService via get()
///     ice_rpc::run_provider!(
///         ConfigServiceProxy::provide_with_init(ConfigServiceImpl::new("config.toml")),
///         DatabaseServiceProxy::provide_with_init(DatabaseServiceImpl::new()),
///     ).await
/// }
/// ```
#[macro_export]
macro_rules! run_provider {
    ($($proxy:expr),+ $(,)?) => {{
        ice_rpc::run_provider_inner(vec![
            $(Box::new($proxy) as Box<dyn ice_rpc::_ProviderService>),+
        ])
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── global_cancel_token / registry_cancel_token ──────────────────

    #[test]
    fn global_cancel_token_is_singleton() {
        let t1 = global_cancel_token();
        let t2 = global_cancel_token();
        assert!(std::ptr::eq(t1, t2), "must return the same instance");
    }

    #[test]
    fn registry_cancel_token_is_singleton() {
        let t1 = registry_cancel_token();
        let t2 = registry_cancel_token();
        assert!(std::ptr::eq(t1, t2), "must return the same instance");
    }

    #[test]
    fn cancel_tokens_are_distinct() {
        let t1 = global_cancel_token();
        let t2 = registry_cancel_token();
        assert!(!std::ptr::eq(t1, t2), "must be different instances");
    }

    #[test]
    fn shutdown_guard_drop_cancels_tokens() {
        let t1 = global_cancel_token().clone();
        let t2 = registry_cancel_token().clone();
        {
            let _guard = ShutdownGuard::new();
            // The guard has not called shutdown() yet → Drop cancels the tokens.
        }
        assert!(t1.is_cancelled());
        assert!(t2.is_cancelled());
    }

    // ── locator ──────────────────────────────────────────────────────

    #[test]
    fn locator_returns_global_instance() {
        let l1 = locator();
        let l2 = ServiceLocator::global();
        assert!(
            std::ptr::eq(l1, l2),
            "locator() must be the global instance"
        );
    }

    // ── wait_for_shutdown / shutdown_and_release ─────────────────────

    #[test]
    fn wait_for_shutdown_returns_when_cancelled() {
        let token = global_cancel_token().clone();
        // Cancels in 10ms
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(10));
            token.cancel();
        });
        // Must not block indefinitely. `test_block_on` provides a tokio runtime
        // when the `tokio` facade is active, since `rt::timeout` relies on
        // `rt::sleep` under that feature.
        crate::rt::test_block_on(crate::rt::timeout(
            std::time::Duration::from_secs(2),
            wait_for_shutdown(),
        ))
        .expect("wait_for_shutdown did not return in time");
    }

    #[test]
    fn shutdown_and_release_does_not_panic_when_no_node() {
        // Without a created iceoryx2 Node, shutdown_and_release must terminate
        // cleanly. It uses `rt::timeout` internally, hence `test_block_on`.
        crate::rt::test_block_on(shutdown_and_release());
    }

    // ── signal handling mode ─────────────────────────────────────────

    #[test]
    fn signal_handling_mode_enabled_maps_to_native_termination_requests() {
        assert_eq!(
            resolve_signal_handling_mode(true),
            iceoryx2::prelude::SignalHandlingMode::HandleTerminationRequests,
        );
    }

    #[test]
    fn signal_handling_mode_disabled_maps_to_disabled() {
        assert_eq!(
            resolve_signal_handling_mode(false),
            iceoryx2::prelude::SignalHandlingMode::Disabled,
        );
    }

    #[test]
    fn waitset_signal_handling_mode_defaults_to_enabled() {
        // No test clears the flag, so the process-wide default stays `true`.
        assert_eq!(
            waitset_signal_handling_mode(),
            iceoryx2::prelude::SignalHandlingMode::HandleTerminationRequests,
        );
    }
}
