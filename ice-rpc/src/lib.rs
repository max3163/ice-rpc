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
//! - `MyServiceClient` — IPC client (publish/subscribe, one call per request id)
//! - `MyServiceServer` — IPC server exposing the method dispatcher
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
//!         // One value, then completion. A handler streaming several values uses
//!         // the wire-level `ice_rpc::gen::channel(capacity)`, which returns a
//!         // `(Sender, Observable)` pair whose `send_complete_with` closes the
//!         // stream with its last value.
//!         Observable::from_events([
//!             ice_rpc::Event::Next(format!("Hello {} !", name)),
//!             ice_rpc::Event::Complete,
//!         ]) // no Result: a service returns the observable itself
//!     }
//! }
//!
//! #[ice_rpc::main(tokio)]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // `#[ice_rpc::main]` owns the bootstrap (init + runtime) and the clean
//!     // shutdown; `run_provider!` only starts the services and waits for Ctrl+C.
//!     ice_rpc::run_provider!(
//!         MyServiceProxy::provide(MyServiceImpl),
//!     ).await
//! }
//! ```
//!
//! If an implementation calls `locator().get()` (cross-service dependency):
//!
//! ```rust,ignore
//! #[ice_rpc::main(tokio)]
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
//! | `ice-rpc-rx`      | Reactive layer: `Observable`, operators, `Subject`, execution facade |
//! | `ice-rpc`         | Core framework (protocol, transport, ServiceLocator) |
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
//! - **ServiceLocator** : registry of the services the process provides, plus a
//!   lazy cache of the consumer proxies
//! - **transport** : iceoryx2 publish/subscribe, one channel per service,
//!   correlated by a 16-byte request id (`ice_rpc::transport`)
//! - **Proxy** : unified entry point supporting 3 modes (Provider / Consumer / ProviderNodeJs)
//!
//! ## Main modules
//!
//! | Module | Role |
//! |--------|------|
//! | `rx` | The reactive layer, re-exported from `ice-rpc-rx`: operators, constructors and multicast primitives, all reachable from the `Observable` stream |
//! | `rt` | Execution facade, re-exported from `ice-rpc-rx`: `spawn`, `sleep`, `block_on`, cancellation |
//! | `types` | Protocol types (`RpcHeader`, `EventKind`, `WireEvent`) and the reactive vocabulary re-exported from `ice-rpc-rx` |
//! | `transport` | Publish/subscribe transport: one channel per service, correlation by id, streaming bridge |
//! | `locator` | `ServiceLocator` : registration, lazy consumer proxies, lifecycle |
//! | `node_liveness` | Crash detection through iceoryx2's native node monitoring |
//! | `gen` | Internal contract for the generated code: wire types, provider primitives, constants, plumbing, dependency re-exports (doc-hidden) |

#![cfg_attr(test, allow(clippy::unwrap_used))] // test code may panic
pub use ice_rpc_macros::{main, service};

// Dependency re-exports (`rkyv`, `serde_json`, `base64`, …) live in `ice_rpc::gen`.

mod config;
mod global;
mod labels;
mod locator;
mod node_liveness;
mod service_traits;
mod shutdown;
mod sync;
mod types;

use crate::global::Global;

/// Internal facade for the code generated by `#[service]`.
#[doc(hidden)]
pub mod gen;

/// Node.js bridge: dynamic dispatch for the ProviderNodeJs mode.
#[doc(hidden)]
pub mod nodejs_dispatch;

/// Publish/subscribe transport (one channel per service, correlated by id).
#[doc(hidden)]
pub mod transport;

#[cfg(feature = "http")]
mod http_gateway;

// ── Public API: service traits ─────────────────────────────────────
// Only `ServiceInit` is implemented by the developer.
pub use service_traits::ServiceInit;

// ── Public API: the reactive vocabulary ─────────────────────────────
// Defined by `ice-rpc-rx` and re-exported unchanged: the crate keeps one single
// stream type, and a consumer never has to name the stream crate.
pub use ice_rpc_rx::{
    from, of, throw_error, CancellationToken, Event, Observable, ObservableError, RpcError,
    Subject, Subscription,
};

// Wire-level items live in `ice_rpc::gen`; the protocol types come from `types`.
pub use types::{CallContext, TraceContext};

/// Reactive layer, re-exported under its historical path.
///
/// The items live in [`ice_rpc_rx`]; this module keeps the paths a consumer of
/// `ice-rpc` may already use (`ice_rpc::rx::from`, `ice_rpc::rx::Subject`, …).
pub mod rx {
    pub use ice_rpc_rx::*;
}

/// Runtime-agnostic execution facade, re-exported under its historical path.
///
/// `spawn`, `sleep`, `block_on` and the cancellation token live in
/// [`ice_rpc_rx::rt`]; the transport and the examples keep using them here.
pub mod rt {
    pub use ice_rpc_rx::rt::*;
}

// ── Public API: locator ─────────────────────────────────────────────
pub use locator::ServiceLocator;

// ── Public API: out-of-band monitoring ──────────────────────────────
pub mod monitor;

/// Global cancellation token for the background IPC threads.
///
/// Triggered on shutdown (see [`ShutdownGuard`]) and by
/// [`shutdown_and_release`].
#[doc(hidden)]
pub fn global_cancel_token() -> &'static CancellationToken {
    static TOKEN: Global<CancellationToken> = Global::new();
    TOKEN.get_or_init(CancellationToken::new)
}

/// Secondary cancellation token, kept distinct from [`global_cancel_token`].
///
/// Reserved for background helpers that must survive a global cancellation;
/// both are cancelled by [`shutdown_and_release`] and by [`ShutdownGuard`].
#[doc(hidden)]
pub fn registry_cancel_token() -> &'static CancellationToken {
    static TOKEN: Global<CancellationToken> = Global::new();
    TOKEN.get_or_init(CancellationToken::new)
}

/// Cancels both tokens to propagate a termination signal to the IPC threads.
///
/// Called by the transport loops when iceoryx2 reports a termination through
/// its `WaitSet`, or when they observe the termination flag while polling.
pub(crate) fn request_shutdown() {
    log::info!("Termination signal received, shutting down...");
    global_cancel_token().cancel();
    registry_cancel_token().cancel();
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

    // `release_node` joins the dispatch threads, and a thread owns its ports:
    // joining it is what makes iceoryx2 unlink the services it created. What
    // survives is the cache of consumed channels, which lives in a `static` —
    // Rust never drops a `static`, so it is released here explicitly.
    let released = crate::transport::release_process_ports();
    if released > 0 {
        log::info!("[ice-rpc] released {released} cached channel port(s)");
    }
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
#[doc(hidden)]
pub async fn wait_for_shutdown() {
    global_cancel_token().cancelled().await;
}

// ────────────────────────────────────────────────────────────────────
// Initialization functions
// ────────────────────────────────────────────────────────────────────

/// Whether the framework handles the process signals through iceoryx2.
///
/// Set by [`init`] (the default) and cleared by [`init_without_ctrl_c`].
static SIGNAL_HANDLING_ENABLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);

/// Maps the signal-handling flag to the iceoryx2 `WaitSet` mode.
fn resolve_signal_handling_mode(enabled: bool) -> iceoryx2::prelude::SignalHandlingMode {
    if enabled {
        iceoryx2::prelude::SignalHandlingMode::HandleTerminationRequests
    } else {
        iceoryx2::prelude::SignalHandlingMode::Disabled
    }
}

/// Returns the signal-handling mode the transport must give to its `WaitSet`s.
pub(crate) fn waitset_signal_handling_mode() -> iceoryx2::prelude::SignalHandlingMode {
    resolve_signal_handling_mode(SIGNAL_HANDLING_ENABLED.load(std::sync::atomic::Ordering::Relaxed))
}

/// Performs the one-time process bootstrap (iceoryx2 global configuration).
///
/// Idempotent: calling it several times has no effect after the first call.
fn ensure_initialized() {
    static CONFIG: Global<()> = Global::new();
    CONFIG.get_or_init(config::setup_iceoryx2_global_config);
}

/// Initializes the framework and returns the RAII shutdown guard.
///
/// Configures iceoryx2 and enables the native signal handling of the `WaitSet`
/// loops (SIGINT/SIGTERM). The returned guard **must be kept alive**: dropping
/// it cancels the global tokens. Call [`ShutdownGuard::shutdown`] for a clean
/// stop.
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
/// Variant of [`init`] for contexts that must manage shutdown manually. The
/// returned guard should be **stored** for the host lifetime.
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

/// Registers and initializes a list of Provider services, then blocks until the
/// process shutdown is requested (Ctrl+C).
///
/// Internal function called by the [`run_provider!`] macro.
/// Prefer the macro for direct usage.
///
/// The process lifecycle (iceoryx2 bootstrap, signal handling, runtime and
/// clean shutdown) is owned by `#[ice_rpc::main]`. This function only starts the
/// services and keeps the process alive until cancellation, so it **must** be
/// awaited from within an `#[ice_rpc::main]` body.
#[doc(hidden)]
pub async fn run_provider_inner(
    services: Vec<Box<dyn _ProviderService>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Defensive and idempotent: the bootstrap is normally performed by
    // `#[ice_rpc::main]`, which also owns the shutdown. Enabling signal handling
    // here guarantees the `WaitSet`s created below report SIGINT/SIGTERM even if
    // the macro was bypassed (e.g. after `init_without_ctrl_c()`).
    ensure_initialized();
    SIGNAL_HANDLING_ENABLED.store(true, std::sync::atomic::Ordering::Relaxed);

    // Reap what previous runs left behind, before creating any service: a provider
    // that was killed is still on the bus, and nothing else removes it — see
    // `transport::cleanup_dead_nodes`. This is what makes a machine self-healing
    // in production, where nobody runs a purge script.
    let reaped = crate::transport::cleanup_dead_nodes();
    if reaped > 0 {
        log::info!("[ice-rpc] reaped {reaped} dead node(s) left by previous runs");
    }

    let loc = ServiceLocator::global();
    for svc in services {
        svc.register_into(loc).await;
    }
    loc.initialize_all().await?;
    log::info!("All services are ready. Press Ctrl+C to stop.");
    wait_for_shutdown().await;
    log::info!("Stopping services...");
    Ok(())
}

/// Starts the provider with the given services.
///
/// Registers each service, initializes everything in topological order, then
/// blocks until the process shutdown is requested (Ctrl+C). The clean shutdown
/// itself is owned by `#[ice_rpc::main]`: this macro only starts the services.
///
/// Returns a `Future` — must be `.await`ed from an `#[ice_rpc::main]` body.
///
/// # Example — Pure provider
/// ```rust,ignore
/// #[ice_rpc::main(tokio)]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     env_logger::init();
///     // `#[ice_rpc::main]` owns init + shutdown; `run_provider!` starts them.
///     ice_rpc::run_provider!(
///         ConfigServiceProxy::provide_with_init(ConfigServiceImpl::new("config.toml")),
///         DatabaseServiceProxy::provide_with_init(DatabaseServiceImpl::new()),
///     ).await
/// }
/// ```
///
/// # Example — Provider that also consumes external services
/// ```rust,ignore
/// #[ice_rpc::main(tokio)]
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
        // Must not block indefinitely.
        crate::rt::test_block_on(crate::rt::timeout(
            std::time::Duration::from_secs(2),
            wait_for_shutdown(),
        ))
        .expect("wait_for_shutdown did not return in time");
    }

    #[test]
    fn shutdown_and_release_does_not_panic_when_no_node() {
        // Without a created iceoryx2 Node, shutdown_and_release must terminate
        // cleanly.
        crate::rt::test_block_on(shutdown_and_release());
    }

    // ── signal handling ──────────────────────────────────────────────

    #[test]
    fn signal_handling_flag_selects_the_waitset_mode() {
        use iceoryx2::prelude::SignalHandlingMode;

        // Enabled: iceoryx2 captures SIGINT/SIGTERM.
        assert_eq!(
            resolve_signal_handling_mode(true),
            SignalHandlingMode::HandleTerminationRequests
        );
        // Disabled: the host keeps the default disposition.
        assert_eq!(
            resolve_signal_handling_mode(false),
            SignalHandlingMode::Disabled
        );
    }
}
