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
//!             let _ = tx.send(ice_rpc::Event::Next(format!("Hello {} !", name))).await;
//!             let _ = tx.send(ice_rpc::Event::Complete).await;
//!         });
//!         Ok(rx)
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Pure provider: no implementation calls locator().get().
//!     ice_rpc::init();
//!
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
//!     ice_rpc::init();
//!
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
//! use ice_rpc::take_one;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // This process consumes services via locator().get().
//!     ice_rpc::init();
//!
//!     // RAII guard: cancels the tokens on Drop, even on panic.
//!     let guard = ice_rpc::ShutdownGuard::new();
//!
//!     // Proxy instantiated lazily, IPC connection on the first RPC call.
//!     let proxy = ice_rpc::locator()
//!         .get::<MyServiceProxy>().await
//!         .expect("MyService unknown");
//!
//!     let response: String = take_one!(proxy.hello("Alice".into()))?;
//!     log::info!("Response: {}", response);
//!
//!     // Clean shutdown: wait for IPC threads, release the iceoryx2 node.
//!     guard.shutdown().await;
//!     Ok(())
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
//! - **Observable** : RPC event stream (`Next` / `Complete` / `Error`)
//! - **NodeHub** : central communication hub managing the IPC publishers/subscribers
//! - **ServiceLocator** : service registry with dependency resolution and topological sort
//! - **NodeDiscovery** : local service→NodeId cache, initial discovery + Event-based updates
//! - **Proxy** : unified entry point supporting 3 modes (Provider / Consumer / ProviderNodeJs)
//!
//! ## Main modules
//!
//! | Module | Role |
//! |--------|------|
//! | `types` | Fundamental types: `NodeId`, `RpcHeader`, `Event`, `Observable`, `RpcError`, `ServiceInfo` |
//! | `hub` | `NodeHub` : centralized dispatch loop, publishers, response handlers |
//! | `locator` | `ServiceLocator` : registration, lifecycle, Kahn topological sort |
//! | `node_discovery` | `NodeDiscovery` : local cache, service→NodeId resolution |
//! | `blackboard` | Discovery registry: 1 Blackboard per node (`ice_rpc_node_{pid}`), key = service name |
//! | `registry_notify` | Event notifications: carries the NodeId via `EventId` |
//! | `registry_listener` | WaitSet loop: receives the Events, updates the cache, cleans dead nodes |
//! | `reconnect` | Reconnection callbacks fired when a node is detected dead |
//! | `node_lock` | Cross-platform kernel named lock (Windows Mutex / Unix flock) |
//! | `macros` | `take_one!`, `take_one_or_cancel!`, `try_or_log!` utilities |
//! | `cache` | `RpcCache` consumer-side TTL cache (`hash_bytes`, `hash_key`) — requires the `cache` feature |

pub use base64;
pub use ice_rpc_macros::{service, timeout};
#[cfg(feature = "cache")]
pub use ice_rpc_macros::cache;
pub use iceoryx2;
pub use log;
pub use rkyv;
pub use serde_json;

// Runtime-agnostic building blocks re-exported for the generated code.
pub use async_channel;
pub use async_lock;
pub use futures;
pub use futures_lite;

mod blackboard;
#[cfg(feature = "cache")]
mod cache;
mod client_core;
mod config;
mod hub;
mod locator;
mod macros;
mod node_discovery;
mod node_lock;
mod reconnect;
mod registry_listener;
mod registry_notify;
pub mod rt;
mod service_traits;
mod shutdown;
mod types;

/// Internal facade for the code generated by `#[service]`.
#[doc(hidden)]
pub mod gen;

/// Node.js bridge: dynamic dispatch for the ProviderNodeJs mode.
pub mod nodejs_dispatch;

#[cfg(feature = "http")]
mod http_gateway;
pub use macros::{take_one, take_one_or_cancel};

// ── Public API: service traits ─────────────────────────────────────
pub use service_traits::{
    HttpCallable, ServiceConsumer, ServiceInit, ServiceLifecycle, ServiceNamed,
};

// ── Public API: cache ───────────────────────────────────────────────
#[cfg(feature = "cache")]
pub use cache::{hash_bytes, hash_key, RpcCache};

// ── Public API: fundamental types ──────────────────────────────────
pub use types::{
    caller_pid_from_cid, channel, fmt_correlation_id, fmt_correlation_id_short, Event, EventKind,
    NodeId, Observable, RpcError, RpcHeader, Sender, StaticString, Stream, TakeOneError,
    BLACKBOARD_MAX_READERS, DEFAULT_TOPIC_BUFFER_SIZE, INITIALIZE_ALL_TIMEOUT_SECS,
    INIT_RETRY_INTERVAL_MS, LARGE_TOPIC_BUFFER_SIZE, METHOD_NAME_LEN,
    PUBLISHER_DEFAULT_MAX_SLICE_LEN, PUBLISHER_LARGE_MAX_SLICE_LEN, RPC_CALL_TIMEOUT_SECS,
    SERVER_READY_POLL_MS, WAITSET_TIMEOUT_US,
};

// ── Public API: configuration ───────────────────────────────────────
pub use config::setup_iceoryx2_global_config;

// ── Public API: locator ─────────────────────────────────────────────
pub use locator::ServiceLocator;

use std::sync::OnceLock;

pub use crate::rt::CancellationToken;

/// Global cancellation token for the WaitSet loops (dispatch loop).
///
/// Triggered by Ctrl+C or by the dispatch loop on fatal error.
pub fn global_cancel_token() -> &'static CancellationToken {
    static TOKEN: OnceLock<CancellationToken> = OnceLock::new();
    TOKEN.get_or_init(CancellationToken::new)
}

/// Cancellation token for the NODE_REGISTRY listener.
///
/// Is NOT triggered by the dispatch loop (TerminationRequest), only
/// by Ctrl+C. This allows the listener to survive the provider death
/// and to receive the restart announcements.
pub fn registry_cancel_token() -> &'static CancellationToken {
    static TOKEN: OnceLock<CancellationToken> = OnceLock::new();
    TOKEN.get_or_init(CancellationToken::new)
}

/// Cancels the IPC threads and waits for their termination in a single call.
///
/// # Example
/// ```rust,ignore
/// // Prefer ShutdownGuard (RAII):
/// let guard = ice_rpc::ShutdownGuard::new();
/// // ... usage ...
/// guard.shutdown().await; // clean shutdown waiting for the IPC threads
/// ```
pub async fn shutdown_and_release() {
    global_cancel_token().cancel();
    registry_cancel_token().cancel();
    ServiceLocator::global().release_node().await;
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
///     let _guard = ice_rpc::ShutdownGuard::new();
///     // ... use ice-rpc ...
///     _guard.shutdown().await;
///     Ok(())
/// }
/// ```
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

/// Waits for the stop signal (Ctrl+C or programmatic cancellation).
///
/// Syntactic sugar over `global_cancel_token().cancelled().await`.
/// Used at the end of `main` to block until shutdown.
pub async fn wait_for_shutdown() {
    global_cancel_token().cancelled().await;
}

// ────────────────────────────────────────────────────────────────────
// Initialization functions
// ────────────────────────────────────────────────────────────────────

/// Initializes the framework: configures iceoryx2 and installs the Ctrl+C
/// handler.
///
/// Suitable for consumers, providers and provider+consumer processes alike:
/// no service registry is needed, proxies are instantiated on demand from
/// their type.
///
/// # Example
/// ```rust,ignore
/// ice_rpc::init();
/// let proxy = ice_rpc::locator().get::<MyServiceProxy>().await.unwrap();
/// ```
pub fn init() {
    setup_iceoryx2_global_config();
    spawn_ctrl_c_handler();
}

/// Initializes the framework **without** a Ctrl+C handler (Node.js gateway, tests…).
///
/// Variant of [`init`] for contexts that must manage shutdown
/// manually (N-API thread, tests, embedded executors).
///
/// # Example
/// ```rust,ignore
/// ice_rpc::init_without_ctrl_c();
/// ```
pub fn init_without_ctrl_c() {
    setup_iceoryx2_global_config();
}

/// Installs the Ctrl+C handler that triggers [`global_cancel_token`].
///
/// Available separately if needed (already called by [`init`]).
/// The handler is installed through the [`ctrlc`] crate, so it works from any
/// context, without an async runtime.
pub fn spawn_ctrl_c_handler() {
    // `ctrlc::set_handler` can only be installed once per process; duplicate
    // calls are ignored.
    let _ = ctrlc::set_handler(move || {
        log::info!("\nCtrl+C received, shutting down...");
        global_cancel_token().cancel();
        registry_cancel_token().cancel();
    });
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
pub async fn start_http_server(
    port: u16,
    factories: std::collections::HashMap<&'static str, fn() -> std::sync::Arc<dyn HttpCallable>>,
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
/// ice_rpc::init();
/// ice_rpc::start_http_gateway!(8080, DatabaseServiceProxy, ConfigServiceProxy).await;
/// ```
#[cfg(feature = "http")]
#[macro_export]
macro_rules! start_http_gateway {
    ($port:expr, $($proxy:ty),+ $(,)?) => {{
        let mut __map: std::collections::HashMap<
            &'static str,
            fn() -> std::sync::Arc<dyn ice_rpc::HttpCallable>,
        > = std::collections::HashMap::new();
        $(
            __map.insert(
                <$proxy>::SERVICE_NAME,
                || <$proxy>::consume() as std::sync::Arc<dyn ice_rpc::HttpCallable>,
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
pub async fn run_provider_inner(
    services: Vec<Box<dyn _ProviderService>>,
) -> Result<(), Box<dyn std::error::Error>> {
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
///     ice_rpc::init();
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
///     ice_rpc::init();
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
        // Must not block indefinitely
        pollster::block_on(crate::rt::timeout(
            std::time::Duration::from_secs(2),
            wait_for_shutdown(),
        ))
        .expect("wait_for_shutdown did not return in time");
    }

    #[test]
    fn shutdown_and_release_does_not_panic_when_no_node() {
        // Without a created iceoryx2 Node, shutdown_and_release must terminate cleanly.
        pollster::block_on(shutdown_and_release());
    }
}
