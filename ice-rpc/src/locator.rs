//! Central service registry (Service Locator pattern).
//!
//! Manages registration, dependency resolution, topological sorting
//! and service initialization. Coordinates the lifecycle of the iceoryx2
//! node (creation, discovery, dispatch, release).
//!
//! ## Lazy consumers
//!
//! Consumers no longer need to be registered manually.
//! The proxy is instantiated on demand during the first [`ServiceLocator::get`]
//! for a given service, directly from its type (`ServiceConsumer`).
//! The proxy is then created, cached, and returned.
//!
//! Only **Providers** still call [`ServiceLocator::register`].

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use async_lock::RwLock;
use futures::FutureExt;

use crate::node_discovery::NodeDiscovery;
use crate::registry_notify::announce_dead_node;
use crate::service_traits::{ServiceConsumer, ServiceInit, ServiceLifecycle, ServiceNamed};

/// Groups the three facets of a service into a single atomic entry.
///
/// Merges the former separate maps (instance, lifecycle, init_hook)
/// to guarantee consistency and reduce contention.
#[derive(Clone)]
struct ServiceEntry {
    instance: Arc<dyn Any + Send + Sync>,
    lifecycle: Arc<dyn ServiceLifecycle>,
    init_hook: Arc<dyn ServiceInit>,
}

/// Central registry of the application services (Service Locator pattern).
///
/// Stores the services indexed by their logical name and manages the lifecycle
/// of the iceoryx2 node (creation, discovery, dispatch, release).
///
/// ## Lazy proxies
///
/// [`get`](Self::get) automatically instantiates and caches a proxy
/// for any service not yet registered locally, directly from
/// its type (`ServiceConsumer`).
///
/// Lazy proxies are stored in `lazy_cache`, **separate** from `entries`.
/// `initialize_all()` only iterates over `entries` (services registered via
/// `register()`), never over `lazy_cache`. A lazy proxy therefore triggers
/// no IPC initialization — it connects on the first RPC call.
pub struct ServiceLocator {
    /// Services explicitly registered via [`register`] (Providers and
    /// legacy API consumers). Only these services are initialized by
    /// [`initialize_all`].
    entries: RwLock<HashMap<&'static str, ServiceEntry>>,
    /// Cache of Consumer proxies instantiated lazily via [`get`].
    /// Invisible to [`initialize_all`] — no lifecycle is called.
    lazy_cache: RwLock<HashMap<&'static str, Arc<dyn std::any::Any + Send + Sync>>>,
    iceoryx2_node: std::sync::RwLock<
        Option<Arc<iceoryx2::node::Node<iceoryx2::service::ipc_threadsafe::Service>>>,
    >,
    shutdown_registry: crate::shutdown::ShutdownRegistry,
    node_discovery: OnceLock<Arc<NodeDiscovery>>,
}

impl ServiceLocator {
    /// Returns the unique global instance of the [`ServiceLocator`] (singleton).
    pub fn global() -> &'static Self {
        static INSTANCE: OnceLock<ServiceLocator> = OnceLock::new();
        INSTANCE.get_or_init(|| Self {
            entries: RwLock::new(HashMap::new()),
            lazy_cache: RwLock::new(HashMap::new()),
            iceoryx2_node: std::sync::RwLock::new(None),
            shutdown_registry: crate::shutdown::ShutdownRegistry::new(),
            node_discovery: OnceLock::new(),
        })
    }

    /// Returns the number of currently registered services.
    ///
    /// # Returns
    /// Number of services (`u8`). Returns `0` if the lock cannot be
    /// acquired without blocking.
    pub fn service_count(&self) -> u8 {
        self.entries.try_read().map(|e| e.len() as u8).unwrap_or(0)
    }

    /// Returns the logical names of all the registered local services.
    pub fn service_names(&self) -> Vec<&'static str> {
        self.entries
            .try_read()
            .map(|e| e.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Returns (or initializes) the shared [`NodeDiscovery`] view.
    pub fn node_discovery(&self) -> &Arc<NodeDiscovery> {
        self.node_discovery
            .get_or_init(|| Arc::new(NodeDiscovery::new()))
    }

    /// Registers a blocking IPC thread handle in the shutdown registry.
    pub fn register_shutdown_handle(&self, handle: crate::rt::BlockingHandle) {
        self.shutdown_registry.register(handle);
    }

    /// Waits for all the registered `spawn_blocking` IPC threads to finish,
    /// publishes a DEAD announcement, then drops the iceoryx2 node.
    ///
    /// Must be called from an async context (Tokio runtime).
    pub async fn release_node(&self) {
        // Only a Provider (which published a discovery registry and acquired
        // the global lock) has to announce its own death. A pure consumer has
        // nothing to announce and must not try to create a notifier during
        // teardown.
        let was_provider = crate::node_lock::has_global_node_lock();
        crate::node_lock::release_global_node_lock();
        if was_provider {
            announce_dead_node(std::process::id());
        }

        self.shutdown_registry.join_all().await;

        crate::rt::sleep(std::time::Duration::from_millis(50)).await;
        if let Ok(mut guard) = self.iceoryx2_node.write() {
            if guard.is_some() {
                log::info!("Releasing the iceoryx2 node (IPC cleanup)...");
                *guard = None;
                log::info!("iceoryx2 node released, IPC artifacts cleaned up.");
            }
        }
    }

    /// Lists the **live** IPC service names from the Blackboard.
    ///
    /// Used by [`initialize_all_with_timeout`](Self::initialize_all_with_timeout)
    /// to short-circuit dependencies already satisfied by a Provider
    /// started in another process.
    ///
    /// each service is validated via `is_node_alive(lock_name)` before being
    /// included in the result.
    pub fn discover_active_ipc_services() -> Vec<String> {
        Self::global().node_discovery().discover_live_services()
    }

    /// Retrieves a proxy by its **type** and returns it ready to use.
    ///
    /// The logical name is read from the `T::SERVICE_NAME` constant — no
    /// name parameter is needed. If the proxy is not yet cached, it is
    /// instantiated via [`ServiceConsumer::consume_proxy`] (generated
    /// by `#[service]`), cached, then returned. Subsequent calls
    /// use the cache.
    ///
    /// # Type parameters
    /// `T` must implement [`ServiceConsumer`] (generated automatically by
    /// `#[service]`) and be the concrete proxy type (e.g. `ContextServiceProxy`).
    ///
    /// # Example
    /// ```rust,ignore
    /// // At usage time, anywhere in the code:
    /// let proxy = ServiceLocator::global()
    ///     .get::<ContextServiceProxy>()
    ///     .await
    ///     .expect("ContextService unknown");
    /// let value = take_one!(proxy.get("my.key".into()))?;
    /// ```
    pub async fn get<T: ServiceConsumer>(&self) -> Option<Arc<T>> {
        let name = T::SERVICE_NAME;

        // Fast-path 1: already in entries (service registered via register()).
        {
            let entries = self.entries.read().await;
            if let Some(entry) = entries.get(name) {
                return entry.instance.clone().downcast::<T>().ok();
            }
        }

        // Fast-path 2: already in the lazy cache (instantiated during a previous get()).
        {
            let cache = self.lazy_cache.read().await;
            if let Some(instance) = cache.get(name) {
                return instance.clone().downcast::<T>().ok();
            }
        }

        // Slow-path: instantiates the proxy via ServiceConsumer and puts it in the
        // lazy cache. The proxy is stored in `lazy_cache`, NOT in `entries`, so
        // that `initialize_all()` never sees it and never attempts to
        // initialize it.
        let proxy: Arc<T> = T::consume_proxy();

        self.lazy_cache
            .write()
            .await
            .entry(name)
            .or_insert_with(|| proxy.clone() as Arc<dyn Any + Send + Sync>);

        // First lazy access: make sure the iceoryx2 Node exists and that
        // discovery is started. These two operations are idempotent.
        // Without them, locate_service() and read_service_blackboard_full()
        // fail because try_get_node() returns None.
        if self.try_get_node().is_none() {
            crate::rt::spawn_blocking(|| {
                let locator = ServiceLocator::global();
                if let Err(e) = locator.get_node_sync() {
                    log::error!(
                        "[ServiceLocator::get] Failed to create iceoryx2 Node: {}",
                        e
                    );
                    return;
                }
                locator.start_discovery();
                locator.start_dispatch_if_needed();
            })
            .await;
        }

        log::debug!("Lazy proxy instantiated and cached: '{}'", name);
        Some(proxy)
    }

    /// Registers a service in the [`ServiceLocator`].
    ///
    /// The key is the logical name injected by the macro via [`ServiceNamed`].
    /// The three facets of the service (instance, lifecycle, init_hook) are
    /// stored atomically in a single `ServiceEntry`.
    pub async fn register<T>(&self, service: Arc<T>)
    where
        T: ServiceLifecycle + ServiceNamed + ServiceInit + Any + Send + Sync + 'static,
    {
        let name = service.service_name();
        log::info!("Registering service: '{}'", name);
        let entry = ServiceEntry {
            instance: service.clone() as Arc<dyn Any + Send + Sync>,
            lifecycle: service.clone() as Arc<dyn ServiceLifecycle>,
            init_hook: service as Arc<dyn ServiceInit>,
        };
        self.entries.write().await.insert(name, entry);
    }

    /// Tries to retrieve the iceoryx2 node without blocking.
    pub fn try_get_node(
        &self,
    ) -> Option<Arc<iceoryx2::node::Node<iceoryx2::service::ipc_threadsafe::Service>>> {
        self.iceoryx2_node.try_read().ok()?.as_ref().cloned()
    }

    /// Retrieves or creates the iceoryx2 node in a thread-safe manner.
    ///
    /// The creation is synchronous because `NodeBuilder` is not async.
    /// Uses double-checked locking.
    ///
    /// **Note**: this function no longer starts the dispatch loop nor the
    /// `NODE_REGISTRY` listener. These steps are separated:
    /// - [`start_discovery`](Self::start_discovery) → discovery channel.
    /// - [`start_dispatch_if_needed`](Self::start_dispatch_if_needed) → dispatch loop.
    pub fn get_node_sync(
        &self,
    ) -> Result<
        Arc<iceoryx2::node::Node<iceoryx2::service::ipc_threadsafe::Service>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        if let Ok(guard) = self.iceoryx2_node.read() {
            if let Some(node) = guard.as_ref() {
                return Ok(node.clone());
            }
        }

        log::info!("Creating the iceoryx2 node (pid={})...", std::process::id());

        let custom_config = crate::config::build_iceoryx2_config();

        let result = iceoryx2::prelude::NodeBuilder::new()
            .config(&custom_config)
            .signal_handling_mode(iceoryx2::prelude::SignalHandlingMode::HandleTerminationRequests)
            .create::<iceoryx2::service::ipc_threadsafe::Service>();

        log::debug!("NodeBuilder result: {:?}", result.is_ok());

        let node = result
            .map(Arc::new)
            .map_err(|e| format!("NodeBuilder::create failed : {:?}", e))?;

        {
            let mut guard = self
                .iceoryx2_node
                .write()
                .map_err(|e| format!("iceoryx2_node write lock poisoned: {}", e))?;

            if let Some(existing) = guard.as_ref() {
                return Ok(existing.clone());
            }
            *guard = Some(node.clone());
        }

        Ok(node)
    }

    /// Starts the discovery channel: subscription to the `NODE_REGISTRY` topic.
    ///
    /// Idempotent (internal AtomicBool). The channel runs in its own
    /// `spawn_blocking` with its own `WaitSet`.
    ///
    /// Must be called **after** [`get_node_sync`](Self::get_node_sync)
    /// and **before** any `locate_service` attempt.
    pub fn start_discovery(&self) {
        crate::registry_listener::spawn(self.node_discovery().clone());
    }

    /// Starts the dispatch loop of the [`NodeHub`](crate::hub::NodeHub) if it
    /// is not already done.
    ///
    /// Idempotent (AtomicBool). Must be called only after all
    /// dependencies are resolved.
    pub fn start_dispatch_if_needed(&self) {
        self.hub().start_dispatch_loop();
    }

    /// Async version of [`get_node_sync`](Self::get_node_sync).
    pub async fn get_node(
        &self,
    ) -> Result<
        Arc<iceoryx2::node::Node<iceoryx2::service::ipc_threadsafe::Service>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        self.get_node_sync()
    }

    /// Initializes all the registered services in topological order
    /// with the default timeout ([`crate::INITIALIZE_ALL_TIMEOUT_SECS`]).
    pub async fn initialize_all(&self) -> Result<(), String> {
        self.initialize_all_with_timeout(crate::INITIALIZE_ALL_TIMEOUT_SECS)
            .await
    }

    /// Initializes all the registered services in topological order
    /// with a custom timeout.
    ///
    /// # Algorithm
    /// 1. Discovery of the IPC services already active in other processes.
    /// 2. Collection of the local services and their dependencies.
    /// 3. Topological sort (Kahn's algorithm).
    /// 4. Sequential initialization with retry per service.
    /// 5. Grouped `NodeReady` announcement to signal consumers that this
    ///    node is operational.
    ///
    /// Ctrl+C is detected at each step for a clean interruption.
    pub async fn initialize_all_with_timeout(&self, timeout_secs: u16) -> Result<(), String> {
        let active_ipc = Self::discover_active_ipc_services();
        if !active_ipc.is_empty() {
            log::info!("IPC services already active detected:");
            for name in &active_ipc {
                log::info!("  - {}", name);
            }
        }

        // Snapshot of the registered services (cheap: only the `Arc` refcounts
        // are cloned). The read lock is released before the initialization
        // loop so that a potential `register()` during `init()` is not blocked.
        let entries: HashMap<&'static str, ServiceEntry> = self.entries.read().await.clone();

        let ordered_names = topological_sort(&entries, &active_ipc)?;

        log::info!(
            "Resolved initialization order ({} service(s)):",
            ordered_names.len()
        );
        for (i, name) in ordered_names.iter().enumerate() {
            log::info!("  {}. {}", i + 1, name);
        }

        let retry_interval = std::time::Duration::from_millis(crate::INIT_RETRY_INTERVAL_MS);
        let max_duration = std::time::Duration::from_secs(timeout_secs.into());
        let started_at = std::time::Instant::now();

        for name in &ordered_names {
            let entry = entries
                .get(name)
                .ok_or_else(|| format!("Service '{}' not found in the service registry", name))?;
            let lifecycle = &entry.lifecycle;

            loop {
                if crate::global_cancel_token().is_cancelled() {
                    crate::registry_cancel_token().cancel();
                    return Err(format!(
                        "Initialization cancelled (Ctrl+C) for service '{}'.",
                        name
                    ));
                }

                if started_at.elapsed() > max_duration {
                    crate::global_cancel_token().cancel();
                    crate::registry_cancel_token().cancel();
                    return Err(format!(
                        "Timeout ({}s): service '{}' could not initialize.",
                        timeout_secs, name
                    ));
                }

                let init_ok = futures::select! {
                    _ = crate::global_cancel_token().cancelled().fuse() => {
                        crate::registry_cancel_token().cancel();
                        log::info!(
                            "Initialization cancelled (Ctrl+C) for '{}'.",
                            name
                        );
                        return Err(format!(
                            "Initialization cancelled (Ctrl+C) for service '{}'.",
                            name
                        ));
                    }
                    result = lifecycle.init().fuse() => result,
                };

                if init_ok {
                    log::info!("Service '{}' initialized successfully.", name);
                    break;
                }

                log::warn!(
                    "Service '{}' waiting, retrying in {}ms...",
                    name,
                    retry_interval.as_millis()
                );

                futures::select! {
                    _ = crate::global_cancel_token().cancelled().fuse() => {
                        crate::registry_cancel_token().cancel();
                        return Err(format!(
                            "Initialization cancelled (Ctrl+C) for service '{}'.",
                            name
                        ));
                    }
                    _ = crate::rt::sleep(retry_interval).fuse() => {}
                }
            }
        }

        if !ordered_names.is_empty() {
            // Writes the discovery registry (1 Blackboard per node).
            let node_id = crate::types::NodeId::current().0;
            let services: Vec<String> = ordered_names.iter().map(|s| s.to_string()).collect();
            crate::blackboard::create_node_blackboard(node_id, &services);
            crate::registry_notify::announce_node_ready(std::process::id());
        }

        Ok(())
    }
}

/// Topological sort of the services according to their dependencies (Kahn's algorithm).
///
/// # Arguments
/// * `entries` — Map of the locally registered services (name → `ServiceEntry`).
/// * `active_ipc` — Names of the services already active in IPC. A dependency
///   present in this list is considered satisfied.
///
/// # Returns
/// * `Ok(Vec<&'static str>)` — Names in topological order.
/// * `Err(String)` — Missing dependency or detected cycle.
fn topological_sort(
    entries: &HashMap<&'static str, ServiceEntry>,
    active_ipc: &[String],
) -> Result<Vec<&'static str>, String> {
    let mut in_degree: HashMap<&'static str, usize> =
        entries.keys().map(|&name| (name, 0)).collect();

    let mut dependents: HashMap<&'static str, Vec<&'static str>> = HashMap::new();

    for (&name, entry) in entries {
        for dep in entry.init_hook.dependencies() {
            if entries.contains_key(dep) {
                *in_degree.entry(name).or_insert(0) += 1;
                dependents.entry(dep).or_default().push(name);
            } else if active_ipc.iter().any(|s| s == dep) {
                log::info!(
                    "'{}' depends on '{}' → already active in IPC, dependency satisfied.",
                    name,
                    dep
                );
            } else {
                return Err(format!(
                    "Service '{}' depends on '{}' which is neither registered locally \
                     nor active in IPC. Register it or start its Provider.",
                    name, dep
                ));
            }
        }
    }

    let mut queue: std::collections::VecDeque<&'static str> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(&name, _)| name)
        .collect();

    let mut ordered = Vec::with_capacity(entries.len());

    while let Some(name) = queue.pop_front() {
        ordered.push(name);
        if let Some(deps) = dependents.get(name) {
            for &dependent in deps {
                let deg = in_degree
                    .get_mut(dependent)
                    .expect("topological_sort: dependent missing from in_degree");
                *deg -= 1;
                if *deg == 0 {
                    queue.push_back(dependent);
                }
            }
        }
    }

    if ordered.len() != entries.len() {
        return Err("Dependency cycle detected between services. \
             Check the ServiceInit::dependencies() implementations."
            .to_string());
    }

    Ok(ordered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct TestService {
        name: &'static str,
        deps: Vec<&'static str>,
    }

    #[async_trait::async_trait]
    impl ServiceLifecycle for TestService {
        async fn init(&self) -> bool {
            true
        }
    }

    impl ServiceNamed for TestService {
        const SERVICE_NAME: &'static str = "TestService";
        fn service_name(&self) -> &'static str {
            self.name
        }
    }

    #[async_trait::async_trait]
    impl ServiceInit for TestService {
        fn dependencies(&self) -> Vec<&'static str> {
            self.deps.clone()
        }
    }

    fn make_entries(
        services: &[Arc<TestService>],
    ) -> HashMap<&'static str, ServiceEntry> {
        services
            .iter()
            .map(|s| {
                (
                    s.name,
                    ServiceEntry {
                        instance: s.clone(),
                        lifecycle: s.clone(),
                        init_hook: s.clone(),
                    },
                )
            })
            .collect()
    }

    #[test]
    fn topological_sort_no_services() {
        let entries: HashMap<&'static str, ServiceEntry> = HashMap::new();
        let active: Vec<String> = vec![];
        let result = topological_sort(&entries, &active);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn topological_sort_single_service_no_deps() {
        let svc = Arc::new(TestService {
            name: "A",
            deps: vec![],
        });
        let entries = make_entries(std::slice::from_ref(&svc));
        let result = topological_sort(&entries, &[]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["A"]);
    }

    #[test]
    fn topological_sort_chain_a_b_c() {
        let svc_a = Arc::new(TestService {
            name: "A",
            deps: vec![],
        });
        let svc_b = Arc::new(TestService {
            name: "B",
            deps: vec!["A"],
        });
        let svc_c = Arc::new(TestService {
            name: "C",
            deps: vec!["B"],
        });
        let entries = make_entries(&[svc_a.clone(), svc_b.clone(), svc_c.clone()]);
        let result = topological_sort(&entries, &[]);
        assert!(result.is_ok());
        let order = result.unwrap();
        let pos_a = order.iter().position(|&n| n == "A").unwrap();
        let pos_b = order.iter().position(|&n| n == "B").unwrap();
        let pos_c = order.iter().position(|&n| n == "C").unwrap();
        assert!(pos_a < pos_b);
        assert!(pos_b < pos_c);
    }

    #[test]
    fn topological_sort_multiple_roots() {
        let svc_a = Arc::new(TestService {
            name: "A",
            deps: vec![],
        });
        let svc_b = Arc::new(TestService {
            name: "B",
            deps: vec![],
        });
        let svc_c = Arc::new(TestService {
            name: "C",
            deps: vec!["A", "B"],
        });
        let entries = make_entries(&[svc_a.clone(), svc_b.clone(), svc_c.clone()]);
        let result = topological_sort(&entries, &[]);
        assert!(result.is_ok());
        let order = result.unwrap();
        let pos_a = order.iter().position(|&n| n == "A").unwrap();
        let pos_b = order.iter().position(|&n| n == "B").unwrap();
        let pos_c = order.iter().position(|&n| n == "C").unwrap();
        assert!(pos_a < pos_c);
        assert!(pos_b < pos_c);
    }

    #[test]
    fn topological_sort_cycle_detected() {
        let svc_a = Arc::new(TestService {
            name: "A",
            deps: vec!["B"],
        });
        let svc_b = Arc::new(TestService {
            name: "B",
            deps: vec!["A"],
        });
        let entries = make_entries(&[svc_a.clone(), svc_b.clone()]);
        let result = topological_sort(&entries, &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cycle"));
    }

    #[test]
    fn topological_sort_missing_dependency() {
        let svc_a = Arc::new(TestService {
            name: "A",
            deps: vec!["UnknownService"],
        });
        let entries = make_entries(std::slice::from_ref(&svc_a));
        let result = topological_sort(&entries, &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("UnknownService"));
    }

    #[test]
    fn topological_sort_dep_satisfied_by_active_ipc() {
        let svc_a = Arc::new(TestService {
            name: "A",
            deps: vec!["ExternalService"],
        });
        let entries = make_entries(std::slice::from_ref(&svc_a));
        let active = vec!["ExternalService".to_string()];
        let result = topological_sort(&entries, &active);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["A"]);
    }
}
