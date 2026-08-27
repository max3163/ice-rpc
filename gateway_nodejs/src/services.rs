//! Registration of the Node.js services (Providers).
//!
//! Only the services that this Node.js process **implements** (Provider mode)
//! are registered here. The consumers are created automatically on demand
//! by [`ice_rpc::ServiceLocator::get`] from their type.
//!
//! # Initialization flow
//!
//! 1. JS: `gateway.registerService("ContextService")` → [`register_service`]
//!    → `ContextServiceProxy::provide_nodejs()` → `ServiceLocator::register()`
//! 2. JS: `gateway.init(callback)` → creates the iceoryx2 Node, starts the
//!    discovery and the dispatch loop.
//! 3. [`start_initialize_all`] in the background:
//!    - `ServiceLifecycle::init()` on each Provider proxy
//!    - Registers the IPC `RequestHandler`
//!    - Announces `NodeReady` on the bus
//!
//! # Adding a new service
//!
//! 1. Add `#[service("MyService")]` in the crate that defines the service.
//! 2. Add it to the list of exposed services in [`register_service`].

/// Dispatch table of the Node.js Providers, generated at compile time.
///
/// Internal syntax: `register_nodejs_providers!(name ; ProxyA, ProxyB, ...)`.
/// The `;` separator separates the service name from the proxy list.
macro_rules! register_nodejs_providers {
    ($service_name:expr ; $($proxy:ty),* $(,)?) => {
        match $service_name {
            $( <$proxy>::SERVICE_NAME => {
                let proxy = <$proxy>::provide_nodejs();
                crate::runtime::block_on(async move {
                    ice_rpc::locator().register(proxy).await;
                });
                true
            })*
            _ => {
                log::error!(
                    "Unknown service '{}'. Check the name passed to registerService() \
                     (expected values: {})",
                    $service_name,
                    stringify!($($proxy),*)
                );
                false
            }
        }
    };
}

/// Registers a Node.js Provider service by its logical name.
///
/// Called from Node.js before [`init`](crate::lib::init). The name must
/// match exactly the parameter of `#[service("Name")]`.
///
/// The consumers do **not** need to be registered: they are created
/// automatically by [`ServiceLocator::get`] on the first access.
///
/// # Returns
/// `true` if the service was registered successfully.
pub fn register_service(service_name: &str) -> bool {
    // Local list of the services that this Node.js process exposes (Provider mode).
    let registered = register_nodejs_providers!(
        service_name ;
        common::ContextServiceProxy,
        common::DatabaseServiceProxy,
        common::ConfigServiceProxy,
        common::HttpServiceProxy
    );

    if registered {
        log::info!(
            "Service Provider '{}' registered ({} total).",
            service_name,
            ice_rpc::locator().service_count()
        );
    }

    registered
}

/// Starts `initialize_all()` in the background.
///
/// Called by [`init`](crate::lib::init) after the creation of the Node and the bridge.
pub fn start_initialize_all() {
    let count = ice_rpc::locator().service_count();
    if count == 0 {
        log::warn!(
            "No service registered. Use gateway.registerService('Name') before gateway.init()."
        );
    }

    let _ = crate::runtime::spawn_task(async move {
        log::info!("Initializing {} service(s)...", count);
        match ice_rpc::locator().initialize_all().await {
            Ok(()) => log::info!("All services are ready and announced on the IPC bus."),
            Err(e) => log::error!("Service initialization failed: {}", e),
        }
    });
}
