//! Lazy registry of the JSON view of the services a gateway exposes.
//!
//! Shared by the HTTP gateway and the Phoenix Channels gateway: both need the
//! same `name -> factory` mapping and the same lazy cache, and a service must
//! not be instantiated twice because two transports are mounted on one server.

use crate::json::{JsonInvoker, JsonInvokerFactory};
use async_lock::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Lazy cache of the JSON views, keyed by logical service name.
#[derive(Clone)]
pub(crate) struct JsonInvokerRegistry {
    inner: Arc<RegistryInner>,
}

/// The shared state behind every clone of a [`JsonInvokerRegistry`].
struct RegistryInner {
    /// JSON view factories (logical name → factory).
    factories: HashMap<&'static str, JsonInvokerFactory>,
    /// JSON view cache (name → invoker).
    cache: RwLock<HashMap<String, Arc<dyn JsonInvoker>>>,
}

impl JsonInvokerRegistry {
    /// Builds a registry from the `name -> factory` mapping a gateway exposes.
    pub(crate) fn new(factories: HashMap<&'static str, JsonInvokerFactory>) -> Self {
        Self {
            inner: Arc::new(RegistryInner {
                factories,
                cache: RwLock::new(HashMap::new()),
            }),
        }
    }

    /// Returns whether this gateway exposes `service_name`, without creating it.
    ///
    /// The Phoenix Channels gateway uses it at join time: a client joins a
    /// service topic, so an unknown service must be refused before it can push
    /// a call. The registry itself stays lazy; this only reads the factory map.
    ///
    /// Only the Channels gateway calls it, so an HTTP-only build would otherwise
    /// carry a dead method.
    #[cfg(any(
        feature = "channels-threads",
        feature = "channels-smol",
        feature = "channels-tokio"
    ))]
    pub(crate) fn has_service(&self, service_name: &str) -> bool {
        self.inner.factories.contains_key(service_name)
    }

    /// Returns the JSON view of `service_name`, creating it on the first call.
    ///
    /// `None` means the service is not exposed by this gateway.
    pub(crate) async fn get_or_create(&self, service_name: &str) -> Option<Arc<dyn JsonInvoker>> {
        // Fast-path: already in the cache.
        {
            let cache = self.inner.cache.read().await;
            if let Some(proxy) = cache.get(service_name) {
                return Some(proxy.clone());
            }
        }

        // Slow-path: build the proxy through the service factory.
        let factory = self.inner.factories.get(service_name)?;
        let proxy = factory();

        // Caches for subsequent calls.
        let mut cache = self.inner.cache.write().await;
        cache
            .entry(service_name.to_string())
            .or_insert_with(|| proxy.clone());

        Some(proxy)
    }
}

#[cfg(all(
    test,
    any(
        feature = "channels-threads",
        feature = "channels-smol",
        feature = "channels-tokio"
    )
))]
mod tests {
    use super::*;

    fn unreachable_factory() -> Arc<dyn JsonInvoker> {
        unreachable!("has_service must not build an invoker")
    }

    #[test]
    fn has_service_reads_the_factory_map_without_creating_an_invoker() {
        let registry = JsonInvokerRegistry::new(HashMap::from([(
            "Database",
            unreachable_factory as JsonInvokerFactory,
        )]));

        assert!(registry.has_service("Database"));
        assert!(!registry.has_service("Unknown"));
    }
}
