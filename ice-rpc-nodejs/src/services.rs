//! The service table: which services an addon serves, and how to reach them.
//!
//! The list itself is **not** here. It is supplied by the addon, through the
//! [`gateway!`](crate::gateway) macro, so this crate stays free of any concrete
//! service. What lives here is the shape of one entry ([`GatewayService`]) and
//! the provider-side registration built on it.
//!
//! # Why function pointers
//!
//! [`ice_rpc::ServiceLocator`] resolves a proxy **by concrete type**
//! (`register::<T>` / `get::<T>`), and only the addon knows those types. Each
//! entry therefore carries two erasing closures — `provide` and `invoker` — and
//! everything downstream works on the erased forms.

use crate::error::{GatewayError, GatewayErrorCode};
use crate::state::{self, Phase};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// The future a [`GatewayService`] function pointer returns.
///
/// Carries a lifetime so a caller can name the `'static` case explicitly
/// (`BoxFuture<'static, ()>`), which is the one the table's function pointers
/// use.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// One service an addon serves.
pub struct GatewayService {
    /// Logical name, i.e. `<Proxy>::SERVICE_NAME`.
    pub name: &'static str,
    /// Registers the `ProviderJson` proxy of this service on the locator.
    pub provide: fn() -> BoxFuture<'static, ()>,
    /// Resolves the consumer proxy as an erased invoker, or `None` when no
    /// provider is connected.
    pub invoker: fn() -> BoxFuture<'static, Option<Arc<dyn ice_rpc::codegen::JsonInvoker>>>,
}

/// The logical names the table advertises, sorted and deduplicated.
pub fn known_service_names(table: &[GatewayService]) -> Vec<&'static str> {
    let mut names: Vec<&'static str> = table.iter().map(|entry| entry.name).collect();
    names.sort_unstable();
    names.dedup();
    names
}

/// Whether `name` is one of the services the table advertises.
pub fn is_known_service(table: &[GatewayService], name: &str) -> bool {
    table.iter().any(|entry| entry.name == name)
}

/// Builds the `E_UNKNOWN_SERVICE` failure, listing the advertised surface.
pub(crate) fn unknown_service_error(table: &[GatewayService], service_name: &str) -> GatewayError {
    GatewayError::new(
        GatewayErrorCode::UnknownService,
        format!(
            "unknown service '{}'; known services: {}",
            service_name,
            known_service_names(table).join(", ")
        ),
    )
}

/// Registers a Node.js provider service by its logical name.
///
/// # Errors
/// `E_UNKNOWN_SERVICE` when the name is absent from the table,
/// `E_GATEWAY_STATE` when the gateway is already running.
pub fn register_service(
    table: &'static [GatewayService],
    service_name: &str,
) -> Result<(), GatewayError> {
    // The name is validated first: "unknown service" is more informative than
    // "wrong phase", and it keeps the answer independent of the current phase.
    if !is_known_service(table, service_name) {
        return Err(unknown_service_error(table, service_name));
    }
    if state::phase() == Phase::Running {
        return Err(GatewayError::new(
            GatewayErrorCode::GatewayState,
            "registerService() must be called before init()",
        ));
    }

    let entry = table
        .iter()
        .find(|entry| entry.name == service_name)
        .ok_or_else(|| unknown_service_error(table, service_name))?;
    ice_rpc::rt::block_on((entry.provide)());

    state::mark_configured()?;
    log::info!(
        "Service provider '{}' registered ({} total).",
        service_name,
        ice_rpc::locator().service_count()
    );
    Ok(())
}

/// Starts `initialize_all()` in the background.
///
/// Called once the bridge is live, so the caller is not blocked by service
/// announcement.
pub fn spawn_initialize_all() {
    let count = ice_rpc::locator().service_count();
    if count == 0 {
        log::warn!(
            "No service registered. Use gateway.registerService('Name') before gateway.init()."
        );
    }

    ice_rpc::rt::spawn(async move {
        log::info!("Initializing {} service(s)...", count);
        match ice_rpc::locator().initialize_all().await {
            Ok(()) => log::info!("All services are ready and announced on the IPC bus."),
            Err(e) => log::error!("Service initialization failed: {}", e),
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A table with two entries and no side effect: `provide` is never reached
    /// on the paths under test.
    fn noop_provide() -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }

    fn noop_invoker() -> BoxFuture<'static, Option<Arc<dyn ice_rpc::codegen::JsonInvoker>>> {
        Box::pin(async { None })
    }

    static TABLE: &[GatewayService] = &[
        GatewayService {
            name: "AlphaService",
            provide: noop_provide,
            invoker: noop_invoker,
        },
        GatewayService {
            name: "BetaService",
            provide: noop_provide,
            invoker: noop_invoker,
        },
    ];

    #[test]
    fn the_advertised_names_are_sorted_and_deduplicated() {
        assert_eq!(known_service_names(TABLE), ["AlphaService", "BetaService"]);
        assert!(is_known_service(TABLE, "AlphaService"));
        assert!(!is_known_service(TABLE, "NoSuchService"));
    }

    #[test]
    fn an_unknown_service_is_reported_with_the_known_ones() {
        let error = register_service(TABLE, "NoSuchService").expect_err("unknown name is refused");
        assert_eq!(error.code(), GatewayErrorCode::UnknownService);
        for name in known_service_names(TABLE) {
            assert!(
                error.message().contains(name),
                "the message must list '{name}': {}",
                error.message()
            );
        }
    }
}
