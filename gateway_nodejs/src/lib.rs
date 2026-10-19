//! Node.js gateway for ice-rpc — the reference addon.
//!
//! This crate is the **reference** addon: it shows everything an addon writes,
//! and it is the one the examples and the tests exercise. The reusable engine
//! lives in [`ice_rpc_nodejs`]; this crate only names the services it serves,
//! through the [`gateway!`](ice_rpc_nodejs::gateway) macro, which generates the
//! eight N-API functions.
//!
//! # Providing and consuming
//!
//! A process that provides `ContextService`:
//!
//! ```javascript
//! const gateway = require('gateway-nodejs');
//! gateway.registerService('ContextService');
//! gateway.init((err, call) => { /* … resolveNodejsCall(call.correlationId, event) */ });
//! ```
//!
//! A process that consumes any advertised service:
//!
//! ```javascript
//! const age = await gateway.callService('DatabaseService', 'get_user_age', 'Alice');
//! ```
//!
//! The full contract (argument convention, event envelope, error codes,
//! lifecycle) is in `docs/nodejs-gateway-api-v3.md`, and the engine's `Readme.md`
//! explains how to build an addon for **your own** services.
//!
//! # The service list is the only thing here
//!
//! Listing a service the compiler cannot find is a build error, and a service
//! that is not listed is reported at runtime as `E_UNKNOWN_SERVICE` — the list is
//! the single declaration the provider side and the consumer side share.

#![cfg_attr(test, allow(clippy::unwrap_used))] // test code may panic

ice_rpc_nodejs::gateway! {
    common::ConfigServiceProxy,
    common::ContextServiceProxy,
    common::DatabaseServiceProxy,
    common::HttpServiceProxy,
    common::NotificationServiceProxy,
    common::WorkloadServiceProxy,
}

#[cfg(test)]
mod tests {
    /// The advertised surface is exactly the six services of `common`.
    ///
    /// The table is read from the macro-generated static, so this is what the
    /// provider registration and the consumer dispatch both see.
    #[test]
    fn the_reference_addon_serves_common_services() {
        assert_eq!(
            ice_rpc_nodejs::known_service_names(crate::__ICE_RPC_GATEWAY_SERVICES),
            [
                "ConfigService",
                "ContextService",
                "DatabaseService",
                "HttpService",
                "NotificationService",
                "WorkloadService",
            ],
        );
    }
}
