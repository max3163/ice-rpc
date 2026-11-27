//! iceoryx2 configuration used by the transport.
//!
//! `ice-rpc` never writes a config file. The effective configuration is the one
//! iceoryx2 resolves itself, via [`Config::global_config`]: `./config/iceoryx2.toml`
//! first, then the user config directory, then the global config directory, and
//! finally the compiled-in default when none exists.
//!
//! On top of that effective configuration, the transport raises iceoryx2's
//! conservative compiled-in **port counts** to the capacity its channels need.
//! The subscriber buffer and the publisher loan limit stay at iceoryx2's
//! defaults: a full buffer is backpressure rather than loss, and raising the loan
//! limit is an explicit throughput/memory trade-off the deployment owns. A value a
//! deployment chose differently is left untouched, so a config file remains the
//! way to request more capacity or a custom root-path. Every participant of a
//! channel must resolve the same configuration, because iceoryx2 records the
//! service's static configuration when it is created.
//!
//! The dead-node cleanup flags are adjusted too: they change the reaping policy,
//! never which entities are visible on the bus.

use iceoryx2::config::Config;

/// Minimum number of publishers a channel service supports.
const MIN_MAX_PUBLISHERS: usize = 16;
/// Minimum number of subscribers a channel service supports.
const MIN_MAX_SUBSCRIBERS: usize = 16;
/// Minimum number of nodes that may open a channel service.
const MIN_MAX_NODES: usize = 32;

/// Returns the iceoryx2 configuration the transport uses.
///
/// Starts from the effective global configuration, so the root-path — the
/// shared-memory domain every peer must agree on — is whatever the application
/// provided or iceoryx2's default. It then raises the service limits that are
/// still at iceoryx2's compiled-in default, and adjusts the dead-node cleanup
/// policy: `ice-rpc` reaps dead nodes explicitly through
/// [`cleanup_dead_nodes`](crate::transport::cleanup_dead_nodes) instead of
/// letting iceoryx2 do it implicitly on every open or drop.
pub(crate) fn build_iceoryx2_config() -> Config {
    let mut config = Config::global_config().clone();
    apply_transport_defaults(&mut config);
    config.global.node.cleanup_dead_nodes_on_creation = true;
    config.global.node.cleanup_dead_nodes_on_destruction = false;
    config.global.service.cleanup_dead_nodes_on_open = false;
    config
}

/// Raises the port counts that are still at iceoryx2's compiled-in default.
///
/// Only a value equal to the compiled-in default is raised: a deployment that
/// chose another value owns it. This is what makes the transport usable without
/// a configuration file while keeping a config file authoritative.
///
/// The subscriber buffer and the publisher loan limit are deliberately **not**
/// raised: the buffer is already correct because safe overflow is disabled, and
/// the loan limit is a throughput/memory trade-off the deployment chooses.
fn apply_transport_defaults(config: &mut Config) {
    let compiled = Config::default();
    let compiled = &compiled.defaults.publish_subscribe;
    let defaults = &mut config.defaults.publish_subscribe;

    if defaults.max_publishers == compiled.max_publishers {
        defaults.max_publishers = MIN_MAX_PUBLISHERS;
    }
    if defaults.max_subscribers == compiled.max_subscribers {
        defaults.max_subscribers = MIN_MAX_SUBSCRIBERS;
    }
    if defaults.max_nodes == compiled.max_nodes {
        defaults.max_nodes = MIN_MAX_NODES;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceoryx2::prelude::SemanticString;

    #[test]
    fn build_config_inherits_the_global_root_path() {
        let config = build_iceoryx2_config();
        // The root-path is the shared-memory domain: two processes must not diverge.
        assert_eq!(
            config.global.root_path().as_bytes(),
            Config::global_config().global.root_path().as_bytes()
        );
    }

    #[test]
    fn build_config_disables_implicit_dead_node_cleanup() {
        let config = build_iceoryx2_config();
        assert!(!config.global.node.cleanup_dead_nodes_on_destruction);
        assert!(!config.global.service.cleanup_dead_nodes_on_open);
    }

    #[test]
    fn the_compiled_defaults_are_raised_to_the_transport_capacity() {
        let mut config = Config::default();
        apply_transport_defaults(&mut config);

        let compiled = Config::default().defaults.publish_subscribe;
        let defaults = &config.defaults.publish_subscribe;
        assert_eq!(defaults.max_publishers, MIN_MAX_PUBLISHERS);
        assert_eq!(defaults.max_subscribers, MIN_MAX_SUBSCRIBERS);
        assert_eq!(defaults.max_nodes, MIN_MAX_NODES);
        assert_eq!(
            defaults.publisher_max_loaned_samples, compiled.publisher_max_loaned_samples,
            "the loan limit stays at iceoryx2's default: raising it is an explicit choice"
        );
        assert_eq!(
            defaults.subscriber_max_buffer_size, compiled.subscriber_max_buffer_size,
            "the subscriber buffer stays at iceoryx2's default: backpressure is enough"
        );
    }

    #[test]
    fn a_deployment_value_is_not_replaced() {
        let mut config = Config::default();
        config.defaults.publish_subscribe.max_publishers = 64;
        config.defaults.publish_subscribe.max_subscribers = 48;
        config.defaults.publish_subscribe.max_nodes = 128;
        config.defaults.publish_subscribe.subscriber_max_buffer_size = 2048;
        config
            .defaults
            .publish_subscribe
            .publisher_max_loaned_samples = 512;

        apply_transport_defaults(&mut config);

        let defaults = &config.defaults.publish_subscribe;
        assert_eq!(defaults.max_publishers, 64);
        assert_eq!(defaults.max_subscribers, 48);
        assert_eq!(defaults.max_nodes, 128);
        assert_eq!(defaults.subscriber_max_buffer_size, 2048);
        assert_eq!(defaults.publisher_max_loaned_samples, 512);
    }

    /// Pins the port floors documented in `ice-rpc/Readme.md`, section
    /// "Configuration", so a floor change cannot land without updating the docs.
    #[test]
    fn the_documented_capacities_are_pinned() {
        assert_eq!(
            (MIN_MAX_PUBLISHERS, MIN_MAX_SUBSCRIBERS, MIN_MAX_NODES),
            (16, 16, 32)
        );
    }
}
