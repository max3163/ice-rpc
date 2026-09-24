//! iceoryx2 configuration used by the transport.
//!
//! `ice-rpc` does not own the iceoryx2 configuration and never writes a config
//! file. The effective configuration is the one iceoryx2 resolves itself, via
//! [`Config::global_config`]: `./config/iceoryx2.toml` first, then the user
//! config directory, then the global config directory, and finally the
//! compiled-in default when none exists.
//!
//! An application that needs a shared, non-default root-path provides that file
//! once; every process then discovers the same configuration, so peers agree on
//! the shared-memory domain without `ice-rpc` acting as a configuration owner.
//!
//! The only adjustments applied locally are the dead-node cleanup flags: they
//! change the reaping policy, never which entities are visible on the bus.

use iceoryx2::config::Config;

/// Returns the iceoryx2 configuration the transport uses.
///
/// Starts from the effective global configuration, so the root-path — the
/// shared-memory domain every peer must agree on — is whatever the application
/// provided or iceoryx2's default, then adjusts the dead-node cleanup policy:
/// `ice-rpc` reaps dead nodes explicitly through
/// [`cleanup_dead_nodes`](crate::transport::cleanup_dead_nodes) instead of
/// letting iceoryx2 do it implicitly on every open or drop.
pub(crate) fn build_iceoryx2_config() -> Config {
    let mut config = Config::global_config().clone();
    config.global.node.cleanup_dead_nodes_on_creation = true;
    config.global.node.cleanup_dead_nodes_on_destruction = false;
    config.global.service.cleanup_dead_nodes_on_open = false;
    config
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceoryx2::prelude::SemanticString;

    #[test]
    fn build_config_inherits_the_global_root_path() {
        let config = build_iceoryx2_config();
        // The root-path is the shared-memory domain. Rewriting it here would let
        // two processes pick different domains and silently stop seeing each
        // other, which is exactly what `ice-rpc` must not do.
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
}
