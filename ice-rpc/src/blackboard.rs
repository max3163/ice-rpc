//! Discovery registry: 1 Blackboard per node.
//!
//! # Architecture
//!
//! Each node creates a Blackboard `ice_rpc_node_{node_id}`.
//! The **key** is the service name (`[u8; 64]`), the **value** is the NodeId (`u32`).
//! `list_keys()` directly returns all the service names of the node.
//!
//! ```text
//!  Blackboard: ice_rpc_node_1234   KeyType = [u8; 64], ValueType = u32
//!
//!  Key "ConfigService"  → 1234
//!  Key "DatabaseService" → 1234
//!  Key "HttpService"     → 1234
//! ```

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use iceoryx2::prelude::*;

use crate::locator::ServiceLocator;
use crate::types::{MAX_SERVICES_PER_NODE, REGISTRY_SERVICE_NAME_LEN};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const NODE_BB_PREFIX: &str = "ice_rpc_node_";

pub fn node_bb_name(node_id: u32) -> String {
    format!("{}{}", NODE_BB_PREFIX, node_id)
}

/// Key type: fixed-size service name.
///
/// The key is a **raw byte array**, not a NUL-terminated C string: a name of
/// exactly [`REGISTRY_SERVICE_NAME_LEN`] bytes fills the whole key and is
/// therefore valid. Zero bytes act as padding for shorter names only.
///
/// The `#[service]` macro rejects any name longer than
/// [`crate::types::SERVICE_NAME_LEN`] (`== REGISTRY_SERVICE_NAME_LEN`) at
/// compile time, so the mapping `name → key → name` is lossless for every
/// accepted name. Carving out a reserved terminator byte here (as the code
/// previously did) silently truncated 64-byte names and made them
/// undiscoverable.
type ServiceKey = [u8; REGISTRY_SERVICE_NAME_LEN];

fn service_name_to_key(name: &str) -> ServiceKey {
    let mut key = [0u8; REGISTRY_SERVICE_NAME_LEN];
    let src = name.as_bytes();
    let len = src.len().min(REGISTRY_SERVICE_NAME_LEN);
    key[..len].copy_from_slice(&src[..len]);
    key
}

fn key_to_service_name(key: &ServiceKey) -> String {
    // Service names never contain a NUL byte (the macro restricts them to
    // ASCII alphanumerics, '_' and '-'), so the first zero marks the padding.
    // A key without any zero holds a full-length name and is returned as-is.
    let len = key
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(REGISTRY_SERVICE_NAME_LEN);
    String::from_utf8_lossy(&key[..len]).to_string()
}

// ---------------------------------------------------------------------------
// KeepAlive
// ---------------------------------------------------------------------------

static BB_WRITERS: OnceLock<Mutex<HashMap<String, Box<dyn std::any::Any + Send>>>> =
    OnceLock::new();

fn keep_writer_alive(bb_name: &str, writer: Box<dyn std::any::Any + Send>) {
    if let Ok(mut map) = BB_WRITERS.get_or_init(|| Mutex::new(HashMap::new())).lock() {
        map.insert(bb_name.to_string(), writer);
    }
}

/// Drops all the cached blackboard writers (called at shutdown).
///
/// The writers live in a process-lifetime singleton; dropping them releases
/// the iceoryx2 blackboard shared-memory segments (`blackboard_mgmt` and
/// `blackboard_data`).
pub fn clear_registry_writers() {
    if let Some(map) = BB_WRITERS.get() {
        let count = match map.lock() {
            Ok(mut guard) => {
                let count = guard.len();
                guard.clear();
                count
            }
            Err(_) => 0,
        };
        if count > 0 {
            log::info!("[ice-rpc] registry: dropped {count} blackboard writer(s).");
        }
    }
}

// ---------------------------------------------------------------------------
// API: creation (Provider)
// ---------------------------------------------------------------------------

/// Creates the node Blackboard with one key per service.
///
/// Called ONLY ONCE after the initialization of all services.
pub fn create_node_blackboard(node_id: u32, service_names: &[String]) {
    // Validate *before* any side effect: an over-sized node must not be marked
    // as a provider, otherwise a later shutdown would announce a node that
    // never published a blackboard. Formerly an `assert!`, which aborted the
    // process under `panic = "abort"` for a mere configuration error.
    if service_names.len() > MAX_SERVICES_PER_NODE {
        log::error!(
            "[registry] too many services ({}), max = {}: blackboard '{}' not published",
            service_names.len(),
            MAX_SERVICES_PER_NODE,
            node_bb_name(node_id)
        );
        return;
    }

    // Crash detection is carried by the iceoryx2 Node's native monitoring token
    crate::node_liveness::mark_provider();
    log::info!(
        "[registry] Liveness via native iceoryx2 node monitoring (pid={})",
        node_id
    );

    let node = match ServiceLocator::global().try_get_node() {
        Some(n) => n,
        None => match ServiceLocator::global().get_node_sync() {
            Ok(n) => n,
            Err(_) => {
                log::error!("[registry] get_node_sync failed");
                return;
            }
        },
    };

    let bb_name = node_bb_name(node_id);
    let name = match ServiceName::new(&bb_name) {
        Ok(n) => n,
        Err(e) => {
            log::error!("[registry] ServiceName('{}') failed: {:?}", bb_name, e);
            return;
        }
    };

    let mut builder = node
        .service_builder(&name)
        .blackboard_creator::<ServiceKey>()
        .max_readers(crate::BLACKBOARD_MAX_READERS);

    for svc_name in service_names {
        let key = service_name_to_key(svc_name);
        builder = builder.add::<u32>(key, 0u32);
    }

    let svc = match builder.create() {
        Ok(s) => {
            log::info!(
                "[registry] Blackboard '{}' created with {} keys",
                bb_name,
                service_names.len()
            );
            s
        }
        Err(_) => {
            match node
                .service_builder(&name)
                .blackboard_opener::<ServiceKey>()
                .open()
            {
                Ok(s) => s,
                Err(e) => {
                    log::error!("[registry] open/create '{}' failed: {:?}", bb_name, e);
                    return;
                }
            }
        }
    };

    let writer = match svc.writer_builder().create() {
        Ok(w) => w,
        Err(e) => {
            log::error!("[registry] writer create '{}' failed: {:?}", bb_name, e);
            return;
        }
    };

    for svc_name in service_names {
        let key = service_name_to_key(svc_name);
        match writer.entry::<u32>(&key) {
            Ok(entry) => {
                entry.update_with_copy(node_id);
                log::info!("[registry] '{}' key '{}' = {}", bb_name, svc_name, node_id);
            }
            Err(e) => {
                log::error!(
                    "[registry] write '{}' key='{}' failed: {:?}",
                    bb_name,
                    svc_name,
                    e
                );
            }
        }
    }

    keep_writer_alive(&bb_name, Box::new(writer));
}

// ---------------------------------------------------------------------------
// API: read (Consumer)
// ---------------------------------------------------------------------------

/// Lists all the service names of a node via `list_keys()`.
pub fn list_services(node_id: u32) -> Vec<String> {
    let node = match ServiceLocator::global().try_get_node() {
        Some(n) => n,
        None => return Vec::new(),
    };

    let bb_name = node_bb_name(node_id);
    let name = match ServiceName::new(&bb_name) {
        Ok(n) => n,
        Err(_) => return Vec::new(),
    };

    let svc = match node
        .service_builder(&name)
        .blackboard_opener::<ServiceKey>()
        .open()
    {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };

    let mut services = Vec::new();
    svc.list_keys(|key| {
        services.push(key_to_service_name(key));
        CallbackProgression::Continue
    });
    log::debug!("[registry] list_services('{}') → {:?}", bb_name, services);
    services
}

/// Cleans up the IPC resources of all dead nodes via the iceoryx2 API.
/// Lists all the NodeIds present via `Service::list()`.
pub fn list_nodes() -> Vec<u32> {
    use iceoryx2::service::ipc_threadsafe;

    let mut nodes = Vec::new();
    let result =
        ipc_threadsafe::Service::list(iceoryx2::config::Config::global_config(), |service| {
            let name = service.static_details.name().to_string();
            if let Some(suffix) = name.strip_prefix(NODE_BB_PREFIX) {
                if let Ok(node_id) = suffix.parse::<u32>() {
                    nodes.push(node_id);
                }
            }
            CallbackProgression::Continue
        });

    match result {
        Ok(()) => log::debug!("[registry] list_nodes: {} node(s)", nodes.len()),
        Err(e) => log::error!("[registry] Service::list failed: {:?}", e),
    }
    nodes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_bb_name_uses_prefix() {
        assert_eq!(node_bb_name(1234), "ice_rpc_node_1234");
    }

    #[test]
    fn service_name_key_roundtrip() {
        let key = service_name_to_key("DatabaseService");
        assert_eq!(key_to_service_name(&key), "DatabaseService");
    }

    #[test]
    fn service_name_key_roundtrip_at_max_length() {
        // A full-capacity name must round-trip losslessly: the key is a raw
        // byte array, not a NUL-terminated C string. Regression test for the
        // 64-byte service name that used to be truncated to 63 bytes and could
        // therefore never be discovered by a consumer.
        let max = "A".repeat(REGISTRY_SERVICE_NAME_LEN);
        let key = service_name_to_key(&max);
        assert_eq!(
            key.iter().filter(|&&byte| byte == 0).count(),
            0,
            "a max-length name must fill the whole key"
        );
        assert_eq!(key_to_service_name(&key), max);
    }

    #[test]
    fn service_name_key_truncates_only_beyond_capacity() {
        // Names longer than the capacity cannot be produced by the `#[service]`
        // macro, but the helper must still behave predictably.
        let too_long = "A".repeat(REGISTRY_SERVICE_NAME_LEN + 10);
        let key = service_name_to_key(&too_long);
        let name = key_to_service_name(&key);
        assert_eq!(name.len(), REGISTRY_SERVICE_NAME_LEN);
        assert!(too_long.starts_with(&name));
    }
}
