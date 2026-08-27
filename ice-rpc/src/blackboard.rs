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
type ServiceKey = [u8; REGISTRY_SERVICE_NAME_LEN];

fn service_name_to_key(name: &str) -> ServiceKey {
    let mut key = [0u8; REGISTRY_SERVICE_NAME_LEN];
    let src = name.as_bytes();
    let len = src.len().min(REGISTRY_SERVICE_NAME_LEN - 1);
    key[..len].copy_from_slice(&src[..len]);
    key
}

fn key_to_service_name(key: &ServiceKey) -> String {
    let len = key
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(REGISTRY_SERVICE_NAME_LEN);
    String::from_utf8_lossy(&key[..len]).to_string()
}

// ---------------------------------------------------------------------------
// KeepAlive
// ---------------------------------------------------------------------------

trait KeepAlive: Send + 'static {}
impl<T: Send + 'static> KeepAlive for T {}

static BB_WRITERS: OnceLock<Mutex<HashMap<String, Box<dyn KeepAlive>>>> = OnceLock::new();

fn keep_writer_alive(bb_name: &str, writer: Box<dyn KeepAlive>) {
    if let Ok(mut map) = BB_WRITERS.get_or_init(|| Mutex::new(HashMap::new())).lock() {
        map.insert(bb_name.to_string(), writer);
    }
}

// ---------------------------------------------------------------------------
// API: creation (Provider)
// ---------------------------------------------------------------------------

/// Creates the node Blackboard with one key per service.
///
/// Called ONLY ONCE after the initialization of all services.
pub fn create_node_blackboard(node_id: u32, service_names: &[String]) {
    // Acquires the kernel lock for crash detection.
    match crate::node_lock::acquire_global_node_lock(crate::types::NodeId(node_id)) {
        Ok(lock_name) => log::info!("[registry] Kernel lock acquired: '{}'", lock_name),
        Err(e) => log::error!("[registry] Failed to acquire kernel lock: {}", e),
    }
    assert!(
        service_names.len() <= MAX_SERVICES_PER_NODE,
        "Too many services ({}), max = {}",
        service_names.len(),
        MAX_SERVICES_PER_NODE
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
