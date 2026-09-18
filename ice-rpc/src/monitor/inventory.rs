//! Inventory of the machine: the iceoryx2 nodes, services and file layout.
//!
//! Everything an observer can learn **without** touching a payload: the nodes
//! and their liveness, the services and their role in a channel, and the
//! filesystem layout iceoryx2 uses for its segments. The decoding of payloads
//! lives in [`super::decode`], so a monitor that only wants statistics never
//! carries the rendering code.

use iceoryx2::node::{NodeState as IoxNodeState, NodeView};
use iceoryx2::prelude::*;
use iceoryx2::service::static_config::messaging_pattern::MessagingPattern;
use iceoryx2::service::ServiceDetails;

use crate::labels::impl_labels;
use crate::types::RpcError;

/// Concrete iceoryx2 flavour used by the transport and the observers.
type Iox = iceoryx2::service::ipc_threadsafe::Service;

// ── Node inventory ──────────────────────────────────────────────────

/// Health state of an iceoryx2 node, as reported by the native monitoring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeHealth {
    /// The owning process is alive.
    Alive,
    /// The process died without cleaning up: the node is a crash candidate.
    Dead,
    /// The process lacks the permissions to tell.
    Inaccessible,
    /// Inconsistent node resources.
    Undefined,
}

impl NodeHealth {}

// The states an observer exports (`state="dead"`).
impl_labels!(NodeHealth {
    NodeHealth::Alive => "alive",
    NodeHealth::Dead => "dead",
    NodeHealth::Inaccessible => "inaccessible",
    NodeHealth::Undefined => "undefined",
});

/// One iceoryx2 node registered on the machine.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// Process id owning the node.
    pub pid: u32,
    /// Native liveness state.
    pub health: NodeHealth,
    /// Executable name, when the process has the permissions to read it.
    pub executable: Option<String>,
    /// Node name (`None` when the details are inaccessible, empty when unnamed).
    pub name: Option<String>,
}

/// Lists every iceoryx2 node of the machine.
///
/// Returns `None` when the scan itself fails, so callers never mistake a failed
/// scan for "no node at all".
pub fn list_nodes() -> Option<Vec<NodeInfo>> {
    let config = crate::config::build_iceoryx2_config();
    let mut nodes = Vec::new();

    let result = Node::<Iox>::list(&config, |state| {
        let pid = crate::types::node_pid_to_u32(state.node_id().pid::<Iox>());
        let (health, details) = match &state {
            IoxNodeState::Alive(view) => (NodeHealth::Alive, view.details()),
            IoxNodeState::Dead(view) => (NodeHealth::Dead, view.details()),
            IoxNodeState::Inaccessible(_) => (NodeHealth::Inaccessible, &None),
            IoxNodeState::Undefined(_) => (NodeHealth::Undefined, &None),
        };
        let (executable, name) = match details {
            Some(details) => (
                Some(String::from_utf8_lossy(details.executable().as_bytes()).into_owned()),
                Some(String::from_utf8_lossy(details.name().as_bytes()).into_owned()),
            ),
            None => (None, None),
        };
        nodes.push(NodeInfo {
            pid,
            health,
            executable,
            name,
        });
        CallbackProgression::Continue
    });

    match result {
        Ok(()) => Some(nodes),
        Err(e) => {
            log::debug!("[monitor] Node::list failed: {e:?}");
            None
        }
    }
}

// ── Service inventory ───────────────────────────────────────────────

/// Role of an iceoryx2 service inside an ice-rpc channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceRole {
    /// `{channel}_req`: consumer to provider requests.
    Request,
    /// `{channel}_resp`: provider to consumer responses.
    Response,
    /// `{channel}_req_notify`: wake-up signal of the request direction.
    RequestNotify,
    /// `{channel}_resp_notify`: wake-up signal of the response direction.
    ResponseNotify,
    /// Any other iceoryx2 service of the machine.
    Other,
}

impl ServiceRole {}

// The roles an observer exports (`role="req"`).
impl_labels!(ServiceRole {
    ServiceRole::Request => "req",
    ServiceRole::Response => "resp",
    ServiceRole::RequestNotify => "req_notify",
    ServiceRole::ResponseNotify => "resp_notify",
    ServiceRole::Other => "other",
});

/// One iceoryx2 service registered on the machine.
#[derive(Debug, Clone)]
pub struct ServiceInfo {
    /// iceoryx2 service name (e.g. `DatabaseService_req`).
    pub name: String,
    /// Messaging pattern, as a stable label.
    pub pattern: &'static str,
    /// ice-rpc role deduced from the name.
    pub role: ServiceRole,
    /// Number of nodes currently registered on the service.
    pub participants: usize,
}

/// Deduces the ice-rpc role of a service from its name.
///
/// The notification services end with their direction suffix + `_notify`, so
/// they must be tested before the plain direction suffixes.
fn role_of(name: &str) -> ServiceRole {
    if name.ends_with("_req_notify") {
        ServiceRole::RequestNotify
    } else if name.ends_with("_resp_notify") {
        ServiceRole::ResponseNotify
    } else if name.ends_with("_req") {
        ServiceRole::Request
    } else if name.ends_with("_resp") {
        ServiceRole::Response
    } else {
        ServiceRole::Other
    }
}

/// Lists every iceoryx2 service of the machine.
///
/// # Errors
/// Returns a [`RpcError`] when the service listing itself fails.
pub fn list_services() -> Result<Vec<ServiceInfo>, RpcError> {
    let config = crate::config::build_iceoryx2_config();
    let mut services = Vec::new();

    <Iox as Service>::list(&config, |details: ServiceDetails<Iox>| {
        let name = details.static_details.name().as_str().to_owned();
        let pattern = match details.static_details.messaging_pattern() {
            MessagingPattern::PublishSubscribe(_) => "PublishSubscribe",
            MessagingPattern::Event(_) => "Event",
            MessagingPattern::RequestResponse(_) => "RequestResponse",
            MessagingPattern::Blackboard(_) => "Blackboard",
            _ => "Other",
        };
        let participants = details
            .dynamic_details
            .as_ref()
            .map(|details| details.nodes.len())
            .unwrap_or(0);
        let role = role_of(&name);
        services.push(ServiceInfo {
            name,
            pattern,
            role,
            participants,
        });
        CallbackProgression::Continue
    })
    .map_err(|e| RpcError::TransportError(format!("service inventory: {e:?}")))?;

    Ok(services)
}

// ── Shared-memory layout ────────────────────────────────────────────

/// Filesystem layout iceoryx2 uses for its segments and configuration files.
///
/// The observer needs it to *measure* the shared-memory footprint on disk: the
/// segments are named concepts (`prefix + name + data_segment_suffix`) created
/// under [`Iceoryx2Layout::root_path`] (and, on Linux, possibly `/dev/shm`).
#[derive(Debug, Clone)]
pub struct Iceoryx2Layout {
    /// Root directory of the iceoryx2 resources.
    pub root_path: String,
    /// Directory holding the service files.
    pub service_dir: String,
    /// Directory holding the node files.
    pub node_dir: String,
    /// Prefix of every created file (default `iox2_`).
    pub prefix: String,
    /// Suffix of the port data segments (default `.data`).
    pub data_segment_suffix: String,
}

/// Returns the filesystem layout currently used by iceoryx2.
pub fn iceoryx2_layout() -> Iceoryx2Layout {
    let config = crate::config::build_iceoryx2_config();
    let global = &config.global;
    Iceoryx2Layout {
        root_path: String::from_utf8_lossy(global.root_path().as_bytes()).into_owned(),
        service_dir: String::from_utf8_lossy(global.service_dir().as_bytes()).into_owned(),
        node_dir: String::from_utf8_lossy(global.node_dir().as_bytes()).into_owned(),
        prefix: String::from_utf8_lossy(global.prefix.as_bytes()).into_owned(),
        data_segment_suffix: String::from_utf8_lossy(global.service.data_segment_suffix.as_bytes())
            .into_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_role_is_deduced_from_the_service_name() {
        assert_eq!(role_of("Db_req"), ServiceRole::Request);
        assert_eq!(role_of("Db_resp"), ServiceRole::Response);
        // The notification suffixes carry the direction one, so they must win.
        assert_eq!(role_of("Db_req_notify"), ServiceRole::RequestNotify);
        assert_eq!(role_of("Db_resp_notify"), ServiceRole::ResponseNotify);
        assert_eq!(role_of("something_else"), ServiceRole::Other);
    }

    #[test]
    fn the_labels_are_stable() {
        assert_eq!(NodeHealth::Alive.label(), "alive");
        assert_eq!(NodeHealth::Dead.label(), "dead");
        assert_eq!(ServiceRole::Request.label(), "req");
        assert_eq!(ServiceRole::ResponseNotify.label(), "resp_notify");
    }

    #[test]
    fn the_layout_exposes_the_paths_iceoryx2_uses() {
        let layout = iceoryx2_layout();
        assert!(!layout.root_path.is_empty());
        assert!(!layout.prefix.is_empty());
        assert!(layout.data_segment_suffix.starts_with('.'));
    }
}
