//! Read-only observation surface used by the `ice-rpc-monitor` observer.
//!
//! Lets an external process attach to the channels an ice-rpc process exposes
//! without touching the hot path: only the zero-copy `RpcHeader` (sequence,
//! timestamp) and the payload length are read, never the rkyv payload. The
//! identity of the emitter (PID, node id, publisher id) is read from the
//! **native** iceoryx2 sample [`Emitter`], so the wire header carries none.
//!
//! The observer is linked against the **same service definitions** as the
//! providers and consumers, so it can also decode the payloads. Each `#[service]`
//! trait generates a `{Trait}Decoder` implementing [`ServiceDecoder`]; register
//! them in a [`Decoders`] registry, then the monitor renders every message with
//! the [`Display`](std::fmt::Display) implementation of the service types.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use iceoryx2::node::{NodeState as IoxNodeState, NodeView};
use iceoryx2::prelude::*;
use iceoryx2::service::static_config::messaging_pattern::MessagingPattern;
use iceoryx2::service::ServiceDetails;

use crate::types::RpcError;

pub use crate::transport::{decode_aligned, discover_channels, Direction, DirectionView, Emitter};

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

impl NodeHealth {
    /// Stable label used by the metrics.
    pub const fn label(self) -> &'static str {
        match self {
            NodeHealth::Alive => "alive",
            NodeHealth::Dead => "dead",
            NodeHealth::Inaccessible => "inaccessible",
            NodeHealth::Undefined => "undefined",
        }
    }
}

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
        let pid = crate::types::raw_pid_to_u32(state.node_id().pid().value());
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

impl ServiceRole {
    /// Stable label used by the metrics.
    pub const fn label(self) -> &'static str {
        match self {
            ServiceRole::Request => "req",
            ServiceRole::Response => "resp",
            ServiceRole::RequestNotify => "req_notify",
            ServiceRole::ResponseNotify => "resp_notify",
            ServiceRole::Other => "other",
        }
    }
}

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

/// Decodes a rkyv **request** payload into its [`Display`] form.
///
/// Returns `None` when the bytes are not a valid encoding of `R` — typically
/// because the observer was not built with the service types.
///
/// [`Display`]: std::fmt::Display
pub fn decode_request<R>(payload: &[u8]) -> Option<String>
where
    R: rkyv::Archive + std::fmt::Display,
    <R as rkyv::Archive>::Archived:
        rkyv::Deserialize<R, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
    for<'a> <R as rkyv::Archive>::Archived:
        rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    crate::transport::decode_aligned::<R>(payload)
        .ok()
        .map(|value| value.to_string())
}

/// Decodes a rkyv **response** payload into its [`Display`] form.
///
/// A response is a [`WireEvent<T, E>`](crate::types::WireEvent): the human
/// rendering is that of the carried value, or the terminal kind name.
///
/// [`Display`]: std::fmt::Display
pub fn decode_response<T, E>(payload: &[u8]) -> Option<String>
where
    T: rkyv::Archive + std::fmt::Display,
    E: rkyv::Archive + std::fmt::Display,
    crate::types::WireEvent<T, E>: rkyv::Archive,
    <crate::types::WireEvent<T, E> as rkyv::Archive>::Archived: rkyv::Deserialize<
        crate::types::WireEvent<T, E>,
        rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
    >,
    for<'a> <crate::types::WireEvent<T, E> as rkyv::Archive>::Archived:
        rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    use crate::types::WireEvent;
    crate::transport::decode_aligned::<WireEvent<T, E>>(payload)
        .ok()
        .map(|event| match event {
            WireEvent::Next(value) | WireEvent::CompleteWith(value) => value.to_string(),
            WireEvent::Complete => "complete".to_owned(),
            WireEvent::Error(error) => format!("error: {error}"),
            WireEvent::RpcError(error) => format!("rpc error: {error}"),
        })
}

/// Like [`decode_response`], for a service whose success type is `()`.
///
/// `()` does not implement [`Display`](std::fmt::Display), so the unit case needs
/// its own decoder — many health-check methods return `Observable<(), E>`.
pub fn decode_response_unit<E>(payload: &[u8]) -> Option<String>
where
    E: rkyv::Archive + std::fmt::Display,
    crate::types::WireEvent<(), E>: rkyv::Archive,
    <crate::types::WireEvent<(), E> as rkyv::Archive>::Archived: rkyv::Deserialize<
        crate::types::WireEvent<(), E>,
        rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
    >,
    for<'a> <crate::types::WireEvent<(), E> as rkyv::Archive>::Archived:
        rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    use crate::types::WireEvent;
    crate::transport::decode_aligned::<WireEvent<(), E>>(payload)
        .ok()
        .map(|event| match event {
            WireEvent::Next(()) | WireEvent::CompleteWith(()) => "()".to_owned(),
            WireEvent::Complete => "complete".to_owned(),
            WireEvent::Error(error) => format!("error: {error}"),
            WireEvent::RpcError(error) => format!("rpc error: {error}"),
        })
}

/// Turns the raw rkyv payload of one service into human-readable text.
///
/// The two directions are separate because a request and a response carry
/// different types (`{Service}Request` and `WireEvent<T, E>`).
pub trait ServiceDecoder: Send + Sync {
    /// Renders a request payload, given the method name carried by the header.
    fn request(&self, method: &str, payload: &[u8]) -> Option<String>;

    /// Renders a response payload, given the method of the matched request.
    fn response(&self, method: &str, payload: &[u8]) -> Option<String>;
}

/// Boxed renderer used by [`ClosureDecoder`].
type Render = Box<dyn Fn(&str, &[u8]) -> Option<String> + Send + Sync>;

/// A [`ServiceDecoder`] assembled from two closures.
pub struct ClosureDecoder {
    request: Render,
    response: Render,
}

impl ClosureDecoder {
    /// Builds a decoder from a request renderer and a response renderer.
    pub fn new(
        request: impl Fn(&str, &[u8]) -> Option<String> + Send + Sync + 'static,
        response: impl Fn(&str, &[u8]) -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            request: Box::new(request),
            response: Box::new(response),
        }
    }
}

impl ServiceDecoder for ClosureDecoder {
    fn request(&self, method: &str, payload: &[u8]) -> Option<String> {
        (self.request)(method, payload)
    }

    fn response(&self, method: &str, payload: &[u8]) -> Option<String> {
        (self.response)(method, payload)
    }
}

/// Registry of the decoders known to an observer, keyed by service id.
///
/// The generated `{Trait}Decoder::register` populates it; an observer binary
/// builds one from the service definitions it links against.
#[derive(Default)]
pub struct Decoders {
    services: HashMap<u32, Arc<dyn ServiceDecoder>>,
}

impl Decoders {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers (or replaces) the decoder of `service_id`.
    pub fn register(&mut self, service_id: u32, decoder: Arc<dyn ServiceDecoder>) -> &mut Self {
        self.services.insert(service_id, decoder);
        self
    }

    /// Number of services with a registered decoder.
    pub fn len(&self) -> usize {
        self.services.len()
    }

    /// Returns `true` when no decoder is registered.
    pub fn is_empty(&self) -> bool {
        self.services.is_empty()
    }

    /// Renders a request payload; `None` without a decoder or on a decode failure.
    pub fn request(&self, service_id: u32, method: &str, payload: &[u8]) -> Option<String> {
        self.services.get(&service_id)?.request(method, payload)
    }

    /// Renders a response payload; `None` without a decoder or on a decode failure.
    pub fn response(&self, service_id: u32, method: &str, payload: &[u8]) -> Option<String> {
        self.services.get(&service_id)?.response(method, payload)
    }
}

impl fmt::Debug for Decoders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Decoders")
            .field("services", &self.services.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rkyv::{Archive, Deserialize, Serialize};

    #[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
    struct Reply {
        value: i32,
    }

    impl fmt::Display for Reply {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "value={}", self.value)
        }
    }

    #[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
    enum Request {
        Add { a: i32, b: i32 },
    }

    impl fmt::Display for Request {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Request::Add { a, b } => write!(f, "add(a={a}, b={b})"),
            }
        }
    }

    fn encode<T>(value: &T) -> Vec<u8>
    where
        T: for<'a> rkyv::Serialize<
            rkyv::rancor::Strategy<
                rkyv::ser::Serializer<
                    rkyv::util::AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'a>,
                    rkyv::ser::sharing::Share,
                >,
                rkyv::rancor::Error,
            >,
        >,
    {
        rkyv::to_bytes::<rkyv::rancor::Error>(value)
            .expect("encode")
            .to_vec()
    }

    #[test]
    fn requests_and_responses_decode_through_display() {
        let request = encode(&Request::Add { a: 1, b: 2 });
        assert_eq!(
            decode_request::<Request>(&request).as_deref(),
            Some("add(a=1, b=2)")
        );

        let response = encode(&crate::types::WireEvent::<Reply, String>::Next(Reply {
            value: 7,
        }));
        assert_eq!(
            decode_response::<Reply, String>(&response).as_deref(),
            Some("value=7")
        );

        let unit = encode(&crate::types::WireEvent::<(), String>::Complete);
        assert_eq!(
            decode_response_unit::<String>(&unit).as_deref(),
            Some("complete")
        );
    }

    #[test]
    fn garbage_does_not_decode() {
        assert_eq!(decode_request::<Request>(&[0xff, 0x00]), None);
        assert_eq!(decode_response::<Reply, String>(&[0xff, 0x00]), None);
    }

    #[test]
    fn a_registered_decoder_renders_both_directions() {
        let mut decoders = Decoders::new();
        decoders.register(
            7,
            Arc::new(ClosureDecoder::new(
                |_method, payload| Some(String::from_utf8_lossy(payload).into_owned()),
                |_method, payload| Some(format!("{} bytes", payload.len())),
            )),
        );

        assert_eq!(decoders.request(7, "echo", b"hi").as_deref(), Some("hi"));
        assert_eq!(
            decoders.response(7, "echo", b"abcd").as_deref(),
            Some("4 bytes")
        );
        assert_eq!(decoders.request(9, "echo", b"hi"), None);
    }
}
