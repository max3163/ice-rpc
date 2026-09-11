//! Native iceoryx2 request/response transport (experimental).
//!
//! Enabled with the `native-transport` feature. This is **increment 2** of the
//! migration described in `plans/simplification.md` (F1): one iceoryx2
//! `request_response::<[u8], [u8]>` service per logical service, a process-wide
//! shared node, and rkyv-framed [`WireEvent`] payloads.
//!
//! Contract:
//! - a **request** is `encode_request(method, payload)` (method name + opaque
//!   payload, later the rkyv-encoded service request enum);
//! - a **response sample** is a rkyv-encoded [`WireEvent`] (the value, a
//!   business error, or a technical error);
//! - the **end of stream** is the connection close (no `Complete` frame);
//! - dropping the returned `Observable` closes the connection, which the server
//!   observes through `ActiveRequest::is_connected`.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;

use iceoryx2::prelude::*;
use iceoryx2::service::ipc_threadsafe;

use crate::types::{
    normalize_wire_event, unbounded_channel, Event, Observable, ObservableError, RpcError,
    WireEvent,
};
use crate::CancellationToken;

/// Concrete iceoryx2 service flavour used by the native transport.
type Iox = ipc_threadsafe::Service;
type IoxNode = iceoryx2::node::Node<Iox>;

/// Maximum slice length requested from iceoryx2 for request/response payloads.
const MAX_SLICE_LEN: usize = 64;

/// Polling interval of the response relay and of the service loop, in ms.
const POLL_MS: u64 = 2;

fn transport_error(context: &str, err: impl std::fmt::Debug) -> RpcError {
    RpcError::TransportError(format!("{context}: {err:?}"))
}

/// Returns the **process-wide** iceoryx2 node, created on first use.
///
/// Sharing one node avoids one node per call (increment 1 limitation) and keeps
/// every native client and service of the process under the same node.
fn shared_node() -> Result<Arc<IoxNode>, RpcError> {
    static NODE: OnceLock<Result<Arc<IoxNode>, String>> = OnceLock::new();
    NODE.get_or_init(|| {
        NodeBuilder::new()
            .create::<Iox>()
            .map(Arc::new)
            .map_err(|e| format!("{e:?}"))
    })
    .clone()
    .map_err(|e| RpcError::TransportError(format!("node creation: {e}")))
}

/// Encodes a request as `[method_len: u16][method utf8][payload]`.
pub fn encode_request(method: &str, payload: &[u8]) -> Vec<u8> {
    let method = method.as_bytes();
    let len = method.len().min(u16::MAX as usize);
    let mut out = Vec::with_capacity(2 + len + payload.len());
    out.extend_from_slice(&(len as u16).to_be_bytes());
    out.extend_from_slice(&method[..len]);
    out.extend_from_slice(payload);
    out
}

/// Decodes a request produced by [`encode_request`].
pub fn decode_request(bytes: &[u8]) -> Option<(String, &[u8])> {
    if bytes.len() < 2 {
        return None;
    }
    let len = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    if bytes.len() < 2 + len {
        return None;
    }
    let method = std::str::from_utf8(&bytes[2..2 + len]).ok()?;
    Some((method.to_owned(), &bytes[2 + len..]))
}

/// Lazy iterator of rkyv-encoded [`WireEvent`] samples streamed by a service.
///
/// Being lazy is what preserves streaming: the service loop pulls one sample at
/// a time and sends it while the client stays connected, instead of buffering
/// the whole response.
pub type ResponseIter = Box<dyn Iterator<Item = Vec<u8>> + Send>;

/// Wraps an [`Observable`] into a lazy [`ResponseIter`] of encoded [`WireEvent`].
///
/// Each `next()` blocks (runtime-agnostic [`crate::rt::block_on`]) until the
/// next event, encodes it as a `WireEvent` sample, and stops after the terminal
/// event. This is the bridge a generated service dispatcher uses to stream a
/// real implementation without buffering it.
pub fn observable_to_responses<T, E>(mut observable: Observable<T, E>) -> ResponseIter
where
    T: Send + 'static,
    E: Send + 'static,
    for<'a> WireEvent<T, E>: rkyv::Serialize<
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
    Box::new(std::iter::from_fn(move || {
        match crate::rt::block_on(observable.recv()) {
            Ok(event) => {
                let wire: WireEvent<T, E> = event.into();
                rkyv::to_bytes::<rkyv::rancor::Error>(&wire)
                    .ok()
                    .map(|bytes| bytes.to_vec())
            }
            // Stream closed or terminal already consumed: end of responses.
            Err(_) => None,
        }
    }))
}

/// Per-service table of method handlers, built by a generated provider.
///
/// Each handler maps a decoded request payload to a lazy [`ResponseIter`]; the
/// generated code typically wraps the service implementation's [`Observable`]
/// with [`observable_to_responses`]. Unknown methods close the stream.
/// Handler of one RPC method: decoded payload → lazy response samples.
pub type MethodHandler = Box<dyn Fn(&[u8]) -> ResponseIter + Send + Sync>;

#[derive(Default)]
pub struct ServiceDispatcher {
    handlers: HashMap<&'static str, MethodHandler>,
}

impl ServiceDispatcher {
    /// Creates an empty dispatcher.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers the handler of one RPC method.
    pub fn method<F>(&mut self, name: &'static str, handler: F) -> &mut Self
    where
        F: Fn(&[u8]) -> ResponseIter + Send + Sync + 'static,
    {
        self.handlers.insert(name, Box::new(handler));
        self
    }

    /// Routes a decoded request to its handler.
    pub fn dispatch(&self, method: &str, payload: &[u8]) -> ResponseIter {
        match self.handlers.get(method) {
            Some(handler) => handler(payload),
            None => Box::new(std::iter::empty()),
        }
    }
}

/// Spawns a native **service** for `service_name`.
///
/// `dispatcher` receives the decoded method name and payload, and returns a
/// [`ResponseIter`] of rkyv-encoded [`WireEvent`] samples to stream back. The
/// connection is closed when the iterator is exhausted (end of stream).
pub fn spawn_native_service<F>(
    service_name: &str,
    dispatcher: F,
    stop: CancellationToken,
) -> JoinHandle<()>
where
    F: Fn(&str, &[u8]) -> ResponseIter + Send + Sync + 'static,
{
    let service_name = service_name.to_owned();
    std::thread::spawn(move || {
        let node = match shared_node() {
            Ok(node) => node,
            Err(e) => {
                log::error!("[native] node creation failed for '{service_name}': {e:?}");
                return;
            }
        };
        let name = match ServiceName::new(&service_name) {
            Ok(name) => name,
            Err(e) => {
                log::error!("[native] invalid service name '{service_name}': {e:?}");
                return;
            }
        };
        // Keep the factory (service) alive for the whole loop.
        let service = match node
            .service_builder(&name)
            .request_response::<[u8], [u8]>()
            .max_active_requests_per_client(1)
            .max_response_buffer_size(64)
            .open_or_create()
        {
            Ok(service) => service,
            Err(e) => {
                log::error!("[native] open service '{service_name}' failed: {e:?}");
                return;
            }
        };
        let server = match service
            .server_builder()
            .initial_max_slice_len(MAX_SLICE_LEN)
            .create()
        {
            Ok(server) => server,
            Err(e) => {
                log::error!("[native] server creation failed for '{service_name}': {e:?}");
                return;
            }
        };

        log::info!("[native] service '{service_name}' ready");
        while !stop.is_cancelled() {
            match server.receive() {
                Ok(Some(active)) => {
                    let request = active.payload().to_vec();
                    let Some((method, payload)) = decode_request(&request) else {
                        log::warn!("[native] '{service_name}': malformed request");
                        continue;
                    };
                    for bytes in dispatcher(&method, payload) {
                        if !active.is_connected() {
                            break;
                        }
                        let len = bytes.len().max(1);
                        let response = match active.loan_slice_uninit(len) {
                            Ok(response) => response,
                            Err(e) => {
                                log::warn!("[native] loan failed: {e:?}");
                                break;
                            }
                        };
                        if let Err(e) = response
                            .write_from_fn(|i| bytes.get(i).copied().unwrap_or(0))
                            .send()
                        {
                            log::warn!("[native] send failed: {e:?}");
                            break;
                        }
                    }
                    // Dropping `active` closes the connection: end of stream.
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(POLL_MS)),
                Err(e) => {
                    log::warn!("[native] service receive error: {e:?}");
                    std::thread::sleep(Duration::from_millis(POLL_MS));
                }
            }
        }
        log::info!("[native] service '{service_name}' stopped");
    })
}

/// Performs one native call and returns the streamed responses as an
/// [`Observable`].
///
/// Each response sample is a rkyv-encoded [`WireEvent`]; it is normalized into
/// the user-facing [`Event`]. The terminal `Event::Complete` is produced when
/// the service closes the connection.
pub fn native_call<T, E>(
    service_name: &str,
    method: &str,
    payload: &[u8],
) -> Result<Observable<T, E>, RpcError>
where
    T: Send + 'static,
    E: Send + 'static,
    WireEvent<T, E>: rkyv::Archive,
    <WireEvent<T, E> as rkyv::Archive>::Archived: rkyv::Deserialize<
        WireEvent<T, E>,
        rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
    >,
    for<'a> <WireEvent<T, E> as rkyv::Archive>::Archived:
        rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    let node = shared_node()?;
    let name = ServiceName::new(service_name).map_err(|e| transport_error("service name", e))?;
    let service = node
        .service_builder(&name)
        .request_response::<[u8], [u8]>()
        .max_active_requests_per_client(1)
        .max_response_buffer_size(64)
        .open_or_create()
        .map_err(|e| transport_error("open service", e))?;
    let client = service
        .client_builder()
        .initial_max_slice_len(MAX_SLICE_LEN)
        .create()
        .map_err(|e| transport_error("client creation", e))?;

    let request = encode_request(method, payload);
    let request_len = request.len().max(1);
    let outbound = client
        .loan_slice_uninit(request_len)
        .map_err(|e| transport_error("loan request", e))?;
    let pending = outbound
        .write_from_fn(|i| request.get(i).copied().unwrap_or(0))
        .send()
        .map_err(|e| transport_error("send request", e))?;

    let (tx, rx) = unbounded_channel::<T, E>();

    std::thread::spawn(move || {
        // Keep node + service + client alive for the whole relay.
        let _keep_alive = (node, service, client);
        loop {
            match pending.receive() {
                Ok(Some(response)) => {
                    let decoded =
                        rkyv::from_bytes::<WireEvent<T, E>, rkyv::rancor::Error>(&response[..]);
                    match decoded {
                        Ok(wire) => {
                            let (event, follow_up) = normalize_wire_event(wire);
                            let terminal = event.is_terminal();
                            if tx.try_send_event(event).is_err() {
                                break;
                            }
                            if let Some(next) = follow_up {
                                let _ = tx.try_send_event(next);
                            }
                            if terminal {
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = tx.try_send_event(Event::Error(ObservableError::Technical(
                                transport_error("decode response", e),
                            )));
                            break;
                        }
                    }
                }
                Ok(None) => {
                    if !pending.is_connected() {
                        let _ = tx.try_send_event(Event::Complete);
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(POLL_MS));
                }
                Err(e) => {
                    let _ = tx.try_send_event(Event::Error(ObservableError::Technical(
                        transport_error("receive response", e),
                    )));
                    break;
                }
            }
        }
        drop(pending);
    });

    Ok(rx)
}
