//! Centralized communication hub of the ice-rpc node.
//!
//! Manages the outgoing publishers (one per target node), the incoming
//! request handlers (Provider mode) and the response handlers (Consumer mode).
//! The dispatch loop drains the `node_{pid}_small`/`node_{pid}_large`
//! subscribers and routes messages to the registered handlers.

use crate::types::{
    node_large_topic, node_notify_topic, node_small_topic, NodeId, RpcHeader,
    LARGE_PAYLOAD_THRESHOLD,
};
use iceoryx2::port::DegradationAction;
use iceoryx2::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, RwLock};

/// Publishers towards a target node, shared under `Arc<NodePublishersArc>`.
///
/// An iceoryx2 publisher does not support simultaneous loans from several
/// threads (ExceedsMaxLoans). The Mutex serializes loan+send.
type NodePublishersArc = Arc<NodePublishersInner>;
struct NodePublishersInner {
    small: Mutex<IpcPublisher>,
    large: Mutex<IpcPublisher>,
    notifier: iceoryx2::port::notifier::Notifier<iceoryx2::service::ipc_threadsafe::Service>,
}

/// Type of a request handler: called from the dispatch loop.
pub type RequestHandler = Arc<dyn Fn(RpcHeader, &[u8]) + Send + Sync + 'static>;
/// Type of a response handler: called from the dispatch loop.
pub type ResponseHandler = Arc<dyn Fn(Result<&[u8], crate::RpcError>) + Send + Sync + 'static>;

type IpcPublisher = iceoryx2::port::publisher::Publisher<
    iceoryx2::service::ipc_threadsafe::Service,
    [u8],
    RpcHeader,
>;

/// Centralized IPC communication hub of the node.
///
/// Manages the outgoing publishers (one per target node), the incoming
/// request handlers (Provider mode) and the response handlers (Consumer mode).
pub struct NodeHub {
    request_handlers: RwLock<HashMap<String, Vec<RequestHandler>>>,
    response_handlers: Mutex<HashMap<[u8; 16], ResponseHandler>>,
    dispatch_started: std::sync::atomic::AtomicBool,
    publishers: RwLock<HashMap<u32, NodePublishersArc>>,
    publishers_create_lock: Mutex<()>,
}

impl NodeHub {
    pub(crate) fn new() -> Self {
        Self {
            request_handlers: RwLock::new(HashMap::new()),
            response_handlers: Mutex::new(HashMap::new()),
            dispatch_started: std::sync::atomic::AtomicBool::new(false),
            publishers: RwLock::new(HashMap::new()),
            publishers_create_lock: Mutex::new(()),
        }
    }

    /// Registers a request handler for a given service.
    pub fn register_request_handler(&self, service_name: &str, handler: RequestHandler) {
        let mut map = self
            .request_handlers
            .write()
            .expect("request_handlers write lock poisoning");
        map.entry(service_name.to_string())
            .or_default()
            .push(handler);
        drop(map);
    }

    /// Returns the list of service names that have a registered handler.
    pub fn registered_services(&self) -> Vec<String> {
        self.request_handlers
            .read()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Registers a response handler for a given correlation_id.
    pub fn register_response_handler(&self, correlation_id: [u8; 16], handler: ResponseHandler) {
        self.response_handlers
            .lock()
            .expect("response_handlers lock poisoning")
            .insert(correlation_id, handler);
    }

    /// Removes the response handler associated with a correlation_id.
    pub fn remove_response_handler(&self, correlation_id: &[u8; 16]) {
        self.response_handlers
            .lock()
            .expect("response_handlers lock poisoning")
            .remove(correlation_id);
    }

    /// Checks whether publishers already exist for a target node.
    pub fn has_publishers(&self, target_node_id: NodeId) -> bool {
        self.publishers
            .read()
            .expect("publishers read lock poisoning")
            .contains_key(&target_node_id.0)
    }

    /// Blocking version of `ensure_publishers` for non-async contexts.
    pub fn ensure_publishers_blocking(
        &self,
        target_node_id: NodeId,
    ) -> Result<(), crate::RpcError> {
        self.ensure_publishers(target_node_id)
    }

    /// Sends an RPC message to a target node through the appropriate publisher.
    ///
    /// On failure, the publishers are invalidated and the node_down callbacks
    /// are fired.
    pub fn send_to_node(
        &self,
        target_node_id: NodeId,
        header: RpcHeader,
        payload: &[u8],
    ) -> Result<(), crate::RpcError> {
        let is_large = payload.len() > LARGE_PAYLOAD_THRESHOLD;

        let node_pubs = {
            let publishers = self
                .publishers
                .read()
                .expect("publishers read lock poisoning");
            publishers
                .get(&target_node_id.0)
                .ok_or_else(|| {
                    crate::RpcError::IpcError(
                        "publisher not found — ensure_publishers not called".into(),
                    )
                })?
                .clone()
        };

        let send_result = {
            let guard = if is_large {
                node_pubs
                    .large
                    .lock()
                    .expect("large publisher lock poisoning")
            } else {
                node_pubs
                    .small
                    .lock()
                    .expect("small publisher lock poisoning")
            };
            Self::do_send(&guard, header, payload)
        };
        if send_result.is_ok() {
            let _ = node_pubs
                .notifier
                .notify_with_custom_event_id(EventId::new(1));
        }

        if let Err(ref e) = send_result {
            log::error!(
                "Send failure to {}: {} -- invalidation + callbacks",
                target_node_id,
                e
            );
            self.invalidate_publishers(target_node_id);
            crate::reconnect::fire(target_node_id.0);
        }
        send_result
    }

    /// Invalidates the publishers of a target node (following a detected crash).
    pub fn invalidate_publishers(&self, target_node_id: NodeId) {
        self.publishers
            .write()
            .expect("publishers write lock poisoning")
            .remove(&target_node_id.0);
        log::warn!("Publishers invalidated for {}", target_node_id);
    }

    /// Performs the low-level send: `loan_slice_uninit` → write → send.
    #[inline]
    fn do_send(
        pub_guard: &std::sync::MutexGuard<'_, IpcPublisher>,
        header: RpcHeader,
        payload: &[u8],
    ) -> Result<(), crate::RpcError> {
        match pub_guard
            .loan_slice_uninit(payload.len())
            .map_err(|e| crate::RpcError::IpcError(format!("loan_slice_uninit: {:?}", e)))
        {
            Ok(mut sample) => {
                *sample.user_header_mut() = header;
                let sample = sample.write_from_slice(payload);
                sample
                    .send()
                    .map(|_| ())
                    .map_err(|e| crate::RpcError::IpcError(format!("send failed: {:?}", e)))
            }
            Err(e) => Err(e),
        }
    }

    /// Creates the publishers towards a target node if they do not exist yet
    /// (double-checked locking).
    fn ensure_publishers(&self, target_node_id: NodeId) -> Result<(), crate::RpcError> {
        if self
            .publishers
            .read()
            .expect("publishers read lock poisoning")
            .contains_key(&target_node_id.0)
        {
            return Ok(());
        }

        let _create_guard = self
            .publishers_create_lock
            .lock()
            .expect("publishers_create_lock poisoning");

        if self
            .publishers
            .read()
            .expect("publishers read lock poisoning")
            .contains_key(&target_node_id.0)
        {
            return Ok(());
        }

        let node = crate::ServiceLocator::global()
            .get_node_sync()
            .map_err(|e| crate::RpcError::IpcError(format!("get_node_sync: {}", e)))?;
        let np = Arc::new(Self::create_node_publishers(&node, target_node_id)?);

        self.publishers
            .write()
            .expect("publishers write lock poisoning")
            .insert(target_node_id.0, np);
        Ok(())
    }

    /// Creates the three publishers (small, large, notifier) for a target node.
    fn create_node_publishers(
        node: &Arc<iceoryx2::node::Node<iceoryx2::service::ipc_threadsafe::Service>>,
        target_node_id: NodeId,
    ) -> Result<NodePublishersInner, crate::RpcError> {
        let small_pub = Self::create_pubsub_publisher(
            node,
            &node_small_topic(target_node_id),
            crate::PUBLISHER_INITIAL_MAX_SLICE_LEN,
        )?;
        let large_pub = Self::create_pubsub_publisher(
            node,
            &node_large_topic(target_node_id),
            crate::PUBLISHER_LARGE_MAX_SLICE_LEN,
        )?;
        let notifier = Self::create_notifier(node, &node_notify_topic(target_node_id))?;
        Ok(NodePublishersInner {
            small: Mutex::new(small_pub),
            large: Mutex::new(large_pub),
            notifier,
        })
    }

    /// Creates an iceoryx2 pub-sub publisher on a given topic.
    fn create_pubsub_publisher(
        node: &Arc<iceoryx2::node::Node<iceoryx2::service::ipc_threadsafe::Service>>,
        topic: &str,
        initial_max_slice_len: usize,
    ) -> Result<IpcPublisher, crate::RpcError> {
        let name = ServiceName::new(topic)
            .map_err(|e| crate::RpcError::IpcError(format!("ServiceName({topic}): {e:?}")))?;

        let is_large_topic = topic.ends_with("_large");
        let subscriber_buffer = if is_large_topic {
            crate::LARGE_TOPIC_BUFFER_SIZE
        } else {
            crate::SMALL_TOPIC_BUFFER_SIZE
        };

        let svc = node
            .service_builder(&name)
            .publish_subscribe::<[u8]>()
            .user_header::<RpcHeader>()
            .subscriber_max_buffer_size(subscriber_buffer)
            .max_publishers(16)
            .open_or_create()
            .map_err(|e| crate::RpcError::IpcError(format!("open_or_create({topic}): {e:?}")))?;
        svc.publisher_builder()
            .initial_max_slice_len(initial_max_slice_len)
            .allocation_strategy(AllocationStrategy::PowerOfTwo)
            .set_degradation_handler(|_, _| DegradationAction::DegradeAndFail)
            .create()
            .map_err(|e| crate::RpcError::IpcError(format!("publisher create({topic}): {e:?}")))
    }

    /// Creates an iceoryx2 notifier on a given topic.
    fn create_notifier(
        node: &Arc<iceoryx2::node::Node<iceoryx2::service::ipc_threadsafe::Service>>,
        topic: &str,
    ) -> Result<
        iceoryx2::port::notifier::Notifier<iceoryx2::service::ipc_threadsafe::Service>,
        crate::RpcError,
    > {
        let name = ServiceName::new(topic)
            .map_err(|e| crate::RpcError::IpcError(format!("ServiceName({topic}): {e:?}")))?;
        let svc = node
            .service_builder(&name)
            .event()
            .open_or_create()
            .map_err(|e| crate::RpcError::IpcError(format!("open_or_create({topic}): {e:?}")))?;
        svc.notifier_builder()
            .create()
            .map_err(|e| crate::RpcError::IpcError(format!("notifier create({topic}): {e:?}")))
    }

    /// Starts the dispatch loop (IPC message pump) in a `spawn_blocking`.
    ///
    /// Idempotent via [`dispatch_started`]. Receives requests and responses
    /// on the `node_{pid}_small`/`node_{pid}_large` topics and dispatches them
    /// to the registered handlers.
    pub(crate) fn start_dispatch_loop(&self) {
        if self
            .dispatch_started
            .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }

        let node = match crate::ServiceLocator::global().get_node_sync() {
            Ok(n) => n,
            Err(e) => {
                log::error!("get_node_sync failed: {}", e);
                return;
            }
        };

        let local_pid = std::process::id();
        let small_topic = node_small_topic(NodeId(local_pid));
        let large_topic = node_large_topic(NodeId(local_pid));
        let notify_topic = node_notify_topic(NodeId(local_pid));

        let small_sub = Self::open_subscriber(&node, &small_topic);
        let large_sub = Self::open_subscriber(&node, &large_topic);
        log::debug!(
            "Subscribers: small={} large={}",
            small_sub.is_some(),
            large_sub.is_some()
        );

        let listener = match ServiceName::new(&notify_topic)
            .ok()
            .and_then(|n| node.service_builder(&n).event().open_or_create().ok())
            .and_then(|s| s.listener_builder().create().ok())
        {
            Some(l) => l,
            None => {
                log::error!("Failed to create listener on {}", notify_topic);
                return;
            }
        };

        let cancel = crate::global_cancel_token().clone();

        let handle = crate::rt::spawn_blocking(move || {
            log::info!(
                "Dispatch loop started (pid={}, topics: {}, {})",
                local_pid,
                small_topic,
                large_topic
            );

            let wait_set = WaitSetBuilder::new()
                .create::<iceoryx2::service::ipc_threadsafe::Service>()
                .expect("WaitSetBuilder creation failed");
            let _guard = wait_set
                .attach_notification(&listener)
                .expect("attach_notification failed");

            loop {
                if cancel.is_cancelled() {
                    break;
                }

                let result = wait_set.wait_and_process_once_with_timeout(
                    |_| {
                        while let Ok(Some(_)) = listener.try_wait_one() {}
                        Self::drain_subscriber(&small_sub, &cancel);
                        Self::drain_subscriber(&large_sub, &cancel);
                        CallbackProgression::Continue
                    },
                    std::time::Duration::from_micros(crate::WAITSET_TIMEOUT_US),
                );

                loop {
                    let s = Self::drain_subscriber(&small_sub, &cancel);
                    let l = Self::drain_subscriber(&large_sub, &cancel);
                    if !s && !l {
                        break;
                    }
                    if cancel.is_cancelled() {
                        break;
                    }
                }

                if let Err(_) | Ok(iceoryx2::waitset::WaitSetRunResult::TerminationRequest) = result
                {
                    crate::global_cancel_token().cancel();
                    break;
                }
            }

            log::info!("Dispatch loop stopped.");
        });

        crate::ServiceLocator::global().register_shutdown_handle(handle);
    }

    /// Opens an iceoryx2 subscriber on a given topic.
    fn open_subscriber(
        node: &Arc<iceoryx2::node::Node<iceoryx2::service::ipc_threadsafe::Service>>,
        topic: &str,
    ) -> Option<
        iceoryx2::port::subscriber::Subscriber<
            iceoryx2::service::ipc_threadsafe::Service,
            [u8],
            RpcHeader,
        >,
    > {
        let name = ServiceName::new(topic).ok()?;
        let is_large_topic = topic.ends_with("_large");
        let subscriber_buffer = if is_large_topic {
            crate::LARGE_TOPIC_BUFFER_SIZE
        } else {
            crate::SMALL_TOPIC_BUFFER_SIZE
        };
        let svc = node
            .service_builder(&name)
            .publish_subscribe::<[u8]>()
            .user_header::<RpcHeader>()
            .subscriber_max_buffer_size(subscriber_buffer)
            .max_publishers(16)
            .open_or_create()
            .ok()?;
        svc.subscriber_builder()
            .buffer_size(subscriber_buffer)
            .set_degradation_handler(|_, _| DegradationAction::Ignore)
            .create()
            .ok()
    }

    /// Drains a subscriber and returns `true` if at least one sample was processed.
    ///
    /// The hub and the request_handlers snapshot are resolved only once
    /// outside the loop. The handlers never change after init.
    fn drain_subscriber(
        sub: &Option<
            iceoryx2::port::subscriber::Subscriber<
                iceoryx2::service::ipc_threadsafe::Service,
                [u8],
                RpcHeader,
            >,
        >,
        cancel: &crate::rt::CancellationToken,
    ) -> bool {
        let Some(sub) = sub else {
            return false;
        };
        let hub = crate::ServiceLocator::global().hub();
        let req_handlers_snapshot = hub
            .request_handlers
            .read()
            .expect("request_handlers read lock poisoning")
            .clone();
        let mut had_work = false;

        while let Ok(Some(sample)) = sub.receive() {
            if cancel.is_cancelled() {
                break;
            }
            had_work = true;
            let hdr = *sample.user_header();
            let payload = sample.payload();
            let svc = hdr.service();

            if hdr.is_request() {
                if let Some(handlers) = req_handlers_snapshot.get(svc) {
                    for handler in handlers {
                        handler(hdr, payload);
                    }
                }
            } else {
                let cid = hdr.correlation_id;
                let terminal = hdr.event_kind.is_terminal();
                let resp_guard = hub
                    .response_handlers
                    .lock()
                    .expect("response_handlers lock poisoning");
                if let Some(handler) = resp_guard.get(&cid).cloned() {
                    drop(resp_guard);
                    handler(Ok(payload));
                    if terminal {
                        hub.response_handlers
                            .lock()
                            .expect("response_handlers lock poisoning")
                            .remove(&cid);
                    }
                }
            }
        }
        had_work
    }
}

impl crate::ServiceLocator {
    /// Returns the global [`NodeHub`] (singleton).
    pub fn hub(&self) -> &NodeHub {
        static HUB: OnceLock<NodeHub> = OnceLock::new();
        HUB.get_or_init(NodeHub::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_hub() -> NodeHub {
        NodeHub::new()
    }

    #[test]
    fn register_request_handlers_and_list_services() {
        let hub = new_hub();
        let handler: RequestHandler = Arc::new(|_, _| {});
        hub.register_request_handler("DatabaseService", handler.clone());
        hub.register_request_handler("ConfigService", handler);

        let mut services = hub.registered_services();
        services.sort();
        assert_eq!(
            services,
            vec!["ConfigService".to_string(), "DatabaseService".to_string()]
        );
    }

    #[test]
    fn response_handler_register_and_remove_are_noop_safe() {
        let hub = new_hub();
        let cid = [1u8; 16];
        let handler: ResponseHandler = Arc::new(|_| {});
        hub.register_response_handler(cid, handler);
        hub.remove_response_handler(&cid);
        // Removing an unknown correlation id must not panic.
        hub.remove_response_handler(&cid);
    }

    #[test]
    fn publishers_start_empty_and_invalidate_is_noop() {
        let hub = new_hub();
        let node = NodeId(0x1234_5678);
        assert!(!hub.has_publishers(node));
        hub.invalidate_publishers(node);
        assert!(!hub.has_publishers(node));
    }
}
