#[allow(unexpected_cfgs)]
#[async_trait::async_trait]
pub trait DatabaseApi: Send + Sync + 'static {
    async fn add(&self, a: i32, b: i32) -> Observable<i32, String>;
    async fn get(&self, key: String) -> Observable<String, String>;
    async fn put(&self, key: String, value: Vec<u8>) -> Observable<(), String>;
}
#[allow(missing_docs)]
#[repr(u8)]
#[derive(
    ice_rpc::codegen::rkyv::Archive,
    ice_rpc::codegen::rkyv::Deserialize,
    ice_rpc::codegen::rkyv::Serialize,
    Debug
)]
#[rkyv(crate = ice_rpc::codegen::rkyv)]
pub enum DatabaseApiRequest {
    Add { a: i32, b: i32 } = 0u8,
    Get { key: String } = 1u8,
    Put { key: String, value: Vec<u8> } = 2u8,
}
#[allow(missing_docs)]
impl DatabaseApiProxy {
    /// Identity of this service contract: its id inside the channel and
    /// its interface version, declared once and shared by the generated
    /// client and provider so the version cannot be lost between them.
    pub const SERVICE: ice_rpc::codegen::ServiceRef = ice_rpc::codegen::ServiceRef::new(
        ice_rpc::codegen::service_id_of("Database"),
        2u16,
    );
}
#[allow(missing_docs)]
pub struct DatabaseApiClient;
#[allow(missing_docs)]
impl DatabaseApiClient {
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self)
    }
    pub async fn add(&self, a: i32, b: i32) -> ice_rpc::Observable<i32, String> {
        let req_val = DatabaseApiRequest::Add { a, b };
        ice_rpc::codegen::serialize_and_call::<
            i32,
            String,
            _,
        >(
                "db",
                <DatabaseApiProxy>::SERVICE,
                ice_rpc::codegen::method_id_of("add"),
                &req_val,
            )
            .unwrap_or_else(ice_rpc::Observable::from_technical_error)
    }
    pub async fn get(&self, key: String) -> ice_rpc::Observable<String, String> {
        let req_val = DatabaseApiRequest::Get { key };
        ice_rpc::codegen::serialize_and_call::<
            String,
            String,
            _,
        >(
                "db",
                <DatabaseApiProxy>::SERVICE,
                ice_rpc::codegen::method_id_of("get"),
                &req_val,
            )
            .unwrap_or_else(ice_rpc::Observable::from_technical_error)
    }
    pub async fn put(
        &self,
        key: String,
        value: Vec<u8>,
    ) -> ice_rpc::Observable<(), String> {
        let req_val = DatabaseApiRequest::Put {
            key,
            value,
        };
        ice_rpc::codegen::serialize_and_call::<
            (),
            String,
            _,
        >(
                "db",
                <DatabaseApiProxy>::SERVICE,
                ice_rpc::codegen::method_id_of("put"),
                &req_val,
            )
            .unwrap_or_else(ice_rpc::Observable::from_technical_error)
    }
}
#[allow(missing_docs)]
#[ice_rpc::codegen::async_trait::async_trait]
impl ice_rpc::codegen::ServiceLifecycle for DatabaseApiClient {
    async fn init(&self) -> bool {
        true
    }
}
#[allow(missing_docs)]
#[derive(Clone)]
pub struct DatabaseApiServer {
    service_impl: std::sync::Arc<dyn DatabaseApi>,
}
#[allow(missing_docs)]
impl DatabaseApiServer {
    fn new(service_impl: std::sync::Arc<dyn DatabaseApi>) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self { service_impl })
    }
    /// Builds the native `request_response` dispatcher of this service.
    ///
    /// Each RPC method is registered with its own handler: it decodes
    /// the rkyv request enum from the payload, invokes the local
    /// implementation, and streams the resulting `Observable` through
    /// `observable_to_responses`.
    ///
    /// Each handler returns a **task**, which the transport polls once on
    /// the channel's thread before detaching it: a handler that answers
    /// without yielding runs on that thread, one that `await`s runs as a
    /// task and cannot hold back the next request.
    fn native_dispatcher(
        self: std::sync::Arc<Self>,
    ) -> ice_rpc::codegen::ServiceDispatcher {
        let mut dispatcher = ice_rpc::codegen::ServiceDispatcher::new(
            <DatabaseApiProxy>::SERVICE,
        );
        {
            let service_impl = self.service_impl.clone();
            dispatcher
                .method(
                    ice_rpc::codegen::method_id_of("add"),
                    move |
                        header: ice_rpc::codegen::RpcHeader,
                        payload: &[u8],
                        emitter: ice_rpc::codegen::OwnedEmitter,
                    | -> ice_rpc::codegen::BoxResponseFuture {
                        let ctx = ice_rpc::codegen::CallContext::new(
                            &header,
                            <DatabaseApiProxy>::SERVICE_NAME,
                            "add",
                        );
                        let impl_ref = service_impl.clone();
                        let request = match ice_rpc::codegen::decode_aligned::<
                            DatabaseApiRequest,
                        >(payload) {
                            Ok(DatabaseApiRequest::Add { a, b }) => (a, b),
                            Err(e) => {
                                ice_rpc::codegen::log::error!(
                                    "[{}::{}] request payload decoding failed: {:?}", <
                                    DatabaseApiProxy > ::SERVICE_NAME, "add", e
                                );
                                let mut emitter = emitter;
                                let _ = ice_rpc::codegen::emit_rpc_error(
                                    ice_rpc::codegen::RpcError::SerializationError,
                                    &mut *emitter,
                                );
                                return Box::pin(async {});
                            }
                            Ok(_) => {
                                ice_rpc::codegen::log::error!(
                                    "[{}::{}] request payload is another method's variant", <
                                    DatabaseApiProxy > ::SERVICE_NAME, "add"
                                );
                                let mut emitter = emitter;
                                let _ = ice_rpc::codegen::emit_rpc_error(
                                    ice_rpc::codegen::RpcError::SerializationError,
                                    &mut *emitter,
                                );
                                return Box::pin(async {});
                            }
                        };
                        ice_rpc::codegen::call_scoped(
                            ctx,
                            async move {
                                let mut emitter = emitter;
                                let (a, b) = request;
                                let stream = impl_ref.add(a, b).await;
                                ice_rpc::codegen::observable_to_responses(
                                        stream,
                                        &mut *emitter,
                                    )
                                    .await;
                            },
                        )
                    },
                );
        }
        {
            let service_impl = self.service_impl.clone();
            dispatcher
                .method(
                    ice_rpc::codegen::method_id_of("get"),
                    move |
                        header: ice_rpc::codegen::RpcHeader,
                        payload: &[u8],
                        emitter: ice_rpc::codegen::OwnedEmitter,
                    | -> ice_rpc::codegen::BoxResponseFuture {
                        let ctx = ice_rpc::codegen::CallContext::new(
                            &header,
                            <DatabaseApiProxy>::SERVICE_NAME,
                            "get",
                        );
                        let impl_ref = service_impl.clone();
                        let request = match ice_rpc::codegen::decode_aligned::<
                            DatabaseApiRequest,
                        >(payload) {
                            Ok(DatabaseApiRequest::Get { key }) => (key),
                            Err(e) => {
                                ice_rpc::codegen::log::error!(
                                    "[{}::{}] request payload decoding failed: {:?}", <
                                    DatabaseApiProxy > ::SERVICE_NAME, "get", e
                                );
                                let mut emitter = emitter;
                                let _ = ice_rpc::codegen::emit_rpc_error(
                                    ice_rpc::codegen::RpcError::SerializationError,
                                    &mut *emitter,
                                );
                                return Box::pin(async {});
                            }
                            Ok(_) => {
                                ice_rpc::codegen::log::error!(
                                    "[{}::{}] request payload is another method's variant", <
                                    DatabaseApiProxy > ::SERVICE_NAME, "get"
                                );
                                let mut emitter = emitter;
                                let _ = ice_rpc::codegen::emit_rpc_error(
                                    ice_rpc::codegen::RpcError::SerializationError,
                                    &mut *emitter,
                                );
                                return Box::pin(async {});
                            }
                        };
                        ice_rpc::codegen::call_scoped(
                            ctx,
                            async move {
                                let mut emitter = emitter;
                                let (key) = request;
                                let stream = impl_ref.get(key).await;
                                ice_rpc::codegen::observable_to_responses(
                                        stream,
                                        &mut *emitter,
                                    )
                                    .await;
                            },
                        )
                    },
                );
        }
        {
            let service_impl = self.service_impl.clone();
            dispatcher
                .method(
                    ice_rpc::codegen::method_id_of("put"),
                    move |
                        header: ice_rpc::codegen::RpcHeader,
                        payload: &[u8],
                        emitter: ice_rpc::codegen::OwnedEmitter,
                    | -> ice_rpc::codegen::BoxResponseFuture {
                        let ctx = ice_rpc::codegen::CallContext::new(
                            &header,
                            <DatabaseApiProxy>::SERVICE_NAME,
                            "put",
                        );
                        let impl_ref = service_impl.clone();
                        let request = match ice_rpc::codegen::decode_aligned::<
                            DatabaseApiRequest,
                        >(payload) {
                            Ok(DatabaseApiRequest::Put { key, value }) => (key, value),
                            Err(e) => {
                                ice_rpc::codegen::log::error!(
                                    "[{}::{}] request payload decoding failed: {:?}", <
                                    DatabaseApiProxy > ::SERVICE_NAME, "put", e
                                );
                                let mut emitter = emitter;
                                let _ = ice_rpc::codegen::emit_rpc_error(
                                    ice_rpc::codegen::RpcError::SerializationError,
                                    &mut *emitter,
                                );
                                return Box::pin(async {});
                            }
                            Ok(_) => {
                                ice_rpc::codegen::log::error!(
                                    "[{}::{}] request payload is another method's variant", <
                                    DatabaseApiProxy > ::SERVICE_NAME, "put"
                                );
                                let mut emitter = emitter;
                                let _ = ice_rpc::codegen::emit_rpc_error(
                                    ice_rpc::codegen::RpcError::SerializationError,
                                    &mut *emitter,
                                );
                                return Box::pin(async {});
                            }
                        };
                        ice_rpc::codegen::call_scoped(
                            ctx,
                            async move {
                                let mut emitter = emitter;
                                let (key, value) = request;
                                let stream = impl_ref.put(key, value).await;
                                ice_rpc::codegen::observable_to_responses(
                                        stream,
                                        &mut *emitter,
                                    )
                                    .await;
                            },
                        )
                    },
                );
        }
        dispatcher
    }
}
#[allow(missing_docs)]
struct __DatabaseApiServiceInitDefault(std::sync::Arc<dyn DatabaseApi>);
#[allow(missing_docs)]
#[ice_rpc::codegen::async_trait::async_trait]
impl ice_rpc::ServiceInit for __DatabaseApiServiceInitDefault {}
#[allow(missing_docs)]
#[allow(dead_code)]
pub enum DatabaseApiMode {
    Provider {
        local_impl: std::sync::Arc<dyn DatabaseApi>,
        init_hook: std::sync::Arc<dyn ice_rpc::ServiceInit>,
        server_started: bool,
    },
    Consumer { ipc_client: std::sync::Arc<DatabaseApiClient> },
    ProviderJson,
}
#[allow(missing_docs)]
pub struct DatabaseApiProxy {
    mode: ice_rpc::codegen::async_lock::RwLock<DatabaseApiMode>,
    deps: Vec<&'static str>,
}
#[allow(missing_docs)]
#[allow(dead_code)]
impl DatabaseApiProxy {
    /// Logical name of the service, injected by the `#[service]` macro.
    pub const SERVICE_NAME: &'static str = "Database";
    pub fn provide<T>(implementation: T) -> std::sync::Arc<Self>
    where
        T: DatabaseApi + Send + Sync + 'static,
    {
        let arc = std::sync::Arc::new(implementation);
        let init_hook = std::sync::Arc::new(
            __DatabaseApiServiceInitDefault(
                arc.clone() as std::sync::Arc<dyn DatabaseApi>,
            ),
        );
        std::sync::Arc::new(Self {
            deps: vec![],
            mode: ice_rpc::codegen::async_lock::RwLock::new(DatabaseApiMode::Provider {
                local_impl: arc as std::sync::Arc<dyn DatabaseApi>,
                init_hook: init_hook as std::sync::Arc<dyn ice_rpc::ServiceInit>,
                server_started: false,
            }),
        })
    }
    pub fn provide_with_init<T>(implementation: T) -> std::sync::Arc<Self>
    where
        T: DatabaseApi + ice_rpc::ServiceInit + Send + Sync + 'static,
    {
        let arc = std::sync::Arc::new(implementation);
        let deps = arc.dependencies();
        std::sync::Arc::new(Self {
            deps,
            mode: ice_rpc::codegen::async_lock::RwLock::new(DatabaseApiMode::Provider {
                local_impl: arc.clone() as std::sync::Arc<dyn DatabaseApi>,
                init_hook: arc as std::sync::Arc<dyn ice_rpc::ServiceInit>,
                server_started: false,
            }),
        })
    }
    pub fn consume() -> std::sync::Arc<Self> {
        ice_rpc::codegen::declare_channel_max_slice_len("db", 4096usize);
        std::sync::Arc::new(Self {
            deps: vec![],
            mode: ice_rpc::codegen::async_lock::RwLock::new(DatabaseApiMode::Consumer {
                ipc_client: DatabaseApiClient::new(),
            }),
        })
    }
    /// Builds the proxy of the `ProviderJson` mode: the Node.js host
    /// implements the methods, and each call is bridged to it over IPC.
    pub fn provide_json() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            deps: vec![],
            mode: ice_rpc::codegen::async_lock::RwLock::new(
                DatabaseApiMode::ProviderJson,
            ),
        })
    }
}
#[allow(missing_docs)]
#[ice_rpc::codegen::async_trait::async_trait]
impl DatabaseApi for DatabaseApiProxy {
    async fn add(&self, a: i32, b: i32) -> Observable<i32, String> {
        let mode = self.mode.read().await;
        match &*mode {
            DatabaseApiMode::Provider { local_impl, .. } => {
                ice_rpc::codegen::local_call_scoped(
                        <DatabaseApiProxy>::SERVICE,
                        <DatabaseApiProxy>::SERVICE_NAME,
                        "add",
                        local_impl.add(a, b),
                    )
                    .await
            }
            DatabaseApiMode::Consumer { ipc_client } => ipc_client.add(a, b).await,
            DatabaseApiMode::ProviderJson => {
                ice_rpc::Observable::from_technical_error(
                    ice_rpc::RpcError::Internal(
                        "ProviderJson: direct calls are not supported — use IPC".into(),
                    ),
                )
            }
        }
    }
    async fn get(&self, key: String) -> Observable<String, String> {
        let mode = self.mode.read().await;
        match &*mode {
            DatabaseApiMode::Provider { local_impl, .. } => {
                ice_rpc::codegen::local_call_scoped(
                        <DatabaseApiProxy>::SERVICE,
                        <DatabaseApiProxy>::SERVICE_NAME,
                        "get",
                        local_impl.get(key),
                    )
                    .await
            }
            DatabaseApiMode::Consumer { ipc_client } => ipc_client.get(key).await,
            DatabaseApiMode::ProviderJson => {
                ice_rpc::Observable::from_technical_error(
                    ice_rpc::RpcError::Internal(
                        "ProviderJson: direct calls are not supported — use IPC".into(),
                    ),
                )
            }
        }
    }
    async fn put(&self, key: String, value: Vec<u8>) -> Observable<(), String> {
        let mode = self.mode.read().await;
        match &*mode {
            DatabaseApiMode::Provider { local_impl, .. } => {
                ice_rpc::codegen::local_call_scoped(
                        <DatabaseApiProxy>::SERVICE,
                        <DatabaseApiProxy>::SERVICE_NAME,
                        "put",
                        local_impl.put(key, value),
                    )
                    .await
            }
            DatabaseApiMode::Consumer { ipc_client } => ipc_client.put(key, value).await,
            DatabaseApiMode::ProviderJson => {
                ice_rpc::Observable::from_technical_error(
                    ice_rpc::RpcError::Internal(
                        "ProviderJson: direct calls are not supported — use IPC".into(),
                    ),
                )
            }
        }
    }
}
#[allow(missing_docs)]
impl ice_rpc::codegen::ServiceConsumer for DatabaseApiProxy {
    fn consume_proxy() -> std::sync::Arc<Self> {
        DatabaseApiProxy::consume()
    }
}
#[allow(missing_docs)]
#[ice_rpc::codegen::async_trait::async_trait]
impl ice_rpc::codegen::ServiceLifecycle for DatabaseApiProxy {
    async fn init(&self) -> bool {
        let mut mode = self.mode.write().await;
        match &mut *mode {
            DatabaseApiMode::ProviderJson => {
                let mut dispatcher = ice_rpc::codegen::ServiceDispatcher::new(
                    <DatabaseApiProxy>::SERVICE,
                );
                {
                    dispatcher
                        .method(
                            ice_rpc::codegen::method_id_of("add"),
                            move |
                                header: ice_rpc::codegen::RpcHeader,
                                payload: &[u8],
                                emitter: ice_rpc::codegen::OwnedEmitter,
                            | -> ice_rpc::codegen::BoxResponseFuture {
                                let ctx = ice_rpc::codegen::CallContext::new(
                                    &header,
                                    <DatabaseApiProxy>::SERVICE_NAME,
                                    "add",
                                );
                                let args = DatabaseApiProxy::deserialize_request_to_value(
                                    "add",
                                    payload,
                                );
                                ice_rpc::codegen::call_scoped(
                                    ctx,
                                    async move {
                                        let mut emitter = emitter;
                                        let Some(args) = args else {
                                            ice_rpc::codegen::log::error!(
                                                "[{}::{}] Failed to deserialize the request", <
                                                DatabaseApiProxy > ::SERVICE_NAME, "add"
                                            );
                                            let _ = ice_rpc::codegen::emit_rpc_error(
                                                ice_rpc::codegen::RpcError::SerializationError,
                                                &mut *emitter,
                                            );
                                            return;
                                        };
                                        let mut events = match ice_rpc::codegen::dispatch_json(
                                                header.correlation_id,
                                                <DatabaseApiProxy>::SERVICE_NAME,
                                                "add",
                                                args,
                                            )
                                            .await
                                        {
                                            Ok(events) => events,
                                            Err(e) => {
                                                ice_rpc::codegen::log::error!(
                                                    "[{}::{}] JSON dispatch failed: {}", < DatabaseApiProxy >
                                                    ::SERVICE_NAME, "add", e
                                                );
                                                let _ = ice_rpc::codegen::emit_rpc_error(
                                                    ice_rpc::codegen::RpcError::Internal(e),
                                                    &mut *emitter,
                                                );
                                                return;
                                            }
                                        };
                                        while let Some(event) = events.next().await {
                                            let value = match event {
                                                ice_rpc::codegen::JsonCallEvent::Event(value) => value,
                                                ice_rpc::codegen::JsonCallEvent::Failed(message) => {
                                                    ice_rpc::codegen::log::error!(
                                                        "[{}::{}] JSON call failed: {}", < DatabaseApiProxy >
                                                        ::SERVICE_NAME, "add", message
                                                    );
                                                    let _ = ice_rpc::codegen::emit_rpc_error(
                                                        ice_rpc::codegen::RpcError::Internal(message),
                                                        &mut *emitter,
                                                    );
                                                    return;
                                                }
                                            };
                                            match DatabaseApiProxy::serialize_response_from_value(
                                                "add",
                                                value,
                                            ) {
                                                Some((kind, sample)) => {
                                                    emitter.emit(kind, &sample);
                                                }
                                                None => {
                                                    ice_rpc::codegen::log::error!(
                                                        "[{}::{}] Failed to serialize the JSON response", <
                                                        DatabaseApiProxy > ::SERVICE_NAME, "add"
                                                    );
                                                    let _ = ice_rpc::codegen::emit_rpc_error(
                                                        ice_rpc::codegen::RpcError::SerializationError,
                                                        &mut *emitter,
                                                    );
                                                    return;
                                                }
                                            }
                                        }
                                    },
                                )
                            },
                        );
                }
                {
                    dispatcher
                        .method(
                            ice_rpc::codegen::method_id_of("get"),
                            move |
                                header: ice_rpc::codegen::RpcHeader,
                                payload: &[u8],
                                emitter: ice_rpc::codegen::OwnedEmitter,
                            | -> ice_rpc::codegen::BoxResponseFuture {
                                let ctx = ice_rpc::codegen::CallContext::new(
                                    &header,
                                    <DatabaseApiProxy>::SERVICE_NAME,
                                    "get",
                                );
                                let args = DatabaseApiProxy::deserialize_request_to_value(
                                    "get",
                                    payload,
                                );
                                ice_rpc::codegen::call_scoped(
                                    ctx,
                                    async move {
                                        let mut emitter = emitter;
                                        let Some(args) = args else {
                                            ice_rpc::codegen::log::error!(
                                                "[{}::{}] Failed to deserialize the request", <
                                                DatabaseApiProxy > ::SERVICE_NAME, "get"
                                            );
                                            let _ = ice_rpc::codegen::emit_rpc_error(
                                                ice_rpc::codegen::RpcError::SerializationError,
                                                &mut *emitter,
                                            );
                                            return;
                                        };
                                        let mut events = match ice_rpc::codegen::dispatch_json(
                                                header.correlation_id,
                                                <DatabaseApiProxy>::SERVICE_NAME,
                                                "get",
                                                args,
                                            )
                                            .await
                                        {
                                            Ok(events) => events,
                                            Err(e) => {
                                                ice_rpc::codegen::log::error!(
                                                    "[{}::{}] JSON dispatch failed: {}", < DatabaseApiProxy >
                                                    ::SERVICE_NAME, "get", e
                                                );
                                                let _ = ice_rpc::codegen::emit_rpc_error(
                                                    ice_rpc::codegen::RpcError::Internal(e),
                                                    &mut *emitter,
                                                );
                                                return;
                                            }
                                        };
                                        while let Some(event) = events.next().await {
                                            let value = match event {
                                                ice_rpc::codegen::JsonCallEvent::Event(value) => value,
                                                ice_rpc::codegen::JsonCallEvent::Failed(message) => {
                                                    ice_rpc::codegen::log::error!(
                                                        "[{}::{}] JSON call failed: {}", < DatabaseApiProxy >
                                                        ::SERVICE_NAME, "get", message
                                                    );
                                                    let _ = ice_rpc::codegen::emit_rpc_error(
                                                        ice_rpc::codegen::RpcError::Internal(message),
                                                        &mut *emitter,
                                                    );
                                                    return;
                                                }
                                            };
                                            match DatabaseApiProxy::serialize_response_from_value(
                                                "get",
                                                value,
                                            ) {
                                                Some((kind, sample)) => {
                                                    emitter.emit(kind, &sample);
                                                }
                                                None => {
                                                    ice_rpc::codegen::log::error!(
                                                        "[{}::{}] Failed to serialize the JSON response", <
                                                        DatabaseApiProxy > ::SERVICE_NAME, "get"
                                                    );
                                                    let _ = ice_rpc::codegen::emit_rpc_error(
                                                        ice_rpc::codegen::RpcError::SerializationError,
                                                        &mut *emitter,
                                                    );
                                                    return;
                                                }
                                            }
                                        }
                                    },
                                )
                            },
                        );
                }
                {
                    dispatcher
                        .method(
                            ice_rpc::codegen::method_id_of("put"),
                            move |
                                header: ice_rpc::codegen::RpcHeader,
                                payload: &[u8],
                                emitter: ice_rpc::codegen::OwnedEmitter,
                            | -> ice_rpc::codegen::BoxResponseFuture {
                                let ctx = ice_rpc::codegen::CallContext::new(
                                    &header,
                                    <DatabaseApiProxy>::SERVICE_NAME,
                                    "put",
                                );
                                let args = DatabaseApiProxy::deserialize_request_to_value(
                                    "put",
                                    payload,
                                );
                                ice_rpc::codegen::call_scoped(
                                    ctx,
                                    async move {
                                        let mut emitter = emitter;
                                        let Some(args) = args else {
                                            ice_rpc::codegen::log::error!(
                                                "[{}::{}] Failed to deserialize the request", <
                                                DatabaseApiProxy > ::SERVICE_NAME, "put"
                                            );
                                            let _ = ice_rpc::codegen::emit_rpc_error(
                                                ice_rpc::codegen::RpcError::SerializationError,
                                                &mut *emitter,
                                            );
                                            return;
                                        };
                                        let mut events = match ice_rpc::codegen::dispatch_json(
                                                header.correlation_id,
                                                <DatabaseApiProxy>::SERVICE_NAME,
                                                "put",
                                                args,
                                            )
                                            .await
                                        {
                                            Ok(events) => events,
                                            Err(e) => {
                                                ice_rpc::codegen::log::error!(
                                                    "[{}::{}] JSON dispatch failed: {}", < DatabaseApiProxy >
                                                    ::SERVICE_NAME, "put", e
                                                );
                                                let _ = ice_rpc::codegen::emit_rpc_error(
                                                    ice_rpc::codegen::RpcError::Internal(e),
                                                    &mut *emitter,
                                                );
                                                return;
                                            }
                                        };
                                        while let Some(event) = events.next().await {
                                            let value = match event {
                                                ice_rpc::codegen::JsonCallEvent::Event(value) => value,
                                                ice_rpc::codegen::JsonCallEvent::Failed(message) => {
                                                    ice_rpc::codegen::log::error!(
                                                        "[{}::{}] JSON call failed: {}", < DatabaseApiProxy >
                                                        ::SERVICE_NAME, "put", message
                                                    );
                                                    let _ = ice_rpc::codegen::emit_rpc_error(
                                                        ice_rpc::codegen::RpcError::Internal(message),
                                                        &mut *emitter,
                                                    );
                                                    return;
                                                }
                                            };
                                            match DatabaseApiProxy::serialize_response_from_value(
                                                "put",
                                                value,
                                            ) {
                                                Some((kind, sample)) => {
                                                    emitter.emit(kind, &sample);
                                                }
                                                None => {
                                                    ice_rpc::codegen::log::error!(
                                                        "[{}::{}] Failed to serialize the JSON response", <
                                                        DatabaseApiProxy > ::SERVICE_NAME, "put"
                                                    );
                                                    let _ = ice_rpc::codegen::emit_rpc_error(
                                                        ice_rpc::codegen::RpcError::SerializationError,
                                                        &mut *emitter,
                                                    );
                                                    return;
                                                }
                                            }
                                        }
                                    },
                                )
                            },
                        );
                }
                ice_rpc::codegen::declare_channel_max_slice_len("db", 4096usize);
                if let Err(e) = ice_rpc::codegen::register_native_service(
                    "db",
                    "Database",
                    dispatcher,
                ) {
                    ice_rpc::codegen::log::error!(
                        "[{}] channel registration failed: {e:?}", "Database"
                    );
                    return false;
                }
                ice_rpc::codegen::log::info!(
                    "[{}] JSON provider registered on channel '{}'.", "Database", "db"
                );
                true
            }
            DatabaseApiMode::Provider { local_impl, init_hook, server_started } => {
                if !*server_started {
                    if !init_hook.on_init().await {
                        ice_rpc::codegen::log::warn!(
                            "[{}] on_init() failed, retrying...", stringify!(DatabaseApi)
                        );
                        return false;
                    }
                    let dispatcher = DatabaseApiServer::new(local_impl.clone())
                        .native_dispatcher();
                    ice_rpc::codegen::declare_channel_max_slice_len("db", 4096usize);
                    if let Err(e) = ice_rpc::codegen::register_native_service(
                        "db",
                        "Database",
                        dispatcher,
                    ) {
                        ice_rpc::codegen::log::error!(
                            "[{}] channel registration failed: {e:?}",
                            stringify!(DatabaseApi)
                        );
                        return false;
                    }
                    *server_started = true;
                    ice_rpc::codegen::log::info!(
                        "[{}] native service registered on channel '{}'.",
                        stringify!(DatabaseApi), "db"
                    );
                }
                true
            }
            DatabaseApiMode::Consumer { ipc_client } => ipc_client.init().await,
        }
    }
}
#[allow(missing_docs)]
impl ice_rpc::codegen::ServiceNamed for DatabaseApiProxy {
    const SERVICE_NAME: &'static str = "Database";
}
#[allow(missing_docs)]
#[ice_rpc::codegen::async_trait::async_trait]
impl ice_rpc::ServiceInit for DatabaseApiProxy {
    async fn on_init(&self) -> bool {
        ice_rpc::codegen::ServiceLifecycle::init(self).await
    }
    fn dependencies(&self) -> Vec<&'static str> {
        self.deps.clone()
    }
}
#[allow(missing_docs)]
#[allow(dead_code)]
impl DatabaseApiProxy {
    pub fn deserialize_request_to_value(
        method: &str,
        bytes: &[u8],
    ) -> Option<ice_rpc::codegen::serde_json::Value> {
        match method {
            "add" => {
                let req: DatabaseApiRequest = ice_rpc::codegen::decode_aligned::<
                    DatabaseApiRequest,
                >(bytes)
                    .ok()?;
                match req {
                    DatabaseApiRequest::Add { a, b } => {
                        Some(
                            ice_rpc::codegen::serde_json::json!(
                                { "a" : ice_rpc::codegen::serde_json::to_value(a).ok() ?,
                                "b" : ice_rpc::codegen::serde_json::to_value(b).ok() ? }
                            ),
                        )
                    }
                    _ => None,
                }
            }
            "get" => {
                let req: DatabaseApiRequest = ice_rpc::codegen::decode_aligned::<
                    DatabaseApiRequest,
                >(bytes)
                    .ok()?;
                match req {
                    DatabaseApiRequest::Get { key } => {
                        Some(ice_rpc::codegen::serde_json::to_value(key).ok()?)
                    }
                    _ => None,
                }
            }
            "put" => {
                let req: DatabaseApiRequest = ice_rpc::codegen::decode_aligned::<
                    DatabaseApiRequest,
                >(bytes)
                    .ok()?;
                match req {
                    DatabaseApiRequest::Put { key, value } => {
                        Some(
                            ice_rpc::codegen::serde_json::json!(
                                { "key" : ice_rpc::codegen::serde_json::to_value(key).ok()
                                ?, "value" : { use ice_rpc::codegen::base64::Engine; let
                                encoded =
                                ice_rpc::codegen::base64::engine::general_purpose::STANDARD
                                .encode(& value);
                                ice_rpc::codegen::serde_json::Value::String(encoded) } }
                            ),
                        )
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }
}
#[allow(missing_docs)]
#[allow(dead_code)]
impl DatabaseApiProxy {
    pub fn serialize_response_from_value(
        method: &str,
        value: ice_rpc::codegen::serde_json::Value,
    ) -> Option<(ice_rpc::codegen::EventKind, Vec<u8>)> {
        match method {
            "add" => {
                let event_type = value
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("next");
                let event = match event_type {
                    "next" => {
                        let data: i32 = match value.get("data") {
                            Some(d) => {
                                match ice_rpc::codegen::serde_json::from_value(d.clone()) {
                                    Ok(v) => v,
                                    Err(_) => return None,
                                }
                            }
                            None => return None,
                        };
                        ice_rpc::codegen::WireEvent::Next(data)
                    }
                    "complete" => {
                        match value.get("data") {
                            Some(d) => {
                                let data: i32 = match ice_rpc::codegen::serde_json::from_value(
                                    d.clone(),
                                ) {
                                    Ok(v) => v,
                                    Err(_) => return None,
                                };
                                ice_rpc::codegen::WireEvent::CompleteWith(data)
                            }
                            None => ice_rpc::codegen::WireEvent::Complete,
                        }
                    }
                    "error" => {
                        let err: String = match value.get("data") {
                            Some(d) => {
                                match ice_rpc::codegen::serde_json::from_value(d.clone()) {
                                    Ok(v) => v,
                                    Err(_) => return None,
                                }
                            }
                            None => return None,
                        };
                        ice_rpc::codegen::WireEvent::Error(err)
                    }
                    _ => return None,
                };
                let kind = event.kind();
                ice_rpc::codegen::rkyv::to_bytes::<
                    ice_rpc::codegen::rkyv::rancor::Error,
                >(&event)
                    .ok()
                    .map(|aligned| (kind, aligned.to_vec()))
            }
            "get" => {
                let event_type = value
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("next");
                let event = match event_type {
                    "next" => {
                        let data: String = match value.get("data") {
                            Some(d) => {
                                match ice_rpc::codegen::serde_json::from_value(d.clone()) {
                                    Ok(v) => v,
                                    Err(_) => return None,
                                }
                            }
                            None => return None,
                        };
                        ice_rpc::codegen::WireEvent::Next(data)
                    }
                    "complete" => {
                        match value.get("data") {
                            Some(d) => {
                                let data: String = match ice_rpc::codegen::serde_json::from_value(
                                    d.clone(),
                                ) {
                                    Ok(v) => v,
                                    Err(_) => return None,
                                };
                                ice_rpc::codegen::WireEvent::CompleteWith(data)
                            }
                            None => ice_rpc::codegen::WireEvent::Complete,
                        }
                    }
                    "error" => {
                        let err: String = match value.get("data") {
                            Some(d) => {
                                match ice_rpc::codegen::serde_json::from_value(d.clone()) {
                                    Ok(v) => v,
                                    Err(_) => return None,
                                }
                            }
                            None => return None,
                        };
                        ice_rpc::codegen::WireEvent::Error(err)
                    }
                    _ => return None,
                };
                let kind = event.kind();
                ice_rpc::codegen::rkyv::to_bytes::<
                    ice_rpc::codegen::rkyv::rancor::Error,
                >(&event)
                    .ok()
                    .map(|aligned| (kind, aligned.to_vec()))
            }
            "put" => {
                let event_type = value
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("next");
                let event = match event_type {
                    "next" => {
                        let data: () = match value.get("data") {
                            Some(d) => {
                                match ice_rpc::codegen::serde_json::from_value(d.clone()) {
                                    Ok(v) => v,
                                    Err(_) => return None,
                                }
                            }
                            None => return None,
                        };
                        ice_rpc::codegen::WireEvent::Next(data)
                    }
                    "complete" => {
                        match value.get("data") {
                            Some(d) => {
                                let data: () = match ice_rpc::codegen::serde_json::from_value(
                                    d.clone(),
                                ) {
                                    Ok(v) => v,
                                    Err(_) => return None,
                                };
                                ice_rpc::codegen::WireEvent::CompleteWith(data)
                            }
                            None => ice_rpc::codegen::WireEvent::Complete,
                        }
                    }
                    "error" => {
                        let err: String = match value.get("data") {
                            Some(d) => {
                                match ice_rpc::codegen::serde_json::from_value(d.clone()) {
                                    Ok(v) => v,
                                    Err(_) => return None,
                                }
                            }
                            None => return None,
                        };
                        ice_rpc::codegen::WireEvent::Error(err)
                    }
                    _ => return None,
                };
                let kind = event.kind();
                ice_rpc::codegen::rkyv::to_bytes::<
                    ice_rpc::codegen::rkyv::rancor::Error,
                >(&event)
                    .ok()
                    .map(|aligned| (kind, aligned.to_vec()))
            }
            _ => None,
        }
    }
}
#[allow(missing_docs)]
impl DatabaseApiProxy {
    /// The JSON view of `add`: decodes the arguments, calls the method and reads its result. Generated so the dispatch table stays one line per method.
    async fn json_view_of_add(
        &self,
        args: ice_rpc::codegen::serde_json::Value,
        read: ice_rpc::codegen::ReadMode,
    ) -> ice_rpc::codegen::JsonResult {
        let a: i32 = {
            let __value = args
                .get("a")
                .cloned()
                .unwrap_or(ice_rpc::codegen::serde_json::Value::Null);
            ice_rpc::codegen::serde_json::from_value(__value)
                .map_err(|e| {
                    ice_rpc::codegen::JsonCallError::InvalidArgs(
                        format!("invalid parameter 'a' for 'add': {e}"),
                    )
                })?
        };
        let b: i32 = {
            let __value = args
                .get("b")
                .cloned()
                .unwrap_or(ice_rpc::codegen::serde_json::Value::Null);
            ice_rpc::codegen::serde_json::from_value(__value)
                .map_err(|e| {
                    ice_rpc::codegen::JsonCallError::InvalidArgs(
                        format!("invalid parameter 'b' for 'add': {e}"),
                    )
                })?
        };
        ice_rpc::codegen::read_json(self.add(a, b).await, read).await
    }
    /// The JSON view of `get`: decodes the arguments, calls the method and reads its result. Generated so the dispatch table stays one line per method.
    async fn json_view_of_get(
        &self,
        args: ice_rpc::codegen::serde_json::Value,
        read: ice_rpc::codegen::ReadMode,
    ) -> ice_rpc::codegen::JsonResult {
        let key: String = ice_rpc::codegen::serde_json::from_value(args)
            .map_err(|e| ice_rpc::codegen::JsonCallError::InvalidArgs(
                format!("invalid parameter for 'get': {e}"),
            ))?;
        ice_rpc::codegen::read_json(self.get(key).await, read).await
    }
    /// The JSON view of `put`: decodes the arguments, calls the method and reads its result. Generated so the dispatch table stays one line per method.
    async fn json_view_of_put(
        &self,
        args: ice_rpc::codegen::serde_json::Value,
        read: ice_rpc::codegen::ReadMode,
    ) -> ice_rpc::codegen::JsonResult {
        let key: String = {
            let __value = args
                .get("key")
                .cloned()
                .unwrap_or(ice_rpc::codegen::serde_json::Value::Null);
            ice_rpc::codegen::serde_json::from_value(__value)
                .map_err(|e| {
                    ice_rpc::codegen::JsonCallError::InvalidArgs(
                        format!("invalid parameter 'key' for 'put': {e}"),
                    )
                })?
        };
        let value: Vec<u8> = {
            use ice_rpc::codegen::base64::Engine;
            let __text = args
                .get("value")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ice_rpc::codegen::JsonCallError::InvalidArgs(
                    "missing base64 string parameter 'value'".to_string(),
                ))?;
            ice_rpc::codegen::base64::engine::general_purpose::STANDARD
                .decode(__text)
                .map_err(|e| ice_rpc::codegen::JsonCallError::InvalidArgs(
                    format!("invalid base64 parameter 'value': {e}"),
                ))?
        };
        ice_rpc::codegen::read_json(self.put(key, value).await, read).await
    }
}
#[allow(missing_docs)]
#[ice_rpc::codegen::async_trait::async_trait]
impl ice_rpc::codegen::JsonInvoker for DatabaseApiProxy {
    fn service_name(&self) -> &'static str {
        <DatabaseApiProxy as ice_rpc::codegen::ServiceNamed>::SERVICE_NAME
    }
    async fn invoke_json(
        &self,
        method: &str,
        args: ice_rpc::codegen::serde_json::Value,
        read: ice_rpc::codegen::ReadMode,
    ) -> Option<ice_rpc::codegen::JsonResult> {
        match method {
            "add" => Some(self.json_view_of_add(args, read).await),
            "get" => Some(self.json_view_of_get(args, read).await),
            "put" => Some(self.json_view_of_put(args, read).await),
            _ => None,
        }
    }
}
#[allow(missing_docs)]
impl ::std::fmt::Display for DatabaseApiRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        match self {
            DatabaseApiRequest::Add { a, b } => {
                ::std::write!(
                    f, "add(a={}, b={})", ice_rpc::monitor::render_value!(a),
                    ice_rpc::monitor::render_value!(b)
                )
            }
            DatabaseApiRequest::Get { key } => {
                ::std::write!(f, "get(key={})", ice_rpc::monitor::render_value!(key))
            }
            DatabaseApiRequest::Put { key, value } => {
                ::std::write!(
                    f, "put(key={}, value={})", ice_rpc::monitor::render_value!(key),
                    ice_rpc::monitor::render_value!(value)
                )
            }
        }
    }
}
#[allow(missing_docs)]
/// Decodes this service's payloads into human-readable text.
#[derive(Debug, Clone, Copy, Default)]
pub struct DatabaseApiDecoder;
#[allow(missing_docs)]
impl DatabaseApiDecoder {
    /// Logical name of the service this decoder handles.
    pub const SERVICE_NAME: &'static str = "Database";
    /// Builds this decoder, shared by the link-time registration and by
    /// [`Self::register`] so both hand out the same decoder type.
    pub fn build() -> ::std::sync::Arc<dyn ice_rpc::monitor::ServiceDecoder> {
        ::std::sync::Arc::new(Self)
    }
    /// Registers this decoder into an observer registry.
    pub fn register(decoders: &mut ice_rpc::monitor::Decoders) {
        decoders
            .register(
                ice_rpc::codegen::service_id_of(Self::SERVICE_NAME),
                Self::build(),
            );
    }
}
#[allow(missing_docs)]
#[allow(dead_code)]
#[ice_rpc::codegen::linkme::distributed_slice(ice_rpc::monitor::DECODERS)]
#[linkme(crate = ice_rpc::codegen::linkme)]
static __ICE_RPC_DECODER_DATABASEAPI: ice_rpc::monitor::DecoderRegistration = ice_rpc::monitor::DecoderRegistration {
    service_name: "Database",
    build: DatabaseApiDecoder::build,
};
#[allow(missing_docs)]
impl ice_rpc::monitor::ServiceDecoder for DatabaseApiDecoder {
    fn request(
        &self,
        method: &str,
        payload: &[u8],
    ) -> ::std::option::Option<::std::string::String> {
        match method {
            "add" => ice_rpc::monitor::decode_request::<DatabaseApiRequest>(payload),
            "get" => ice_rpc::monitor::decode_request::<DatabaseApiRequest>(payload),
            "put" => ice_rpc::monitor::decode_request::<DatabaseApiRequest>(payload),
            _ => ::std::option::Option::None,
        }
    }
    fn response(
        &self,
        method: &str,
        payload: &[u8],
    ) -> ::std::option::Option<::std::string::String> {
        match method {
            "add" => {
                ice_rpc::monitor::decode_response::<
                    i32,
                    String,
                >(
                    payload,
                    |value| ice_rpc::monitor::render_value!(value),
                    |error| ice_rpc::monitor::render_value!(error),
                )
            }
            "get" => {
                ice_rpc::monitor::decode_response::<
                    String,
                    String,
                >(
                    payload,
                    |value| ice_rpc::monitor::render_value!(value),
                    |error| ice_rpc::monitor::render_value!(error),
                )
            }
            "put" => {
                ice_rpc::monitor::decode_response_unit::<
                    String,
                >(payload, |error| ice_rpc::monitor::render_value!(error))
            }
            _ => ::std::option::Option::None,
        }
    }
    fn method_name(&self, method_id: u32) -> ::std::option::Option<&'static str> {
        if method_id == ice_rpc::codegen::method_id_of("add") {
            return ::std::option::Option::Some("add");
        }
        if method_id == ice_rpc::codegen::method_id_of("get") {
            return ::std::option::Option::Some("get");
        }
        if method_id == ice_rpc::codegen::method_id_of("put") {
            return ::std::option::Option::Some("put");
        }
        ::std::option::Option::None
    }
}
#[allow(missing_docs)]
#[doc(hidden)]
#[unsafe(no_mangle)]
static __ICE_RPC_SVC_Database: u8 = 0;
