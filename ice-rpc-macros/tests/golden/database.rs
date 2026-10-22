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
pub enum DatabaseApiMode {
    Provider {
        local_impl: std::sync::Arc<dyn DatabaseApi>,
        init_hook: std::sync::Arc<dyn ice_rpc::ServiceInit>,
        server_started: bool,
    },
    Consumer { ipc_client: std::sync::Arc<DatabaseApiClient> },
}
#[allow(missing_docs)]
pub struct DatabaseApiProxy {
    mode: ice_rpc::codegen::async_lock::RwLock<DatabaseApiMode>,
    deps: Vec<&'static str>,
}
#[allow(missing_docs)]
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
    /// Whether a provider is currently connected to this service's
    /// channel.
    ///
    /// Synchronous and **read-only**: it opens the response direction of
    /// the channel without creating it and reports whether a
    /// provider-side publisher is attached. `false` therefore means no
    /// provider is serving the channel right now — the process is not
    /// started, has stopped, or failed to open its channel — which is
    /// what a readiness check, or a guard before an optional call, wants
    /// to know.
    ///
    /// The answer is per **channel** (the `group` of `#[service]`):
    /// every service sharing it shares the answer, so it says whether
    /// *some* provider serves the channel, not which service of it is
    /// answering.
    ///
    /// # Cost
    /// ~40 ns once this process has consumed the channel (the ports it
    /// keeps are read, not opened). The first probe on a channel it
    /// never consumed opens the service first (~0.13 ms in release,
    /// ~1 ms in a debug build): poll it, as a readiness endpoint does,
    /// rather than calling it before every RPC.
    ///
    /// # Errors
    /// Returns an [`ice_rpc::RpcError`] when the channel exists but
    /// cannot be opened — a configuration mismatch with its creator, or
    /// state left behind by a process killed while it held it. Those are
    /// reported rather than folded into `false`, because a "not
    /// connected" answer would hide a deployment problem no amount of
    /// retrying resolves.
    pub fn is_provider_connected() -> Result<bool, ice_rpc::RpcError> {
        ice_rpc::codegen::is_provider_connected("db")
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
#[doc(hidden)]
#[unsafe(no_mangle)]
static __ICE_RPC_SVC_Database: u8 = 0;
