#[allow(unexpected_cfgs)]
#[async_trait::async_trait]
pub trait DatabaseApi: Send + Sync + 'static {
    async fn get(&self, key: String) -> Observable<String, String>;
    async fn put(&self, key: String, value: Vec<u8>) -> Observable<(), String>;
}
#[allow(missing_docs)]
#[repr(u8)]
#[derive(
    ice_rpc::gen::rkyv::Archive,
    ice_rpc::gen::rkyv::Deserialize,
    ice_rpc::gen::rkyv::Serialize,
    Debug
)]
pub enum DatabaseApiRequest {
    Get { key: String } = 0u8,
    Put { key: String, value: Vec<u8> } = 1u8,
}
#[allow(missing_docs)]
impl DatabaseApiProxy {
    /// Identity of this service contract: its id inside the channel and
    /// its interface version, declared once and shared by the generated
    /// client and provider so the version cannot be lost between them.
    pub const SERVICE: ice_rpc::gen::ServiceRef = ice_rpc::gen::ServiceRef::new(
        ice_rpc::gen::service_id_of("Database"),
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
    pub async fn get(&self, key: String) -> ice_rpc::Observable<String, String> {
        let req_val = DatabaseApiRequest::Get { key };
        ice_rpc::gen::serialize_and_call::<
            String,
            String,
            _,
        >("db", <DatabaseApiProxy>::SERVICE, "get", &req_val)
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
        ice_rpc::gen::serialize_and_call::<
            (),
            String,
            _,
        >("db", <DatabaseApiProxy>::SERVICE, "put", &req_val)
            .unwrap_or_else(ice_rpc::Observable::from_technical_error)
    }
}
#[allow(missing_docs)]
#[async_trait::async_trait]
impl ice_rpc::gen::ServiceLifecycle for DatabaseApiClient {
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
    fn native_dispatcher(self: std::sync::Arc<Self>) -> ice_rpc::gen::ServiceDispatcher {
        let mut dispatcher = ice_rpc::gen::ServiceDispatcher::new(
            <DatabaseApiProxy>::SERVICE,
        );
        {
            let service_impl = self.service_impl.clone();
            dispatcher
                .method(
                    "get",
                    move |
                        header: ice_rpc::gen::RpcHeader,
                        payload: Vec<u8>,
                        emitter: ice_rpc::gen::OwnedEmitter,
                    | -> ice_rpc::gen::BoxResponseFuture {
                        let ctx = ice_rpc::gen::CallContext::new(&header, "get");
                        let impl_ref = service_impl.clone();
                        ice_rpc::gen::call_scoped(
                            ctx,
                            async move {
                                let mut emitter = emitter;
                                match ice_rpc::gen::decode_aligned::<
                                    DatabaseApiRequest,
                                >(&payload) {
                                    Ok(DatabaseApiRequest::Get { key }) => {
                                        let stream = impl_ref.get(key).await;
                                        ice_rpc::gen::observable_to_responses(stream, &mut *emitter)
                                            .await;
                                    }
                                    _ => {}
                                }
                            },
                        )
                    },
                );
        }
        {
            let service_impl = self.service_impl.clone();
            dispatcher
                .method(
                    "put",
                    move |
                        header: ice_rpc::gen::RpcHeader,
                        payload: Vec<u8>,
                        emitter: ice_rpc::gen::OwnedEmitter,
                    | -> ice_rpc::gen::BoxResponseFuture {
                        let ctx = ice_rpc::gen::CallContext::new(&header, "put");
                        let impl_ref = service_impl.clone();
                        ice_rpc::gen::call_scoped(
                            ctx,
                            async move {
                                let mut emitter = emitter;
                                match ice_rpc::gen::decode_aligned::<
                                    DatabaseApiRequest,
                                >(&payload) {
                                    Ok(DatabaseApiRequest::Put { key, value }) => {
                                        let stream = impl_ref.put(key, value).await;
                                        ice_rpc::gen::observable_to_responses(stream, &mut *emitter)
                                            .await;
                                    }
                                    _ => {}
                                }
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
#[async_trait::async_trait]
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
    mode: ice_rpc::gen::async_lock::RwLock<DatabaseApiMode>,
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
            mode: ice_rpc::gen::async_lock::RwLock::new(DatabaseApiMode::Provider {
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
            mode: ice_rpc::gen::async_lock::RwLock::new(DatabaseApiMode::Provider {
                local_impl: arc.clone() as std::sync::Arc<dyn DatabaseApi>,
                init_hook: arc as std::sync::Arc<dyn ice_rpc::ServiceInit>,
                server_started: false,
            }),
        })
    }
    pub fn consume() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            deps: vec![],
            mode: ice_rpc::gen::async_lock::RwLock::new(DatabaseApiMode::Consumer {
                ipc_client: DatabaseApiClient::new(),
            }),
        })
    }
}
#[allow(missing_docs)]
#[async_trait::async_trait]
impl DatabaseApi for DatabaseApiProxy {
    async fn get(&self, key: String) -> Observable<String, String> {
        let mode = self.mode.read().await;
        match &*mode {
            DatabaseApiMode::Provider { local_impl, .. } => local_impl.get(key).await,
            DatabaseApiMode::Consumer { ipc_client } => ipc_client.get(key).await,
        }
    }
    async fn put(&self, key: String, value: Vec<u8>) -> Observable<(), String> {
        let mode = self.mode.read().await;
        match &*mode {
            DatabaseApiMode::Provider { local_impl, .. } => {
                local_impl.put(key, value).await
            }
            DatabaseApiMode::Consumer { ipc_client } => ipc_client.put(key, value).await,
        }
    }
}
#[allow(missing_docs)]
impl ice_rpc::gen::ServiceConsumer for DatabaseApiProxy {
    fn consume_proxy() -> std::sync::Arc<Self> {
        DatabaseApiProxy::consume()
    }
}
#[allow(missing_docs)]
#[async_trait::async_trait]
impl ice_rpc::gen::ServiceLifecycle for DatabaseApiProxy {
    async fn init(&self) -> bool {
        let mut mode = self.mode.write().await;
        match &mut *mode {
            DatabaseApiMode::Provider { local_impl, init_hook, server_started } => {
                if !*server_started {
                    if !init_hook.on_init().await {
                        ::log::warn!(
                            "[{}] on_init() failed, retrying...", stringify!(DatabaseApi)
                        );
                        return false;
                    }
                    let dispatcher = DatabaseApiServer::new(local_impl.clone())
                        .native_dispatcher();
                    if let Err(e) = ice_rpc::gen::register_native_service(
                        "db",
                        "Database",
                        dispatcher,
                    ) {
                        ::log::error!(
                            "[{}] channel registration failed: {e:?}",
                            stringify!(DatabaseApi)
                        );
                        return false;
                    }
                    *server_started = true;
                    ::log::info!(
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
impl ice_rpc::gen::ServiceNamed for DatabaseApiProxy {
    const SERVICE_NAME: &'static str = "Database";
}
#[allow(missing_docs)]
#[async_trait::async_trait]
impl ice_rpc::ServiceInit for DatabaseApiProxy {
    async fn on_init(&self) -> bool {
        ice_rpc::gen::ServiceLifecycle::init(self).await
    }
    fn dependencies(&self) -> Vec<&'static str> {
        self.deps.clone()
    }
}
#[allow(missing_docs)]
#[doc(hidden)]
#[no_mangle]
static __ICE_RPC_SVC_Database: u8 = 0;
