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
#[allow(dead_code)]
pub enum DatabaseApiMode {
    Provider {
        local_impl: std::sync::Arc<dyn DatabaseApi>,
        init_hook: std::sync::Arc<dyn ice_rpc::ServiceInit>,
        server_started: bool,
    },
    Consumer { ipc_client: std::sync::Arc<DatabaseApiClient> },
    ProviderNodeJs,
}
#[allow(missing_docs)]
pub struct DatabaseApiProxy {
    mode: ice_rpc::gen::async_lock::RwLock<DatabaseApiMode>,
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
    /// Builds the proxy of the `ProviderNodeJs` mode: the Node.js host
    /// implements the methods, and each call is bridged to it over IPC.
    pub fn provide_nodejs() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            deps: vec![],
            mode: ice_rpc::gen::async_lock::RwLock::new(DatabaseApiMode::ProviderNodeJs),
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
            DatabaseApiMode::ProviderNodeJs => {
                ice_rpc::Observable::from_technical_error(
                    ice_rpc::RpcError::Internal(
                        "ProviderNodeJs: direct calls are not supported — use IPC"
                            .into(),
                    ),
                )
            }
        }
    }
    async fn put(&self, key: String, value: Vec<u8>) -> Observable<(), String> {
        let mode = self.mode.read().await;
        match &*mode {
            DatabaseApiMode::Provider { local_impl, .. } => {
                local_impl.put(key, value).await
            }
            DatabaseApiMode::Consumer { ipc_client } => ipc_client.put(key, value).await,
            DatabaseApiMode::ProviderNodeJs => {
                ice_rpc::Observable::from_technical_error(
                    ice_rpc::RpcError::Internal(
                        "ProviderNodeJs: direct calls are not supported — use IPC"
                            .into(),
                    ),
                )
            }
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
            DatabaseApiMode::ProviderNodeJs => {
                let mut dispatcher = ice_rpc::gen::ServiceDispatcher::new(
                    <DatabaseApiProxy>::SERVICE,
                );
                {
                    dispatcher
                        .method(
                            "get",
                            move |
                                header: ice_rpc::gen::RpcHeader,
                                payload: Vec<u8>,
                                emitter: ice_rpc::gen::OwnedEmitter,
                            | -> ice_rpc::gen::BoxResponseFuture {
                                Box::pin(async move {
                                    let mut emitter = emitter;
                                    let Some(args) = DatabaseApiProxy::deserialize_request_to_value(
                                        "get",
                                        &payload,
                                    ) else {
                                        ::log::error!(
                                            "[{}::{}] Failed to deserialize the request", <
                                            DatabaseApiProxy > ::SERVICE_NAME, "get"
                                        );
                                        return;
                                    };
                                    let value = match ice_rpc::nodejs_dispatch::call(
                                        header.correlation_id,
                                        <DatabaseApiProxy>::SERVICE_NAME,
                                        "get",
                                        args,
                                    ) {
                                        Ok(value) => value,
                                        Err(e) => {
                                            ::log::error!(
                                                "[{}::{}] NodeJS dispatch failed: {}", < DatabaseApiProxy >
                                                ::SERVICE_NAME, "get", e
                                            );
                                            return;
                                        }
                                    };
                                    if let Some((kind, sample)) = DatabaseApiProxy::serialize_response_from_value(
                                        "get",
                                        value,
                                    ) {
                                        emitter.emit(kind, &sample);
                                    }
                                })
                            },
                        );
                }
                {
                    dispatcher
                        .method(
                            "put",
                            move |
                                header: ice_rpc::gen::RpcHeader,
                                payload: Vec<u8>,
                                emitter: ice_rpc::gen::OwnedEmitter,
                            | -> ice_rpc::gen::BoxResponseFuture {
                                Box::pin(async move {
                                    let mut emitter = emitter;
                                    let Some(args) = DatabaseApiProxy::deserialize_request_to_value(
                                        "put",
                                        &payload,
                                    ) else {
                                        ::log::error!(
                                            "[{}::{}] Failed to deserialize the request", <
                                            DatabaseApiProxy > ::SERVICE_NAME, "put"
                                        );
                                        return;
                                    };
                                    let value = match ice_rpc::nodejs_dispatch::call(
                                        header.correlation_id,
                                        <DatabaseApiProxy>::SERVICE_NAME,
                                        "put",
                                        args,
                                    ) {
                                        Ok(value) => value,
                                        Err(e) => {
                                            ::log::error!(
                                                "[{}::{}] NodeJS dispatch failed: {}", < DatabaseApiProxy >
                                                ::SERVICE_NAME, "put", e
                                            );
                                            return;
                                        }
                                    };
                                    if let Some((kind, sample)) = DatabaseApiProxy::serialize_response_from_value(
                                        "put",
                                        value,
                                    ) {
                                        emitter.emit(kind, &sample);
                                    }
                                })
                            },
                        );
                }
                if let Err(e) = ice_rpc::gen::register_native_service(
                    "db",
                    "Database",
                    dispatcher,
                ) {
                    ::log::error!("[{}] channel registration failed: {e:?}", "Database");
                    return false;
                }
                ::log::info!(
                    "[{}] NodeJS provider registered on channel '{}'.", "Database", "db"
                );
                true
            }
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
#[allow(dead_code)]
impl DatabaseApiProxy {
    pub fn deserialize_request_to_value(
        method: &str,
        bytes: &[u8],
    ) -> Option<ice_rpc::gen::serde_json::Value> {
        match method {
            "get" => {
                let req: DatabaseApiRequest = ice_rpc::gen::decode_aligned::<
                    DatabaseApiRequest,
                >(bytes)
                    .ok()?;
                match req {
                    DatabaseApiRequest::Get { key } => {
                        Some(ice_rpc::gen::serde_json::to_value(key).ok()?)
                    }
                    _ => None,
                }
            }
            "put" => {
                let req: DatabaseApiRequest = ice_rpc::gen::decode_aligned::<
                    DatabaseApiRequest,
                >(bytes)
                    .ok()?;
                match req {
                    DatabaseApiRequest::Put { key, value } => {
                        Some(
                            ice_rpc::gen::serde_json::json!(
                                { "key" : ice_rpc::gen::serde_json::to_value(key).ok() ?,
                                "value" : { use ice_rpc::gen::base64::Engine; let encoded =
                                ice_rpc::gen::base64::engine::general_purpose::STANDARD
                                .encode(& value);
                                ice_rpc::gen::serde_json::Value::String(encoded) } }
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
        value: ice_rpc::gen::serde_json::Value,
    ) -> Option<(ice_rpc::gen::EventKind, Vec<u8>)> {
        match method {
            "get" => {
                let event_type = value
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("next");
                let event = match event_type {
                    "next" => {
                        let data: String = match value.get("data") {
                            Some(d) => {
                                match ice_rpc::gen::serde_json::from_value(d.clone()) {
                                    Ok(v) => v,
                                    Err(_) => return None,
                                }
                            }
                            None => return None,
                        };
                        ice_rpc::gen::WireEvent::Next(data)
                    }
                    "complete" => {
                        match value.get("data") {
                            Some(d) => {
                                let data: String = match ice_rpc::gen::serde_json::from_value(
                                    d.clone(),
                                ) {
                                    Ok(v) => v,
                                    Err(_) => return None,
                                };
                                ice_rpc::gen::WireEvent::CompleteWith(data)
                            }
                            None => ice_rpc::gen::WireEvent::Complete,
                        }
                    }
                    "error" => {
                        let err: String = match value.get("data") {
                            Some(d) => {
                                match ice_rpc::gen::serde_json::from_value(d.clone()) {
                                    Ok(v) => v,
                                    Err(_) => return None,
                                }
                            }
                            None => return None,
                        };
                        ice_rpc::gen::WireEvent::Error(err)
                    }
                    _ => return None,
                };
                let kind = event.kind();
                ice_rpc::gen::rkyv::to_bytes::<ice_rpc::gen::rkyv::rancor::Error>(&event)
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
                                match ice_rpc::gen::serde_json::from_value(d.clone()) {
                                    Ok(v) => v,
                                    Err(_) => return None,
                                }
                            }
                            None => return None,
                        };
                        ice_rpc::gen::WireEvent::Next(data)
                    }
                    "complete" => {
                        match value.get("data") {
                            Some(d) => {
                                let data: () = match ice_rpc::gen::serde_json::from_value(
                                    d.clone(),
                                ) {
                                    Ok(v) => v,
                                    Err(_) => return None,
                                };
                                ice_rpc::gen::WireEvent::CompleteWith(data)
                            }
                            None => ice_rpc::gen::WireEvent::Complete,
                        }
                    }
                    "error" => {
                        let err: String = match value.get("data") {
                            Some(d) => {
                                match ice_rpc::gen::serde_json::from_value(d.clone()) {
                                    Ok(v) => v,
                                    Err(_) => return None,
                                }
                            }
                            None => return None,
                        };
                        ice_rpc::gen::WireEvent::Error(err)
                    }
                    _ => return None,
                };
                let kind = event.kind();
                ice_rpc::gen::rkyv::to_bytes::<ice_rpc::gen::rkyv::rancor::Error>(&event)
                    .ok()
                    .map(|aligned| (kind, aligned.to_vec()))
            }
            _ => None,
        }
    }
}
#[allow(missing_docs)]
#[async_trait::async_trait]
impl ice_rpc::gen::HttpCallable for DatabaseApiProxy {
    fn service_name(&self) -> &'static str {
        "Database"
    }
    async fn http_invoke(
        &self,
        method: &str,
        params: ice_rpc::gen::serde_json::Value,
    ) -> Result<ice_rpc::gen::serde_json::Value, String> {
        match method {
            "get" => {
                let key: String = match ice_rpc::gen::serde_json::from_value(params) {
                    Ok(v) => v,
                    Err(e) => {
                        return Err(
                            format!(
                                "Invalid parameter for '{}': {} (expected type: {})", "get",
                                e, "String"
                            ),
                        );
                    }
                };
                let mut rx = self.get(key).await;
                match rx.recv().await {
                    Ok(ice_rpc::Event::Next(value)) => {
                        let data = ice_rpc::gen::serde_json::to_value(&value)
                            .map_err(|e| {
                                format!("Failed to serialize the response: {}", e)
                            })?;
                        Ok(
                            ice_rpc::gen::serde_json::json!(
                                { "status" : "ok", "data" : data }
                            ),
                        )
                    }
                    Ok(ice_rpc::Event::Complete) => {
                        Ok(ice_rpc::gen::serde_json::json!({ "status" : "ok" }))
                    }
                    Ok(ice_rpc::Event::Error(e)) => {
                        Ok(
                            ice_rpc::gen::serde_json::json!(
                                { "status" : "error", "error" : e.to_string() }
                            ),
                        )
                    }
                    Err(_) => Err("No response received from the service".to_string()),
                }
            }
            "put" => {
                let key: String = {
                    let field_name = "key";
                    let val = params
                        .get(field_name)
                        .cloned()
                        .unwrap_or(ice_rpc::gen::serde_json::Value::Null);
                    match ice_rpc::gen::serde_json::from_value(val) {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(
                                format!(
                                    "Invalid parameter '{}' for '{}': {}", field_name, "put", e
                                ),
                            );
                        }
                    }
                };
                let value: Vec<u8> = {
                    let field_name = "value";
                    let val = params
                        .get(field_name)
                        .cloned()
                        .unwrap_or(ice_rpc::gen::serde_json::Value::Null);
                    match ice_rpc::gen::serde_json::from_value(val) {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(
                                format!(
                                    "Invalid parameter '{}' for '{}': {}", field_name, "put", e
                                ),
                            );
                        }
                    }
                };
                let mut rx = self.put(key, value).await;
                match rx.recv().await {
                    Ok(ice_rpc::Event::Next(value)) => {
                        let data = ice_rpc::gen::serde_json::to_value(&value)
                            .map_err(|e| {
                                format!("Failed to serialize the response: {}", e)
                            })?;
                        Ok(
                            ice_rpc::gen::serde_json::json!(
                                { "status" : "ok", "data" : data }
                            ),
                        )
                    }
                    Ok(ice_rpc::Event::Complete) => {
                        Ok(ice_rpc::gen::serde_json::json!({ "status" : "ok" }))
                    }
                    Ok(ice_rpc::Event::Error(e)) => {
                        Ok(
                            ice_rpc::gen::serde_json::json!(
                                { "status" : "error", "error" : e.to_string() }
                            ),
                        )
                    }
                    Err(_) => Err("No response received from the service".to_string()),
                }
            }
            _ => Err(format!("Unknown method '{}' for service 'Database'", method)),
        }
    }
}
#[allow(missing_docs)]
impl ::std::fmt::Display for DatabaseApiRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        match self {
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
    /// Registers this decoder into an observer registry.
    pub fn register(decoders: &mut ice_rpc::monitor::Decoders) {
        decoders
            .register(
                ice_rpc::gen::service_id_of(Self::SERVICE_NAME),
                ::std::sync::Arc::new(Self),
            );
    }
}
#[allow(missing_docs)]
impl ice_rpc::monitor::ServiceDecoder for DatabaseApiDecoder {
    fn request(
        &self,
        method: &str,
        payload: &[u8],
    ) -> ::std::option::Option<::std::string::String> {
        match method {
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
}
#[allow(missing_docs)]
#[doc(hidden)]
#[no_mangle]
static __ICE_RPC_SVC_Database: u8 = 0;
