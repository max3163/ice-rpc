#[allow(unexpected_cfgs)]
#[async_trait::async_trait]
pub trait Calculator: Send + Sync + 'static {
    async fn add(&self, a: i32, b: i32) -> Observable<i32, String>;
}
#[allow(missing_docs)]
#[repr(u8)]
#[derive(
    ice_rpc::gen::rkyv::Archive,
    ice_rpc::gen::rkyv::Deserialize,
    ice_rpc::gen::rkyv::Serialize,
    Debug
)]
pub enum CalculatorRequest {
    Add { a: i32, b: i32 } = 0u8,
}
#[allow(missing_docs)]
impl CalculatorProxy {
    /// Identity of this service contract: its id inside the channel and
    /// its interface version, declared once and shared by the generated
    /// client and provider so the version cannot be lost between them.
    pub const SERVICE: ice_rpc::gen::ServiceRef = ice_rpc::gen::ServiceRef::new(
        ice_rpc::gen::service_id_of("calculator"),
        1u16,
    );
}
#[allow(missing_docs)]
pub struct CalculatorClient;
#[allow(missing_docs)]
impl CalculatorClient {
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self)
    }
    pub async fn add(&self, a: i32, b: i32) -> ice_rpc::Observable<i32, String> {
        let req_val = CalculatorRequest::Add { a, b };
        ice_rpc::gen::serialize_and_call::<
            i32,
            String,
            _,
        >("calculator", <CalculatorProxy>::SERVICE, "add", &req_val)
            .unwrap_or_else(ice_rpc::Observable::from_technical_error)
    }
}
#[allow(missing_docs)]
#[async_trait::async_trait]
impl ice_rpc::gen::ServiceLifecycle for CalculatorClient {
    async fn init(&self) -> bool {
        true
    }
}
#[allow(missing_docs)]
#[derive(Clone)]
pub struct CalculatorServer {
    service_impl: std::sync::Arc<dyn Calculator>,
}
#[allow(missing_docs)]
impl CalculatorServer {
    fn new(service_impl: std::sync::Arc<dyn Calculator>) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self { service_impl })
    }
    /// Builds the native `request_response` dispatcher of this service.
    ///
    /// Each RPC method is registered with its own handler: it decodes
    /// the rkyv request enum from the payload, invokes the local
    /// implementation, and streams the resulting `Observable` through
    /// `observable_to_responses`.
    fn native_dispatcher(self: std::sync::Arc<Self>) -> ice_rpc::gen::ServiceDispatcher {
        let mut dispatcher = ice_rpc::gen::ServiceDispatcher::new(
            <CalculatorProxy>::SERVICE,
        );
        {
            let service_impl = self.service_impl.clone();
            dispatcher
                .method(
                    "add",
                    move |
                        payload: &[u8],
                        emitter: &mut dyn ice_rpc::gen::ResponseEmitter|
                    {
                        match ice_rpc::gen::decode_aligned::<
                            CalculatorRequest,
                        >(payload) {
                            Ok(CalculatorRequest::Add { a, b }) => {
                                let impl_ref = service_impl.clone();
                                let stream = ice_rpc::rt::block_on(async move {
                                    impl_ref.add(a, b).await
                                });
                                ice_rpc::gen::observable_to_responses(stream, emitter);
                            }
                            _ => {}
                        }
                    },
                );
        }
        dispatcher
    }
}
#[allow(missing_docs)]
struct __CalculatorServiceInitDefault(std::sync::Arc<dyn Calculator>);
#[allow(missing_docs)]
#[async_trait::async_trait]
impl ice_rpc::ServiceInit for __CalculatorServiceInitDefault {}
#[allow(missing_docs)]
#[allow(dead_code)]
pub enum CalculatorMode {
    Provider {
        local_impl: std::sync::Arc<dyn Calculator>,
        init_hook: std::sync::Arc<dyn ice_rpc::ServiceInit>,
        server_started: bool,
    },
    Consumer { ipc_client: std::sync::Arc<CalculatorClient> },
    ProviderNodeJs,
}
#[allow(missing_docs)]
pub struct CalculatorProxy {
    mode: ice_rpc::gen::async_lock::RwLock<CalculatorMode>,
    deps: Vec<&'static str>,
}
#[allow(missing_docs)]
#[allow(dead_code)]
impl CalculatorProxy {
    /// Logical name of the service, injected by the `#[service]` macro.
    pub const SERVICE_NAME: &'static str = "calculator";
    pub fn provide<T>(implementation: T) -> std::sync::Arc<Self>
    where
        T: Calculator + Send + Sync + 'static,
    {
        let arc = std::sync::Arc::new(implementation);
        let init_hook = std::sync::Arc::new(
            __CalculatorServiceInitDefault(arc.clone() as std::sync::Arc<dyn Calculator>),
        );
        std::sync::Arc::new(Self {
            deps: vec![],
            mode: ice_rpc::gen::async_lock::RwLock::new(CalculatorMode::Provider {
                local_impl: arc as std::sync::Arc<dyn Calculator>,
                init_hook: init_hook as std::sync::Arc<dyn ice_rpc::ServiceInit>,
                server_started: false,
            }),
        })
    }
    pub fn provide_with_init<T>(implementation: T) -> std::sync::Arc<Self>
    where
        T: Calculator + ice_rpc::ServiceInit + Send + Sync + 'static,
    {
        let arc = std::sync::Arc::new(implementation);
        let deps = arc.dependencies();
        std::sync::Arc::new(Self {
            deps,
            mode: ice_rpc::gen::async_lock::RwLock::new(CalculatorMode::Provider {
                local_impl: arc.clone() as std::sync::Arc<dyn Calculator>,
                init_hook: arc as std::sync::Arc<dyn ice_rpc::ServiceInit>,
                server_started: false,
            }),
        })
    }
    pub fn consume() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            deps: vec![],
            mode: ice_rpc::gen::async_lock::RwLock::new(CalculatorMode::Consumer {
                ipc_client: CalculatorClient::new(),
            }),
        })
    }
    /// Builds the proxy of the `ProviderNodeJs` mode: the Node.js host
    /// implements the methods, and each call is bridged to it over IPC.
    pub fn provide_nodejs() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            deps: vec![],
            mode: ice_rpc::gen::async_lock::RwLock::new(CalculatorMode::ProviderNodeJs),
        })
    }
}
#[allow(missing_docs)]
#[async_trait::async_trait]
impl Calculator for CalculatorProxy {
    async fn add(&self, a: i32, b: i32) -> Observable<i32, String> {
        let mode = self.mode.read().await;
        match &*mode {
            CalculatorMode::Provider { local_impl, .. } => local_impl.add(a, b).await,
            CalculatorMode::Consumer { ipc_client } => ipc_client.add(a, b).await,
            CalculatorMode::ProviderNodeJs => {
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
impl ice_rpc::gen::ServiceConsumer for CalculatorProxy {
    fn consume_proxy() -> std::sync::Arc<Self> {
        CalculatorProxy::consume()
    }
}
#[allow(missing_docs)]
#[async_trait::async_trait]
impl ice_rpc::gen::ServiceLifecycle for CalculatorProxy {
    async fn init(&self) -> bool {
        let mut mode = self.mode.write().await;
        match &mut *mode {
            CalculatorMode::ProviderNodeJs => {
                let mut dispatcher = ice_rpc::gen::ServiceDispatcher::new(
                    <CalculatorProxy>::SERVICE,
                );
                {
                    dispatcher
                        .method(
                            "add",
                            move |
                                payload: &[u8],
                                emitter: &mut dyn ice_rpc::gen::ResponseEmitter|
                            {
                                let Some(args) = CalculatorProxy::deserialize_request_to_value(
                                    "add",
                                    payload,
                                ) else {
                                    ::log::error!(
                                        "[{}::{}] Failed to deserialize the request", <
                                        CalculatorProxy > ::SERVICE_NAME, "add"
                                    );
                                    return;
                                };
                                let value = match ice_rpc::nodejs_dispatch::call(
                                    [0u8; 16],
                                    <CalculatorProxy>::SERVICE_NAME,
                                    "add",
                                    args,
                                ) {
                                    Ok(value) => value,
                                    Err(e) => {
                                        ::log::error!(
                                            "[{}::{}] NodeJS dispatch failed: {}", < CalculatorProxy >
                                            ::SERVICE_NAME, "add", e
                                        );
                                        return;
                                    }
                                };
                                if let Some((kind, sample)) = CalculatorProxy::serialize_response_from_value(
                                    "add",
                                    value,
                                ) {
                                    emitter.emit(kind, &sample);
                                }
                            },
                        );
                }
                if let Err(e) = ice_rpc::gen::register_native_service(
                    "calculator",
                    "calculator",
                    dispatcher,
                ) {
                    ::log::error!(
                        "[{}] channel registration failed: {e:?}", "calculator"
                    );
                    return false;
                }
                ::log::info!(
                    "[{}] NodeJS provider registered on channel '{}'.", "calculator",
                    "calculator"
                );
                true
            }
            CalculatorMode::Provider { local_impl, init_hook, server_started } => {
                if !*server_started {
                    if !init_hook.on_init().await {
                        ::log::warn!(
                            "[{}] on_init() failed, retrying...", stringify!(Calculator)
                        );
                        return false;
                    }
                    let dispatcher = CalculatorServer::new(local_impl.clone())
                        .native_dispatcher();
                    if let Err(e) = ice_rpc::gen::register_native_service(
                        "calculator",
                        "calculator",
                        dispatcher,
                    ) {
                        ::log::error!(
                            "[{}] channel registration failed: {e:?}",
                            stringify!(Calculator)
                        );
                        return false;
                    }
                    *server_started = true;
                    ::log::info!(
                        "[{}] native service registered on channel '{}'.",
                        stringify!(Calculator), "calculator"
                    );
                }
                true
            }
            CalculatorMode::Consumer { ipc_client } => ipc_client.init().await,
        }
    }
}
#[allow(missing_docs)]
impl ice_rpc::gen::ServiceNamed for CalculatorProxy {
    const SERVICE_NAME: &'static str = "calculator";
}
#[allow(missing_docs)]
#[async_trait::async_trait]
impl ice_rpc::ServiceInit for CalculatorProxy {
    async fn on_init(&self) -> bool {
        ice_rpc::gen::ServiceLifecycle::init(self).await
    }
    fn dependencies(&self) -> Vec<&'static str> {
        self.deps.clone()
    }
}
#[allow(missing_docs)]
#[allow(dead_code)]
impl CalculatorProxy {
    pub fn deserialize_request_to_value(
        method: &str,
        bytes: &[u8],
    ) -> Option<ice_rpc::gen::serde_json::Value> {
        match method {
            "add" => {
                let req: CalculatorRequest = ice_rpc::gen::decode_aligned::<
                    CalculatorRequest,
                >(bytes)
                    .ok()?;
                match req {
                    CalculatorRequest::Add { a, b } => {
                        Some(
                            ice_rpc::gen::serde_json::json!(
                                { "a" : ice_rpc::gen::serde_json::to_value(a).ok() ?, "b" :
                                ice_rpc::gen::serde_json::to_value(b).ok() ? }
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
impl CalculatorProxy {
    pub fn serialize_response_from_value(
        method: &str,
        value: ice_rpc::gen::serde_json::Value,
    ) -> Option<(ice_rpc::gen::EventKind, Vec<u8>)> {
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
                                let data: i32 = match ice_rpc::gen::serde_json::from_value(
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
impl ice_rpc::gen::HttpCallable for CalculatorProxy {
    fn service_name(&self) -> &'static str {
        "calculator"
    }
    async fn http_invoke(
        &self,
        method: &str,
        params: ice_rpc::gen::serde_json::Value,
    ) -> Result<ice_rpc::gen::serde_json::Value, String> {
        match method {
            "add" => {
                let a: i32 = {
                    let field_name = "a";
                    let val = params
                        .get(field_name)
                        .cloned()
                        .unwrap_or(ice_rpc::gen::serde_json::Value::Null);
                    match ice_rpc::gen::serde_json::from_value(val) {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(
                                format!(
                                    "Invalid parameter '{}' for '{}': {}", field_name, "add", e
                                ),
                            );
                        }
                    }
                };
                let b: i32 = {
                    let field_name = "b";
                    let val = params
                        .get(field_name)
                        .cloned()
                        .unwrap_or(ice_rpc::gen::serde_json::Value::Null);
                    match ice_rpc::gen::serde_json::from_value(val) {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(
                                format!(
                                    "Invalid parameter '{}' for '{}': {}", field_name, "add", e
                                ),
                            );
                        }
                    }
                };
                let mut rx = self.add(a, b).await;
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
            _ => Err(format!("Unknown method '{}' for service 'calculator'", method)),
        }
    }
}
#[allow(missing_docs)]
impl ::std::fmt::Display for CalculatorRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        match self {
            CalculatorRequest::Add { a, b } => {
                ::std::write!(
                    f, "add(a={}, b={})", ice_rpc::monitor::render_value!(a),
                    ice_rpc::monitor::render_value!(b)
                )
            }
        }
    }
}
#[allow(missing_docs)]
/// Decodes this service's payloads into human-readable text.
#[derive(Debug, Clone, Copy, Default)]
pub struct CalculatorDecoder;
#[allow(missing_docs)]
impl CalculatorDecoder {
    /// Logical name of the service this decoder handles.
    pub const SERVICE_NAME: &'static str = "calculator";
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
impl ice_rpc::monitor::ServiceDecoder for CalculatorDecoder {
    fn request(
        &self,
        method: &str,
        payload: &[u8],
    ) -> ::std::option::Option<::std::string::String> {
        match method {
            "add" => ice_rpc::monitor::decode_request::<CalculatorRequest>(payload),
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
            _ => ::std::option::Option::None,
        }
    }
}
#[allow(missing_docs)]
#[doc(hidden)]
#[no_mangle]
static __ICE_RPC_SVC_calculator: u8 = 0;
