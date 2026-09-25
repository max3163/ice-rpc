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
    ///
    /// Each handler returns a **task**, which the transport polls once on
    /// the channel's thread before detaching it: a handler that answers
    /// without yielding runs on that thread, one that `await`s runs as a
    /// task and cannot hold back the next request.
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
                        header: ice_rpc::gen::RpcHeader,
                        payload: Vec<u8>,
                        emitter: ice_rpc::gen::OwnedEmitter,
                    | -> ice_rpc::gen::BoxResponseFuture {
                        let ctx = ice_rpc::gen::CallContext::new(
                            &header,
                            <CalculatorProxy>::SERVICE_NAME,
                            "add",
                        );
                        let impl_ref = service_impl.clone();
                        ice_rpc::gen::call_scoped(
                            ctx,
                            async move {
                                let mut emitter = emitter;
                                match ice_rpc::gen::decode_aligned::<
                                    CalculatorRequest,
                                >(&payload) {
                                    Ok(CalculatorRequest::Add { a, b }) => {
                                        let stream = impl_ref.add(a, b).await;
                                        ice_rpc::gen::observable_to_responses(stream, &mut *emitter)
                                            .await;
                                    }
                                    Err(e) => {
                                        ice_rpc::gen::log::error!(
                                            "[{}::{}] request payload decoding failed: {:?}", <
                                            CalculatorProxy > ::SERVICE_NAME, "add", e
                                        );
                                        let _ = ice_rpc::gen::emit_rpc_error(
                                            ice_rpc::gen::RpcError::SerializationError,
                                            &mut *emitter,
                                        );
                                    }
                                    Ok(_) => {
                                        ice_rpc::gen::log::error!(
                                            "[{}::{}] request payload is another method's variant", <
                                            CalculatorProxy > ::SERVICE_NAME, "add"
                                        );
                                        let _ = ice_rpc::gen::emit_rpc_error(
                                            ice_rpc::gen::RpcError::SerializationError,
                                            &mut *emitter,
                                        );
                                    }
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
struct __CalculatorServiceInitDefault(std::sync::Arc<dyn Calculator>);
#[allow(missing_docs)]
#[async_trait::async_trait]
impl ice_rpc::ServiceInit for __CalculatorServiceInitDefault {}
#[allow(missing_docs)]
pub enum CalculatorMode {
    Provider {
        local_impl: std::sync::Arc<dyn Calculator>,
        init_hook: std::sync::Arc<dyn ice_rpc::ServiceInit>,
        server_started: bool,
    },
    Consumer { ipc_client: std::sync::Arc<CalculatorClient> },
}
#[allow(missing_docs)]
pub struct CalculatorProxy {
    mode: ice_rpc::gen::async_lock::RwLock<CalculatorMode>,
    deps: Vec<&'static str>,
}
#[allow(missing_docs)]
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
        ice_rpc::gen::declare_channel_max_slice_len(
            "calculator",
            ice_rpc::gen::DEFAULT_MAX_SLICE_LEN,
        );
        std::sync::Arc::new(Self {
            deps: vec![],
            mode: ice_rpc::gen::async_lock::RwLock::new(CalculatorMode::Consumer {
                ipc_client: CalculatorClient::new(),
            }),
        })
    }
}
#[allow(missing_docs)]
#[async_trait::async_trait]
impl Calculator for CalculatorProxy {
    async fn add(&self, a: i32, b: i32) -> Observable<i32, String> {
        let mode = self.mode.read().await;
        match &*mode {
            CalculatorMode::Provider { local_impl, .. } => {
                ice_rpc::gen::local_call_scoped(
                        <CalculatorProxy>::SERVICE,
                        <CalculatorProxy>::SERVICE_NAME,
                        "add",
                        local_impl.add(a, b),
                    )
                    .await
            }
            CalculatorMode::Consumer { ipc_client } => ipc_client.add(a, b).await,
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
                    ice_rpc::gen::declare_channel_max_slice_len(
                        "calculator",
                        ice_rpc::gen::DEFAULT_MAX_SLICE_LEN,
                    );
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
#[doc(hidden)]
#[no_mangle]
static __ICE_RPC_SVC_calculator: u8 = 0;
