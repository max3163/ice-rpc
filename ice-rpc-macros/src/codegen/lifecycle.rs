//! Codegen: `ServiceLifecycle`, `ServiceInit`, `ServiceNamed` implementations
//! for the proxy.

use proc_macro2::TokenStream;
use quote::quote;
use syn::Ident;

/// Generation parameters of the lifecycle code.
pub struct LifecycleGenInput<'a> {
    pub trait_name: &'a Ident,
    pub proxy_name: &'a Ident,
    pub server_name: &'a Ident,
    pub mode_name: &'a Ident,
    pub logical_name_lit: &'a str,
}

/// Generates the [`ServiceLifecycle`], [`ServiceInit`] and
/// [`ServiceNamed`] implementations for the proxy.
pub fn gen_lifecycle(input: &LifecycleGenInput<'_>) -> TokenStream {
    let LifecycleGenInput {
        trait_name,
        proxy_name,
        server_name,
        mode_name,
        logical_name_lit,
    } = input;

    quote! {
        #[async_trait::async_trait]
        impl ice_rpc::ServiceLifecycle for #proxy_name {
            async fn init(&self) -> bool {
                let mut mode = self.mode.write().await;
                match &mut *mode {
                    #mode_name::ProviderNodeJs => {
                        let svc_name: &'static str = #logical_name_lit;

                        let locator = ice_rpc::ServiceLocator::global();
                        let init_ok = ice_rpc::rt::spawn_blocking_value(move || {
                            if locator.get_node_sync().is_err() {
                                ::log::error!("[{}] Failed to create iceoryx2 Node", svc_name);
                                return false;
                            }
                            locator.start_discovery();
                            true
                        }).await;

                        if !init_ok {
                            ::log::warn!("[{}] NodeJS Provider: Node init failed, retrying...", svc_name);
                            return false;
                        }

                        let handler: ice_rpc::RequestHandler = std::sync::Arc::new({
                            let svc = svc_name;
                            move |hdr: ice_rpc::RpcHeader, raw: &[u8]| {
                                let cid = hdr.correlation_id;
                                let method: &str = hdr.method();
                                let client_pid = hdr.caller_pid;
                                let client_node = ice_rpc::NodeId(client_pid);

                                let args = match #proxy_name::deserialize_request_to_value(method, raw) {
                                    Some(v) => v,
                                    None => {
                                        ::log::error!("[{}::{}] Failed to deserialize request", svc, method);
                                        return;
                                    }
                                };

                                let method_owned: String = method.to_owned();
                                let svc_static: &'static str = svc;

                                ice_rpc::rt::spawn(async move {
                                    let method_for_blocking = method_owned.clone();
                                    let args_for_blocking = args.clone();
                                    let result = match ice_rpc::rt::spawn_blocking_value(move || {
                                        ice_rpc::nodejs_dispatch::call(cid, svc_static, &method_for_blocking, args_for_blocking)
                                    }).await {
                                        Ok(v) => v,
                                        Err(e) => {
                                            ::log::error!("[{}::{}] JS bridge: {}", svc_static, method_owned, e);
                                            return;
                                        }
                                    };

                                    let (response_bytes, event_kind) = match #proxy_name::serialize_response_from_value(&method_owned, result) {
                                        Some((bytes, kind)) => (bytes, kind),
                                        None => {
                                            ::log::error!("[{}::{}] Failed to serialize response", svc_static, method_owned);
                                            return;
                                        }
                                    };

                                    let hub = ice_rpc::ServiceLocator::global().hub();
                                    if !hub.has_publishers(client_node) {
                                        if let Err(e) = hub.ensure_publishers_blocking(client_node) {
                                            ::log::error!("[{}::{}] ensure_publishers: {:?}", svc_static, method_owned, e);
                                            return;
                                        }
                                    }

                                    let resp_hdr = ice_rpc::RpcHeader {
                                        correlation_id: cid,
                                        sent_at_ns: ice_rpc::RpcHeader::now_ns(),
                                        caller_pid: std::process::id(),
                                        service_name: ice_rpc::StaticString::from_bytes_truncated(svc_static.as_bytes()).unwrap_or_default(),
                                        method_name: ice_rpc::StaticString::from_bytes_truncated(method_owned.as_bytes()).unwrap_or_default(),
                                        event_kind,
                                    };

                                    if let Err(e) = hub.send_to_node(client_node, resp_hdr, &response_bytes) {
                                        ::log::error!("[{}::{}] send_to_node: {:?}", svc_static, method_owned, e);
                                    }
                                });
                            }
                        });

                        ice_rpc::ServiceLocator::global().hub().register_request_handler(svc_name, handler);
                        ice_rpc::ServiceLocator::global().start_dispatch_if_needed();

                        ::log::info!("[{}] NodeJS Provider ready.", svc_name);
                        true
                    }
                    #mode_name::Provider { local_impl, init_hook, server_started } => {
                        if !*server_started {
                            if !init_hook.on_init().await {
                                ::log::warn!("[{}] on_init() failed, retrying...",
                                    stringify!(#trait_name));
                                return false;
                            }

                            let locator = ice_rpc::ServiceLocator::global();
                            let init_ok = ice_rpc::rt::spawn_blocking_value(move || {
                                if locator.get_node_sync().is_err() {
                                    return false;
                                }
                                locator.start_discovery();
                                true
                            }).await;

                            if !init_ok {
                                ::log::warn!("[{}] Failed to create the iceoryx2 Node. Retrying...",
                                    stringify!(#trait_name));
                                return false;
                            }

                            let server = #server_name::new(local_impl.clone());

                            let (ready_tx, ready_rx) = ice_rpc::rt::oneshot::channel::<Result<(), String>>();

                            ice_rpc::rt::spawn(async move {
                                match server.run(ready_tx).await {
                                    Ok(()) => {},
                                    Err(_e) => {
                                        let mut backoff_ms = 200u64;
                                        loop {
                                            ::log::warn!("[{}] Restarting the IPC server in {}ms...",
                                                stringify!(#trait_name), backoff_ms);
                                            ice_rpc::rt::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                                            backoff_ms = (backoff_ms * 2).min(5000);
                                            let (dummy_tx, _) = ice_rpc::rt::oneshot::channel();
                                            match server.run(dummy_tx).await {
                                                Ok(()) => break,
                                                Err(e) => {
                                                    ::log::error!("[{}] Restart error: {}",
                                                        stringify!(#trait_name), e);
                                                }
                                            }
                                        }
                                    }
                                }
                            });

                            match ready_rx.await {
                                Ok(Ok(())) => {
                                    *server_started = true;
                                    ice_rpc::ServiceLocator::global().start_dispatch_if_needed();
                                    ::log::info!("[{}] IPC server started and ready.", stringify!(#trait_name));
                                }
                                Ok(Err(e)) => {
                                    ::log::error!("[{}] IPC startup failed: {}. Retrying...",
                                        stringify!(#trait_name), e);
                                    return false;
                                }
                                Err(_) => {
                                    ::log::error!("[{}] run() exited without signaling. Retrying...",
                                        stringify!(#trait_name));
                                    return false;
                                }
                            }
                        }
                        true
                    },
                    #mode_name::Consumer { ipc_client } => ipc_client.init().await,
                }
            }
        }

        impl ice_rpc::ServiceNamed for #proxy_name {
            const SERVICE_NAME: &'static str = #logical_name_lit;
        }

        #[async_trait::async_trait]
        impl ice_rpc::ServiceInit for #proxy_name {
            async fn on_init(&self) -> bool {
                ice_rpc::ServiceLifecycle::init(self).await
            }
            fn dependencies(&self) -> Vec<&'static str> {
                self.deps.clone()
            }
        }
    }
}
