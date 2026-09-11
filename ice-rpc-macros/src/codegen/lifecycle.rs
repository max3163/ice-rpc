//! Codegen: `ServiceLifecycle`, `ServiceInit`, `ServiceNamed` implementations
//! for the proxy.

use proc_macro2::TokenStream;
use quote::quote;
use syn::Ident;

use super::helpers::gen_hub_config;

/// Generation parameters of the lifecycle code.
pub struct LifecycleGenInput<'a> {
    pub trait_name: &'a Ident,
    pub proxy_name: &'a Ident,
    pub server_name: &'a Ident,
    pub mode_name: &'a Ident,
    pub logical_name_lit: &'a str,
    pub allow_large_payload: bool,
    pub default_size_message_kb: Option<u64>,
    pub service_version: u16,
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
        allow_large_payload,
        default_size_message_kb,
        service_version,
    } = input;

    let hub_config = gen_hub_config(*allow_large_payload, *default_size_message_kb);

    quote! {
        #[async_trait::async_trait]
        impl ice_rpc::gen::ServiceLifecycle for #proxy_name {
            async fn init(&self) -> bool {
                #hub_config

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
                        }).await.unwrap_or_else(|panic| {
                            ::log::error!(
                                "[{}] blocking init task panicked: {}",
                                svc_name,
                                panic
                            );
                            false
                        });

                        if !init_ok {
                            ::log::warn!("[{}] NodeJS Provider: Node init failed, retrying...", svc_name);
                            return false;
                        }

                        let handler: ice_rpc::gen::RequestHandler = std::sync::Arc::new({
                            let svc = svc_name;
                            move |hdr: ice_rpc::gen::RpcHeader, caller: ice_rpc::gen::NodeId, raw: &[u8]| {
                                let cid = hdr.correlation_id;
                                let method: &str = hdr.method();
                                // Caller identity from iceoryx2's native header.
                                let client_node = caller;

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
                                        // Outer `Err` = the blocking task panicked;
                                        // inner `Err` = the JS bridge itself failed.
                                        Ok(Ok(v)) => v,
                                        Ok(Err(e)) => {
                                            ::log::error!("[{}::{}] JS bridge: {}", svc_static, method_owned, e);
                                            return;
                                        }
                                        Err(panic) => {
                                            ::log::error!("[{}::{}] JS bridge task panicked: {}", svc_static, method_owned, panic);
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
                                        if let Err(e) = hub.ensure_publishers(client_node) {
                                            ::log::error!("[{}::{}] ensure_publishers: {:?}", svc_static, method_owned, e);
                                            return;
                                        }
                                    }

                                    let resp_hdr = ice_rpc::gen::RpcHeader::response_from(
                                        &hdr,
                                        event_kind,
                                        #service_version,
                                    );

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

                            // Native iceoryx2 request/response service: one
                            // dispatcher per service, one background thread.
                            let dispatcher = #server_name::new(local_impl.clone()).native_dispatcher();
                            ice_rpc::gen::spawn_native_service(
                                #logical_name_lit,
                                move |method, payload| dispatcher.dispatch(method, payload),
                                ice_rpc::global_cancel_token().clone(),
                            );

                            *server_started = true;
                            ::log::info!("[{}] native service started and ready.", stringify!(#trait_name));
                        }
                        true
                    },
                    #mode_name::Consumer { ipc_client } => ipc_client.init().await,
                }
            }
        }

        impl ice_rpc::gen::ServiceNamed for #proxy_name {
            const SERVICE_NAME: &'static str = #logical_name_lit;
        }

        #[async_trait::async_trait]
        impl ice_rpc::ServiceInit for #proxy_name {
            async fn on_init(&self) -> bool {
                ice_rpc::gen::ServiceLifecycle::init(self).await
            }
            fn dependencies(&self) -> Vec<&'static str> {
                self.deps.clone()
            }
        }
    }
}
