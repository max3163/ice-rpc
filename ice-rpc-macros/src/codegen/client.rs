//! Codegen: `{Trait}Client` struct and its IPC consumption methods.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, Type, Visibility};

use super::helpers::gen_hub_config;

/// Client generation parameters.
pub struct ClientGenInput<'a> {
    pub visibility: &'a Visibility,
    pub client_name: &'a Ident,
    pub logical_name: &'a str,
    pub client_methods: &'a [TokenStream],
    pub allow_large_payload: bool,
    pub default_size_message_kb: Option<u64>,
}

/// Generates the `{Trait}Client` struct with the `new()` constructor.
///
/// The constructor creates the client core holding the connection state.
pub fn gen_client_struct(input: &ClientGenInput<'_>) -> TokenStream {
    let ClientGenInput {
        visibility,
        client_name,
        logical_name,
        client_methods,
        allow_large_payload,
        default_size_message_kb,
        ..
    } = input;

    let hub_config = gen_hub_config(*allow_large_payload, *default_size_message_kb);

    quote! {
        #visibility struct #client_name {
            core: ice_rpc::gen::ClientCore,
        }

        impl #client_name {
            #visibility fn new() -> std::sync::Arc<Self> {
                #hub_config

                std::sync::Arc::new(Self {
                    core: ice_rpc::gen::ClientCore::new(#logical_name),
                })
            }

            #(#client_methods)*
        }
    }
}

/// Generates the [`ServiceLifecycle::init`] implementation for the client.
///
/// # Initialization flow
/// 1. Create the iceoryx2 Node.
/// 2. Start the discovery channel (NODE_REGISTRY listener).
/// 3. Locate the provider Node (cache + Blackboard).
/// 4. Start the dispatch loop.
/// 5. Pre-create the publishers towards the provider.
/// 6. Populate the atomic cache of the target NodeId.
pub fn gen_client_lifecycle(input: &ClientGenInput<'_>) -> TokenStream {
    let ClientGenInput {
        logical_name,
        client_name,
        allow_large_payload,
        default_size_message_kb,
        ..
    } = input;

    let hub_config = gen_hub_config(*allow_large_payload, *default_size_message_kb);

    quote! {
        #[async_trait::async_trait]
        impl ice_rpc::ServiceLifecycle for #client_name {
            async fn init(&self) -> bool {
                #hub_config
                self.core.init(#logical_name).await
            }
        }
    }
}

/// Generates the body of a client RPC method.
///
/// # Call flow
/// 1. Serialization of the request (rkyv).
/// 2. Location of the target Node (atomic cache → locate_service).
/// 3. Registration of the reconnection callback (idempotent).
/// 4. Creation of the response channel + handler.
/// 5. Registration of the response handler.
/// 6. `send_to_node`.
pub struct ClientMethodGenInput<'a> {
    pub visibility: &'a Visibility,
    pub fn_name: &'a Ident,
    pub var_name: &'a Ident,
    pub arg_names: &'a [&'a Ident],
    pub arg_types: &'a [&'a Type],
    pub ok_type: &'a Type,
    pub err_type: &'a Type,
    pub req_enum_name: &'a Ident,
    pub logical_name: &'a str,
    /// Discovery timeout in seconds, shared by every method of the service
    /// (set once via `#[service(..., discovery_timeout = "5s")]`).
    pub discovery_timeout_secs: Option<u64>,
    pub service_version: u16,
}

pub fn gen_client_method(input: &ClientMethodGenInput) -> TokenStream {
    let visibility = input.visibility;
    let fn_name = input.fn_name;
    let var_name = input.var_name;
    let arg_names = input.arg_names;
    let arg_types = input.arg_types;
    let ok_type = input.ok_type;
    let err_type = input.err_type;
    let req_enum_name = input.req_enum_name;
    let logical_name = input.logical_name;
    let service_version = input.service_version;

    let method_name_str = fn_name.to_string();
    // Service-wide discovery deadline; mirrors `RPC_CALL_TIMEOUT_SECS` (30s).
    let locate_timeout = input.discovery_timeout_secs.unwrap_or(30);

    // Response handler.
    let handler_body: TokenStream = quote! {
        std::sync::Arc::new(move |result: Result<&[u8], ice_rpc::RpcError>| {
            match result {
                Ok(bytes) => {
                    match ice_rpc::rkyv::from_bytes::<
                        ice_rpc::WireEvent<#ok_type, #err_type>,
                        ice_rpc::rkyv::rancor::Error
                    >(bytes) {
                        Ok(ice_rpc::WireEvent::CompleteWith(v)) => {
                            // Transport shortcut: single sample carrying the last value.
                            let _ = tx.try_send_next(v);
                            let _ = tx.try_send_complete();
                        }
                        Ok(ice_rpc::WireEvent::Next(v)) => {
                            let _ = tx.try_send_next(v);
                        }
                        Ok(ice_rpc::WireEvent::Complete) => {
                            let _ = tx.try_send_complete();
                        }
                        Ok(ice_rpc::WireEvent::Error(e)) => {
                            let _ = tx.try_send_error(e);
                        }
                        Ok(ice_rpc::WireEvent::RpcError(e)) => {
                            let _ = tx.try_send_event(ice_rpc::Event::Error(
                                ice_rpc::ObservableError::Technical(e)
                            ));
                        }
                        Err(_) => {
                            let _ = tx.try_send_event(ice_rpc::Event::Error(
                                ice_rpc::ObservableError::Technical(
                                    ice_rpc::RpcError::SerializationError
                                )
                            ));
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.try_send_event(ice_rpc::Event::Error(
                        ice_rpc::ObservableError::Technical(e)
                    ));
                }
            }
        })
    };

    // ── Single method body ──────────────────────────────────────────
    quote! {
        #visibility async fn #fn_name(&self, #(#arg_names: #arg_types),*)
            -> ice_rpc::Observable<#ok_type, #err_type>
        {
            let req_val = #req_enum_name::#var_name { #(#arg_names),* };

            let bytes = match ice_rpc::rkyv::to_bytes::<ice_rpc::rkyv::rancor::Error>(&req_val) {
                Ok(bytes) => bytes,
                Err(_) => {
                    return ice_rpc::Stream::from_technical_error(
                        ice_rpc::RpcError::SerializationError,
                    );
                }
            };

            // ── IPC call ─────────────────────────────────────────────
            let svc_name = #logical_name;

            let target_node = match self.core.resolve_target(svc_name, #locate_timeout).await {
                Ok(node) => node,
                Err(e) => return ice_rpc::Stream::from_technical_error(e),
            };

            let rpc_header = ice_rpc::RpcHeader::request(
                svc_name,
                #method_name_str,
                #service_version,
            );
            let correlation_id = rpc_header.correlation_id;

            let (tx, rx) = ice_rpc::channel::<#ok_type, #err_type>(8);

            let handler: std::sync::Arc<dyn Fn(Result<&[u8], ice_rpc::RpcError>) + Send + Sync>
                = #handler_body;

            let hub = ice_rpc::ServiceLocator::global().hub();

            if !hub.has_publishers(target_node) {
                let hub2 = ice_rpc::ServiceLocator::global().hub();
                let node = target_node;
                ice_rpc::rt::spawn_blocking(move || {
                    if let Err(e) = hub2.ensure_publishers(node) {
                        ::log::error!("[{}Client] ensure_publishers (fallback): {}", #logical_name, e);
                    }
                }).await;
            }

            hub.register_response_handler(correlation_id, handler);
            hub.register_pending_call(correlation_id, target_node.0);

            if let Err(e) = hub.send_to_node(target_node, rpc_header, &bytes) {
                hub.remove_response_handler(&correlation_id);
                return ice_rpc::Stream::from_technical_error(e);
            }

            rx
        }
    }
}
