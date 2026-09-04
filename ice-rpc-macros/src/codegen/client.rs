//! Codegen: `{Trait}Client` struct and its IPC consumption methods.
//!
//! Supports the `#[cache(ttl = "60s")]` attribute on the methods
//! to enable a local TTL cache on the consumer side.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, Type, Visibility};

use super::helpers::gen_hub_config;

/// Cache configuration for an RPC method.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Cache lifetime in seconds.
    pub ttl_secs: u64,
    /// Maximum number of entries in the cache (default: 1024).
    pub max_entries: usize,
}

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
/// The constructor creates the atomic flags, the reconnection callback
/// and the per-method caches if the `#[cache]` attribute is present.
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

/// Generates the body of a client RPC method, with optional cache support.
///
/// # Call flow (without cache)
/// 1. Serialization of the request (rkyv).
/// 2. Location of the target Node (atomic cache → locate_service).
/// 3. Registration of the reconnection callback (idempotent).
/// 4. Creation of the response channel + handler.
/// 5. Registration of the response handler.
/// 6. `send_to_node`.
///
/// # Call flow (with cache)
/// 1. Computation of the cache key (hash of the rkyv bytes).
/// 2. Cache lookup → on hit, immediate return (synthetic stream).
/// 3. Otherwise, normal IPC call, and storage in the cache on return.
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
    pub cache_config: Option<&'a CacheConfig>,
    pub timeout_secs: Option<u64>,
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
    let cache_config = input.cache_config;
    let timeout_secs = input.timeout_secs;
    let service_version = input.service_version;

    let method_name_str = fn_name.to_string();
    let locate_timeout = timeout_secs.unwrap_or(30); // RPC_CALL_TIMEOUT_SECS default

    // Conditional blocks: inserted only if the cache is enabled.
    let cache_init_block: TokenStream = if let Some(cc) = cache_config {
        let ttl_secs = cc.ttl_secs;
        let max_entries = cc.max_entries;
        quote! {
            static CACHE: std::sync::OnceLock<
                ice_rpc::RpcCache<Vec<u8>>
            > = std::sync::OnceLock::new();
            let cache = CACHE.get_or_init(|| {
                ice_rpc::RpcCache::with_max_entries(
                    std::time::Duration::from_secs(#ttl_secs),
                    #max_entries,
                )
            });
            let cache_key = ice_rpc::hash_bytes(&bytes);
            if let Some(cached_bytes) = cache.get(cache_key) {
                if let Ok(event) = ice_rpc::rkyv::from_bytes::<
                    ice_rpc::Event<#ok_type, #err_type>,
                    ice_rpc::rkyv::rancor::Error
                >(&cached_bytes) {
                    let (tx, rx) = ice_rpc::channel::<#ok_type, #err_type>(2);
                    let _ = tx.try_send(event);
                    let _ = tx.try_send(ice_rpc::Event::Complete);
                    return Ok(rx);
                }
                cache.clear();
            }
        }
    } else {
        TokenStream::new()
    };

    // Response handler: with or without cache storage.
    let handler_body: TokenStream = if cache_config.is_some() {
        quote! {
            {
                let cache_key_store = cache_key;
                let cache_ref = cache;
                std::sync::Arc::new(move |result: Result<&[u8], ice_rpc::RpcError>| {
                    match result {
                        Ok(response_bytes) => {
                            match ice_rpc::rkyv::from_bytes::<
                                ice_rpc::Event<#ok_type, #err_type>,
                                ice_rpc::rkyv::rancor::Error
                            >(response_bytes) {
                                Ok(event) => {
                                    // Only cache Next values (not Complete/Error).
                                    if matches!(event, ice_rpc::Event::Next(_)) {
                                        cache_ref.insert(cache_key_store, response_bytes.to_vec());
                                    }
                                    let _ = tx.try_send(event);
                                }
                                Err(_) => {
                                    let _ = tx.try_send(ice_rpc::Event::RpcError(
                                        ice_rpc::RpcError::SerializationError
                                    ));
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.try_send(ice_rpc::Event::RpcError(e));
                        }
                    }
                })
            }
        }
    } else {
        quote! {
            std::sync::Arc::new(move |result: Result<&[u8], ice_rpc::RpcError>| {
                match result {
                    Ok(bytes) => {
                        match ice_rpc::rkyv::from_bytes::<
                            ice_rpc::Event<#ok_type, #err_type>,
                            ice_rpc::rkyv::rancor::Error
                        >(bytes) {
                            Ok(event) => {
                                let _ = tx.try_send(event);
                            }
                            Err(_) => {
                                let _ = tx.try_send(ice_rpc::Event::RpcError(
                                    ice_rpc::RpcError::SerializationError
                                ));
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.try_send(ice_rpc::Event::RpcError(e));
                    }
                }
            })
        }
    };

    // ── Single method body (optional cache) ─────────────────────────
    quote! {
        #visibility async fn #fn_name(&self, #(#arg_names: #arg_types),*)
            -> Result<ice_rpc::Stream<#ok_type, #err_type>, ice_rpc::RpcError>
        {
            let req_val = #req_enum_name::#var_name { #(#arg_names),* };

            let bytes = ice_rpc::rkyv::to_bytes::<ice_rpc::rkyv::rancor::Error>(&req_val)
                .map_err(|_| ice_rpc::RpcError::SerializationError)?;

            // ── Cache (if enabled) ───────────────────────────────────
            #cache_init_block

            // ── IPC call ─────────────────────────────────────────────
            let svc_name = #logical_name;

            let target_node = self.core.resolve_target(svc_name, #locate_timeout).await?;

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
                return Err(e);
            }

            Ok(rx)
        }
    }
}
