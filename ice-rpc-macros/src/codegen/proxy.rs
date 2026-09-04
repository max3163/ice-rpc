//! Codegen: `{Trait}Proxy` struct with Provider, Consumer, ProviderNodeJs modes.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, Visibility};

/// Proxy generation parameters.
pub struct ProxyGenInput<'a> {
    pub trait_name: &'a Ident,
    pub visibility: &'a Visibility,
    pub proxy_name: &'a Ident,
    pub client_name: &'a Ident,
    pub mode_name: &'a Ident,
    pub init_default_name: &'a Ident,
    pub logical_name_lit: &'a str,
    pub node_methods: &'a [TokenStream],
}

/// Generates the `{Trait}Proxy` (smart node) with its Provider, Consumer
/// and ProviderNodeJs modes, as well as the constructors.
pub fn gen_proxy(input: &ProxyGenInput<'_>) -> TokenStream {
    let ProxyGenInput {
        trait_name,
        visibility,
        proxy_name,
        client_name,
        mode_name,
        init_default_name,
        logical_name_lit,
        node_methods,
    } = input;

    quote! {
        struct #init_default_name(std::sync::Arc<dyn #trait_name>);

        #[async_trait::async_trait]
        impl ice_rpc::ServiceInit for #init_default_name {}

        #visibility enum #mode_name {
            Provider {
                local_impl:     std::sync::Arc<dyn #trait_name>,
                init_hook:      std::sync::Arc<dyn ice_rpc::ServiceInit>,
                server_started: bool,
            },
            Consumer { ipc_client: std::sync::Arc<#client_name> },
            #[allow(dead_code)]
            ProviderNodeJs,
        }

        #visibility struct #proxy_name {
            mode: ice_rpc::async_lock::RwLock<#mode_name>,
            deps: Vec<&'static str>,
        }

        impl #proxy_name {
            /// Logical name of the service, injected by the `#[service]` macro.
            pub const SERVICE_NAME: &'static str = #logical_name_lit;

            #visibility fn provide<T>(implementation: T) -> std::sync::Arc<Self>
            where T: #trait_name + Send + Sync + 'static
            {
                let arc       = std::sync::Arc::new(implementation);
                let init_hook = std::sync::Arc::new(#init_default_name(
                    arc.clone() as std::sync::Arc<dyn #trait_name>
                ));
                std::sync::Arc::new(Self {
                    deps: vec![],
                    mode: ice_rpc::async_lock::RwLock::new(#mode_name::Provider {
                        local_impl:     arc       as std::sync::Arc<dyn #trait_name>,
                        init_hook:      init_hook as std::sync::Arc<dyn ice_rpc::ServiceInit>,
                        server_started: false,
                    }),
                })
            }

            #visibility fn provide_with_init<T>(implementation: T) -> std::sync::Arc<Self>
            where T: #trait_name + ice_rpc::ServiceInit + Send + Sync + 'static
            {
                let arc  = std::sync::Arc::new(implementation);
                let deps = arc.dependencies();
                std::sync::Arc::new(Self {
                    deps,
                    mode: ice_rpc::async_lock::RwLock::new(#mode_name::Provider {
                        local_impl:     arc.clone() as std::sync::Arc<dyn #trait_name>,
                        init_hook:      arc         as std::sync::Arc<dyn ice_rpc::ServiceInit>,
                        server_started: false,
                    }),
                })
            }

            #visibility fn consume() -> std::sync::Arc<Self> {
                std::sync::Arc::new(Self {
                    deps: vec![],
                    mode: ice_rpc::async_lock::RwLock::new(#mode_name::Consumer {
                        ipc_client: #client_name::new(),
                    }),
                })
            }

            #[allow(dead_code)]
            #visibility fn provide_nodejs() -> std::sync::Arc<Self> {
                std::sync::Arc::new(Self {
                    deps: vec![],
                    mode: ice_rpc::async_lock::RwLock::new(#mode_name::ProviderNodeJs),
                })
            }
        }

        #[async_trait::async_trait]
        impl #trait_name for #proxy_name {
            #(#node_methods)*
        }

        impl ice_rpc::ServiceConsumer for #proxy_name {
            fn consume_proxy() -> std::sync::Arc<Self> {
                #proxy_name::consume()
            }
        }
    }
}

/// Generates the body of a proxy delegation method.
///
/// In Provider mode, calls the local implementation (in-process).
/// In Consumer mode, calls the IPC client.
/// In ProviderNodeJs mode, returns an error (calls go through IPC).
pub fn gen_proxy_method(
    fn_name: &Ident,
    arg_names: &[&Ident],
    arg_types: &[&syn::Type],
    output_type: &syn::Type,
    mode_name: &Ident,
) -> TokenStream {
    quote! {
        async fn #fn_name(&self, #(#arg_names: #arg_types),*) -> #output_type {
            let mode = self.mode.read().await;
            match &*mode {
                #mode_name::Provider { local_impl, .. } => {
                    local_impl.#fn_name(#(#arg_names),*).await
                },
                #mode_name::Consumer { ipc_client } => {
                    ipc_client.#fn_name(#(#arg_names),*).await
                }
                #mode_name::ProviderNodeJs => {
                    Err(ice_rpc::RpcError::Internal(
                        "ProviderNodeJs: direct calls are not supported — use IPC".into()
                    ))
                }
            }
        }
    }
}
