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
    /// Whether the `ProviderNodeJs` mode and its constructor belong to the
    /// expansion (the `nodejs` feature).
    pub nodejs: bool,
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
        nodejs,
    } = input;

    // The Node.js mode is a whole variant and constructor, not a flag: without
    // the feature the proxy must not advertise a mode it cannot serve.
    let nodejs_variant = if *nodejs {
        quote! { ProviderNodeJs, }
    } else {
        quote! {}
    };

    // The variant and the constructor are unused in a crate that does not link
    // the bridge, and the proxy is `pub`: nothing else silences them.
    let nodejs_allow = if *nodejs {
        quote! { #[allow(dead_code)] }
    } else {
        quote! {}
    };

    let provide_nodejs = if *nodejs {
        quote! {
            /// Builds the proxy of the `ProviderNodeJs` mode: the Node.js host
            /// implements the methods, and each call is bridged to it over IPC.
            #visibility fn provide_nodejs() -> std::sync::Arc<Self> {
                std::sync::Arc::new(Self {
                    deps: vec![],
                    mode: ice_rpc::gen::async_lock::RwLock::new(#mode_name::ProviderNodeJs),
                })
            }
        }
    } else {
        quote! {}
    };

    quote! {
        struct #init_default_name(std::sync::Arc<dyn #trait_name>);

        #[async_trait::async_trait]
        impl ice_rpc::ServiceInit for #init_default_name {}

        #nodejs_allow
        #visibility enum #mode_name {
            Provider {
                local_impl:     std::sync::Arc<dyn #trait_name>,
                init_hook:      std::sync::Arc<dyn ice_rpc::ServiceInit>,
                server_started: bool,
            },
            Consumer { ipc_client: std::sync::Arc<#client_name> },
            #nodejs_variant
        }

        #visibility struct #proxy_name {
            mode: ice_rpc::gen::async_lock::RwLock<#mode_name>,
            deps: Vec<&'static str>,
        }

        #nodejs_allow
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
                    mode: ice_rpc::gen::async_lock::RwLock::new(#mode_name::Provider {
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
                    mode: ice_rpc::gen::async_lock::RwLock::new(#mode_name::Provider {
                        local_impl:     arc.clone() as std::sync::Arc<dyn #trait_name>,
                        init_hook:      arc         as std::sync::Arc<dyn ice_rpc::ServiceInit>,
                        server_started: false,
                    }),
                })
            }

            #visibility fn consume() -> std::sync::Arc<Self> {
                std::sync::Arc::new(Self {
                    deps: vec![],
                    mode: ice_rpc::gen::async_lock::RwLock::new(#mode_name::Consumer {
                        ipc_client: #client_name::new(),
                    }),
                })
            }

            #provide_nodejs
        }

        #[async_trait::async_trait]
        impl #trait_name for #proxy_name {
            #(#node_methods)*
        }

        impl ice_rpc::gen::ServiceConsumer for #proxy_name {
            fn consume_proxy() -> std::sync::Arc<Self> {
                #proxy_name::consume()
            }
        }
    }
}

/// Parameters of one proxy delegation method.
pub struct ProxyMethodGenInput<'a> {
    /// The RPC method being delegated to.
    pub fn_name: &'a Ident,
    /// Parameter names, in declaration order.
    pub arg_names: &'a [&'a Ident],
    /// Parameter types, in declaration order.
    pub arg_types: &'a [&'a syn::Type],
    /// The method's declared return type (the `Observable`).
    pub output_type: &'a syn::Type,
    /// The generated mode enum (`{Trait}Mode`).
    pub mode_name: &'a Ident,
    /// The service identity constant: `<Proxy>::SERVICE`.
    pub service_ref: &'a TokenStream,
    /// The service **name** constant: `<Proxy>::SERVICE_NAME`.
    pub service_name: &'a TokenStream,
    /// Whether the `ProviderNodeJs` arm belongs to the expansion.
    pub nodejs: bool,
}

/// Generates the body of a proxy delegation method.
///
/// In Provider mode, calls the local implementation (in-process) through
/// `ice_rpc::gen::local_call_scoped`: a plain `.await` unless the `tracing`
/// feature is on, in which case the callee gets its own `CallContext` and a span
/// parented on the caller's. The `#[cfg]` lives in that helper, never here — this
/// file is compiled by `ice-rpc-macros`, which does not carry the feature.
///
/// In Consumer mode, calls the IPC client.
/// In ProviderNodeJs mode — only when `nodejs` is set — returns an error: the
/// calls go through IPC to the channel the bridge registered.
pub fn gen_proxy_method(input: &ProxyMethodGenInput<'_>) -> TokenStream {
    let ProxyMethodGenInput {
        fn_name,
        arg_names,
        arg_types,
        output_type,
        mode_name,
        service_ref,
        service_name,
        nodejs,
    } = input;

    let method_name_str = fn_name.to_string();

    let nodejs_arm = if *nodejs {
        quote! {
            #mode_name::ProviderNodeJs => {
                ice_rpc::Observable::from_technical_error(ice_rpc::RpcError::Internal(
                    "ProviderNodeJs: direct calls are not supported — use IPC".into()
                ))
            }
        }
    } else {
        quote! {}
    };

    quote! {
        async fn #fn_name(&self, #(#arg_names: #arg_types),*) -> #output_type {
            let mode = self.mode.read().await;
            match &*mode {
                #mode_name::Provider { local_impl, .. } => {
                    // A direct call carries no wire, so its context is built here
                    // from the callee's own identity
                    ice_rpc::gen::local_call_scoped(
                        #service_ref,
                        #service_name,
                        #method_name_str,
                        local_impl.#fn_name(#(#arg_names),*),
                    )
                    .await
                },
                #mode_name::Consumer { ipc_client } => {
                    ipc_client.#fn_name(#(#arg_names),*).await
                }
                #nodejs_arm
            }
        }
    }
}
