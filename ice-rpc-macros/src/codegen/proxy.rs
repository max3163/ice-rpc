//! Codegen: `{Trait}Proxy` struct with Provider, Consumer, ProviderJson modes.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
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
    /// Channel the service belongs to (the `group` of `#[service]`).
    pub group_lit: &'a str,
    /// Expression of the initial slice length of the channel's publishers
    /// (`#[service(max_slice_len = N)]`, or the runtime default).
    pub max_slice_len: &'a TokenStream,
    /// Expression of the shared [`ServiceRef`] of the service (id + version).
    pub service_ref: &'a TokenStream,
    pub node_methods: &'a [TokenStream],
    /// Blocking twins (`{method}_blocking`) emitted on the **inherent** impl, one
    /// per method when the `blocking` parameter of `#[service]` is set.
    pub blocking_methods: &'a [TokenStream],
    /// Callers of the **one-way** methods, emitted on the inherent impl.
    ///
    /// They carry the plain method name: the inherent method wins over the trait
    /// one for a `proxy.method(..)`, so a consumer gets the useful
    /// `Result<bool, RpcError>` while `dyn Trait` keeps the notification shape.
    pub oneway_calls: &'a [TokenStream],
    /// Whether the `ProviderJson` mode and its constructor belong to the
    /// expansion (the `json` block).
    pub json_provider: bool,
    /// One JSON `ServiceDispatcher::method(...)` registration per RPC method.
    ///
    /// They are consumed by [`provide_json`](gen_proxy), **not** by `init()`:
    /// building the dispatcher is the only code that names the JSON handlers, so
    /// a binary that never constructs a `ProviderJson` proxy does not link them.
    /// Empty when `json_provider` is unset.
    pub json_provider_methods: &'a [TokenStream],
}

/// Generates the `{Trait}Proxy` (smart node) with its Provider, Consumer
/// and ProviderJson modes, as well as the constructors.
pub fn gen_proxy(input: &ProxyGenInput<'_>) -> TokenStream {
    let ProxyGenInput {
        trait_name,
        visibility,
        proxy_name,
        client_name,
        mode_name,
        init_default_name,
        logical_name_lit,
        group_lit,
        max_slice_len,
        service_ref,
        node_methods,
        blocking_methods,
        oneway_calls,
        json_provider,
        json_provider_methods,
    } = input;

    // The Node.js mode is a whole variant and constructor, not a flag: without
    // the feature the proxy must not advertise a mode it cannot serve.
    //
    // The variant carries the **already built** dispatcher, opaque here: `init()`
    // only hands it to the transport, so no JSON symbol is reachable from the
    // lifecycle code and a binary that never hosts JSON links none of it.
    let json_variant = if *json_provider {
        quote! {
            ProviderJson {
                dispatcher: Option<Box<ice_rpc::codegen::ServiceDispatcher>>,
            },
        }
    } else {
        quote! {}
    };

    // The variant and the constructor are unused in a crate that does not link
    // the bridge, and the proxy is `pub`: nothing else silences them.
    let json_allow = if *json_provider {
        quote! { #[allow(dead_code)] }
    } else {
        quote! {}
    };

    let provide_json = if *json_provider {
        quote! {
            /// Builds the proxy of the `ProviderJson` mode: the Node.js host
            /// implements the methods, and each call is bridged to it over IPC.
            ///
            /// The dispatcher is built **here**, not in `init()`: this
            /// constructor is the only place that names the JSON handlers, so a
            /// binary that never calls it does not link them at all. `init()`
            /// sees an opaque [`ice_rpc::codegen::ServiceDispatcher`].
            #visibility fn provide_json() -> std::sync::Arc<Self> {
                let mut dispatcher = ice_rpc::codegen::ServiceDispatcher::new(#service_ref);
                #(#json_provider_methods)*
                std::sync::Arc::new(Self {
                    deps: vec![],
                    mode: ice_rpc::codegen::async_lock::RwLock::new(#mode_name::ProviderJson {
                        dispatcher: Some(Box::new(dispatcher)),
                    }),
                })
            }
        }
    } else {
        quote! {}
    };

    quote! {
        struct #init_default_name(std::sync::Arc<dyn #trait_name>);

        #[ice_rpc::codegen::async_trait::async_trait]
        impl ice_rpc::ServiceInit for #init_default_name {}

        #json_allow
        #visibility enum #mode_name {
            Provider {
                local_impl:     std::sync::Arc<dyn #trait_name>,
                init_hook:      std::sync::Arc<dyn ice_rpc::ServiceInit>,
                server_started: bool,
            },
            Consumer { ipc_client: std::sync::Arc<#client_name> },
            #json_variant
        }

        #visibility struct #proxy_name {
            mode: ice_rpc::codegen::async_lock::RwLock<#mode_name>,
            deps: Vec<&'static str>,
        }

        #json_allow
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
                    mode: ice_rpc::codegen::async_lock::RwLock::new(#mode_name::Provider {
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
                    mode: ice_rpc::codegen::async_lock::RwLock::new(#mode_name::Provider {
                        local_impl:     arc.clone() as std::sync::Arc<dyn #trait_name>,
                        init_hook:      arc         as std::sync::Arc<dyn ice_rpc::ServiceInit>,
                        server_started: false,
                    }),
                })
            }

            #visibility fn consume() -> std::sync::Arc<Self> {
                // The channel's publishers are sized from this declaration; being
                // a property of the channel, it is recorded once here rather than
                // carried by every call.
                ice_rpc::codegen::declare_channel_max_slice_len(#group_lit, #max_slice_len);
                std::sync::Arc::new(Self {
                    deps: vec![],
                    mode: ice_rpc::codegen::async_lock::RwLock::new(#mode_name::Consumer {
                        ipc_client: #client_name::new(),
                    }),
                })
            }

            /// Whether a provider is currently connected to this service's
            /// channel.
            ///
            /// Synchronous and **read-only**: it opens the response direction of
            /// the channel without creating it and reports whether a
            /// provider-side publisher is attached. `false` therefore means no
            /// provider is serving the channel right now — the process is not
            /// started, has stopped, or failed to open its channel — which is
            /// what a readiness check, or a guard before an optional call, wants
            /// to know.
            ///
            /// The answer is per **channel** (the `group` of `#[service]`):
            /// every service sharing it shares the answer, so it says whether
            /// *some* provider serves the channel, not which service of it is
            /// answering.
            ///
            /// # Cost
            /// ~40 ns once this process has consumed the channel (the ports it
            /// keeps are read, not opened). The first probe on a channel it
            /// never consumed opens the service first (~0.13 ms in release,
            /// ~1 ms in a debug build): poll it, as a readiness endpoint does,
            /// rather than calling it before every RPC.
            ///
            /// # Errors
            /// Returns an [`ice_rpc::RpcError`] when the channel exists but
            /// cannot be opened — a configuration mismatch with its creator, or
            /// state left behind by a process killed while it held it. Those are
            /// reported rather than folded into `false`, because a "not
            /// connected" answer would hide a deployment problem no amount of
            /// retrying resolves.
            #visibility fn is_provider_connected() -> Result<bool, ice_rpc::RpcError> {
                ice_rpc::codegen::is_provider_connected(#group_lit)
            }

            #provide_json

            #(#oneway_calls)*

            #(#blocking_methods)*
        }

        #[ice_rpc::codegen::async_trait::async_trait]
        impl #trait_name for #proxy_name {
            #(#node_methods)*
        }

        impl ice_rpc::codegen::ServiceConsumer for #proxy_name {
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
    /// Visibility of the proxy type, carried onto the inherent blocking twin.
    pub visibility: &'a Visibility,
    /// Whether the `ProviderJson` arm belongs to the expansion.
    pub json_provider: bool,
}

/// Generates the body of a proxy delegation method.
///
/// In Provider mode, calls the local implementation (in-process) through
/// `ice_rpc::codegen::local_call_scoped`: a plain `.await` unless the `tracing`
/// feature is on, in which case the callee gets its own `CallContext` and a span
/// parented on the caller's. The `#[cfg]` lives in that helper, never here — this
/// file is compiled by `ice-rpc-macros`, which does not carry the feature.
///
/// In Consumer mode, calls the IPC client.
/// In ProviderJson mode — only when `json_provider` is set — returns an error: the
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
        visibility: _,
        json_provider,
    } = input;

    let method_name_str = fn_name.to_string();

    let json_arm = if *json_provider {
        quote! {
            #mode_name::ProviderJson { .. } => {
                ice_rpc::Observable::from_technical_error(ice_rpc::RpcError::Internal(
                    "ProviderJson: direct calls are not supported — use IPC".into()
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
                    ice_rpc::codegen::local_call_scoped(
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
                #json_arm
            }
        }
    }
}

/// Generates the **inherent** blocking twin of a proxy method.
///
/// It reads the mode with `read_blocking` — the lock read is the only reason the
/// delegating method was `async` — and returns the `Observable` directly: the
/// Consumer arm calls the client's blocking twin, which publishes with no
/// future, and the Provider arm drives the in-process call with
/// [`ice_rpc::rt::block_on`], the one future the light facade keeps.
pub fn gen_proxy_blocking_method(input: &ProxyMethodGenInput<'_>) -> TokenStream {
    let ProxyMethodGenInput {
        fn_name,
        arg_names,
        arg_types,
        output_type,
        mode_name,
        service_ref,
        service_name,
        visibility,
        json_provider,
    } = input;

    let method_name_str = fn_name.to_string();
    let blocking_name = format_ident!("{}_blocking", fn_name);
    let blocking_doc = format!(
        "Blocking twin of [`{fn_name}`]: reads the mode without an `.await` and \
         returns the response stream directly, for a caller that runs no async \
         runtime."
    );

    let json_arm = if *json_provider {
        quote! {
            #mode_name::ProviderJson { .. } => {
                ice_rpc::Observable::from_technical_error(ice_rpc::RpcError::Internal(
                    "ProviderJson: direct calls are not supported — use IPC".into()
                ))
            }
        }
    } else {
        quote! {}
    };

    quote! {
        #[doc = #blocking_doc]
        #visibility fn #blocking_name(&self, #(#arg_names: #arg_types),*) -> #output_type {
            let mode = self.mode.read_blocking();
            match &*mode {
                #mode_name::Provider { local_impl, .. } => {
                    ice_rpc::rt::block_on(ice_rpc::codegen::local_call_scoped(
                        #service_ref,
                        #service_name,
                        #method_name_str,
                        local_impl.#fn_name(#(#arg_names),*),
                    ))
                },
                #mode_name::Consumer { ipc_client } => {
                    ipc_client.#blocking_name(#(#arg_names),*)
                }
                #json_arm
            }
        }
    }
}

/// Parameters of one **one-way** proxy method.
pub struct ProxyOnewayGenInput<'a> {
    /// The RPC method delegated to.
    pub fn_name: &'a Ident,
    /// Parameter names, in declaration order.
    pub arg_names: &'a [&'a Ident],
    /// Parameter types, in declaration order.
    pub arg_types: &'a [&'a syn::Type],
    /// The generated mode enum (`{Trait}Mode`).
    pub mode_name: &'a Ident,
    /// Visibility of the proxy type.
    pub visibility: &'a Visibility,
    /// Whether the `ProviderJson` arm belongs to the expansion.
    pub json_provider: bool,
}

/// Generates the **trait implementation** body of a one-way method.
///
/// The proxy implements the service trait so it can stand in for the
/// implementation (`dyn Trait`, the gateway), and the shape is exactly the
/// declared one: an `async fn` returning nothing. The emitter's `delivered`
/// answer has nowhere to go here, which is why the inherent caller below exists —
/// `proxy.method(..)` resolves to *it*, not to this.
pub fn gen_proxy_oneway_method(input: &ProxyOnewayGenInput<'_>) -> TokenStream {
    let ProxyOnewayGenInput {
        fn_name,
        arg_names,
        arg_types,
        mode_name,
        visibility: _,
        json_provider,
    } = input;

    let json_arm = if *json_provider {
        quote! {
            #mode_name::ProviderJson { .. } => {
                // A notification cannot be routed through the JSON bridge: the
                // bridge answers on behalf of a host, and a one-way call has no
                // answer to fetch.
                ice_rpc::codegen::log::warn!(
                    "[{}] a one-way method cannot be served in ProviderJson mode",
                    <Self as ice_rpc::codegen::ServiceNamed>::SERVICE_NAME
                );
            }
        }
    } else {
        quote! {}
    };

    quote! {
        async fn #fn_name(&self, #(#arg_names: #arg_types),*) {
            let mode = self.mode.read().await;
            match &*mode {
                #mode_name::Provider { local_impl, .. } => {
                    local_impl.#fn_name(#(#arg_names),*).await;
                }
                #mode_name::Consumer { ipc_client } => {
                    // The outcome is the emitter's local observation, and this
                    // signature has no way to report it: the inherent caller is
                    // where a consumer reads it.
                    let _ = ipc_client.#fn_name(#(#arg_names),*);
                }
                #json_arm
            }
        }
    }
}

/// Generates the **inherent** caller of a one-way method.
///
/// Synchronous on purpose: a notification has nothing to wait for, so it needs
/// neither an executor nor a future. In Provider mode the local implementation is
/// driven with `rt::block_on` — the primitive the `{method}_blocking` twins use —
/// and the sample counts as delivered, since the sink is right there.
pub fn gen_proxy_oneway_call(input: &ProxyOnewayGenInput<'_>) -> TokenStream {
    let ProxyOnewayGenInput {
        fn_name,
        arg_names,
        arg_types,
        mode_name,
        visibility,
        json_provider,
    } = input;

    let doc = format!(
        "One-way call of [`{fn_name}`]: publishes the notification and returns. \
         `Ok(true)` means at least one subscriber took the sample, `Ok(false)` that \
         nobody did — an expected outcome, not an error. In Provider mode the local \
         implementation is called and the answer is always `Ok(true)`."
    );

    let json_arm = if *json_provider {
        quote! {
            #mode_name::ProviderJson { .. } => Err(ice_rpc::RpcError::Internal(
                "ProviderJson: direct calls are not supported — use IPC".into()
            )),
        }
    } else {
        quote! {}
    };

    quote! {
        #[doc = #doc]
        #visibility fn #fn_name(&self, #(#arg_names: #arg_types),*)
            -> Result<bool, ice_rpc::RpcError>
        {
            let mode = self.mode.read_blocking();
            match &*mode {
                #mode_name::Provider { local_impl, .. } => {
                    ice_rpc::rt::block_on(local_impl.#fn_name(#(#arg_names),*));
                    Ok(true)
                }
                #mode_name::Consumer { ipc_client } => {
                    ipc_client.#fn_name(#(#arg_names),*)
                }
                #json_arm
            }
        }
    }
}
