//! Codegen: `ServiceLifecycle`, `ServiceInit` and `ServiceNamed`
//! implementations for the proxy.

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
    /// Channel the service is registered on (the `group` of `#[service]`).
    pub group_lit: &'a str,
    /// One native `ServiceDispatcher::method(...)` registration per RPC method,
    /// used by the `ProviderNodeJs` mode to bridge calls to the Node.js host.
    pub nodejs_native_methods: &'a [TokenStream],
}

/// Generates the [`ServiceLifecycle`](ice_rpc), `ServiceInit` and
/// `ServiceNamed` implementations for the proxy.
pub fn gen_lifecycle(input: &LifecycleGenInput<'_>) -> TokenStream {
    let LifecycleGenInput {
        trait_name,
        proxy_name,
        server_name,
        mode_name,
        logical_name_lit,
        group_lit,
        nodejs_native_methods,
    } = input;

    quote! {
        #[async_trait::async_trait]
        impl ice_rpc::gen::ServiceLifecycle for #proxy_name {
            async fn init(&self) -> bool {
                let mut mode = self.mode.write().await;
                match &mut *mode {
                    #mode_name::ProviderNodeJs => {
                        // The Node.js host implements the methods: each RPC
                        // method is bridged to the injected dispatch callback.
                        let mut dispatcher = ice_rpc::gen::ServiceDispatcher::new();
                        #(#nodejs_native_methods)*
                        // Registered on the channel: the channel thread starts
                        // once every provider of the process is registered.
                        if let Err(e) = ice_rpc::gen::register_native_service(
                            #group_lit,
                            ice_rpc::gen::service_id_of(#logical_name_lit),
                            #logical_name_lit,
                            dispatcher,
                        ) {
                            ::log::error!(
                                "[{}] channel registration failed: {e:?}",
                                #logical_name_lit
                            );
                            return false;
                        }
                        ::log::info!(
                            "[{}] NodeJS provider registered on channel '{}'.",
                            #logical_name_lit,
                            #group_lit
                        );
                        true
                    }
                    #mode_name::Provider { local_impl, init_hook, server_started } => {
                        if !*server_started {
                            if !init_hook.on_init().await {
                                ::log::warn!("[{}] on_init() failed, retrying...",
                                    stringify!(#trait_name));
                                return false;
                            }

                            // The dispatcher is registered on the channel of the
                            // service; the channel thread itself starts in
                            // `ServiceLocator::initialize_all`, once every
                            // provider has registered, so a request never
                            // reaches a channel before its dispatcher exists.
                            let dispatcher = #server_name::new(local_impl.clone()).native_dispatcher();
                            if let Err(e) = ice_rpc::gen::register_native_service(
                                #group_lit,
                                ice_rpc::gen::service_id_of(#logical_name_lit),
                                #logical_name_lit,
                                dispatcher,
                            ) {
                                ::log::error!(
                                    "[{}] channel registration failed: {e:?}",
                                    stringify!(#trait_name)
                                );
                                return false;
                            }

                            *server_started = true;
                            ::log::info!(
                                "[{}] native service registered on channel '{}'.",
                                stringify!(#trait_name),
                                #group_lit
                            );
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
