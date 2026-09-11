//! Codegen: `{Trait}Server` struct and its native request/response dispatcher.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, Visibility};

/// Generation parameters of the server struct.
pub struct ServerGenInput<'a> {
    /// The user's `#[service]` trait.
    pub trait_name: &'a Ident,
    /// Visibility inherited from the trait.
    pub visibility: &'a Visibility,
    /// Name of the generated server type (`{Trait}Server`).
    pub server_name: &'a Ident,
    /// One `ServiceDispatcher::method(...)` registration per RPC method.
    pub server_native_methods: &'a [TokenStream],
}

/// Generates the `{Trait}Server` struct and its native dispatcher.
///
/// The service is exposed through the iceoryx2 native request/response
/// transport: one [`ServiceDispatcher`](ice_rpc) entry per RPC method, each of
/// them decoding the rkyv request enum and streaming the resulting `Observable`
/// back as rkyv `WireEvent` samples.
pub fn gen_server(input: &ServerGenInput<'_>) -> TokenStream {
    let ServerGenInput {
        trait_name,
        visibility,
        server_name,
        server_native_methods,
    } = input;

    quote! {
        #[derive(Clone)]
        #visibility struct #server_name {
            service_impl: std::sync::Arc<dyn #trait_name>,
        }

        impl #server_name {
            fn new(service_impl: std::sync::Arc<dyn #trait_name>) -> std::sync::Arc<Self> {
                std::sync::Arc::new(Self { service_impl })
            }

            /// Builds the native `request_response` dispatcher of this service.
            ///
            /// Each RPC method is registered with its own handler: it decodes
            /// the rkyv request enum from the payload, invokes the local
            /// implementation, and streams the resulting `Observable` through
            /// `observable_to_responses`.
            fn native_dispatcher(self: std::sync::Arc<Self>) -> ice_rpc::gen::ServiceDispatcher {
                let mut dispatcher = ice_rpc::gen::ServiceDispatcher::new();
                #(#server_native_methods)*
                dispatcher
            }
        }
    }
}

/// Generates one `ServiceDispatcher::method(...)` registration for the native
/// request/response transport.
pub fn gen_native_method(
    fn_name: &Ident,
    var_name: &Ident,
    arg_names: &[&Ident],
    req_enum_name: &Ident,
) -> TokenStream {
    let method_name_str = fn_name.to_string();
    quote! {
        {
            let service_impl = self.service_impl.clone();
            dispatcher.method(#method_name_str, move |payload: &[u8]| -> ice_rpc::gen::ResponseIter {
                match ice_rpc::gen::rkyv::from_bytes::<
                    #req_enum_name,
                    ice_rpc::gen::rkyv::rancor::Error,
                >(payload) {
                    Ok(#req_enum_name::#var_name { #(#arg_names),* }) => {
                        // Clone per invocation: the closure is `Fn`, so it must
                        // not move the captured `Arc` into the coroutine.
                        let impl_ref = service_impl.clone();
                        let stream = ice_rpc::rt::block_on(async move {
                            impl_ref.#fn_name(#(#arg_names),*).await
                        });
                        ice_rpc::gen::observable_to_responses(stream)
                    }
                    _ => Box::new(std::iter::empty()),
                }
            });
        }
    }
}
