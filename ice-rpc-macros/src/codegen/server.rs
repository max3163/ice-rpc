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
    /// Expression of the shared [`ServiceRef`] of the service (id + version).
    pub service_ref: &'a TokenStream,
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
        service_ref,
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
                let mut dispatcher = ice_rpc::gen::ServiceDispatcher::new(#service_ref);
                #(#server_native_methods)*
                dispatcher
            }
        }
    }
}

/// Generates one `ServiceDispatcher::method(...)` registration for the native
/// request/response transport, plus the method's terminal-error emitter.
pub fn gen_native_method(
    fn_name: &Ident,
    var_name: &Ident,
    arg_names: &[&Ident],
    req_enum_name: &Ident,
    ok_type: &syn::Type,
    err_type: &syn::Type,
) -> TokenStream {
    let method_name_str = fn_name.to_string();
    quote! {
        {
            let service_impl = self.service_impl.clone();
            dispatcher.method(
                #method_name_str,
                move |payload: &[u8], emitter: &mut dyn ice_rpc::gen::ResponseEmitter| {
                    // The framed payload is not necessarily aligned for rkyv, so
                    // the decode goes through an aligned copy.
                    match ice_rpc::gen::decode_aligned::<#req_enum_name>(payload) {
                        Ok(#req_enum_name::#var_name { #(#arg_names),* }) => {
                            // Clone per invocation: the closure is `Fn`, so it must
                            // not move the captured `Arc` into the coroutine.
                            let impl_ref = service_impl.clone();
                            let stream = ice_rpc::rt::block_on(async move {
                                impl_ref.#fn_name(#(#arg_names),*).await
                            });
                            ice_rpc::gen::observable_to_responses(stream, emitter);
                        }
                        // A payload of another method, or one that does not decode:
                        // no response is emitted, so the call times out.
                        _ => {}
                    }
                },
            );

            // The transport is type-erased, so it borrows this closure to answer
            // a version mismatch with the typed `(T, E)` of this very method.
            dispatcher.on_error(
                #method_name_str,
                |err: ice_rpc::gen::RpcError,
                 emitter: &mut dyn ice_rpc::gen::ResponseEmitter| {
                    ice_rpc::gen::emit_rpc_error::<#ok_type, #err_type>(err, emitter);
                },
            );
        }
    }
}
