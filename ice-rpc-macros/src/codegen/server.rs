//! Codegen: the native dispatcher of a service.
//!
//! The per-method body lives in `ice_rpc::transport::native_host`; this module
//! emits one registration per method, carrying only the request decoding and the
//! call closure. There is no `{Trait}Server` type any more: the dispatcher is an
//! inherent function of the proxy, built from the implementation it already
//! holds.

use proc_macro2::TokenStream;
use quote::quote;
use syn::Ident;

use crate::model::RpcMethod;

/// Generates the proxy's `native_dispatcher` associated function.
///
/// The function builds the `ServiceDispatcher` the transport registers on the
/// channel. Every handler is a call to the shared runtime entry point
/// (`native_handler` or its `_untraced` twin); what varies per method is the
/// decode closure and the call closure.
pub fn gen_native_dispatcher(
    proxy_name: &Ident,
    trait_name: &Ident,
    req_enum_name: &Ident,
    methods: &[RpcMethod],
    no_tracing: bool,
    service_ref: &TokenStream,
) -> TokenStream {
    let handler = if no_tracing {
        quote! { ice_rpc::codegen::native_handler_untraced }
    } else {
        quote! { ice_rpc::codegen::native_handler }
    };
    let oneway_handler = if no_tracing {
        quote! { ice_rpc::codegen::native_oneway_handler_untraced }
    } else {
        quote! { ice_rpc::codegen::native_oneway_handler }
    };

    let registrations: Vec<TokenStream> = methods
        .iter()
        .map(|method| {
            gen_registration(proxy_name, req_enum_name, method, &handler, &oneway_handler)
        })
        .collect();

    quote! {
        impl #proxy_name {
            /// Builds the native `request_response` dispatcher of this service.
            fn native_dispatcher(
                service_impl: std::sync::Arc<dyn #trait_name>,
            ) -> ice_rpc::codegen::ServiceDispatcher {
                let mut dispatcher = ice_rpc::codegen::ServiceDispatcher::new(#service_ref);
                #(#registrations)*
                dispatcher
            }
        }
    }
}

/// One `ServiceDispatcher::method`/`oneway_method` registration.
fn gen_registration(
    proxy_name: &Ident,
    req_enum_name: &Ident,
    method: &RpcMethod,
    handler: &TokenStream,
    oneway_handler: &TokenStream,
) -> TokenStream {
    let fn_name = &method.fn_name;
    let var_name = &method.var_name;
    let arg_names = &method.arg_names;
    let arg_types = &method.arg_types;
    let method_name_str = fn_name.to_string();

    // The decode closure: the request enum variant becomes the method's
    // arguments. `Ok(())` for a zero-argument method, the value itself for one
    // argument, a tuple for two or more — the same shape the call closure takes.
    let decode = quote! {
        |payload| {
            match ice_rpc::codegen::decode_aligned::<#req_enum_name>(payload) {
                Ok(#req_enum_name::#var_name { #(#arg_names),* }) => Ok((#(#arg_names),*)),
                Err(e) => Err(ice_rpc::codegen::DecodeFailure::Invalid(format!("{e:?}"))),
                Ok(_) => Err(ice_rpc::codegen::DecodeFailure::WrongVariant),
            }
        }
    };

    if method.oneway {
        return quote! {
            {
                let service_impl = service_impl.clone();
                dispatcher.oneway_method(
                    ice_rpc::codegen::method_id_of(#method_name_str),
                    move |header: ice_rpc::codegen::RpcHeader,
                          payload: &[u8],
                          _emitter: ice_rpc::codegen::OwnedEmitter|
                          -> ice_rpc::codegen::BoxResponseFuture {
                        let impl_ref = service_impl.clone();
                        #oneway_handler(
                            <#proxy_name>::SERVICE_NAME,
                            #method_name_str,
                            header,
                            payload,
                            #decode,
                            move |(#(#arg_names),*): (#(#arg_types),*)| async move {
                                impl_ref.#fn_name(#(#arg_names),*).await;
                            },
                        )
                    },
                );
            }
        };
    }

    quote! {
        {
            let service_impl = service_impl.clone();
            dispatcher.method(
                ice_rpc::codegen::method_id_of(#method_name_str),
                move |header: ice_rpc::codegen::RpcHeader,
                      payload: &[u8],
                      emitter: ice_rpc::codegen::OwnedEmitter|
                      -> ice_rpc::codegen::BoxResponseFuture {
                    let impl_ref = service_impl.clone();
                    #handler(
                        <#proxy_name>::SERVICE_NAME,
                        #method_name_str,
                        header,
                        payload,
                        emitter,
                        #decode,
                        move |(#(#arg_names),*): (#(#arg_types),*)| async move {
                            impl_ref.#fn_name(#(#arg_names),*).await
                        },
                    )
                },
            );
        }
    }
}
