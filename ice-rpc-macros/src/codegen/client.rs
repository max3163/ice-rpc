//! Codegen: `{Trait}Client` struct and its IPC consumption methods.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, Type, Visibility};

/// Client generation parameters.
pub struct ClientGenInput<'a> {
    pub visibility: &'a Visibility,
    pub client_name: &'a Ident,
    pub client_methods: &'a [TokenStream],
}

/// Generates the `{Trait}Client` struct with the `new()` constructor.
pub fn gen_client_struct(input: &ClientGenInput<'_>) -> TokenStream {
    let ClientGenInput {
        visibility,
        client_name,
        client_methods,
        ..
    } = input;

    quote! {
        #visibility struct #client_name;

        impl #client_name {
            #visibility fn new() -> std::sync::Arc<Self> {
                std::sync::Arc::new(Self)
            }

            #(#client_methods)*
        }
    }
}

/// Generates the [`ServiceLifecycle::init`] implementation for the client.
///
/// The native transport connects lazily on the first call, so `init` is a no-op
/// that always reports success.
pub fn gen_client_lifecycle(input: &ClientGenInput<'_>) -> TokenStream {
    let ClientGenInput { client_name, .. } = input;

    quote! {
        #[async_trait::async_trait]
        impl ice_rpc::gen::ServiceLifecycle for #client_name {
            async fn init(&self) -> bool {
                // The native transport connects lazily on the first call.
                true
            }
        }
    }
}

/// Client method generation parameters.
///
/// The generated body is one call to `ice_rpc::gen::serialize_and_call`, which
/// encodes the request into a reusable per-thread buffer and publishes it,
/// streaming the responses back as an [`Observable`](ice_rpc).
pub struct ClientMethodGenInput<'a> {
    pub visibility: &'a Visibility,
    pub fn_name: &'a Ident,
    pub var_name: &'a Ident,
    pub arg_names: &'a [&'a Ident],
    pub arg_types: &'a [&'a Type],
    pub ok_type: &'a Type,
    pub err_type: &'a Type,
    pub req_enum_name: &'a Ident,
    /// Channel the request is published on (the `group` of `#[service]`).
    pub group: &'a str,
    /// Expression of the shared [`ServiceRef`] of the service (id + version).
    pub service_ref: &'a TokenStream,
}

pub fn gen_client_method(input: &ClientMethodGenInput) -> TokenStream {
    let visibility = input.visibility;
    let fn_name = input.fn_name;
    let var_name = input.var_name;
    let arg_names = input.arg_names;
    let arg_types = input.arg_types;
    let ok_type = input.ok_type;
    let req_enum_name = input.req_enum_name;
    let group = input.group;
    let service_ref = input.service_ref;
    let err_type = input.err_type;

    let method_name_str = fn_name.to_string();

    quote! {
        #visibility async fn #fn_name(&self, #(#arg_names: #arg_types),*)
            -> ice_rpc::Observable<#ok_type, #err_type>
        {
            let req_val = #req_enum_name::#var_name { #(#arg_names),* };

            // Encoding and publishing are one call: the request buffer is the
            // thread's, so a call no longer allocates one.
            ice_rpc::gen::serialize_and_call::<#ok_type, #err_type, _>(
                #group,
                #service_ref,
                #method_name_str,
                &req_val,
            )
            .unwrap_or_else(ice_rpc::Observable::from_technical_error)
        }

    }
}
