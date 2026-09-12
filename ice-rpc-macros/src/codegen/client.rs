//! Codegen: `{Trait}Client` struct and its IPC consumption methods.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, Type, Visibility};

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

    let _ = (logical_name, allow_large_payload, default_size_message_kb);
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
    let ClientGenInput {
        logical_name,
        client_name,
        allow_large_payload,
        default_size_message_kb,
        ..
    } = input;

    let _ = (logical_name, allow_large_payload, default_size_message_kb);
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
/// The generated body serializes the request (rkyv) then calls
/// `ice_rpc::gen::native_call`, streaming the responses back as an
/// [`Observable`](ice_rpc).
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
    /// Channel the request is published on (the `group` of `#[service]`).
    pub group: &'a str,
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
    let req_enum_name = input.req_enum_name;
    let logical_name = input.logical_name;
    let group = input.group;
    let _service_version = input.service_version;
    let err_type = input.err_type;

    let method_name_str = fn_name.to_string();
    // Service-wide discovery deadline; mirrors `RPC_CALL_TIMEOUT_SECS` (30s).
    let _locate_timeout = input.discovery_timeout_secs.unwrap_or(30);

    quote! {
        #visibility async fn #fn_name(&self, #(#arg_names: #arg_types),*)
            -> ice_rpc::Observable<#ok_type, #err_type>
        {
            let req_val = #req_enum_name::#var_name { #(#arg_names),* };

            let bytes = match ice_rpc::gen::rkyv::to_bytes::<ice_rpc::gen::rkyv::rancor::Error>(&req_val) {
                Ok(bytes) => bytes,
                Err(_) => {
                    return ice_rpc::Observable::from_technical_error(
                        ice_rpc::RpcError::SerializationError,
                    );
                }
            };

            ice_rpc::gen::native_call::<#ok_type, #err_type>(
                #group,
                ice_rpc::gen::service_id_of(#logical_name),
                #method_name_str,
                &bytes,
            )
            .unwrap_or_else(ice_rpc::Observable::from_technical_error)
        }

    }
}
