//! Codegen: Node.js converters (rkyv ↔ serde_json::Value) for the ProviderNodeJs mode.
//!
//! Generates the `deserialize_request_to_value()` and `serialize_response_from_value()`
//! functions for each service.
//!
//! These functions are always generated (not feature-gated) and reference only
//! types re-exported by `ice-rpc`, so no extra dependency nor feature is required
//! from the consuming crate.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, Type, Visibility};

/// Common parameters for the Node.js generation.
pub struct NodeJsGenInput<'a> {
    pub visibility: &'a Visibility,
    pub proxy_name: &'a Ident,
    pub req_enum_name: &'a Ident,
    pub methods: Vec<NodeJsMethod>,
}

/// Description of a method for the Node.js generation (owned data).
pub struct NodeJsMethod {
    pub fn_name: Ident,
    pub var_name: Ident,
    pub arg_names: Vec<Ident>,
    pub arg_types: Vec<Type>,
    pub ok_type: Type,
    pub err_type: Type,
}

/// Generates the `deserialize_request_to_value(method, bytes) -> Option<Value>` function.
///
/// For each method, deserializes the rkyv request and converts it into
/// `serde_json::Value` (native JS object after passing through N-API).
///
/// # Optimizations
/// * **0 argument** → empty object `{}`.
/// * **1 argument** → the value directly, without wrapping in an object.
/// * **2+ arguments** → object `{ "arg1": val1, "arg2": val2, ... }`.
/// * **Vec<u8>** → base64 encoding instead of a JSON array.
pub fn gen_nodejs_deserialize_fn(input: &NodeJsGenInput<'_>) -> TokenStream {
    let NodeJsGenInput {
        visibility,
        proxy_name,
        req_enum_name,
        methods,
        ..
    } = input;

    let match_arms: Vec<TokenStream> = methods
        .iter()
        .map(|m| {
            let fn_name_str = m.fn_name.to_string();
            let var_name = &m.var_name;
            let arg_names = &m.arg_names;
            let arg_types = &m.arg_types;

            let args_expr: TokenStream = match arg_names.len() {
                0 => {
                    quote! { ice_rpc::gen::serde_json::Value::Object(ice_rpc::gen::serde_json::Map::new()) }
                }
                1 => {
                    let arg_name = &arg_names[0];
                    let arg_type = &arg_types[0];
                    if is_type_vec_u8(arg_type) {
                        quote! { {
                            use ice_rpc::gen::base64::Engine;
                            let encoded = ice_rpc::gen::base64::engine::general_purpose::STANDARD.encode(&#arg_name);
                            ice_rpc::gen::serde_json::Value::String(encoded)
                        } }
                    } else {
                        quote! { ice_rpc::gen::serde_json::to_value(#arg_name).ok()? }
                    }
                }
                _ => {
                    let json_fields: Vec<TokenStream> = arg_names
                        .iter()
                        .enumerate()
                        .map(|(idx, name)| {
                            let field_str = name.to_string();
                            let arg_type = &arg_types[idx];
                            if is_type_vec_u8(arg_type) {
                                quote! { #field_str: {
                                    use ice_rpc::gen::base64::Engine;
                                    let encoded = ice_rpc::gen::base64::engine::general_purpose::STANDARD.encode(&#name);
                                    ice_rpc::gen::serde_json::Value::String(encoded)
                                } }
                            } else {
                                quote! { #field_str: ice_rpc::gen::serde_json::to_value(#name).ok()? }
                            }
                        })
                        .collect();
                    quote! { ice_rpc::gen::serde_json::json!({ #(#json_fields),* }) }
                }
            };

            quote! {
                #fn_name_str => {
                    let req: #req_enum_name = ice_rpc::gen::decode_aligned::<#req_enum_name>(bytes).ok()?;
                    match req {
                        #req_enum_name::#var_name { #(#arg_names),* } => {
                            Some(#args_expr)
                        }
                        _ => None,
                    }
                }
            }
        })
        .collect();

    quote! {
        // Emitted unconditionally but called only by the `gateway_nodejs` bridge.
        #[allow(dead_code)]
        impl #proxy_name {
            #visibility fn deserialize_request_to_value(method: &str, bytes: &[u8]) -> Option<ice_rpc::gen::serde_json::Value> {
                match method {
                    #(#match_arms)*
                    _ => None,
                }
            }
        }
    }
}

/// Detects whether a syn type is `Vec<u8>`.
fn is_type_vec_u8(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        let path = &type_path.path;
        if let Some(seg) = path.segments.last() {
            if seg.ident == "Vec" {
                if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                    if args.args.len() == 1 {
                        if let syn::GenericArgument::Type(Type::Path(inner_path)) = &args.args[0] {
                            if let Some(inner_seg) = inner_path.path.segments.last() {
                                return inner_seg.ident == "u8";
                            }
                        }
                    }
                }
            }
        }
    }
    false
}

/// Generates the `serialize_response_from_value(method, value) -> Option<Vec<u8>>`
/// function.
///
/// The JS returns an object `{ "type": "next"|"complete"|"error", "data": ... }`.
/// We manually build a `WireEvent` then serialize it to rkyv.
pub fn gen_nodejs_serialize_fn(input: &NodeJsGenInput<'_>) -> TokenStream {
    let NodeJsGenInput {
        visibility,
        proxy_name,
        methods,
        ..
    } = input;

    let match_arms: Vec<TokenStream> = methods
        .iter()
        .map(|m| {
            let fn_name_str = m.fn_name.to_string();
            let ok_type = &m.ok_type;
            let err_type = &m.err_type;

            quote! {
                #fn_name_str => {
                    let event_type = value.get("type").and_then(|v| v.as_str()).unwrap_or("next");
                    let event = match event_type {
                        "next" => {
                            let data: #ok_type = match value.get("data") {
                                Some(d) => match ice_rpc::gen::serde_json::from_value(d.clone()) {
                                    Ok(v) => v,
                                    Err(_) => return None,
                                },
                                None => return None,
                            };
                            ice_rpc::gen::WireEvent::Next(data)
                        }
                        "complete" => match value.get("data") {
                            Some(d) => {
                                let data: #ok_type = match ice_rpc::gen::serde_json::from_value(d.clone()) {
                                    Ok(v) => v,
                                    Err(_) => return None,
                                };
                                ice_rpc::gen::WireEvent::CompleteWith(data)
                            }
                            None => ice_rpc::gen::WireEvent::Complete,
                        },
                        "error" => {
                            let err: #err_type = match value.get("data") {
                                Some(d) => match ice_rpc::gen::serde_json::from_value(d.clone()) {
                                    Ok(v) => v,
                                    Err(_) => return None,
                                },
                                None => return None,
                            };
                            ice_rpc::gen::WireEvent::Error(err)
                        }
                        _ => return None,
                    };
                    ice_rpc::gen::rkyv::to_bytes::<ice_rpc::gen::rkyv::rancor::Error>(&event)
                        .ok()
                        .map(|aligned| aligned.to_vec())
                }
            }
        })
        .collect();

    quote! {
        // Same rationale as `deserialize_request_to_value` above.
        #[allow(dead_code)]
        impl #proxy_name {
            #visibility fn serialize_response_from_value(method: &str, value: ice_rpc::gen::serde_json::Value) -> Option<Vec<u8>> {
                match method {
                    #(#match_arms)*
                    _ => None,
                }
            }
        }
    }
}

/// Generates one `ServiceDispatcher::method(...)` registration that bridges an
/// RPC method to the injected Node.js dispatch callback.
///
/// The callback receives the JSON-decoded request and returns a JSON response
/// that is converted back into rkyv-encoded `WireEvent` samples. The service
/// name comes from the proxy's own `SERVICE_NAME` constant, so no extra
/// parameter is needed.
pub fn gen_nodejs_native_method(proxy_name: &Ident, fn_name: &Ident) -> TokenStream {
    let method_name_str = fn_name.to_string();
    quote! {
        {
            dispatcher.method(#method_name_str, move |payload: &[u8]| -> ice_rpc::gen::ResponseIter {
                let Some(args) =
                    #proxy_name::deserialize_request_to_value(#method_name_str, payload)
                else {
                    ::log::error!(
                        "[{}::{}] Failed to deserialize the request",
                        <#proxy_name>::SERVICE_NAME,
                        #method_name_str
                    );
                    return Box::new(std::iter::empty());
                };
                let value = match ice_rpc::nodejs_dispatch::call(
                    [0u8; 16],
                    <#proxy_name>::SERVICE_NAME,
                    #method_name_str,
                    args,
                ) {
                    Ok(value) => value,
                    Err(e) => {
                        ::log::error!(
                            "[{}::{}] NodeJS dispatch failed: {}",
                            <#proxy_name>::SERVICE_NAME,
                            #method_name_str,
                            e
                        );
                        return Box::new(std::iter::empty());
                    }
                };
                match #proxy_name::serialize_response_from_value(#method_name_str, value) {
                    Some(bytes) => Box::new(std::iter::once(bytes)),
                    None => Box::new(std::iter::empty()),
                }
            });
        }
    }
}
