//! Codegen: [`HttpCallable`] implementation for the ice-rpc proxies.
//!
//! Generates the [`ice_rpc::HttpCallable`] trait implementation which allows
//! the dynamic invocation of RPC methods through HTTP/JSON.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, Type};

/// Generation parameters of the [`HttpCallable`] implementation.
pub struct HttpGenInput {
    pub proxy_name: Ident,
    pub logical_name: String,
    pub http_methods: Vec<HttpMethodData>,
}

/// Owned data of a method needed for the HTTP dispatch.
#[allow(dead_code)]
pub struct HttpMethodData {
    pub fn_name: Ident,
    pub arg_names: Vec<Ident>,
    pub arg_types: Vec<Type>,
    pub ok_type: Type,
    pub err_type: Type,
}

/// Generates the [`HttpCallable`] implementation for a Proxy type.
pub fn gen_http_callable_impl(input: &HttpGenInput) -> TokenStream {
    let HttpGenInput {
        proxy_name,
        logical_name,
        http_methods,
    } = input;

    let match_arms: Vec<TokenStream> = http_methods.iter().map(gen_http_method_arm).collect();

    let unknown_method_error = format!("Unknown method '{{}}' for service '{}'", logical_name);

    quote! {
        #[async_trait::async_trait]
        impl ice_rpc::HttpCallable for #proxy_name {
            fn service_name(&self) -> &'static str {
                #logical_name
            }

            async fn http_invoke(
                &self,
                method: &str,
                params: ice_rpc::serde_json::Value,
            ) -> Result<ice_rpc::serde_json::Value, String> {
                match method {
                    #(#match_arms)*
                    _ => Err(format!(#unknown_method_error, method)),
                }
            }
        }
    }
}

/// Generates an individual match arm for an RPC method.
fn gen_http_method_arm(method: &HttpMethodData) -> TokenStream {
    let fn_name = &method.fn_name;
    let fn_name_str = fn_name.to_string();
    let arg_names = &method.arg_names;
    let arg_types = &method.arg_types;

    // Generates the parameter deserialization according to the number of arguments.
    let deser_block: TokenStream = match arg_names.len() {
        0 => {
            quote! {}
        }
        1 => {
            let arg_name = &arg_names[0];
            let arg_type = &arg_types[0];
            let type_name = format!("{}", quote!(#arg_type));
            quote! {
                let #arg_name: #arg_type = match ice_rpc::serde_json::from_value(params) {
                    Ok(v) => v,
                    Err(e) => return Err(format!(
                        "Invalid parameter for '{}': {} (expected type: {})",
                        #fn_name_str,
                        e,
                        #type_name
                    )),
                };
            }
        }
        _ => {
            let field_extractions: Vec<TokenStream> = arg_names
                .iter()
                .zip(arg_types.iter())
                .map(|(arg_name, arg_type)| {
                    let field_name_str = arg_name.to_string();
                    quote! {
                        let #arg_name: #arg_type = {
                            let field_name = #field_name_str;
                            let val = params.get(field_name)
                                .cloned()
                                .unwrap_or(ice_rpc::serde_json::Value::Null);
                            match ice_rpc::serde_json::from_value(val) {
                                Ok(v) => v,
                                Err(e) => return Err(format!(
                                    "Invalid parameter '{}' for '{}': {}",
                                    field_name, #fn_name_str, e
                                )),
                            }
                        };
                    }
                })
                .collect();

            quote! { #(#field_extractions)* }
        }
    };

    // The method call.
    let call_block: TokenStream = if arg_names.is_empty() {
        quote! {
            self.#fn_name().await
        }
    } else {
        let args = arg_names;
        quote! {
            self.#fn_name(#(#args),*).await
        }
    };

    quote! {
        #fn_name_str => {
            #deser_block
            match #call_block {
                Ok(mut rx) => match rx.recv().await {
                    Ok(ice_rpc::Event::Next(value)) => {
                        let data = ice_rpc::serde_json::to_value(&value)
                            .map_err(|e| format!("Failed to serialize the response: {}", e))?;
                        Ok(ice_rpc::serde_json::json!({"status":"ok","data":data}))
                    }
                    Ok(ice_rpc::Event::Complete) => {
                        Ok(ice_rpc::serde_json::json!({"status":"ok"}))
                    }
                    Ok(ice_rpc::Event::Error(e)) => {
                        Ok(ice_rpc::serde_json::json!({"status":"error","error":e.to_string()}))
                    }
                    Err(_) => {
                        Err("No response received from the service".to_string())
                    }
                },
                Err(e) => {
                    Err(format!("IPC error: {}", e))
                }
            }
        }
    }
}
