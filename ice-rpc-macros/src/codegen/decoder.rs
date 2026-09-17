//! Codegen: human-readable decoding of the service payloads.
//!
//! For each `#[service]` trait this generates:
//!
//! - `impl Display for {Trait}Request`, rendering every argument through
//!   `ice_rpc::monitor::render_value!` — the argument's own
//!   [`Display`](std::fmt::Display) when it has one, its
//!   [`Debug`](std::fmt::Debug) otherwise — so a service type never has to
//!   implement more than `Debug` (already required by the generated request
//!   enum);
//! - a `{Trait}Decoder` implementing `ice_rpc::monitor::ServiceDecoder`, which
//!   decodes the request enum and the `WireEvent` response of each method.
//!
//! An observer linked against the service definitions registers these decoders
//! (usually through a crate-level inventory) and can then print every observed
//! message in clear text instead of raw rkyv bytes.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, LitStr, Type, Visibility};

use super::helpers::is_unit_type;

/// One RPC method, as needed by the decoder.
pub struct DecoderMethod {
    /// Method name as it travels in the header.
    pub method_name: String,
    /// Generated request-enum variant of this method.
    pub var_name: Ident,
    /// Argument names, bound by the request-enum variant.
    pub arg_names: Vec<Ident>,
    /// Success type `T` of `Observable<T, E>`.
    pub ok_type: Type,
    /// Error type `E` of `Observable<T, E>`.
    pub err_type: Type,
}

/// Parameters of the decoder generation.
pub struct DecoderGenInput<'a> {
    /// Visibility inherited from the trait.
    pub visibility: &'a Visibility,
    /// Name of the generated request enum (`{Trait}Request`).
    pub req_enum_name: &'a Ident,
    /// Name of the generated decoder (`{Trait}Decoder`).
    pub decoder_name: &'a Ident,
    /// Logical service name.
    pub logical_name_lit: &'a str,
    /// One entry per RPC method.
    pub methods: &'a [DecoderMethod],
}

/// Generates the `Display` implementation and the `{Trait}Decoder`.
pub fn gen_decoder(input: &DecoderGenInput<'_>) -> TokenStream {
    let DecoderGenInput {
        visibility,
        req_enum_name,
        decoder_name,
        logical_name_lit,
        methods,
    } = input;

    let mut display_arms = Vec::new();
    let mut request_arms = Vec::new();
    let mut response_arms = Vec::new();

    for method in methods.iter() {
        let var_name = &method.var_name;
        let method_lit = LitStr::new(&method.method_name, var_name.span());
        let arg_names = &method.arg_names;

        // `Display`: `method(arg1=…, arg2=…)`, each argument rendered by
        // `render_value!` — its `Display` form when it has one, else its `Debug`.
        let mut format = String::from(method.method_name.as_str());
        format.push('(');
        for (index, arg) in arg_names.iter().enumerate() {
            if index > 0 {
                format.push_str(", ");
            }
            format.push_str(&arg.to_string());
            format.push_str("={}");
        }
        format.push(')');
        let format_lit = LitStr::new(&format, var_name.span());
        display_arms.push(quote! {
            #req_enum_name::#var_name { #(#arg_names),* } => ::std::write!(
                f,
                #format_lit
                #(, ice_rpc::monitor::render_value!(#arg_names))*
            )
        });

        // Request: the whole enum decodes; `Display` renders the variant.
        request_arms.push(quote! {
            #method_lit => ice_rpc::monitor::decode_request::<#req_enum_name>(payload)
        });

        // Response: the value and the error are rendered by renderers expanded
        // *here*, where their types are concrete — the decoder itself imposes no
        // formatting bound on them. `()` carries nothing worth rendering, so the
        // unit case only passes the error renderer.
        let ok_type = &method.ok_type;
        let err_type = &method.err_type;
        if is_unit_type(ok_type) {
            response_arms.push(quote! {
                #method_lit => ice_rpc::monitor::decode_response_unit::<#err_type>(
                    payload,
                    |error| ice_rpc::monitor::render_value!(error),
                )
            });
        } else {
            response_arms.push(quote! {
                #method_lit => ice_rpc::monitor::decode_response::<#ok_type, #err_type>(
                    payload,
                    |value| ice_rpc::monitor::render_value!(value),
                    |error| ice_rpc::monitor::render_value!(error),
                )
            });
        }
    }

    quote! {
        impl ::std::fmt::Display for #req_enum_name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                match self {
                    #(#display_arms),*
                }
            }
        }

        /// Decodes this service's payloads into human-readable text.
        #[derive(Debug, Clone, Copy, Default)]
        #visibility struct #decoder_name;

        impl #decoder_name {
            /// Logical name of the service this decoder handles.
            pub const SERVICE_NAME: &'static str = #logical_name_lit;

            /// Registers this decoder into an observer registry.
            pub fn register(decoders: &mut ice_rpc::monitor::Decoders) {
                decoders.register(
                    ice_rpc::gen::service_id_of(Self::SERVICE_NAME),
                    ::std::sync::Arc::new(Self),
                );
            }
        }

        impl ice_rpc::monitor::ServiceDecoder for #decoder_name {
            fn request(
                &self,
                method: &str,
                payload: &[u8],
            ) -> ::std::option::Option<::std::string::String> {
                match method {
                    #(#request_arms,)*
                    _ => ::std::option::Option::None,
                }
            }

            fn response(
                &self,
                method: &str,
                payload: &[u8],
            ) -> ::std::option::Option<::std::string::String> {
                match method {
                    #(#response_arms,)*
                    _ => ::std::option::Option::None,
                }
            }
        }
    }
}
