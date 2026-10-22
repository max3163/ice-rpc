//! Codegen: the JSON view of a service, in both directions.
//!
//! Four things are emitted here, and **none of them names a transport**:
//!
//! | Emitted | Used by |
//! |---|---|
//! | [`gen_json_provider_from_request_fn`] | the provider side: rkyv request → JS value |
//! | [`gen_json_provider_from_response_fn`] | the provider side: JS event → rkyv `WireEvent` |
//! | [`gen_json_invoker_impl`] | the consumer side: [`JsonInvoker`] for both JSON bridges |
//! | [`gen_json_provider_method`] | the provider side: one call may carry several events |
//!
//! A JSON bridge is the Node.js gateway today; the HTTP gateway consumes the very
//! same [`JsonInvoker`] implementation. A Rust-to-Rust call does not go through
//! any of this.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, Type, Visibility};

/// Common parameters of the JSON generation.
pub struct JsonGenInput<'a> {
    pub visibility: &'a Visibility,
    pub proxy_name: &'a Ident,
    pub req_enum_name: &'a Ident,
    pub methods: Vec<JsonMethod>,
}

/// Description of a method for the JSON generation (owned data).
pub struct JsonMethod {
    pub fn_name: Ident,
    pub var_name: Ident,
    pub arg_names: Vec<Ident>,
    pub arg_types: Vec<Type>,
    /// Success type `T` of `Observable<T, E>` (`()` for a one-way method).
    pub ok_type: Type,
    /// Error type `E` of `Observable<T, E>` (`()` for a one-way method).
    pub err_type: Type,
    /// Whether the method is a notification: it has no result to convert, and the
    /// JSON view answers `JsonOutcome::Accepted` (HTTP `202`) instead of a value.
    pub oneway: bool,
}

/// Generates the provider-side `deserialize_request_to_value(method, bytes)`.
///
/// For each method, deserializes the rkyv request and converts it into a JSON
/// value — a native JS object once it crosses N-API.
///
/// # Argument convention
/// * **0 argument** → empty object `{}`;
/// * **1 argument** → the value directly, unwrapped;
/// * **2 or more** → an object `{ "arg1": …, "arg2": … }`;
/// * **`Vec<u8>`** → base64 instead of a JSON array of numbers.
pub fn gen_json_provider_from_request_fn(input: &JsonGenInput<'_>) -> TokenStream {
    let JsonGenInput {
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
                    quote! { ice_rpc::codegen::serde_json::Value::Object(ice_rpc::codegen::serde_json::Map::new()) }
                }
                1 => {
                    let arg_name = &arg_names[0];
                    let arg_type = &arg_types[0];
                    if is_type_vec_u8(arg_type) {
                        quote! { {
                            use ice_rpc::codegen::base64::Engine;
                            let encoded = ice_rpc::codegen::base64::engine::general_purpose::STANDARD.encode(&#arg_name);
                            ice_rpc::codegen::serde_json::Value::String(encoded)
                        } }
                    } else if is_type_string(arg_type) {
                        // A top-level `String` is moved into the `Value`; going
                        // through `to_value` would copy the whole buffer.
                        quote! { ice_rpc::codegen::serde_json::Value::String(#arg_name) }
                    } else {
                        quote! { ice_rpc::codegen::serde_json::to_value(#arg_name).ok()? }
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
                                    use ice_rpc::codegen::base64::Engine;
                                    let encoded = ice_rpc::codegen::base64::engine::general_purpose::STANDARD.encode(&#name);
                                    ice_rpc::codegen::serde_json::Value::String(encoded)
                                } }
                            } else if is_type_string(arg_type) {
                                // Same move as the single-argument path.
                                quote! { #field_str: ice_rpc::codegen::serde_json::Value::String(#name) }
                            } else {
                                quote! { #field_str: ice_rpc::codegen::serde_json::to_value(#name).ok()? }
                            }
                        })
                        .collect();
                    quote! { ice_rpc::codegen::serde_json::json!({ #(#json_fields),* }) }
                }
            };

            quote! {
                #fn_name_str => {
                    let req: #req_enum_name = ice_rpc::codegen::decode_aligned::<#req_enum_name>(bytes).ok()?;
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
        // Emitted with the JSON feature set but called only by a JSON bridge.
        #[allow(dead_code)]
        impl #proxy_name {
            #visibility fn deserialize_request_to_value(method: &str, bytes: &[u8]) -> Option<ice_rpc::codegen::serde_json::Value> {
                match method {
                    #(#match_arms)*
                    _ => None,
                }
            }
        }
    }
}

/// Detects whether a syn type is `String`.
///
/// The last segment is checked, so `String` and `std::string::String` are both
/// recognized. A type alias is not: its expansion falls back to `to_value`,
/// which is correct, only less efficient.
fn is_type_string(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
    {
        return segment.ident == "String";
    }
    false
}

/// Detects whether a syn type is `Vec<u8>`.
fn is_type_vec_u8(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        let path = &type_path.path;
        if let Some(seg) = path.segments.last()
            && seg.ident == "Vec"
            && let syn::PathArguments::AngleBracketed(args) = &seg.arguments
            && args.args.len() == 1
            && let syn::GenericArgument::Type(Type::Path(inner_path)) = &args.args[0]
            && let Some(inner_seg) = inner_path.path.segments.last()
        {
            return inner_seg.ident == "u8";
        }
    }
    false
}

/// Generates the provider-side
/// `serialize_response_from_value(method, value) -> Option<(EventKind, AlignedVec)>`.
///
/// The host returns an object `{ "type": "next"|"complete"|"error", "data": … }`;
/// [`event_from_json`](ice_rpc::json::event_from_json) builds the matching
/// `WireEvent`, derives its `EventKind` (stamped in the zero-copy header by the
/// transport) and serializes it to rkyv. Only the two result types differ per
/// method, so the table is one line per method.
pub fn gen_json_provider_from_response_fn(input: &JsonGenInput<'_>) -> TokenStream {
    let JsonGenInput {
        visibility,
        proxy_name,
        methods,
        ..
    } = input;

    let match_arms: Vec<TokenStream> = methods
        .iter()
        // A one-way method never emits a response: there is no `WireEvent` to
        // build for it, and no arm to emit.
        .filter(|m| !m.oneway)
        .map(|m| {
            let fn_name_str = m.fn_name.to_string();
            let ok_type = &m.ok_type;
            let err_type = &m.err_type;

            quote! {
                #fn_name_str => ice_rpc::codegen::event_from_json::<#ok_type, #err_type>(value),
            }
        })
        .collect();

    quote! {
        // Same rationale as `deserialize_request_to_value` above.
        #[allow(dead_code)]
        impl #proxy_name {
            #visibility fn serialize_response_from_value(
                method: &str,
                value: ice_rpc::codegen::serde_json::Value,
            ) -> Option<(
                ice_rpc::codegen::EventKind,
                ice_rpc::codegen::rkyv::util::AlignedVec<{ ice_rpc::codegen::PAYLOAD_ALIGNMENT }>,
            )> {
                match method {
                    #(#match_arms)*
                    _ => None,
                }
            }
        }
    }
}

/// Generates the consumer-side [`JsonInvoker`] implementation.
///
/// **One** implementation per service, whatever the JSON transport: the Node.js
/// gateway and the HTTP gateway both consume it, so neither carries generated
/// code of its own. The reading policy travels as an argument
/// ([`ReadMode`](ice_rpc::codegen::ReadMode)) rather than as a second entry point,
/// which is what keeps one match table instead of two.
///
/// # Argument convention
/// Exactly the provider-side convention, so both directions agree (see
/// [`gen_json_provider_from_request_fn`]).
///
/// # Bounds
/// `OkType: Serialize` and `ErrType: Display`, imposed by
/// [`read_json`](ice_rpc::codegen::read_json).
///
/// Returns `None` for an unknown method, so a caller can tell "no such method"
/// from "the method failed".
pub fn gen_json_invoker_impl(input: &JsonGenInput<'_>) -> TokenStream {
    let JsonGenInput {
        proxy_name,
        methods,
        ..
    } = input;

    let helpers: Vec<TokenStream> = methods.iter().map(gen_invoker_helper).collect();
    let match_arms: Vec<TokenStream> = methods.iter().map(gen_invoker_arm).collect();

    quote! {
        // One inherent helper per method, emitted next to the table that calls
        // it. The decoding and the call live there, which is what keeps the
        // table down to one line per method.
        impl #proxy_name {
            #(#helpers)*
        }

        #[ice_rpc::codegen::async_trait::async_trait]
        impl ice_rpc::codegen::JsonInvoker for #proxy_name {
            fn service_name(&self) -> &'static str {
                <#proxy_name as ice_rpc::codegen::ServiceNamed>::SERVICE_NAME
            }

            async fn invoke_json(
                &self,
                method: &str,
                args: ice_rpc::codegen::serde_json::Value,
                read: ice_rpc::codegen::ReadMode,
            ) -> Option<ice_rpc::codegen::JsonResult> {
                match method {
                    #(#match_arms)*
                    _ => None,
                }
            }
        }
    }
}

/// Builds the inherent helper carrying the JSON view of one method.
///
/// It is an **inherent** method, and not part of the `JsonInvoker`
/// implementation, for one reason: `invoke_json` answers `Option<Result<…>>`, so
/// a `match` arm cannot use `?`, and the decoding had to live in an `async` block
/// whose result was awaited inside the arm. Here it has a real `Result` return
/// type, `?` reads normally, and the arm is left with the dispatch alone.
fn gen_invoker_helper(method: &JsonMethod) -> TokenStream {
    let fn_name = &method.fn_name;
    let fn_name_str = fn_name.to_string();
    let helper = helper_name(method);
    let arg_names = &method.arg_names;
    let extraction = gen_arg_extraction(&fn_name_str, arg_names, &method.arg_types);

    if method.oneway {
        // A one-way method has no stream to read: `read_json` would wait
        // `JSON_CALL_TIMEOUT` for an event that never exists. The inherent caller
        // is synchronous, and its `bool` maps straight onto `JsonOutcome::Accepted`.
        let call = if arg_names.is_empty() {
            quote! { self.#fn_name() }
        } else {
            quote! { self.#fn_name(#(#arg_names),*) }
        };
        let doc = format!(
            " The JSON view of `{fn_name_str}`: decodes the arguments and publishes the \
             one-way notification. No result exists to read."
        );
        return quote! {
            #[doc = #doc]
            async fn #helper(
                &self,
                args: ice_rpc::codegen::serde_json::Value,
                _read: ice_rpc::codegen::ReadMode,
            ) -> ice_rpc::codegen::JsonResult {
                #extraction
                match #call {
                    Ok(delivered) => Ok(ice_rpc::codegen::JsonOutcome::Accepted { delivered }),
                    Err(e) => Err(ice_rpc::codegen::JsonCallError::Transport(e.to_string())),
                }
            }
        };
    }

    let call = if arg_names.is_empty() {
        quote! { self.#fn_name().await }
    } else {
        quote! { self.#fn_name(#(#arg_names),*).await }
    };

    // The leading space is what rustfmt turns into `/// The …` instead of
    // `///The …`, so the generated item reads like a hand-written one.
    let doc = format!(
        " The JSON view of `{fn_name_str}`: decodes the arguments, calls the method and reads \
         its result. Generated so the dispatch table stays one line per method."
    );

    quote! {
        #[doc = #doc]
        async fn #helper(
            &self,
            args: ice_rpc::codegen::serde_json::Value,
            read: ice_rpc::codegen::ReadMode,
        ) -> ice_rpc::codegen::JsonResult {
            #extraction
            ice_rpc::codegen::read_json(#call, read).await
        }
    }
}

/// Builds one invoker match arm: the dispatch, and nothing else.
///
/// The whole "first value vs every value" policy and the mapping of a business
/// error live in `read_json`; the decoding lives in the generated helper. What is
/// left here is the table, and a table reads best when each line names what it
/// calls.
fn gen_invoker_arm(method: &JsonMethod) -> TokenStream {
    let fn_name_str = method.fn_name.to_string();
    let helper = helper_name(method);

    quote! {
        #fn_name_str => Some(self.#helper(args, read).await),
    }
}

/// Name of the generated helper of one method.
///
/// Prefixed, so it cannot collide with a method of the service: the trait methods
/// keep their own names on the proxy.
fn helper_name(method: &JsonMethod) -> Ident {
    Ident::new(
        &format!("json_view_of_{}", method.fn_name),
        method.fn_name.span(),
    )
}

/// Generates the parameter bindings of one method, mirroring the provider-side
/// convention.
///
/// Each binding is one line: the decoding, the base64 transport and the failure
/// messages live in `ice_rpc::codegen::{arg_from_json, arg_from_base64,
/// take_field}`, which are generic over the argument type only — the one thing the
/// expansion knows that the runtime cannot. A service whose methods reuse a type
/// therefore decodes it through one instantiation instead of one per parameter.
fn gen_arg_extraction(fn_name_str: &str, arg_names: &[Ident], arg_types: &[Type]) -> TokenStream {
    match arg_names.len() {
        // Zero arguments: the whole value is the (empty) argument object.
        0 => quote! {},
        1 => {
            let arg_name = &arg_names[0];
            let arg_type = &arg_types[0];
            let name_str = arg_name.to_string();
            if is_type_vec_u8(arg_type) {
                quote! {
                    let #arg_name: #arg_type = ice_rpc::codegen::arg_from_base64(
                        #fn_name_str,
                        #name_str,
                        Some(&args),
                    )?;
                }
            } else {
                quote! {
                    let #arg_name: #arg_type =
                        ice_rpc::codegen::arg_from_json(#fn_name_str, #name_str, args)?;
                }
            }
        }
        _ => {
            let fields: Vec<TokenStream> = arg_names
                .iter()
                .zip(arg_types.iter())
                .map(|(name, ty)| {
                    let field_str = name.to_string();
                    if is_type_vec_u8(ty) {
                        quote! {
                            let #name: #ty = ice_rpc::codegen::arg_from_base64(
                                #fn_name_str,
                                #field_str,
                                args.get(#field_str),
                            )?;
                        }
                    } else {
                        quote! {
                            let #name: #ty = ice_rpc::codegen::arg_from_json(
                                #fn_name_str,
                                #field_str,
                                ice_rpc::codegen::take_field(&mut args, #field_str),
                            )?;
                        }
                    }
                })
                .collect();
            quote! {
                let mut args = args;
                #(#fields)*
            }
        }
    }
}

/// Generates one `ServiceDispatcher::method(...)` registration that bridges an
/// RPC method to the JSON host.
///
/// The body itself lives in `ice_rpc::codegen::json_host_handler`: it is
/// service-agnostic, so the expansion names only the two per-method converters the
/// proxy already exposes and hands them over as function pointers. That is what
/// keeps one copy of the handler per binary instead of one per method.
pub fn gen_json_provider_method(proxy_name: &Ident, fn_name: &Ident, oneway: bool) -> TokenStream {
    let method_name_str = fn_name.to_string();

    if oneway {
        // A one-way method served by a JSON host publishes nothing: the host is
        // handed the call, and its answer stream — if it opens one — is dropped,
        // because there is no caller waiting for it. The dispatcher is told the
        // method is one-way, so it registers no in-flight entry either.
        return quote! {
            {
                dispatcher.oneway_method(
                    ice_rpc::codegen::method_id_of(#method_name_str),
                    move |header: ice_rpc::codegen::RpcHeader,
                          payload: &[u8],
                          _emitter: ice_rpc::codegen::OwnedEmitter|
                          -> ice_rpc::codegen::BoxResponseFuture {
                        ice_rpc::codegen::json_host_oneway_handler(
                            <#proxy_name>::SERVICE_NAME,
                            #method_name_str,
                            header,
                            payload,
                            #proxy_name::deserialize_request_to_value,
                        )
                    },
                );
            }
        };
    }

    quote! {
        {
            dispatcher.method(
                ice_rpc::codegen::method_id_of(#method_name_str),
                move |header: ice_rpc::codegen::RpcHeader,
                      payload: &[u8],
                      emitter: ice_rpc::codegen::OwnedEmitter|
                      -> ice_rpc::codegen::BoxResponseFuture {
                    // The service name is the constant the proxy declares, so the
                    // span shows it instead of the 4-byte hash the header carries.
                    ice_rpc::codegen::json_host_handler(
                        <#proxy_name>::SERVICE_NAME,
                        #method_name_str,
                        header,
                        payload,
                        emitter,
                        #proxy_name::deserialize_request_to_value,
                        #proxy_name::serialize_response_from_value,
                    )
                },
            );
        }
    }
}
