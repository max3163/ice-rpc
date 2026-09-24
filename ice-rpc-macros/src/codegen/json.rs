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
    pub ok_type: Type,
    pub err_type: Type,
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
        // Emitted with the JSON feature set but called only by a JSON bridge.
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

/// Generates the provider-side
/// `serialize_response_from_value(method, value) -> Option<(EventKind, Vec<u8>)>`.
///
/// The host returns an object `{ "type": "next"|"complete"|"error", "data": … }`;
/// this builds the matching `WireEvent`, derives its `EventKind` (stamped in the
/// zero-copy header by the transport) and serializes it to rkyv.
pub fn gen_json_provider_from_response_fn(input: &JsonGenInput<'_>) -> TokenStream {
    let JsonGenInput {
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
                    let kind = event.kind();
                    ice_rpc::gen::rkyv::to_bytes::<ice_rpc::gen::rkyv::rancor::Error>(&event)
                        .ok()
                        .map(|aligned| (kind, aligned.to_vec()))
                }
            }
        })
        .collect();

    quote! {
        // Same rationale as `deserialize_request_to_value` above.
        #[allow(dead_code)]
        impl #proxy_name {
            #visibility fn serialize_response_from_value(method: &str, value: ice_rpc::gen::serde_json::Value) -> Option<(ice_rpc::gen::EventKind, Vec<u8>)> {
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
/// ([`ReadMode`](ice_rpc::gen::ReadMode)) rather than as a second entry point,
/// which is what keeps one match table instead of two.
///
/// # Argument convention
/// Exactly the provider-side convention, so both directions agree (see
/// [`gen_json_provider_from_request_fn`]).
///
/// # Bounds
/// `OkType: Serialize` and `ErrType: Display`, imposed by
/// [`read_json`](ice_rpc::gen::read_json).
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

        #[async_trait::async_trait]
        impl ice_rpc::gen::JsonInvoker for #proxy_name {
            fn service_name(&self) -> &'static str {
                <#proxy_name as ice_rpc::gen::ServiceNamed>::SERVICE_NAME
            }

            async fn invoke_json(
                &self,
                method: &str,
                args: ice_rpc::gen::serde_json::Value,
                read: ice_rpc::gen::ReadMode,
            ) -> Option<ice_rpc::gen::JsonResult> {
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
            args: ice_rpc::gen::serde_json::Value,
            read: ice_rpc::gen::ReadMode,
        ) -> ice_rpc::gen::JsonResult {
            #extraction
            ice_rpc::gen::read_json(#call, read).await
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
/// Every literal the macro knows — the parameter name, the method name — is
/// **inlined** in the emitted format strings: `format!("unknown key '{key}'")`
/// reads as a message, where `format!("unknown key '{}'", "key")` reads as
/// machine output. Only the decoding error itself stays a captured variable.
fn gen_arg_extraction(fn_name_str: &str, arg_names: &[Ident], arg_types: &[Type]) -> TokenStream {
    match arg_names.len() {
        0 => quote! {},
        1 => {
            let arg_name = &arg_names[0];
            let arg_type = &arg_types[0];
            if is_type_vec_u8(arg_type) {
                let not_a_string =
                    format!("parameter '{arg_name}' of '{fn_name_str}' must be a base64 string");
                let undecodable = with_error_suffix(&not_a_string);
                quote! {
                    let #arg_name: #arg_type = {
                        use ice_rpc::gen::base64::Engine;
                        let __text = args.as_str().ok_or_else(|| {
                            ice_rpc::gen::JsonCallError::InvalidArgs(#not_a_string.to_string())
                        })?;
                        ice_rpc::gen::base64::engine::general_purpose::STANDARD
                            .decode(__text)
                            .map_err(|e| ice_rpc::gen::JsonCallError::InvalidArgs(
                                format!(#undecodable),
                            ))?
                    };
                }
            } else {
                let invalid = format!("invalid parameter for '{fn_name_str}'");
                let describe = with_error_suffix(&invalid);
                quote! {
                    let #arg_name: #arg_type = ice_rpc::gen::serde_json::from_value(args)
                        .map_err(|e| ice_rpc::gen::JsonCallError::InvalidArgs(
                            format!(#describe),
                        ))?;
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
                        let missing = format!("missing base64 string parameter '{name}'");
                        let undecodable =
                            with_error_suffix(&format!("invalid base64 parameter '{name}'"));
                        quote! {
                            let #name: #ty = {
                                use ice_rpc::gen::base64::Engine;
                                let __text = args
                                    .get(#field_str)
                                    .and_then(|v| v.as_str())
                                    .ok_or_else(|| ice_rpc::gen::JsonCallError::InvalidArgs(
                                        #missing.to_string(),
                                    ))?;
                                ice_rpc::gen::base64::engine::general_purpose::STANDARD
                                    .decode(__text)
                                    .map_err(|e| ice_rpc::gen::JsonCallError::InvalidArgs(
                                        format!(#undecodable),
                                    ))?
                            };
                        }
                    } else {
                        let invalid = with_error_suffix(&format!(
                            "invalid parameter '{name}' for '{fn_name_str}'"
                        ));
                        quote! {
                            let #name: #ty = {
                                let __value = args
                                    .get(#field_str)
                                    .cloned()
                                    .unwrap_or(ice_rpc::gen::serde_json::Value::Null);
                                ice_rpc::gen::serde_json::from_value(__value).map_err(|e| {
                                    ice_rpc::gen::JsonCallError::InvalidArgs(format!(#invalid))
                                })?
                            };
                        }
                    }
                })
                .collect();
            quote! { #(#fields)* }
        }
    }
}

/// Appends the decoding error to a message prefix.
///
/// The emitted code is `format!("<prefix>: {e}")`, `e` being bound by the
/// surrounding `map_err`, so the two parts read as one sentence in the failure a
/// caller receives.
fn with_error_suffix(prefix: &str) -> String {
    format!("{prefix}: {{e}}")
}

/// Generates one `ServiceDispatcher::method(...)` registration that bridges an
/// RPC method to the JSON host.
///
/// A call may carry **several** events: the host pushes intermediate ones and
/// closes with the terminal one, so a multi-value `Observable` is servable. Each
/// event becomes its own wire sample. The service name comes from the proxy's own
/// `SERVICE_NAME`, so no extra parameter is needed.
///
/// Like the native dispatcher, the handler is wrapped in `call_scoped`, so
/// `CallContext::current()` works inside a JSON-served method and a call it emits
/// continues the incoming trace instead of starting a new root.
pub fn gen_json_provider_method(proxy_name: &Ident, fn_name: &Ident) -> TokenStream {
    let method_name_str = fn_name.to_string();
    quote! {
        {
            dispatcher.method(
                #method_name_str,
                move |header: ice_rpc::gen::RpcHeader,
                      payload: Vec<u8>,
                      emitter: ice_rpc::gen::OwnedEmitter|
                      -> ice_rpc::gen::BoxResponseFuture {
                    // Built before the coroutine: it is copied into the task, and
                    // the header itself is not needed past this point. The service
                    // name is the constant the proxy declares, so the span shows it
                    // instead of the 4-byte hash the header carries.
                    let ctx = ice_rpc::gen::CallContext::new(
                        &header,
                        <#proxy_name>::SERVICE_NAME,
                        #method_name_str,
                    );
                    // Same wrapper as the native dispatcher: the context is
                    // installed around every poll, so the JSON-served method reads
                    // it with `CallContext::current()`, and a call it emits
                    // continues the incoming trace instead of starting a new root.
                    ice_rpc::gen::call_scoped(
                        ctx,
                        async move {
                            let mut emitter = emitter;
                            let Some(args) =
                                #proxy_name::deserialize_request_to_value(#method_name_str, &payload)
                            else {
                                ::log::error!(
                                    "[{}::{}] Failed to deserialize the request",
                                    <#proxy_name>::SERVICE_NAME,
                                    #method_name_str
                                );
                                let _ = ice_rpc::gen::emit_rpc_error(
                                    ice_rpc::gen::RpcError::SerializationError,
                                    &mut *emitter,
                                );
                                return;
                            };
                            // The correlation id is the real one now that the handler
                            // receives the header: the host can correlate its logs.
                            let mut events = match ice_rpc::gen::dispatch_json(
                                header.correlation_id,
                                <#proxy_name>::SERVICE_NAME,
                                #method_name_str,
                                args,
                            )
                            .await
                            {
                                Ok(events) => events,
                                Err(e) => {
                                    ::log::error!(
                                        "[{}::{}] JSON dispatch failed: {}",
                                        <#proxy_name>::SERVICE_NAME,
                                        #method_name_str,
                                        e
                                    );
                                    // The failure is the provider's, not the framing:
                                    // reported as an internal error rather than dropped.
                                    let _ = ice_rpc::gen::emit_rpc_error(
                                        ice_rpc::gen::RpcError::Internal(e),
                                        &mut *emitter,
                                    );
                                    return;
                                }
                            };
                            // One call may carry several events: the host pushes
                            // each intermediate one and closes with the terminal
                            // one. Every event becomes its own wire sample, which
                            // is what lets a multi-value Observable be served.
                            // The loop ends when the host closes the call.
                            while let Some(event) = events.next().await {
                                let value = match event {
                                    ice_rpc::gen::JsonCallEvent::Event(value) => value,
                                    ice_rpc::gen::JsonCallEvent::Failed(message) => {
                                        ::log::error!(
                                            "[{}::{}] JSON call failed: {}",
                                            <#proxy_name>::SERVICE_NAME,
                                            #method_name_str,
                                            message
                                        );
                                        let _ = ice_rpc::gen::emit_rpc_error(
                                            ice_rpc::gen::RpcError::Internal(message),
                                            &mut *emitter,
                                        );
                                        return;
                                    }
                                };
                                match #proxy_name::serialize_response_from_value(#method_name_str, value) {
                                    Some((kind, sample)) => {
                                        emitter.emit(kind, &sample);
                                    }
                                    // An event that cannot be encoded would leave the
                                    // call unanswered: it is reported instead of
                                    // dropped, and the stream stops here.
                                    None => {
                                        ::log::error!(
                                            "[{}::{}] Failed to serialize the JSON response",
                                            <#proxy_name>::SERVICE_NAME,
                                            #method_name_str
                                        );
                                        let _ = ice_rpc::gen::emit_rpc_error(
                                            ice_rpc::gen::RpcError::SerializationError,
                                            &mut *emitter,
                                        );
                                        return;
                                    }
                                }
                            }
                        },
                    )
                },
            );
        }
    }
}
