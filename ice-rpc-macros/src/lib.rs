//! Procedural macros for the ice-rpc framework.
//!
//! The `#[service]` macro is the single entry point. It automatically
//! generates the Proxy, Client, Server and the lifecycle code
//! for an RPC service trait.

#![cfg_attr(test, allow(clippy::unwrap_used))] // test code may panic; production libs keep the deny, see [workspace.lints]
mod codegen;
mod entry;

// PRIVATE constants — the public versions are in ice-rpc (`types.rs`).
// The values MUST be identical to `ice_rpc::types::{SERVICE_NAME_LEN, METHOD_NAME_LEN}`
// (64) because `RpcHeader` stores the names in a `StaticString<SERVICE_NAME_LEN>`
// and truncates silently past that length.
const SERVICE_NAME_LEN: usize = 64;
const METHOD_NAME_LEN: usize = 64;

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse::ParseStream, parse_macro_input, ItemTrait, LitBool, LitInt, LitStr, TraitItem};

use crate::codegen::{
    client::{
        gen_client_lifecycle, gen_client_method, gen_client_struct, ClientGenInput,
        ClientMethodGenInput,
    },
    helpers::{extract_rpc_result_types, g_variant_name},
    http::{gen_http_callable_impl, HttpGenInput, HttpMethodData},
    lifecycle::{gen_lifecycle, LifecycleGenInput},
    nodejs::{gen_nodejs_deserialize_fn, gen_nodejs_serialize_fn, NodeJsGenInput, NodeJsMethod},
    proxy::{gen_proxy, gen_proxy_method, ProxyGenInput},
    server::{gen_server, gen_server_match_arm, ServerGenInput},
};

/// Optional parameters of the `#[service]` macro.
///
/// - `#[service]` → the logical name = the trait name in lowercase.
/// - `#[service("MyService")]` → explicit logical name.
/// - `#[service(allow_large_payload = true)]` → enables the second shared-memory
///   segment (default: `false`).
/// - `#[service(default_size_message = 8)]` → initial size (in KiB) of the
///   default shared-memory segment.
/// - `#[service(version = 1)]` → service interface version (default: `1`).
/// - `#[service(discovery_timeout = "5s")]` → **service-wide** deadline for
///   locating the provider before the first call (default:
///   `ice_rpc::gen::RPC_CALL_TIMEOUT_SECS`, 30s). Accepts the `s` / `m` / `h`
///   suffixes. It bounds the *discovery* phase only, never the response wait.
/// - `#[service("MyService", allow_large_payload = true, default_size_message = 8, version = 2, discovery_timeout = "5s")]` → all.
struct ServiceAttr {
    logical_name: Option<String>,
    allow_large_payload: bool,
    default_size_message_kb: Option<u64>,
    service_version: u16,
    discovery_timeout_secs: Option<u64>,
}

impl syn::parse::Parse for ServiceAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut logical_name: Option<String> = None;
        let mut allow_large_payload = false;
        let mut default_size_message_kb: Option<u64> = None;
        let mut service_version: u16 = 1;
        let mut discovery_timeout_secs: Option<u64> = None;

        if input.is_empty() {
            return Ok(Self {
                logical_name: None,
                allow_large_payload: false,
                default_size_message_kb: None,
                service_version,
                discovery_timeout_secs: None,
            });
        }

        while !input.is_empty() {
            if input.peek(syn::LitStr) {
                let name: LitStr = input.parse()?;
                logical_name = Some(name.value());
            } else {
                let ident: syn::Ident = input.parse()?;
                if ident == "allow_large_payload" {
                    input.parse::<syn::Token![=]>()?;
                    let lit: LitBool = input.parse()?;
                    allow_large_payload = lit.value;
                } else if ident == "default_size_message" {
                    input.parse::<syn::Token![=]>()?;
                    let lit: LitInt = input.parse()?;
                    default_size_message_kb = Some(lit.base10_parse::<u64>()?);
                } else if ident == "version" {
                    input.parse::<syn::Token![=]>()?;
                    let lit: LitInt = input.parse()?;
                    service_version = lit.base10_parse::<u16>()?;
                } else if ident == "discovery_timeout" {
                    input.parse::<syn::Token![=]>()?;
                    let lit: LitStr = input.parse()?;
                    discovery_timeout_secs =
                        Some(parse_duration_str(&lit.value()).ok_or_else(|| {
                            syn::Error::new(
                                lit.span(),
                                "invalid duration; expected forms like \"30s\", \"5m\" or \"1h\"",
                            )
                        })?);
                } else {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!("unknown parameter `{}` for #[service]", ident),
                    ));
                }
            }

            if input.is_empty() {
                break;
            }
            input.parse::<syn::Token![,]>()?;
        }

        Ok(Self {
            logical_name,
            allow_large_payload,
            default_size_message_kb,
            service_version,
            discovery_timeout_secs,
        })
    }
}

/// Parses a duration string like `"60s"`, `"5m"`, `"1h"` into seconds.
///
/// Used by the `discovery_timeout` parameter of `#[service]`.
fn parse_duration_str(s: &str) -> Option<u64> {
    let s = s.trim();
    if let Some(rest) = s.strip_suffix('s') {
        rest.parse::<u64>().ok()
    } else if let Some(rest) = s.strip_suffix('m') {
        rest.parse::<u64>().ok().map(|v| v * 60)
    } else if let Some(rest) = s.strip_suffix('h') {
        rest.parse::<u64>().ok().map(|v| v * 3600)
    } else {
        s.parse::<u64>().ok()
    }
}

/// Extracts the return type of an RPC method, plus its `(T, E)` pair.
///
/// This is the single validation path for method signatures: both the
/// request-enum generation and the Node.js converters go through it, so they
/// cannot accept different shapes.
///
/// # Errors
/// Returns a [`syn::Error`] spanned on the method signature (missing return
/// type) or on the offending type. The caller turns it into a `compile_error!`
/// pointing at the user's code instead of panicking inside the macro.
#[allow(clippy::type_complexity)]
fn rpc_method_types(
    method: &syn::TraitItemFn,
) -> syn::Result<(&syn::Type, Box<syn::Type>, Box<syn::Type>)> {
    let output_type = match &method.sig.output {
        syn::ReturnType::Type(_, ty) => ty.as_ref(),
        syn::ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &method.sig,
                "RPC methods must declare a return type, e.g. `-> Observable<T, E>`",
            ))
        }
    };
    let (ok_type, err_type) = extract_rpc_result_types(output_type)?;
    Ok((output_type, ok_type, err_type))
}

fn nodejs_methods_vec(items: &[TraitItem]) -> syn::Result<Vec<NodeJsMethod>> {
    let mut methods = Vec::new();
    for item in items {
        if let TraitItem::Fn(method) = item {
            let fn_name = method.sig.ident.clone();
            let var_name = syn::Ident::new(&g_variant_name(&fn_name.to_string()), fn_name.span());

            let mut arg_names = Vec::new();
            let mut arg_types = Vec::new();

            for arg in method.sig.inputs.iter().skip(1) {
                if let syn::FnArg::Typed(pat_type) = arg {
                    if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                        arg_names.push(pat_ident.ident.clone());
                        arg_types.push((*pat_type.ty).clone());
                    }
                }
            }

            let (_, ok_type, err_type) = rpc_method_types(method)?;

            methods.push(NodeJsMethod {
                fn_name,
                var_name,
                arg_names,
                arg_types,
                ok_type: (*ok_type).clone(),
                err_type: (*err_type).clone(),
            });
        }
    }
    Ok(methods)
}

/// `#[service]` attribute macro: generates the Proxy, Client, Server, and the
/// lifecycle code for an RPC service trait.
///
/// # Parameters
///
/// `"LogicalName"`, `allow_large_payload`, `default_size_message` (KiB),
/// `version` and `discovery_timeout` (duration string such as `"5s"`, `"2m"`,
/// `"1h"`). The discovery timeout is **service-wide**: it bounds the provider
/// lookup performed by `ClientCore::resolve_target` for every method of the
/// service. It does not bound the response wait — use the `timeout` operator
/// (provider-side `ice-rpc-rx`) for that.
///
/// Automatically injects `#[async_trait::async_trait]`, `Send + Sync + 'static`
/// as supertraits, and generates:
/// - The `{Trait}Request` enum (rkyv-serializable)
/// - The `{Trait}Client` struct (IPC consumer)
/// - The `{Trait}Server` struct (IPC provider)
/// - The `{Trait}Proxy` struct (Provider/Consumer/ProviderNodeJs smart node)
/// - The `ServiceLifecycle`, `ServiceInit`, `ServiceNamed` implementations
/// - The Node.js converters (rkyv ↔ serde_json::Value) — always generated, used by the `ProviderNodeJs` mode
#[proc_macro_attribute]
pub fn service(attr: TokenStream, item: TokenStream) -> TokenStream {
    let service_attr = parse_macro_input!(attr as ServiceAttr);
    let mut input_trait = parse_macro_input!(item as ItemTrait);

    input_trait
        .attrs
        .push(syn::parse_quote! { #[async_trait::async_trait] });

    input_trait.supertraits.push(syn::parse_quote! { Send });
    input_trait.supertraits.push(syn::parse_quote! { Sync });
    input_trait.supertraits.push(syn::parse_quote! { 'static });

    let trait_name = &input_trait.ident;
    let visibility = &input_trait.vis;

    let logical_name = service_attr
        .logical_name
        .unwrap_or_else(|| trait_name.to_string().to_lowercase());

    let allow_large_payload = service_attr.allow_large_payload;
    let default_size_message_kb = service_attr.default_size_message_kb;
    let service_version = service_attr.service_version;
    // Discovery timeout is a *service-wide* setting: every method of the
    // service shares the same provider-lookup deadline.
    let discovery_timeout_secs = service_attr.discovery_timeout_secs;

    // ── Service name validation ──────────────────────────────────
    if logical_name.len() > SERVICE_NAME_LEN {
        let max = SERVICE_NAME_LEN;
        return syn::Error::new(
            trait_name.span(),
            format!(
                "Service name '{}' too long ({} > {} characters). \
                 Use #[service(\"ShortName\")] to specify a shorter name.",
                logical_name,
                logical_name.len(),
                max,
            ),
        )
        .to_compile_error()
        .into();
    }
    // Allowed characters: ASCII alphanumerics, underscore, hyphen
    if !logical_name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return syn::Error::new(
            trait_name.span(),
            format!(
                "Invalid service name '{}': only ASCII alphanumeric characters, '_' and '-' are allowed.",
                logical_name,
            ),
        )
        .to_compile_error()
        .into();
    }
    // First letter must be alphanumeric
    if let Some(first) = logical_name.chars().next() {
        if !first.is_ascii_alphanumeric() {
            return syn::Error::new(
                trait_name.span(),
                format!(
                    "Invalid service name '{}': must start with a letter or a digit.",
                    logical_name,
                ),
            )
            .to_compile_error()
            .into();
        }
    }
    // ── End of validation ────────────────────────────────────────

    let ipc_prefix = logical_name.to_lowercase();

    let topic_ready = format!("{}_server_ready", ipc_prefix);
    let logical_name_lit = logical_name.clone();
    let blackboard_key: u8 = 1u8;

    let req_enum_name = format_ident!("{}Request", trait_name);
    let client_name = format_ident!("{}Client", trait_name);
    let server_name = format_ident!("{}Server", trait_name);
    let proxy_name = format_ident!("{}Proxy", trait_name);
    let mode_name = format_ident!("{}Mode", trait_name);
    let init_default_name = format_ident!("__{}ServiceInitDefault", trait_name);

    let mut req_variants = Vec::new();
    let mut client_methods = Vec::new();
    let mut variant_discriminant: u8 = 0;
    let mut server_match_arms = Vec::new();
    let mut node_methods = Vec::new();
    let mut http_methods_data: Vec<HttpMethodData> = Vec::new();
    for item in &input_trait.items {
        if let TraitItem::Fn(method) = item {
            let fn_name = &method.sig.ident;
            let fn_name_str = fn_name.to_string();

            // ── Method name validation ────────────────────────────
            if fn_name_str.len() > METHOD_NAME_LEN {
                return syn::Error::new(
                    fn_name.span(),
                    format!(
                        "Method name '{}' too long ({} > {} characters). \
                         Rename the method so that it is at most {} characters long.",
                        fn_name_str,
                        fn_name_str.len(),
                        METHOD_NAME_LEN,
                        METHOD_NAME_LEN,
                    ),
                )
                .to_compile_error()
                .into();
            }
            // ── End of validation ──────────────────────────────────

            let var_name = syn::Ident::new(&g_variant_name(&fn_name_str), fn_name.span());

            let mut arg_names = Vec::new();
            let mut arg_types = Vec::new();

            for arg in method.sig.inputs.iter().skip(1) {
                if let syn::FnArg::Typed(pat_type) = arg {
                    if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                        arg_names.push(&pat_ident.ident);
                        arg_types.push(pat_type.ty.as_ref());
                    }
                }
            }

            let (output_type, ok_type, err_type) = match rpc_method_types(method) {
                Ok(types) => types,
                Err(e) => return e.to_compile_error().into(),
            };

            req_variants.push(quote! {
                #var_name { #(#arg_names: #arg_types),* } = #variant_discriminant
            });
            variant_discriminant += 1;

            client_methods.push(gen_client_method(&ClientMethodGenInput {
                visibility,
                fn_name,
                var_name: &var_name,
                arg_names: &arg_names,
                arg_types: &arg_types,
                ok_type: &ok_type,
                err_type: &err_type,
                req_enum_name: &req_enum_name,
                logical_name: &logical_name_lit,
                discovery_timeout_secs,
                service_version,
            }));

            server_match_arms.push(gen_server_match_arm(
                trait_name,
                fn_name,
                &var_name,
                &arg_names,
                &req_enum_name,
                service_version,
                (&*ok_type, &*err_type),
            ));

            node_methods.push(gen_proxy_method(
                fn_name,
                &arg_names,
                &arg_types,
                output_type,
                &mode_name,
            ));

            // Collects the data for the HttpCallable implementation.
            http_methods_data.push(HttpMethodData {
                fn_name: fn_name.clone(),
                arg_names: arg_names.iter().map(|id| (*id).clone()).collect(),
                arg_types: arg_types.iter().map(|ty| (**ty).clone()).collect(),
            });
        }
    }

    let client_input = ClientGenInput {
        visibility,
        client_name: &client_name,
        logical_name: &logical_name_lit,
        client_methods: &client_methods,
        allow_large_payload,
        default_size_message_kb,
    };
    let client_struct = gen_client_struct(&client_input);
    let client_lifecycle = gen_client_lifecycle(&client_input);

    let server_input = ServerGenInput {
        trait_name,
        logical_name: &logical_name_lit,
        visibility,
        server_name: &server_name,
        req_enum_name: &req_enum_name,
        topic_ready: &topic_ready,
        blackboard_key,
        server_match_arms: &server_match_arms,
        allow_large_payload,
        default_size_message_kb,
        service_version,
    };
    let server_output = gen_server(&server_input);

    let proxy_input = ProxyGenInput {
        trait_name,
        visibility,
        proxy_name: &proxy_name,
        client_name: &client_name,
        mode_name: &mode_name,
        init_default_name: &init_default_name,
        logical_name_lit: &logical_name_lit,
        node_methods: &node_methods,
    };
    let proxy_output = gen_proxy(&proxy_input);

    let lifecycle_input = LifecycleGenInput {
        trait_name,
        proxy_name: &proxy_name,
        server_name: &server_name,
        mode_name: &mode_name,
        logical_name_lit: &logical_name_lit,
        allow_large_payload,
        default_size_message_kb,
        service_version,
    };
    let lifecycle_output = gen_lifecycle(&lifecycle_input);

    let nodejs_methods: Vec<NodeJsMethod> = match nodejs_methods_vec(&input_trait.items) {
        Ok(methods) => methods,
        Err(e) => return e.to_compile_error().into(),
    };
    let nodejs_input = NodeJsGenInput {
        visibility,
        proxy_name: &proxy_name,
        req_enum_name: &req_enum_name,
        methods: nodejs_methods,
    };
    let nodejs_deserialize = gen_nodejs_deserialize_fn(&nodejs_input);
    let nodejs_serialize = gen_nodejs_serialize_fn(&nodejs_input);

    // Generates the HttpCallable implementation for the proxy.
    let http_input = HttpGenInput {
        proxy_name: proxy_name.clone(),
        logical_name: logical_name_lit.to_string(),
        http_methods: http_methods_data,
    };
    let http_callable_impl = gen_http_callable_impl(&http_input);

    // Generates a unique symbol to detect name collisions.
    // If two services have the same logical_name, the linker will fail
    // with "duplicate symbol".
    let collision_symbol = syn::Ident::new(
        &format!("__ICE_RPC_SVC_{}", logical_name.replace('-', "_")),
        proc_macro2::Span::call_site(),
    );

    let generated = quote! {
        #[repr(u8)]
        #[derive(ice_rpc::gen::rkyv::Archive, ice_rpc::gen::rkyv::Deserialize, ice_rpc::gen::rkyv::Serialize, Debug)]
        #visibility enum #req_enum_name { #(#req_variants),* }

        #client_struct
        #client_lifecycle

        #server_output

        #proxy_output

        #lifecycle_output

        #nodejs_deserialize
        #nodejs_serialize

        #http_callable_impl

        #[doc(hidden)]
        #[no_mangle]
        static #collision_symbol: u8 = 0;
    };

    // The generated wrappers are named after the user's trait and cannot be
    // documented by the consumer, so they must not trip its `missing_docs`
    // lint. The annotated trait itself is exempted from this guard: it stays
    // subject to the consumer's lint configuration.
    let generated = codegen::helpers::allow_missing_docs(generated);

    let expanded = quote! {
        #[allow(unexpected_cfgs)]
        #input_trait

        #generated
    };

    expanded.into()
}

/// Bootstraps ice-rpc around an `async fn main`.
///
/// Generates a synchronous `fn main` that:
/// 1. initializes ice-rpc (`ice_rpc::gen::init()`);
/// 2. awaits the annotated body;
/// 3. shuts ice-rpc down (waiting for the IPC threads and releasing the
///    iceoryx2 node) — **even when the body returns early via `?` or
///    `return`**, because the body runs inside its own `async` block.
///
/// # Runtime
///
/// No runtime is hard-coded:
/// - `#[ice_rpc::main]` → runtime-agnostic, driven by `ice_rpc::rt::block_on`;
/// - `#[ice_rpc::main(tokio)]` → a dedicated multi-thread tokio runtime
///   (requires `tokio` with the `rt-multi-thread` and `time` features);
/// - `#[ice_rpc::main(smol::block_on)]` → any user-provided `fn(Future) -> T`.
///
/// # Example
/// ```rust,ignore
/// #[ice_rpc::main(tokio)]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let proxy = ice_rpc::locator().get::<MyServiceProxy>().await?;
///     // ...
///     Ok(())
/// }
/// ```
#[proc_macro_attribute]
pub fn main(attr: TokenStream, item: TokenStream) -> TokenStream {
    entry::expand_main(attr.into(), item.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#[cfg(test)]
mod entry_tests {
    use super::entry::expand_main;
    use proc_macro2::TokenStream;
    use quote::quote;

    fn expand(attr: TokenStream, item: TokenStream) -> String {
        expand_main(attr, item)
            .expect("expansion should succeed")
            .to_string()
    }

    #[test]
    fn main_default_uses_the_agnostic_driver() {
        let out = expand(
            quote! {},
            quote! { async fn main() -> Result<(), String> { Ok(()) } },
        );
        assert!(out.contains("ice_rpc :: rt :: block_on"), "{out}");
        assert!(out.contains("ice_rpc :: gen :: init ()"), "{out}");
        assert!(out.contains("-> Result < () , String >"), "{out}");
        assert!(
            out.starts_with("fn main"),
            "the generated main must be sync: {out}"
        );
        assert!(!out.contains("async fn main"), "{out}");
    }

    #[test]
    fn main_tokio_builds_a_tokio_runtime() {
        let out = expand(quote! { tokio }, quote! { async fn main() {} });
        assert!(out.contains("new_multi_thread"), "{out}");
        assert!(!out.contains("ice_rpc :: rt :: block_on"), "{out}");
    }

    #[test]
    fn main_custom_driver_is_used_verbatim() {
        let out = expand(quote! { smol::block_on }, quote! { async fn main() {} });
        assert!(out.contains("smol :: block_on"), "{out}");
    }

    #[test]
    fn main_wraps_body_in_an_inner_async_block() {
        let out = expand(quote! {}, quote! { async fn main() {} });
        // The shutdown must be emitted after the awaited body, so an early
        // `return` / `?` inside the body cannot skip it.
        let shutdown = out.find("shutdown").expect("shutdown() missing");
        let body_await = out.find(". await").expect("body await missing");
        assert!(body_await < shutdown, "{out}");
    }

    #[test]
    fn main_rejects_a_synchronous_function() {
        let err = expand_main(quote! {}, quote! { fn main() {} }).unwrap_err();
        assert!(err.to_string().contains("async fn main"));
    }
}
