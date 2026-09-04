//! Procedural macros for the ice-rpc framework.
//!
//! The `#[service]` macro is the single entry point. It automatically
//! generates the Proxy, Client, Server and the lifecycle code
//! for an RPC service trait.

mod codegen;

// PRIVATE constants — the public versions are in ice-rpc (`types.rs`).
// The values MUST be identical to `ice_rpc::types::{SERVICE_NAME_LEN, METHOD_NAME_LEN}`
// (64) because `RpcHeader` stores the names in a `StaticString<SERVICE_NAME_LEN>`
// and truncates silently past that length.
const SERVICE_NAME_LEN: usize = 64;
const METHOD_NAME_LEN: usize = 64;

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse::ParseStream, parse_macro_input, ItemTrait, LitBool, LitInt, LitStr, TraitItem};

/// `#[cache(ttl = "60s")]` attribute for service trait methods.
///
/// This attribute is a pass-through: the real cache logic is implemented
/// by the `#[service]` macro which reads this attribute on the trait methods.
/// It is exported only so that the Rust compiler recognizes it
/// as a valid attribute.
#[proc_macro_attribute]
pub fn cache(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// `#[timeout("30s")]` attribute for service trait methods.
///
/// Defines a custom timeout (in seconds) for locating the service
/// before the first RPC call. Defaults to
/// `ice_rpc::RPC_CALL_TIMEOUT_SECS` (30s) when omitted.
#[proc_macro_attribute]
pub fn timeout(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

use crate::codegen::{
    client::{
        gen_client_lifecycle, gen_client_method, gen_client_struct, CacheConfig, ClientGenInput,
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
/// - `#[service("MyService", allow_large_payload = true, default_size_message = 8)]` → all.
struct ServiceAttr {
    logical_name: Option<String>,
    allow_large_payload: bool,
    default_size_message_kb: Option<u64>,
}

impl syn::parse::Parse for ServiceAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut logical_name: Option<String> = None;
        let mut allow_large_payload = false;
        let mut default_size_message_kb: Option<u64> = None;

        if input.is_empty() {
            return Ok(Self {
                logical_name: None,
                allow_large_payload: false,
                default_size_message_kb: None,
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
        })
    }
}

/// Parses `#[timeout("30s")]` and returns the duration in seconds.
fn parse_timeout_attr(attrs: &[syn::Attribute]) -> Option<u64> {
    for attr in attrs {
        if !attr.path().is_ident("timeout") {
            continue;
        }
        if let Ok(syn::Meta::NameValue(nv)) = attr.parse_args::<syn::Meta>() {
            if nv.path.is_ident("ttl") {
                if let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(lit_str),
                    ..
                }) = nv.value
                {
                    return parse_duration_str(&lit_str.value());
                }
            }
        }
        // Also supports #[timeout("30s")] without ttl=
        if let Ok(lit_str) = attr.parse_args::<syn::LitStr>() {
            return parse_duration_str(&lit_str.value());
        }
    }
    None
}

/// Extracts the cache configuration from a method's attributes.
///
/// Parses `#[cache(ttl = "60s")]` or `#[cache(ttl = "60s", max_entries = 256)]`.
/// Returns `None` if the `#[cache]` attribute is absent.
fn parse_cache_config(attrs: &[syn::Attribute]) -> Option<CacheConfig> {
    for attr in attrs {
        if !attr.path().is_ident("cache") {
            continue;
        }
        // Parses the attribute content: ttl = "60s", max_entries = 256
        let mut ttl_secs: Option<u64> = None;
        let mut max_entries: usize = 1024;

        if let Ok(list) = attr.parse_args_with(
            syn::punctuated::Punctuated::<syn::MetaNameValue, syn::Token![,]>::parse_separated_nonempty,
        ) {
            for nv in list {
                if nv.path.is_ident("ttl") {
                    if let syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(lit_str),
                        ..
                    }) = nv.value
                    {
                        ttl_secs = parse_duration_str(&lit_str.value());
                    }
                } else if nv.path.is_ident("max_entries") {
                    if let syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Int(lit_int),
                        ..
                    }) = nv.value
                    {
                        if let Ok(v) = lit_int.base10_parse::<usize>() {
                            max_entries = v;
                        }
                    }
                }
            }
        }

        if let Some(ttl) = ttl_secs {
            return Some(CacheConfig {
                ttl_secs: ttl,
                max_entries,
            });
        }
    }
    None
}

/// Parses a duration string like "60s", "5m", "1h" into seconds.
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

fn nodejs_methods_vec(items: &[TraitItem]) -> Vec<NodeJsMethod> {
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

            let output_type = match &method.sig.output {
                syn::ReturnType::Type(_, ty) => ty.clone(),
                _ => panic!("RPC methods must return an Observable<T, E>"),
            };

            let (ok_type, err_type) = extract_rpc_result_types(&output_type);

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
    methods
}

/// `#[service]` attribute macro: generates the Proxy, Client, Server, and the
/// lifecycle code for an RPC service trait.
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

            let output_type = match &method.sig.output {
                syn::ReturnType::Type(_, ty) => ty.clone(),
                _ => panic!("RPC methods must return an Observable<T, E>"),
            };

            let (ok_type, err_type) = extract_rpc_result_types(&output_type);

            // Extracts the cache configuration for this method.
            let cache_config = parse_cache_config(&method.attrs);
            // Extracts the custom timeout.
            let timeout_secs = parse_timeout_attr(&method.attrs);

            req_variants.push(quote! {
                #var_name { #(#arg_names: #arg_types),* }
            });

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
                cache_config: cache_config.as_ref(),
                timeout_secs,
            }));

            server_match_arms.push(gen_server_match_arm(
                trait_name,
                &logical_name_lit,
                fn_name,
                &var_name,
                &arg_names,
                &req_enum_name,
            ));

            node_methods.push(gen_proxy_method(
                fn_name,
                &arg_names,
                &arg_types,
                &output_type,
                &mode_name,
            ));

            // Collects the data for the HttpCallable implementation.
            http_methods_data.push(HttpMethodData {
                fn_name: fn_name.clone(),
                arg_names: arg_names.iter().map(|id| (*id).clone()).collect(),
                arg_types: arg_types.iter().map(|ty| (**ty).clone()).collect(),
                ok_type: (*ok_type).clone(),
                err_type: (*err_type).clone(),
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
    };
    let lifecycle_output = gen_lifecycle(&lifecycle_input);

    let nodejs_methods: Vec<NodeJsMethod> = nodejs_methods_vec(&input_trait.items);
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

    let expanded = quote! {
        #[allow(unexpected_cfgs)]
        #input_trait

        #[derive(ice_rpc::rkyv::Archive, ice_rpc::rkyv::Deserialize, ice_rpc::rkyv::Serialize, Debug)]
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

    expanded.into()
}
