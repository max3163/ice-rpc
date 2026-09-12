//! Procedural macros for the ice-rpc framework.
//!
//! The `#[service]` macro is the single entry point. It automatically
//! generates the Proxy, Client, Server and the lifecycle code
//! for an RPC service trait.

#![cfg_attr(test, allow(clippy::unwrap_used))] // test code may panic
mod codegen;
mod entry;

// Private: the public versions live in `ice-rpc` (`types/consts.rs`). The values
// MUST stay identical (64), the maximum name lengths the wire framing accepts.
const SERVICE_NAME_LEN: usize = 64;
const METHOD_NAME_LEN: usize = 64;

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse::ParseStream, parse_macro_input, ItemTrait, LitInt, LitStr, TraitItem};

use crate::codegen::{
    client::{
        gen_client_lifecycle, gen_client_method, gen_client_struct, ClientGenInput,
        ClientMethodGenInput,
    },
    helpers::{extract_rpc_result_types, g_variant_name},
    http::{gen_http_callable_impl, HttpGenInput, HttpMethodData},
    lifecycle::{gen_lifecycle, LifecycleGenInput},
    nodejs::{
        gen_nodejs_deserialize_fn, gen_nodejs_native_method, gen_nodejs_serialize_fn,
        NodeJsGenInput, NodeJsMethod,
    },
    proxy::{gen_proxy, gen_proxy_method, ProxyGenInput},
    server::{gen_native_method, gen_server, ServerGenInput},
};

/// Validates the `group` parameter of `#[service]`.
///
/// Same rules as the service name: the group becomes part of the iceoryx2
/// service names of the channel (`{group}_req`, `{group}_resp`, …).
fn validate_channel_name(name: &str, span: proc_macro2::Span) -> syn::Result<()> {
    if name.len() > SERVICE_NAME_LEN {
        return Err(syn::Error::new(
            span,
            format!(
                "Channel name '{name}' too long ({} > {SERVICE_NAME_LEN} characters). \
                 Use #[service(..., group = \"ShortName\")].",
                name.len(),
            ),
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(syn::Error::new(
            span,
            format!(
                "Invalid channel name '{name}': only ASCII alphanumeric characters, '_' and '-' are allowed."
            ),
        ));
    }
    if let Some(first) = name.chars().next() {
        if !first.is_ascii_alphanumeric() {
            return Err(syn::Error::new(
                span,
                format!("Invalid channel name '{name}': must start with a letter or a digit."),
            ));
        }
    }
    Ok(())
}

/// Optional parameters of the `#[service]` macro.
///
/// - `#[service]` → the logical name = the trait name in lowercase.
/// - `#[service("MyService")]` → explicit logical name.
/// - `#[service(version = 1)]` → service interface version (default: `1`).
/// - `#[service(..., group = "db")]` → the **channel** this service shares with
///   the other services of the same group. A channel is the unit of transport:
///   it owns one request channel, one response channel and one dispatch thread,
///   and the samples are routed by the service id carried in the header.
///   Defaults to the service name, i.e. one channel per service.
/// - `#[service("MyService", version = 2, group = "db")]` → all.
struct ServiceAttr {
    logical_name: Option<String>,
    group: Option<String>,
    service_version: u16,
}

impl syn::parse::Parse for ServiceAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut logical_name: Option<String> = None;
        let mut group: Option<String> = None;
        let mut service_version: u16 = 1;

        if input.is_empty() {
            return Ok(Self {
                logical_name: None,
                group: None,
                service_version,
            });
        }

        while !input.is_empty() {
            if input.peek(syn::LitStr) {
                let name: LitStr = input.parse()?;
                logical_name = Some(name.value());
            } else {
                let ident: syn::Ident = input.parse()?;
                if ident == "group" {
                    input.parse::<syn::Token![=]>()?;
                    let lit: LitStr = input.parse()?;
                    group = Some(lit.value());
                } else if ident == "version" {
                    input.parse::<syn::Token![=]>()?;
                    let lit: LitInt = input.parse()?;
                    service_version = lit.base10_parse::<u16>()?;
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
            group,
            service_version,
        })
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
/// `"LogicalName"`, `version` and `group` (see [`ServiceAttr`]).
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

    let service_version = service_attr.service_version;

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

    // The channel a service belongs to (defaults to the service name).
    let group = service_attr.group.unwrap_or_else(|| logical_name.clone());
    if let Err(e) = validate_channel_name(&group, trait_name.span()) {
        return e.to_compile_error().into();
    }

    let logical_name_lit = logical_name.clone();
    let group_lit = group.clone();

    let req_enum_name = format_ident!("{}Request", trait_name);
    let client_name = format_ident!("{}Client", trait_name);
    let server_name = format_ident!("{}Server", trait_name);
    let proxy_name = format_ident!("{}Proxy", trait_name);
    let mode_name = format_ident!("{}Mode", trait_name);
    let init_default_name = format_ident!("__{}ServiceInitDefault", trait_name);

    let mut req_variants = Vec::new();
    let mut client_methods = Vec::new();
    let mut variant_discriminant: u8 = 0;
    let mut server_native_methods = Vec::new();
    let mut nodejs_native_methods = Vec::new();
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
                group: &group_lit,
                service_version,
            }));

            server_native_methods.push(gen_native_method(
                fn_name,
                &var_name,
                &arg_names,
                &req_enum_name,
            ));

            nodejs_native_methods.push(gen_nodejs_native_method(&proxy_name, fn_name));

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
        client_methods: &client_methods,
    };
    let client_struct = gen_client_struct(&client_input);
    let client_lifecycle = gen_client_lifecycle(&client_input);

    let server_input = ServerGenInput {
        trait_name,
        visibility,
        server_name: &server_name,
        server_native_methods: &server_native_methods,
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
        group_lit: &group_lit,
        nodejs_native_methods: &nodejs_native_methods,
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

    // Unique symbol to detect name collisions: two services with the same
    // logical name make the linker fail with "duplicate symbol".
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

    // The generated wrappers cannot be documented by the consumer, so they must
    // not trip its `missing_docs` lint; the annotated trait is not exempted.
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
/// Generates a synchronous `fn main` that initializes ice-rpc, awaits the
/// annotated body, then shuts ice-rpc down — **even when the body returns early
/// via `?` or `return`**.
///
/// # Runtime
///
/// No runtime is hard-coded:
/// - `#[ice_rpc::main]` → runtime-agnostic, driven by `ice_rpc::rt::block_on`;
/// - `#[ice_rpc::main(tokio)]` → a dedicated multi-thread tokio runtime;
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
        // The shutdown must come after the awaited body, so an early `return`
        // cannot skip it.
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
