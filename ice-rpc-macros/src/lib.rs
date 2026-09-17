//! Procedural macros for the ice-rpc framework.
//!
//! The `#[service]` macro is the single entry point. It automatically
//! generates the Proxy, Client, Server and the lifecycle code
//! for an RPC service trait.

#![cfg_attr(test, allow(clippy::unwrap_used))] // test code may panic
mod codegen;
mod entry;
mod model;

/// Golden comparison of the whole `#[service]` expansion.
#[cfg(test)]
mod golden_tests;

// Private: the public versions live in `ice-rpc` (`types/consts.rs`). The values
// MUST stay identical (64), the maximum name lengths the wire framing accepts.
pub(crate) const SERVICE_NAME_LEN: usize = 64;
pub(crate) const METHOD_NAME_LEN: usize = 64;

use proc_macro::TokenStream;
use quote::quote;
use syn::{Ident, ItemTrait, Type};

use crate::model::{ServiceAttr, ServiceModel};

#[cfg(feature = "monitoring")]
use crate::codegen::decoder::{gen_decoder, DecoderGenInput, DecoderMethod};
use crate::codegen::{
    client::{
        gen_client_lifecycle, gen_client_method, gen_client_struct, ClientGenInput,
        ClientMethodGenInput,
    },
    http::{gen_http_callable_impl, HttpGenInput, HttpMethodData},
    lifecycle::{gen_lifecycle, LifecycleGenInput},
    nodejs::{
        gen_nodejs_deserialize_fn, gen_nodejs_native_method, gen_nodejs_serialize_fn,
        NodeJsGenInput, NodeJsMethod,
    },
    proxy::{gen_proxy, gen_proxy_method, ProxyGenInput},
    server::{gen_native_method, gen_server, ServerGenInput},
};

/// The Node.js view of the model's methods.
///
/// A narrow projection of [`ServiceModel`], **not** a second reading of the
/// trait: the Node.js converters need the same names and the same types as
/// everybody else, so re-deriving them could only let the two disagree.
fn nodejs_methods(model: &ServiceModel) -> Vec<NodeJsMethod> {
    model
        .methods
        .iter()
        .map(|method| NodeJsMethod {
            fn_name: method.fn_name.clone(),
            var_name: method.var_name.clone(),
            arg_names: method.arg_names.clone(),
            arg_types: method.arg_types.clone(),
            ok_type: method.ok_type.clone(),
            err_type: method.err_type.clone(),
        })
        .collect()
}

/// `#[service]` attribute macro: generates the Proxy, Client, Server, and the
/// lifecycle code for an RPC service trait.
///
/// # Parameters
///
/// `"LogicalName"`, `version` and `group`.
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
    expand_service(attr.into(), item.into()).into()
}

/// The body of [`service`], on `proc_macro2` tokens so that it stays testable.
///
/// A `proc_macro::TokenStream` cannot be built outside an actual expansion, so
/// everything below the wrapper is written on the `proc_macro2` type: it is what
/// lets the golden test in `tests/golden` expand a trait and read the result.
/// `entry::expand_main` uses the same indirection.
fn expand_service(
    attr: proc_macro2::TokenStream,
    item: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let service_attr = match syn::parse2::<ServiceAttr>(attr) {
        Ok(service_attr) => service_attr,
        Err(e) => return e.to_compile_error(),
    };
    let mut input_trait = match syn::parse2::<ItemTrait>(item) {
        Ok(input_trait) => input_trait,
        Err(e) => return e.to_compile_error(),
    };

    inject_trait_requirements(&mut input_trait);

    // Names, versions, generated type names and the per-method data are read and
    // validated once, by the model. Every generator below works from it, so none
    // of them can accept a signature another one refused.
    let model = match ServiceModel::read(&service_attr, &input_trait) {
        Ok(model) => model,
        Err(e) => return e.to_compile_error(),
    };

    let trait_name = &model.trait_name;
    let visibility = &model.visibility;
    let logical_name_lit = model.logical_name.clone();
    let group_lit = model.group.clone();
    let service_version = model.service_version;

    let req_enum_name = &model.req_enum_name;
    let client_name = &model.client_name;
    let server_name = &model.server_name;
    let proxy_name = &model.proxy_name;
    let mode_name = &model.mode_name;
    let init_default_name = &model.init_default_name;

    let mut req_variants = Vec::new();
    let mut client_methods = Vec::new();
    let mut server_native_methods = Vec::new();
    let mut nodejs_native_methods = Vec::new();
    let mut node_methods = Vec::new();
    let mut http_methods_data: Vec<HttpMethodData> = Vec::new();
    #[cfg(feature = "monitoring")]
    let mut decoder_methods: Vec<DecoderMethod> = Vec::new();

    // One pass over the model. The discriminant is the position in the trait, so
    // the request enum, the header and the router agree by construction.
    for (discriminant, method) in model.numbered_methods() {
        let fn_name = &method.fn_name;
        let var_name = &method.var_name;
        let arg_names: Vec<&Ident> = method.arg_names.iter().collect();
        let arg_types: Vec<&Type> = method.arg_types.iter().collect();

        req_variants.push(quote! {
            #var_name { #(#arg_names: #arg_types),* } = #discriminant
        });

        client_methods.push(gen_client_method(&ClientMethodGenInput {
            visibility,
            fn_name,
            var_name,
            arg_names: &arg_names,
            arg_types: &arg_types,
            ok_type: &method.ok_type,
            err_type: &method.err_type,
            req_enum_name,
            logical_name: &logical_name_lit,
            group: &group_lit,
            service_version,
        }));

        server_native_methods.push(gen_native_method(
            fn_name,
            var_name,
            &arg_names,
            req_enum_name,
        ));

        nodejs_native_methods.push(gen_nodejs_native_method(proxy_name, fn_name));

        node_methods.push(gen_proxy_method(
            fn_name,
            &arg_names,
            &arg_types,
            &method.output_type,
            mode_name,
        ));

        // Collects the data for the HttpCallable implementation.
        http_methods_data.push(HttpMethodData {
            fn_name: fn_name.clone(),
            arg_names: method.arg_names.clone(),
            arg_types: method.arg_types.clone(),
        });

        // Collects the data for the generated decoder (`Display` + decoder).
        #[cfg(feature = "monitoring")]
        decoder_methods.push(DecoderMethod {
            method_name: method.fn_name.to_string(),
            var_name: var_name.clone(),
            arg_names: method.arg_names.clone(),
            ok_type: method.ok_type.clone(),
            err_type: method.err_type.clone(),
        });
    }

    let client_input = ClientGenInput {
        visibility,
        client_name,
        client_methods: &client_methods,
    };
    let client_struct = gen_client_struct(&client_input);
    let client_lifecycle = gen_client_lifecycle(&client_input);

    let server_input = ServerGenInput {
        trait_name,
        visibility,
        server_name,
        server_native_methods: &server_native_methods,
    };
    let server_output = gen_server(&server_input);

    let proxy_input = ProxyGenInput {
        trait_name,
        visibility,
        proxy_name,
        client_name,
        mode_name,
        init_default_name,
        logical_name_lit: &logical_name_lit,
        node_methods: &node_methods,
    };
    let proxy_output = gen_proxy(&proxy_input);

    let lifecycle_input = LifecycleGenInput {
        trait_name,
        proxy_name,
        server_name,
        mode_name,
        logical_name_lit: &logical_name_lit,
        group_lit: &group_lit,
        nodejs_native_methods: &nodejs_native_methods,
    };
    let lifecycle_output = gen_lifecycle(&lifecycle_input);

    let nodejs_methods: Vec<NodeJsMethod> = nodejs_methods(&model);
    let nodejs_input = NodeJsGenInput {
        visibility,
        proxy_name,
        req_enum_name,
        methods: nodejs_methods,
    };
    let nodejs_deserialize = gen_nodejs_deserialize_fn(&nodejs_input);
    let nodejs_serialize = gen_nodejs_serialize_fn(&nodejs_input);

    // Generates the HttpCallable implementation for the proxy.
    let http_input = HttpGenInput {
        proxy_name: proxy_name.to_owned(),
        logical_name: logical_name_lit.to_string(),
        http_methods: http_methods_data,
    };
    let http_callable_impl = gen_http_callable_impl(&http_input);

    // Human-readable decoding of the service payloads, opt-in via the
    // `monitoring` feature: it is the only part that forces `Display` on every
    // argument and return type, so a plain provider/consumer must not carry it.
    #[cfg(feature = "monitoring")]
    let decoder_output = {
        let decoder_name = Ident::new(&format!("{trait_name}Decoder"), trait_name.span());
        let decoder_input = DecoderGenInput {
            visibility,
            req_enum_name,
            decoder_name: &decoder_name,
            logical_name_lit: logical_name_lit.as_str(),
            methods: &decoder_methods,
        };
        gen_decoder(&decoder_input)
    };
    #[cfg(not(feature = "monitoring"))]
    let decoder_output = quote! {};

    // Unique symbol to detect name collisions: two services with the same
    // logical name make the linker fail with "duplicate symbol".
    let collision_symbol = syn::Ident::new(
        &format!("__ICE_RPC_SVC_{}", logical_name_lit.replace('-', "_")),
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

        #decoder_output

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

    expanded
}

/// Adds what the generated wrappers rely on, and only what is missing.
///
/// The documented order is `#[service]` above `#[async_trait::async_trait]`,
/// with the supertraits spelled out, so the annotated trait already carries both
/// by the time the macro runs. Adding them unconditionally produced
/// `pub trait Calculator: Send + Sync + 'static + Send + Sync + 'static` and a
/// doubled `#[async_trait::async_trait]` — equivalent to the compiler, but the
/// generated code is what a user reads to understand a misbehaving call, so it
/// has to look like something a human wrote. Both duplicates were found by the
/// golden test, not by a failing build.
fn inject_trait_requirements(input_trait: &mut ItemTrait) {
    let already_annotated = input_trait.attrs.iter().any(|attr| {
        attr.path()
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "async_trait")
    });
    if !already_annotated {
        input_trait
            .attrs
            .push(syn::parse_quote! { #[async_trait::async_trait] });
    }

    // Compared as rendered tokens: the bound must be recognized whatever the
    // user's spacing, and a path-qualified `std::marker::Send` simply counts as
    // missing, which costs one redundant bound and nothing else.
    let present: Vec<String> = input_trait
        .supertraits
        .iter()
        .map(|bound| quote!(#bound).to_string())
        .collect();

    for required in [quote!(Send), quote!(Sync), quote!('static)] {
        if !present.contains(&required.to_string()) {
            input_trait.supertraits.push(syn::parse_quote!(#required));
        }
    }
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

#[cfg(test)]
mod trait_requirements_tests {
    use super::inject_trait_requirements;
    use syn::ItemTrait;

    #[test]
    fn a_bare_trait_gets_the_attribute_and_the_three_bounds() {
        let mut item: ItemTrait = syn::parse_quote! { trait Bare {} };
        inject_trait_requirements(&mut item);
        assert_eq!(item.attrs.len(), 1);
        assert_eq!(item.supertraits.len(), 3);
    }

    #[test]
    fn a_trait_that_already_declares_them_keeps_one_copy() {
        let mut item: ItemTrait = syn::parse_quote! {
            #[async_trait::async_trait]
            trait Declared: Send + Sync + 'static {}
        };
        inject_trait_requirements(&mut item);
        // `syn` nodes have no `Debug` without the `extra-traits` feature, so the
        // message shows the rendered tokens.
        let rendered = quote::quote!(#item).to_string();
        assert_eq!(
            item.attrs.len(),
            1,
            "the attribute must not be duplicated: {rendered}"
        );
        assert_eq!(
            item.supertraits.len(),
            3,
            "the bounds must not be duplicated: {rendered}"
        );
    }

    #[test]
    fn a_partially_declared_trait_is_completed_in_order() {
        let mut item: ItemTrait = syn::parse_quote! { trait Partial: Send {} };
        inject_trait_requirements(&mut item);
        let rendered = quote::quote!(#item).to_string();
        assert!(
            rendered.contains("Send + Sync + 'static"),
            "the missing bounds must be appended after the declared one: {rendered}"
        );
    }
}
