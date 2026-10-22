//! Procedural macros for the ice-rpc framework.
//!
//! The `#[service]` macro is the single entry point. It automatically
//! generates the Proxy, Client, Server and the lifecycle code
//! for an RPC service trait.

#![cfg_attr(test, allow(clippy::unwrap_used))] // test code may panic
mod codegen;
mod entry;
mod features;
mod model;

/// Golden comparison of the whole `#[service]` expansion.
#[cfg(test)]
mod golden_tests;

// Private: the public version lives in `ice-rpc` (`types/consts.rs`). The value
// MUST stay identical. It caps the service or channel name, which becomes the
// iceoryx2 service name (`{channel}{suffix}`, capped at 255 bytes by iceoryx2).
// Method names have no limit: the wire carries the method id, not the name.
pub(crate) const SERVICE_NAME_LEN: usize = 64;

use proc_macro::TokenStream;
use quote::quote;
use syn::{Ident, ItemTrait, Type};

use crate::features::Features;
use crate::model::{ServiceAttr, ServiceModel};

use crate::codegen::{
    client::{
        ClientGenInput, ClientMethodGenInput, ClientOnewayGenInput, gen_client_lifecycle,
        gen_client_method, gen_client_oneway_method, gen_client_struct,
    },
    decoder::{DecoderGenInput, DecoderMethod, gen_decoder},
    json::{
        JsonGenInput, JsonMethod, gen_json_invoker_impl, gen_json_provider_from_request_fn,
        gen_json_provider_from_response_fn, gen_json_provider_method,
    },
    lifecycle::{LifecycleGenInput, gen_lifecycle},
    proxy::{
        ProxyGenInput, ProxyMethodGenInput, ProxyOnewayGenInput, gen_proxy,
        gen_proxy_blocking_method, gen_proxy_method, gen_proxy_oneway_call,
        gen_proxy_oneway_method,
    },
    server::{ServerGenInput, gen_native_method, gen_native_oneway_method, gen_server},
};

/// The JSON view of the model's methods.
///
/// A narrow projection of [`ServiceModel`], **not** a second reading of the
/// trait: the JSON converters need the same names and the same types as
/// everybody else, so re-deriving them could only let the two disagree.
fn json_methods(model: &ServiceModel) -> Vec<JsonMethod> {
    model
        .methods
        .iter()
        .map(|method| JsonMethod {
            fn_name: method.fn_name.clone(),
            var_name: method.var_name.clone(),
            arg_names: method.arg_names.clone(),
            arg_types: method.arg_types.clone(),
            ok_type: method.ok_type.clone(),
            err_type: method.err_type.clone(),
            oneway: method.oneway,
        })
        .collect()
}

/// `#[service]` attribute macro: generates the Proxy, Client, Server, and the
/// lifecycle code for an RPC service trait.
///
/// # Parameters
///
/// `"LogicalName"`, `version`, `group`, `max_slice_len` and `blocking`.
///
/// `max_slice_len` sizes the initial slice of the channel's publishers. It is a
/// property of the **channel** (the `group`), so every service of a group must
/// declare the same value; when omitted, the runtime default
/// (`DEFAULT_MAX_SLICE_LEN`, 256) applies.
///
/// # The return type decides the kind of call
///
/// - `async fn f(..) -> Observable<T, E>` is a **request/response** method: the
///   caller gets a stream, and the provider answers.
/// - `async fn f(..);` — **no return type** — is a **one-way** method: the call is
///   published once, nothing is answered, and no terminal event exists. The
///   generated caller is therefore synchronous and returns
///   `Result<bool, RpcError>`, where the `bool` says whether a subscriber took
///   the sample; the provider handler runs and emits nothing.
///
/// There is no marker attribute and no implicit shape: a value-producing body
/// cannot compile without its return type, so the two vocabularies cannot be
/// confused by mistake. An explicit `-> ()` is refused with both alternatives
/// named — drop the return type, or answer `Observable<(), E>`.
///
/// A one-way method is already synchronous, so the `blocking` parameter adds no
/// `{method}_blocking` twin for it.
///
/// Automatically injects the async-trait attribute
/// (`#[ice_rpc::codegen::async_trait::async_trait]`), `Send + Sync + 'static`
/// as supertraits, and generates:
/// - The `{Trait}Request` enum (rkyv-serializable)
/// - The `{Trait}Client` struct (IPC consumer)
/// - The `{Trait}Server` struct (IPC provider)
/// - The `{Trait}Proxy` struct (Provider/Consumer smart node, plus the
///   `ProviderJson` mode when the `json` block is on)
/// - The `ServiceLifecycle`, `ServiceInit`, `ServiceNamed` implementations
/// - The optional blocks its features ask for: the observer decoder
///   (`monitoring`) and the JSON view (`json`) — the `impl JsonInvoker` every
///   JSON transport dispatches to, the rkyv ↔ `serde_json::Value` converters,
///   and the `ProviderJson` mode with its `provide_json()` constructor. They
///   follow the Cargo features of this crate, read in one place
///   (`Features::from_cfg`).
///
/// The JSON block is emitted whole whatever the deployment: the host half is
/// reachable only from `provide_json()`, so a binary that never calls it does not
/// link it. Splitting the block by feature would instead ask every crate that
/// declares a service to know which process will call it, which a service cannot.
#[proc_macro_attribute]
pub fn service(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand_service_with(attr.into(), item.into(), Features::from_cfg()).into()
}

/// The body of [`service`], on `proc_macro2` tokens so that it stays testable.
///
/// A `proc_macro::TokenStream` cannot be built outside an actual expansion, so
/// everything below the wrapper is written on the `proc_macro2` type: it is what
/// lets the golden test in `tests/golden` expand a trait and read the result.
/// `entry::expand_main` uses the same indirection.
///
/// `features` is the only thing that decides what is generated: no generator
/// reads a Cargo feature, so the same call with the same set produces the same
/// expansion in every build.
fn expand_service_with(
    attr: proc_macro2::TokenStream,
    item: proc_macro2::TokenStream,
    features: Features,
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

    // Whether the `blocking` parameter asked for a synchronous twin of every
    // method, on both the client and the proxy.
    let blocking = service_attr.blocking;

    let trait_name = &model.trait_name;
    let visibility = &model.visibility;
    let logical_name_lit = model.logical_name.clone();
    let group_lit = model.group.clone();
    let service_version = model.service_version;

    // The slice length of the channel's publishers. The default lives in
    // `ice-rpc` (`DEFAULT_MAX_SLICE_LEN`), so the value is declared once and the
    // two crates cannot drift apart.
    let max_slice_len = match model.max_slice_len {
        Some(value) => quote! { #value },
        None => quote! { ice_rpc::codegen::DEFAULT_MAX_SLICE_LEN },
    };

    let req_enum_name = &model.req_enum_name;
    let client_name = &model.client_name;
    let server_name = &model.server_name;
    let proxy_name = &model.proxy_name;
    let mode_name = &model.mode_name;
    let init_default_name = &model.init_default_name;

    // Identity of the contract declared once: the client call and the provider
    // registration both read this same constant, so the interface version can
    // neither be dropped nor drift between them.
    let service_ref = quote! { <#proxy_name>::SERVICE };
    // The logical name travels next to the identity: a span shows it, because the
    // header only carries the 4-byte hash of the name.
    let service_name = quote! { <#proxy_name>::SERVICE_NAME };

    let mut req_variants = Vec::new();
    let mut client_methods = Vec::new();
    let mut proxy_blocking_methods = Vec::new();
    let mut proxy_oneway_calls = Vec::new();
    let mut server_native_methods = Vec::new();
    let mut json_provider_methods = Vec::new();
    let mut node_methods = Vec::new();
    let mut decoder_methods: Vec<DecoderMethod> = Vec::new();

    // One pass over the model. The discriminant is the position in the trait, so
    // the request enum, the header and the router agree by construction.
    for (discriminant, method) in model.numbered_methods() {
        let fn_name = &method.fn_name;
        let var_name = &method.var_name;
        let arg_names: Vec<&Ident> = method.arg_names.iter().collect();
        let arg_types: Vec<&Type> = method.arg_types.iter().collect();

        // The request enum carries the arguments of *every* method: a one-way
        // call travels as a `Request` sample like any other, only its handler
        // never answers.
        req_variants.push(quote! {
            #var_name { #(#arg_names: #arg_types),* } = #discriminant
        });

        if method.oneway {
            client_methods.push(gen_client_oneway_method(&ClientOnewayGenInput {
                visibility,
                fn_name,
                var_name,
                arg_names: &arg_names,
                arg_types: &arg_types,
                req_enum_name,
                group: &group_lit,
                service_ref: &service_ref,
            }));

            server_native_methods.push(gen_native_oneway_method(
                proxy_name,
                fn_name,
                var_name,
                &arg_names,
                req_enum_name,
            ));

            let proxy_oneway_input = ProxyOnewayGenInput {
                fn_name,
                arg_names: &arg_names,
                arg_types: &arg_types,
                mode_name,
                visibility,
                json_provider: features.json,
            };
            node_methods.push(gen_proxy_oneway_method(&proxy_oneway_input));
            proxy_oneway_calls.push(gen_proxy_oneway_call(&proxy_oneway_input));
            // A one-way method is already synchronous: the `blocking` parameter
            // adds no `{method}_blocking` twin for it.
        } else {
            client_methods.push(gen_client_method(&ClientMethodGenInput {
                visibility,
                fn_name,
                var_name,
                arg_names: &arg_names,
                arg_types: &arg_types,
                ok_type: &method.ok_type,
                err_type: &method.err_type,
                req_enum_name,
                group: &group_lit,
                service_ref: &service_ref,
                blocking,
            }));

            server_native_methods.push(gen_native_method(
                proxy_name,
                fn_name,
                var_name,
                &arg_names,
                req_enum_name,
            ));

            let proxy_method_input = ProxyMethodGenInput {
                fn_name,
                arg_names: &arg_names,
                arg_types: &arg_types,
                output_type: &method.output_type,
                mode_name,
                service_ref: &service_ref,
                service_name: &service_name,
                visibility,
                json_provider: features.json,
            };
            node_methods.push(gen_proxy_method(&proxy_method_input));
            if blocking {
                proxy_blocking_methods.push(gen_proxy_blocking_method(&proxy_method_input));
            }
        }

        // The JSON *host* side only: these handlers serve a service whose
        // implementation lives in a JSON host, so a build that only consumes JSON
        // (the HTTP gateway) must not carry them.
        if features.json {
            json_provider_methods.push(gen_json_provider_method(
                proxy_name,
                fn_name,
                method.oneway,
            ));
        }

        // Collects the data for the generated decoder (`Display` + decoder).
        if features.monitoring {
            decoder_methods.push(DecoderMethod {
                method_name: method.fn_name.to_string(),
                var_name: var_name.clone(),
                arg_names: method.arg_names.clone(),
                ok_type: method.ok_type.clone(),
                err_type: method.err_type.clone(),
                oneway: method.oneway,
            });
        }
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
        service_ref: &service_ref,
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
        group_lit: &group_lit,
        max_slice_len: &max_slice_len,
        service_ref: &service_ref,
        node_methods: &node_methods,
        blocking_methods: &proxy_blocking_methods,
        oneway_calls: &proxy_oneway_calls,
        json_provider: features.json,
        json_provider_methods: &json_provider_methods,
    };
    let proxy_output = gen_proxy(&proxy_input);

    let lifecycle_input = LifecycleGenInput {
        trait_name,
        proxy_name,
        server_name,
        mode_name,
        logical_name_lit: &logical_name_lit,
        group_lit: &group_lit,
        max_slice_len: &max_slice_len,
        json_provider: features.json,
    };
    let lifecycle_output = gen_lifecycle(&lifecycle_input);

    // The three optional blocks. Their inputs are built in every configuration —
    // that is what consumes the vectors filled above — and only the generator
    // call is skipped, so nothing here can drift into "declared but unused".
    let json_methods: Vec<JsonMethod> = json_methods(&model);
    let json_input = JsonGenInput {
        visibility,
        proxy_name,
        req_enum_name,
        methods: json_methods,
    };
    // The provider-side converters: they turn an rkyv request into a JSON value and
    // a host answer back into a wire sample. Only `provide_json()` reaches them, so
    // they follow the same flag as the rest of the block and the linker decides.
    let (json_provider_from_request, json_provider_from_response) = if features.json {
        (
            gen_json_provider_from_request_fn(&json_input),
            gen_json_provider_from_response_fn(&json_input),
        )
    } else {
        (quote! {}, quote! {})
    };

    // **One** JSON view per service, whatever the JSON transport: the Node.js
    // gateway and the HTTP gateway both consume it, so neither carries generated
    // code of its own and the two cannot drift apart. The reading policy travels
    // as an argument, which is what keeps a single match table.
    let json_invoker = if features.json {
        gen_json_invoker_impl(&json_input)
    } else {
        quote! {}
    };

    // Human-readable decoding of the service payloads, opt-in via the
    // `monitoring` feature: it is the only part that forces `Display` on every
    // argument and return type, so a plain provider/consumer must not carry it.
    let decoder_output = if features.monitoring {
        let decoder_name = Ident::new(&format!("{trait_name}Decoder"), trait_name.span());
        let decoder_input = DecoderGenInput {
            visibility,
            trait_name,
            req_enum_name,
            decoder_name: &decoder_name,
            logical_name_lit: logical_name_lit.as_str(),
            methods: &decoder_methods,
        };
        gen_decoder(&decoder_input)
    } else {
        quote! {}
    };

    // Unique symbol to detect name collisions: two services with the same
    // logical name make the linker fail with "duplicate symbol".
    let collision_symbol = syn::Ident::new(
        &format!("__ICE_RPC_SVC_{}", logical_name_lit.replace('-', "_")),
        proc_macro2::Span::call_site(),
    );

    let generated = quote! {
        #[repr(u8)]
        // The derives are named through the re-export, and `#[rkyv(crate = …)]`
        // redirects their expansion to it as well: without that attribute the
        // derive defaults to `::rkyv`, which would force every service crate to
        // declare the dependency just to compile its own `{Trait}Request`.
        #[derive(ice_rpc::codegen::rkyv::Archive, ice_rpc::codegen::rkyv::Deserialize, ice_rpc::codegen::rkyv::Serialize, Debug)]
        #[rkyv(crate = ice_rpc::codegen::rkyv)]
        #visibility enum #req_enum_name { #(#req_variants),* }

        impl #proxy_name {
            /// Identity of this service contract: its id inside the channel and
            /// its interface version, declared once and shared by the generated
            /// client and provider so the version cannot be lost between them.
            #visibility const SERVICE: ice_rpc::codegen::ServiceRef =
                ice_rpc::codegen::ServiceRef::new(
                    ice_rpc::codegen::service_id_of(#logical_name_lit),
                    #service_version,
                );
        }

        #client_struct
        #client_lifecycle

        #server_output

        #proxy_output

        #lifecycle_output

        #json_provider_from_request
        #json_provider_from_response
        #json_invoker

        #decoder_output

        #[doc(hidden)]
        #[unsafe(no_mangle)]
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

/// The injected path is `ice_rpc::codegen::async_trait::async_trait`, not the bare
/// `async_trait`, so the service crate needs no `async-trait` dependency. A trait
/// that already names the attribute — either spelling — is left untouched: the
/// detection reads the last path segment, so `#[async_trait::async_trait]` and
/// `#[ice_rpc::async_trait]` are both recognized.
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
            .push(syn::parse_quote! { #[ice_rpc::codegen::async_trait::async_trait] });
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
/// The macro bundles no runtime of its own; it only chooses how `main` is
/// driven:
/// - `#[ice_rpc::main]` → driven by `ice_rpc::rt::block_on`, which polls the
///   body on the calling thread. The body still runs on the execution facade
///   the crate is built with — `rt-async` (the default) provides the executor
///   and the timers, `smol` its own — so this is *not* a runtime-free mode;
/// - `#[ice_rpc::main(tokio)]` → a dedicated multi-thread tokio runtime;
/// - `#[ice_rpc::main(pollster::block_on)]` → any user-provided `fn(Future) -> T`;
/// - `#[ice_rpc::main(rt-none)]` → **no runtime at all**: the annotated function
///   is a plain synchronous `fn main`, the body runs blocking, and ice-rpc is
///   shut down with `shutdown_blocking` on the way out. This is the only mode
///   that pulls in no async runtime.
///
/// # Example (blocking main)
/// ```rust,ignore
/// #[ice_rpc::main(rt-none)]
/// fn main() {
///     let proxy = ice_rpc::locator()
///         .get_blocking::<MyServiceProxy>()
///         .expect("the locator builds the proxy");
///     let value = proxy.hello_blocking("Alice".into()).first_value_blocking();
/// }
/// ```
///
/// # Example Async main
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

/// `#[payload]` attribute macro: derives the traits an ice-rpc service payload
/// needs, **without the crate naming `rkyv`** — and without its author having to
/// ask for `Debug`.
///
/// Expands to the annotated item preceded by
/// `#[derive(ice_rpc::codegen::rkyv::Archive, ice_rpc::codegen::rkyv::Deserialize, ice_rpc::codegen::rkyv::Serialize)]`
/// and `#[rkyv(crate = ice_rpc::codegen::rkyv)]`. The second attribute is what makes
/// the dependency disappear: without it the rkyv derive expands against the
/// default `::rkyv` path, and the service crate would have to declare it.
///
/// # `Debug`
///
/// The generated derivation carries `Debug` as well, **unless the item derives
/// it already or opts out**. A payload type must be `Debug` to be observable: a
/// service built with the `monitoring` feature derives `Debug` on its generated
/// `{Trait}Request` enum, so an out-of-band observer can render the arguments.
/// Supplying the trait here turns a puzzling error on generated code into a
/// populated payload type.
///
/// It is never derived twice — that would be an `E0119`, two `impl Debug for X`.
/// An item whose own `#[derive(..)]` names `Debug`, whatever the spelling
/// (`std::fmt::Debug` included), keeps its derivation alone. A **hand-written
/// `impl Debug`** is invisible to an attribute macro, which only sees the item it
/// annotates, so such a type says so:
///
/// ```rust,ignore
/// #[ice_rpc::payload(no_debug)] // `impl Debug` is written by hand
/// pub struct Query {
///     pub key: String,
/// }
/// ```
///
/// `Display` is deliberately not part of the set: it is not derivable — there is
/// no `#[derive(Display)]` — and it has no canonical generated form. The
/// framework prefers a hand-written `Display` when the type has one, and falls
/// back to this `Debug` form otherwise.
///
/// # `Display`, for an error payload
///
/// `std::error::Error` requires `Debug` **and** `Display`, and `?` converts into
/// a `Box<dyn Error>` only through that trait: a payload used as a service error
/// therefore needs a `Display` as soon as it is propagated with `?` from a `main`
/// returning `Result<(), Box<dyn Error>>`. `Debug` comes from the macro; the
/// `Display` is asked for explicitly, because the *good* rendering — a sentence
/// rather than a field dump — is the one the author writes by hand and an
/// attribute macro cannot detect:
///
/// ```rust,ignore
/// #[ice_rpc::payload(display)] // `impl Display` delegating to `Debug`
/// pub enum MyError {
///     NotFound,
/// }
/// ```
///
/// The generated implementation is `Debug::fmt`, so it must not be combined with
/// a hand-written `impl Display` (an `E0119`), and `no_debug` is refused
/// alongside it, since the delegation would have nothing to render.
///
/// Any `#[derive(..)]`, doc comment or other attribute already on the item is
/// preserved — the generated derives are added in a **second** `#[derive(..)]`,
/// which the compiler merges with the item's own.
///
/// # serde
///
/// When the `json` block is compiled in (the `json` feature of `ice-rpc`, which
/// forwards to this crate), the macro also derives `Serialize` and `Deserialize`,
/// through `ice_rpc::codegen::serde`: the JSON converters read every argument and
/// response through them. Writing them by hand — and keeping them on the same
/// feature as the codegen — used to be the caller's job; it is now the macro's, so
/// the two cannot drift apart, and the service crate needs no `serde` dependency.
///
/// A `#[derive(serde::Serialize, serde::Deserialize)]` already on the item is left
/// alone (a second derivation of one trait is an `E0119`), as is an item that
/// carries its own `#[serde(crate = ..)]`. `no_serde` opts out for a type whose
/// implementations are hand-written, which an attribute macro cannot see.
///
/// Applies to a `struct` or an `enum` only, and refuses an item that already
/// carries an rkyv `crate` attribute, since this macro sets that attribute
/// itself.
///
/// ```rust,ignore
/// #[ice_rpc::payload] // rkyv derives + `Debug`
/// pub struct Query {
///     pub key: String,
/// }
/// ```
#[proc_macro_attribute]
pub fn payload(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand_payload(attr.into(), item.into(), Features::from_cfg()).into()
}

/// Options of [`payload`], read from the attribute's argument list.
#[derive(Default)]
struct PayloadOptions {
    /// `no_debug`: never add the `Debug` derivation. For a type whose `Debug` is
    /// written by hand — an `impl` an attribute macro cannot see — or one that
    /// must stay free of the trait.
    no_debug: bool,
    /// `display`: additionally generate `impl Display`, delegating to `Debug`.
    /// For an error payload that is propagated with `?` into a `Box<dyn Error>`,
    /// which needs the trait.
    display: bool,
    /// `no_serde`: never add the `Serialize` / `Deserialize` derivations. Same
    /// reason as `no_debug`: a hand-written impl is invisible to an attribute
    /// macro, and a second one is an `E0119`.
    no_serde: bool,
}

impl syn::parse::Parse for PayloadOptions {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut options = Self::default();
        while !input.is_empty() {
            let ident: Ident = input.parse()?;
            if ident == "no_debug" {
                options.no_debug = true;
            } else if ident == "display" {
                options.display = true;
            } else if ident == "no_serde" {
                options.no_serde = true;
            } else {
                return Err(syn::Error::new(
                    ident.span(),
                    format!("unknown parameter `{ident}` for #[ice_rpc::payload]"),
                ));
            }
            if input.is_empty() {
                break;
            }
            input.parse::<syn::Token![,]>()?;
        }
        Ok(options)
    }
}

/// Body of [`payload`], kept on `proc_macro2` so a unit test can call it: a
/// `#[proc_macro_attribute]` function is only invocable from the compiler.
fn expand_payload(
    attr: proc_macro2::TokenStream,
    item: proc_macro2::TokenStream,
    features: Features,
) -> proc_macro2::TokenStream {
    let options = match syn::parse2::<PayloadOptions>(attr) {
        Ok(options) => options,
        Err(error) => return error.to_compile_error(),
    };

    let parsed = match syn::parse2::<syn::Item>(item.clone()) {
        Ok(parsed) => parsed,
        Err(error) => return error.to_compile_error(),
    };

    // Whatever the item carries: a `struct` and an `enum` are the two accepted
    // shapes, and both hold their attributes, name and generics in the same
    // fields.
    let (item_attrs, item_ident, item_generics) = match &parsed {
        syn::Item::Struct(item) => (&item.attrs, &item.ident, &item.generics),
        syn::Item::Enum(item) => (&item.attrs, &item.ident, &item.generics),
        other => {
            return syn::Error::new_spanned(
                other,
                "#[ice_rpc::payload] may only be applied to a struct or an enum",
            )
            .to_compile_error();
        }
    };

    if item_attrs.iter().any(is_rkyv_crate_attr) {
        return syn::Error::new_spanned(
            &parsed,
            "#[ice_rpc::payload] already sets `#[rkyv(crate = ..)]`; remove the one written on the item",
        )
        .to_compile_error();
    }

    // The generated `Display` renders through `Debug`, so refusing the pair here
    // is clearer than letting the delegation fail on a payload that has none.
    if options.display && options.no_debug {
        return syn::Error::new_spanned(
            &parsed,
            "#[ice_rpc::payload] cannot combine `display` with `no_debug`: the generated `Display` renders through `Debug`",
        )
        .to_compile_error();
    }

    // `Debug` joins the set only when nothing else provides it: a second
    // derivation of one trait is an `E0119`, and a hand-written `impl` is out of
    // an attribute macro's sight, hence `no_debug`.
    let debug = if options.no_debug || derives_trait(item_attrs, "Debug") {
        quote! {}
    } else {
        quote! { Debug, }
    };

    // The JSON converters read every argument and response through `serde`, so
    // the two derivations follow the same switch as the codegen — emitted here,
    // once, instead of being repeated as a `#[cfg_attr(..)]` on every payload.
    // They name `ice_rpc::codegen::serde`, so the crate declaring the service
    // needs no `serde` dependency of its own.
    //
    // A type that already derives either trait is left untouched: a second
    // derivation is an `E0119`, and its author also owns the `#[serde(..)]`
    // container attributes.
    let add_serde = features.json
        && !options.no_serde
        && !derives_trait(item_attrs, "Serialize")
        && !derives_trait(item_attrs, "Deserialize");

    let serde_derives = if add_serde {
        quote! {
            ice_rpc::codegen::serde::Serialize,
            ice_rpc::codegen::serde::Deserialize,
        }
    } else {
        quote! {}
    };

    // Only meaningful next to the derivations above: without them the item either
    // brings its own `#[serde(..)]` or does not use serde at all.
    let serde_crate_attr = if add_serde {
        quote! { #[serde(crate = "ice_rpc::codegen::serde")] }
    } else {
        quote! {}
    };

    // Opt-in, because the rendering worth having is a hand-written one: an
    // `impl Display` cannot be detected — there is no `#[derive(Display)]` to
    // inspect — so the macro never guesses. `Debug::fmt` is the fallback shape,
    // and the bound it needs is stated, since a derived `Debug` on a generic type
    // carries the `T: Debug` bounds itself.
    let display = if options.display {
        let (impl_generics, ty_generics, where_clause) = item_generics.split_for_impl();
        let mut predicates = where_clause
            .map(|clause| clause.predicates.clone())
            .unwrap_or_default();
        predicates.push(syn::parse_quote!(
            #item_ident #ty_generics: ::std::fmt::Debug
        ));
        quote! {
            impl #impl_generics ::std::fmt::Display for #item_ident #ty_generics
            where #predicates
            {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    ::std::fmt::Debug::fmt(self, f)
                }
            }
        }
    } else {
        quote! {}
    };

    quote! {
        #[derive(
            ice_rpc::codegen::rkyv::Archive,
            ice_rpc::codegen::rkyv::Deserialize,
            ice_rpc::codegen::rkyv::Serialize,
            #serde_derives
            #debug
        )]
        #[rkyv(crate = ice_rpc::codegen::rkyv)]
        #serde_crate_attr
        #item
        #display
    }
}

/// Whether one of the item's **own** `#[derive(..)]` already asks for `trait_name`.
///
/// The trait is matched on the last path segment, so `#[derive(std::fmt::Debug)]`
/// and `#[derive(serde::Serialize)]` count as well: what matters is that the trait
/// is derived once, whatever path spelling names it.
fn derives_trait(attrs: &[syn::Attribute], trait_name: &str) -> bool {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("derive"))
        .any(|attr| {
            let mut found = false;
            // A malformed `derive` is left for the compiler to diagnose: only the
            // presence of the trait matters here.
            let _ = attr.parse_nested_meta(|meta| {
                if meta
                    .path
                    .segments
                    .last()
                    .is_some_and(|segment| segment.ident == trait_name)
                {
                    found = true;
                }
                Ok(())
            });
            found
        })
}

/// Whether one attribute is an rkyv `crate` attribute — `#[rkyv(crate = ..)]` or
/// the bare `#[rkyv(crate)]` form.
fn is_rkyv_crate_attr(attr: &syn::Attribute) -> bool {
    if !attr.path().is_ident("rkyv") {
        return false;
    }
    let mut found = false;
    // A malformed attribute is left to the `rkyv` derive to diagnose; here only
    // the presence of the `crate` key matters.
    let _ = attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("crate") {
            found = true;
            // Consume `= <path>` when present, so parsing does not stop early on
            // the bare `#[rkyv(crate)]` form either.
            let _ = meta.value().and_then(|value| value.parse::<syn::Path>());
        }
        Ok(())
    });
    found
}

#[cfg(test)]
mod payload_tests {
    use super::{Features, expand_payload};
    use quote::quote;

    /// Expands without any optional block, which is the state most assertions
    /// below are written against.
    ///
    /// The set is explicit, never `Features::from_cfg()`: a test must not depend
    /// on the features of the build that runs it.
    fn expand(
        attr: proc_macro2::TokenStream,
        item: proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        expand_payload(
            attr,
            item,
            Features {
                monitoring: false,
                json: false,
            },
        )
    }

    /// Same, with the `json` block compiled in: the serde derivations belong to it.
    fn expand_json(
        attr: proc_macro2::TokenStream,
        item: proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        expand_payload(
            attr,
            item,
            Features {
                monitoring: false,
                json: true,
            },
        )
    }

    /// The generated derivation alone — everything before the `#[rkyv(..)]` the
    /// macro also emits. The item, and thus its own `#[derive(..)]`, comes after.
    fn generated_derivation(out: &proc_macro2::TokenStream) -> String {
        let rendered = out.to_string();
        rendered
            .split("# [rkyv")
            .next()
            .unwrap_or_default()
            .to_string()
    }

    #[test]
    fn payload_adds_the_rkyv_derives_and_redirects_the_crate() {
        let out = expand(
            quote! {},
            quote! {
                #[derive(Debug, Clone)]
                pub struct Query {
                    pub key: String,
                }
            },
        )
        .to_string();
        assert!(
            out.contains("ice_rpc :: codegen :: rkyv :: Archive"),
            "{out}"
        );
        assert!(
            out.contains("ice_rpc :: codegen :: rkyv :: Deserialize"),
            "{out}"
        );
        assert!(
            out.contains("ice_rpc :: codegen :: rkyv :: Serialize"),
            "{out}"
        );
        assert!(
            out.contains("rkyv (crate = ice_rpc :: codegen :: rkyv)"),
            "{out}"
        );
        // The item's own derive and declaration are preserved.
        assert!(out.contains("derive (Debug , Clone)"), "{out}");
        assert!(out.contains("pub struct Query"), "{out}");
    }

    #[test]
    fn payload_adds_debug_when_the_item_does_not_derive_it() {
        let out = expand(
            quote! {},
            quote! {
                pub struct Query { pub key: String }
            },
        );
        // `monitoring` derives `Debug` on the generated request enum, so a payload
        // that asks for nothing must still come out observable.
        let generated = generated_derivation(&out);
        assert!(generated.contains("Debug"), "{out}");
    }

    #[test]
    fn payload_keeps_the_items_own_debug_derivation() {
        let out = expand(
            quote! {},
            quote! {
                #[derive(Debug, Clone)]
                pub struct Query { pub key: String }
            },
        );
        // Deriving it twice would be an `E0119`: the item's own derivation is the
        // only one allowed to name `Debug`.
        let generated = generated_derivation(&out);
        assert!(!generated.contains("Debug"), "{out}");
    }

    #[test]
    fn payload_detects_a_fully_qualified_debug_derive() {
        let out = expand(
            quote! {},
            quote! {
                #[derive(std::fmt::Debug)]
                pub struct Query { pub key: String }
            },
        );
        let generated = generated_derivation(&out);
        assert!(!generated.contains("Debug"), "{out}");
    }

    #[test]
    fn payload_no_debug_leaves_the_derivation_to_a_hand_written_impl() {
        let out = expand(
            quote! { no_debug },
            quote! {
                pub struct Point { pub x: i32 }
            },
        );
        let generated = generated_derivation(&out);
        assert!(!generated.contains("Debug"), "{out}");
        assert!(
            generated.contains("ice_rpc :: codegen :: rkyv :: Archive"),
            "{out}"
        );
    }

    #[test]
    fn payload_display_delegates_to_debug() {
        let out = expand(
            quote! { display },
            quote! {
                pub enum MyError { NotFound }
            },
        );
        let rendered = out.to_string();
        // The trait `?` needs to reach a `Box<dyn Error>`, rendered by `Debug`.
        assert!(
            rendered.contains(
                "impl :: std :: fmt :: Display for MyError where MyError : :: std :: fmt :: Debug"
            ),
            "{rendered}"
        );
        assert!(
            rendered.contains(":: std :: fmt :: Debug :: fmt (self , f)"),
            "{rendered}"
        );
        // `Display` needs something to render, so `Debug` is still derived.
        assert!(generated_derivation(&out).contains("Debug"), "{rendered}");
    }

    #[test]
    fn payload_display_on_a_generic_type_bounds_the_delegation() {
        let out = expand(
            quote! { display },
            quote! {
                pub struct Wrapper<T> { pub inner: T }
            },
        )
        .to_string();
        // The derived `Debug` carries the `T: Debug` bounds, so the delegation
        // states them rather than assuming them.
        assert!(
            out.contains("where Wrapper < T > : :: std :: fmt :: Debug"),
            "{out}"
        );
    }

    #[test]
    fn payload_refuses_display_combined_with_no_debug() {
        let out = expand(
            quote! { display, no_debug },
            quote! {
                pub struct Query { pub key: String }
            },
        )
        .to_string();
        assert!(
            out.contains("cannot combine `display` with `no_debug`"),
            "{out}"
        );
    }

    #[test]
    fn payload_refuses_an_unknown_parameter() {
        let out = expand(
            quote! { debug },
            quote! {
                pub struct Query { pub key: String }
            },
        )
        .to_string();
        assert!(out.contains("unknown parameter `debug`"), "{out}");
    }

    #[test]
    fn payload_works_on_an_enum() {
        let out = expand(
            quote! {},
            quote! {
                pub enum Status { Ok, Nok }
            },
        )
        .to_string();
        assert!(
            out.contains("ice_rpc :: codegen :: rkyv :: Archive"),
            "{out}"
        );
        assert!(out.contains("pub enum Status"), "{out}");
    }

    #[test]
    fn payload_adds_serde_derives_when_the_json_block_is_on() {
        let out = expand_json(
            quote! {},
            quote! {
                pub struct Query { pub key: String }
            },
        )
        .to_string();
        assert!(
            out.contains("ice_rpc :: codegen :: serde :: Serialize"),
            "{out}"
        );
        assert!(
            out.contains("ice_rpc :: codegen :: serde :: Deserialize"),
            "{out}"
        );
        // The derive macro must resolve `serde` through the re-export, since the
        // service crate has no `serde` dependency of its own.
        assert!(
            out.contains(r#"serde (crate = "ice_rpc::codegen::serde")"#),
            "{out}"
        );
    }

    #[test]
    fn payload_without_the_json_block_adds_no_serde() {
        let out = expand(
            quote! {},
            quote! {
                pub struct Query { pub key: String }
            },
        )
        .to_string();
        assert!(!out.contains("serde"), "{out}");
    }

    /// A type that already derives the pair keeps its own — a second derivation
    /// would be an `E0119` — and its author keeps ownership of the container
    /// attributes.
    #[test]
    fn payload_keeps_a_serde_derivation_already_written() {
        let out = expand_json(
            quote! {},
            quote! {
                #[derive(serde::Serialize, serde::Deserialize)]
                pub struct Query { pub key: String }
            },
        )
        .to_string();
        // The item's own derive stays, and nothing of ours is added: a second
        // derivation would be an `E0119`.
        assert_eq!(out.matches("serde :: Serialize").count(), 1, "{out}");
        assert!(!out.contains("codegen :: serde :: Serialize"), "{out}");
        assert!(!out.contains("codegen :: serde :: Deserialize"), "{out}");
        assert!(!out.contains("serde (crate = "), "{out}");
    }

    #[test]
    fn payload_no_serde_opts_out() {
        let out = expand_json(
            quote! { no_serde },
            quote! {
                pub struct Query { pub key: String }
            },
        )
        .to_string();
        assert!(!out.contains("serde"), "{out}");
    }

    #[test]
    fn payload_refuses_a_non_struct_or_enum_item() {
        let out = expand(quote! {}, quote! { fn free() {} }).to_string();
        assert!(
            out.contains("may only be applied to a struct or an enum"),
            "{out}"
        );
    }

    #[test]
    fn payload_refuses_an_item_that_already_sets_the_rkyv_crate() {
        let out = expand(
            quote! {},
            quote! {
                #[rkyv(crate = other::place)]
                pub struct Query { pub key: String }
            },
        )
        .to_string();
        assert!(out.contains("already sets"), "{out}");
    }
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
        assert!(out.contains("ice_rpc :: codegen :: init ()"), "{out}");
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

    #[test]
    fn main_rt_none_wraps_a_synchronous_main() {
        let out = expand(quote! { rt-none }, quote! { fn main() {} });
        assert!(out.contains("ice_rpc :: codegen :: init ()"), "{out}");
        assert!(out.contains("shutdown_blocking"), "{out}");
        assert!(out.starts_with("fn main"), "{out}");
        // No runtime, no future: nothing `async`, nothing `block_on`.
        assert!(!out.contains("async"), "{out}");
        assert!(!out.contains("block_on"), "{out}");
    }

    #[test]
    fn main_rt_none_rejects_an_async_function() {
        let err = expand_main(quote! { rt-none }, quote! { async fn main() {} }).unwrap_err();
        assert!(err.to_string().contains("synchronous"), "{err}");
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
