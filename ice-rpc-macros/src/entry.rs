//! Expansion of the `#[ice_rpc::main]` attribute.
//!
//! Wraps an `async fn main` so that ice-rpc is bootstrapped before the body and
//! shut down after it, including on early `return` / `?`. The macro does not
//! bundle a runtime of its own; it only chooses how `main` is driven:
//! - `#[ice_rpc::main]` → `ice_rpc::rt::block_on`, which polls the body on the
//!   calling thread. It needs no *external* runtime, but the body still runs on
//!   the execution facade the crate was built with — `rt-async` (the default)
//!   supplies the executor and the timers, `smol` its own;
//! - `#[ice_rpc::main(tokio)]` → a dedicated tokio multi-thread runtime;
//! - `#[ice_rpc::main(pollster::block_on)]` → any `fn(Future) -> T` driver;
//! - `#[ice_rpc::main(rt-none)]` → **no runtime at all**: a synchronous `main`.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{ItemFn, Path, ReturnType};

/// How the generated `main` is driven.
enum Driver {
    /// `ice_rpc::rt::block_on` (the default: driven on the calling thread).
    Agnostic,
    /// A dedicated tokio multi-thread runtime.
    Tokio,
    /// `#[ice_rpc::main(rt-none)]`: a **synchronous** main with no runtime at all.
    NoRuntime,
    /// A user-provided `fn(Future) -> T`, e.g. `smol::block_on`.
    Custom(Path),
}

/// Parses the optional attribute argument.
fn parse_driver(attr: TokenStream) -> syn::Result<Driver> {
    if attr.is_empty() {
        return Ok(Driver::Agnostic);
    }
    // `rt-none` carries a hyphen, so it is not a Rust identifier and cannot be
    // parsed as a path: match it on the raw tokens, with the spacing trimmed.
    let compact: String = attr
        .to_string()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    if compact == "rt-none" {
        return Ok(Driver::NoRuntime);
    }
    let path: Path = syn::parse2(attr)?;
    if path.is_ident("tokio") {
        return Ok(Driver::Tokio);
    }
    Ok(Driver::Custom(path))
}

/// Expands `#[ice_rpc::main]` around a `main`.
///
/// Without `rt-none` the annotated function must be `async`: the body is driven
/// by the chosen runtime. With `rt-none` the function is a plain synchronous
/// `fn main` and no runtime is involved at all.
pub fn expand_main(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let driver = parse_driver(attr)?;
    let func: ItemFn = syn::parse2(item)?;

    if func.sig.ident != "main" {
        return Err(syn::Error::new_spanned(
            &func.sig.ident,
            "#[ice_rpc::main] must be applied to `main`",
        ));
    }
    if !func.sig.inputs.is_empty() {
        return Err(syn::Error::new_spanned(
            &func.sig,
            "#[ice_rpc::main] does not accept arguments on `main`",
        ));
    }

    let attrs = &func.attrs;
    let vis = &func.vis;
    let ret = match &func.sig.output {
        ReturnType::Default => quote! {},
        ReturnType::Type(_, ty) => quote! { -> #ty },
    };
    let body = &func.block;

    // `rt-none`: a fully synchronous main. The body runs blocking and is
    // isolated in an immediately-invoked closure, so a `return` or a `?` inside
    // it leaves *the closure* — the `shutdown_blocking` below always runs.
    if matches!(driver, Driver::NoRuntime) {
        if func.sig.asyncness.is_some() {
            return Err(syn::Error::new_spanned(
                func.sig.fn_token,
                "#[ice_rpc::main(rt-none)] must be applied to a synchronous `fn main` (drop `async`)",
            ));
        }
        return Ok(quote! {
            #(#attrs)*
            #vis fn main() #ret {
                let __ice_rpc_guard = ice_rpc::codegen::init();
                #[allow(clippy::redundant_closure_call)]
                let __ice_rpc_result = (|| #body)();
                __ice_rpc_guard.shutdown_blocking();
                __ice_rpc_result
            }
        });
    }

    if func.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            func.sig.fn_token,
            "#[ice_rpc::main] must be applied to an `async fn main` \
             (use `#[ice_rpc::main(rt-none)]` for a synchronous one)",
        ));
    }

    // The body is isolated in its own `async` block: a `return` or a `?` inside
    // it returns from *that* block, so the shutdown below always runs.
    let wrapped = quote! {
        async {
            let __ice_rpc_guard = ice_rpc::codegen::init();
            let __ice_rpc_result = async #body .await;
            __ice_rpc_guard.shutdown().await;
            __ice_rpc_result
        }
    };

    let run = match driver {
        Driver::Agnostic => quote! { ice_rpc::rt::block_on(#wrapped) },
        Driver::Custom(path) => quote! { #path(#wrapped) },
        Driver::Tokio => quote! {
            {
                let __ice_rpc_runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build the tokio runtime for #[ice_rpc::main]");
                let __ice_rpc_output = __ice_rpc_runtime.block_on(#wrapped);
                // Blocking tasks cannot be cancelled, and dropping the runtime
                // waits for them: a bounded grace period lets the process exit.
                __ice_rpc_runtime.shutdown_timeout(std::time::Duration::from_millis(500));
                __ice_rpc_output
            }
        },
        Driver::NoRuntime => unreachable!("handled above"),
    };

    Ok(quote! {
        #(#attrs)*
        #vis fn main() #ret {
            #run
        }
    })
}
