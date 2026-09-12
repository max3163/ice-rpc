//! Expansion of the `#[ice_rpc::main]` attribute.
//!
//! Wraps an `async fn main` so that ice-rpc is bootstrapped before the body and
//! shut down after it — **including on early `return` / `?`**, which is what
//! removes the per-application shutdown wrapper.
//!
//! No runtime is hard-coded; the macro only chooses how `main` is driven:
//! - `#[ice_rpc::main]` → the runtime-agnostic `ice_rpc::rt::block_on`;
//! - `#[ice_rpc::main(tokio)]` → a dedicated tokio multi-thread runtime;
//! - `#[ice_rpc::main(smol::block_on)]` → any `fn(Future) -> T` driver.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{ItemFn, Path, ReturnType};

/// How the generated `main` is driven.
enum Driver {
    /// `ice_rpc::rt::block_on` (default, runtime-agnostic).
    Agnostic,
    /// A dedicated tokio multi-thread runtime.
    Tokio,
    /// A user-provided `fn(Future) -> T`, e.g. `smol::block_on`.
    Custom(Path),
}

/// Parses the optional attribute argument.
fn parse_driver(attr: TokenStream) -> syn::Result<Driver> {
    if attr.is_empty() {
        return Ok(Driver::Agnostic);
    }
    let path: Path = syn::parse2(attr)?;
    if path.is_ident("tokio") {
        return Ok(Driver::Tokio);
    }
    Ok(Driver::Custom(path))
}

/// Expands `#[ice_rpc::main]` applied to `async fn main`.
pub fn expand_main(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let driver = parse_driver(attr)?;
    let func: ItemFn = syn::parse2(item)?;

    if func.sig.ident != "main" {
        return Err(syn::Error::new_spanned(
            &func.sig.ident,
            "#[ice_rpc::main] must be applied to `main`",
        ));
    }
    if func.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            func.sig.fn_token,
            "#[ice_rpc::main] must be applied to an `async fn main`",
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

    // The body is isolated in its own `async` block: a `return` or a `?` inside
    // it returns from *that* block, so the shutdown below always runs.
    let wrapped = quote! {
        async {
            let __ice_rpc_guard = ice_rpc::gen::init();
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
                // waits for them: a parked one (a console read, a user
                // `spawn_blocking` loop) would then keep the process alive *after*
                // the clean shutdown above. A bounded grace period is enough for
                // the tasks shutdown stops, and the process can exit.
                __ice_rpc_runtime.shutdown_timeout(std::time::Duration::from_millis(500));
                __ice_rpc_output
            }
        },
    };

    Ok(quote! {
        #(#attrs)*
        #vis fn main() #ret {
            #run
        }
    })
}
