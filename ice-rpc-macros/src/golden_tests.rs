//! Golden comparison of the whole `#[service]` expansion.
//!
//! # Why a golden file and not a `trybuild` case
//!
//! `trybuild` compares **diagnostics**: it is the right tool for asserting that
//! a bad declaration is refused (which `ice-rpc-macros-tests/tests/compile_fail`
//! already does). It says nothing about the code a valid declaration produces.
//!
//! Comparing `to_string()` directly was the other option, and it is unreadable:
//! the expansion is several thousand characters on a single line, so a reviewer
//! cannot see what a generator change moved — which is precisely the point of
//! having the test. The expansion is therefore parsed back with `syn` and
//! rendered by `prettyplease`, and that rendering is compared with a checked-in
//! file under `tests/golden`.
//!
//! The parse-back step is a second, free guarantee: a generator that emits
//! unbalanced braces or a malformed item fails here rather than only at the
//! first use of the macro.
//!
//! # Regenerating
//!
//! ```text
//! ICE_RPC_BLESS=1 cargo test -p ice-rpc-macros
//! ICE_RPC_BLESS=1 cargo test -p ice-rpc-macros --all-features
//! ```
//!
//! On a mismatch the current output is written next to the golden as
//! `<name>.actual.rs` so that a `diff` shows what moved; it is removed again on
//! the next successful run.
//!
//! # Two sets of goldens, one per feature state
//!
//! The `monitoring` feature adds the `{Trait}Decoder` and the generated `Display`
//! implementation to the same expansion, so an expansion cannot have a single
//! golden: `<base>.rs` is the default build, `<base>.monitoring.rs` the build with
//! the feature on. Both are committed and both are compared, each in its own
//! configuration — a change to the decoder codegen therefore cannot slip through
//! by being tested only where the feature is off, which is exactly what a single
//! golden would have allowed.

use std::path::{Path, PathBuf};

use proc_macro2::TokenStream;
use quote::quote;

/// Where the checked-in expansions live.
fn golden_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden")
}

/// The golden file of a service, in the configuration being tested.
fn golden_file(base: &str) -> String {
    if cfg!(feature = "monitoring") {
        format!("{base}.monitoring.rs")
    } else {
        format!("{base}.rs")
    }
}

/// Expands a `#[service]` declaration and returns its pretty-printed form.
fn render(attr: TokenStream, item: TokenStream) -> String {
    let expanded = crate::expand_service(attr, item);
    let file = syn::parse2::<syn::File>(expanded)
        .expect("the #[service] expansion must parse as a whole file");
    prettyplease::unparse(&file)
}

/// Compares one expansion with its golden file (or rewrites it under
/// `ICE_RPC_BLESS`).
fn assert_golden(base: &str, attr: TokenStream, item: TokenStream) {
    let rendered = render(attr, item);
    let path = golden_dir().join(golden_file(base));
    let actual_path = path.with_extension("actual.rs");

    if std::env::var_os("ICE_RPC_BLESS").is_some() {
        std::fs::create_dir_all(golden_dir()).expect("the golden directory must be creatable");
        std::fs::write(&path, &rendered).expect("the golden file must be writable");
        let _ = std::fs::remove_file(&actual_path);
        return;
    }

    let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "cannot read {} ({e}).\nRun `ICE_RPC_BLESS=1 cargo test -p ice-rpc-macros` to create it.",
            path.display()
        )
    });

    // A checkout with `core.autocrlf=true` rewrites the file's line endings;
    // normalizing them keeps the comparison about the generated code alone.
    let expected = expected.replace("\r\n", "\n");

    if expected == rendered {
        let _ = std::fs::remove_file(&actual_path);
        return;
    }

    std::fs::write(&actual_path, &rendered).expect("the current output must be writable");
    panic!(
        "the #[service] expansion no longer matches {}.\n\
         See what moved:  diff {} {}\n\
         If the change is intended:  ICE_RPC_BLESS=1 cargo test -p ice-rpc-macros\n\
         then review the diff of the golden file.",
        path.display(),
        path.display(),
        actual_path.display()
    );
}

/// The smallest declaration: no parameter, one method, one argument type.
///
/// It pins the default naming (`Calculator` → `calculator`), the request enum,
/// and the shape of every generated wrapper.
#[test]
fn a_nominal_service_expands_to_its_golden_file() {
    assert_golden(
        "calculator",
        quote! {},
        quote! {
            #[async_trait::async_trait]
            pub trait Calculator: Send + Sync + 'static {
                async fn add(&self, a: i32, b: i32) -> Observable<i32, String>;
            }
        },
    );
}

/// Every parameter of the attribute, and the three argument shapes the
/// generators special-case: a scalar, an owned `String`, and a `Vec<u8>` (the
/// Node.js bridge converts the last one without a JSON round-trip).
///
/// A unit-returning method and a fallible one are both present, so the golden
/// also fixes the `Ok`/`Err` extraction on both sides.
#[test]
fn a_service_with_a_name_a_version_and_a_group_expands_to_its_golden_file() {
    assert_golden(
        "database",
        quote! { "Database", version = 2, group = "db" },
        quote! {
            #[async_trait::async_trait]
            pub trait DatabaseApi: Send + Sync + 'static {
                async fn get(&self, key: String) -> Observable<String, String>;
                async fn put(&self, key: String, value: Vec<u8>) -> Observable<(), String>;
            }
        },
    );
}
