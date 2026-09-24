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
//! # Three pinned feature sets, checked in every build
//!
//! The expansion depends on which optional blocks a build asks for. That set is
//! passed to [`expand_service_with`](crate::expand_service_with) as a value
//! instead of being read from `cfg!` inside the generators, which is what makes
//! this test deterministic: it expands the same trait with the **same three
//! sets** — none, the observer decoder alone, and everything — whatever features
//! the build running it happens to have.
//!
//! Reading `cfg!` here instead would have meant one golden per combination of
//! features (eight), of which a given build can only ever check one, and it
//! would have compared a *different* file depending on which other crate in the
//! workspace enabled which feature. The three sets cover every optional block
//! at least once, since the blocks are independent.
//!
//! # Regenerating
//!
//! ```text
//! ICE_RPC_BLESS=1 cargo test -p ice-rpc-macros
//! ```
//!
//! One command: the three sets are committed together. On a mismatch the current
//! output is written next to the golden as `<name>.actual.rs` so that a `diff`
//! shows what moved; it is removed again on the next successful run.

use std::path::{Path, PathBuf};

use proc_macro2::TokenStream;
use quote::quote;

use crate::features::Features;

/// The smallest expansion: no optional block at all.
const NONE: Features = Features {
    monitoring: false,
    json: false,
    http: false,
};

/// The observer decoder alone, which is what the `monitoring` feature asks for.
const MONITORING: Features = Features {
    monitoring: true,
    json: false,
    http: false,
};

/// Every optional block.
const ALL: Features = Features {
    monitoring: true,
    json: true,
    http: true,
};

/// The feature sets the expansion is pinned in, and the label naming each one.
const PINNED: [(Features, &str); 3] = [(NONE, "none"), (MONITORING, "monitoring"), (ALL, "all")];

/// A nominal service, used by several tests.
const CALCULATOR: fn() -> TokenStream = || {
    quote! {
        #[async_trait::async_trait]
        pub trait Calculator: Send + Sync + 'static {
            async fn add(&self, a: i32, b: i32) -> Observable<i32, String>;
        }
    }
};

/// Where the checked-in expansions live.
fn golden_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden")
}

/// The golden file of one pinned set.
///
/// The name spells the set out (`none` has no suffix); the `Display` of [`ALL`]
/// would otherwise give `<base>.monitoring.json.http.rs`.
fn golden_file(base: &str, label: &str) -> String {
    match label {
        "none" => format!("{base}.rs"),
        other => format!("{base}.{other}.rs"),
    }
}

/// Expands a `#[service]` declaration and returns its pretty-printed form.
fn render(features: Features, attr: TokenStream, item: TokenStream) -> String {
    let expanded = crate::expand_service_with(attr, item, features);
    let file = syn::parse2::<syn::File>(expanded)
        .expect("the #[service] expansion must parse as a whole file");
    prettyplease::unparse(&file)
}

/// Compares one expansion with its golden file (or rewrites it under
/// `ICE_RPC_BLESS`).
fn assert_golden(
    label: &str,
    features: Features,
    base: &str,
    attr: TokenStream,
    item: TokenStream,
) {
    let rendered = render(features, attr, item);
    let path = golden_dir().join(golden_file(base, label));
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
fn a_nominal_service_is_pinned_in_every_feature_set() {
    for (features, label) in PINNED {
        assert_golden(label, features, "calculator", quote! {}, CALCULATOR());
    }
}

/// Every parameter of the attribute, and the three argument shapes the
/// generators special-case: a scalar, an owned `String`, and a `Vec<u8>` (the
/// Node.js bridge converts the last one without a JSON round-trip).
///
/// A unit-returning method and a fallible one are both present, so the golden
/// also fixes the `Ok`/`Err` extraction on both sides.
#[test]
fn a_service_with_a_name_a_version_and_a_group_is_pinned_in_every_feature_set() {
    let item = quote! {
        #[async_trait::async_trait]
        pub trait DatabaseApi: Send + Sync + 'static {
            async fn get(&self, key: String) -> Observable<String, String>;
            async fn put(&self, key: String, value: Vec<u8>) -> Observable<(), String>;
        }
    };
    for (features, label) in PINNED {
        assert_golden(
            label,
            features,
            "database",
            quote! { "Database", version = 2, group = "db" },
            item.clone(),
        );
    }
}

/// Each optional block follows its own flag, in **all** eight combinations.
///
/// This is the contract the goldens cannot express — they show three sets, this
/// one checks the other five — and it costs a few milliseconds instead of five
/// more files.
#[test]
fn every_optional_block_follows_its_own_flag() {
    for monitoring in [false, true] {
        for json in [false, true] {
            for http in [false, true] {
                let features = Features {
                    monitoring,
                    json,
                    http,
                };
                let rendered = render(features, quote! {}, CALCULATOR());

                assert_eq!(
                    rendered.contains("CalculatorDecoder"),
                    monitoring,
                    "the observer decoder must follow `monitoring`, in {features}"
                );
                assert_eq!(
                    rendered.contains("deserialize_request_to_value"),
                    json,
                    "the Node.js converters must follow `json`, in {features}"
                );
                // The Node.js mode is a whole variant and constructor, not only a
                // pair of converters.
                assert_eq!(
                    rendered.contains("fn provide_json"),
                    json,
                    "`provide_json` must follow `json`, in {features}"
                );
                // One JSON view per service, whichever JSON feature asked for it.
                assert_eq!(
                    rendered.contains("impl ice_rpc::gen::JsonInvoker"),
                    json || http,
                    "the JsonInvoker implementation must follow `json` or `http`, in {features}"
                );
            }
        }
    }
}
