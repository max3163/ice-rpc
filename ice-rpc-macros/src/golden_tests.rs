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
//! # One pinned declaration, and one assertion
//!
//! A single declaration carries every shape the generators special-case: all four
//! parameters of `#[service]`, a scalar argument, an owned `String`, a `Vec<u8>`
//! (which the Node.js bridge converts without a JSON round-trip), a value return,
//! a unit return and a failure. Three files — one per feature set — are then
//! enough, and that is what keeps the golden readable: it is what a reviewer opens
//! to see what a generator change moved.
//!
//! That declaration passes every parameter, so the fallbacks a **bare** trait
//! falls back to — its lowercased name, its version 1 — cannot be read off its
//! golden. They are asserted directly on the expansion by
//! `the_default_naming_of_a_bare_trait_is_pinned`, which is the price of the
//! merge: a substring check instead of three more files to review.
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
};

/// The observer decoder alone, which is what the `monitoring` feature asks for.
const MONITORING: Features = Features {
    monitoring: true,
    json: false,
};

/// Every optional block: the observer decoder and the JSON view.
const ALL: Features = Features {
    monitoring: true,
    json: true,
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

/// The pinned declaration: every parameter of the attribute, every argument shape
/// the generators special-case, and every response shape.
///
/// Four methods, so the numbering, the routing and the two request-enum variants
/// are all visible in one file — `put` returns `()`, which expands differently
/// from a value return, while `get` can fail, which fixes the `Ok`/`Err` extraction
/// on both sides. `notify` has **no return type**: it is the one-way shape, whose
/// client caller is synchronous and whose server handler never emits.
#[test]
fn the_pinned_service_carries_every_shape_in_every_feature_set() {
    let item = quote! {
        #[async_trait::async_trait]
        pub trait DatabaseApi: Send + Sync + 'static {
            async fn add(&self, a: i32, b: i32) -> Observable<i32, String>;
            async fn get(&self, key: String) -> Observable<String, String>;
            async fn put(&self, key: String, value: Vec<u8>) -> Observable<(), String>;
            async fn notify(&self, message: String);
        }
    };
    for (features, label) in PINNED {
        assert_golden(
            label,
            features,
            "database",
            quote! { "Database", version = 2, group = "db", max_slice_len = 4096 },
            item.clone(),
        );
    }
}

/// The default naming, which only a trait **without** attribute parameters can show.
///
/// The declaration above carries all four, so the fallbacks cannot be read off its
/// golden: they are asserted here, on the identifiers the expansion emits. The
/// trade against the two-declaration version is deliberate — a substring check
/// instead of a whole-expansion comparison, for three fewer files to review.
#[test]
fn the_default_naming_of_a_bare_trait_is_pinned() {
    let rendered = render(NONE, quote! {}, CALCULATOR());

    // The logical name falls back to the trait name lowercased. A trait named
    // `Calculator` only ever produces this literal if the fallback ran — the same
    // string names the service on the bus (`SERVICE_NAME`) and its channel (the
    // group).
    assert!(
        rendered.contains("\"calculator\""),
        "the logical name must fall back to the lowercased trait name: {rendered}"
    );
    // The version falls back to 1, in the identity the client and the provider
    // share. Compared whitespace-free and without the call's closing tokens:
    // `prettyplease` decides where to break the call and whether it needs a
    // trailing comma, but it cannot invent the `u16` suffix nor the version.
    assert!(
        compact(&rendered).contains("ice_rpc::codegen::service_id_of(\"calculator\"),1u16"),
        "the identity must be built from the fallback name and version: {rendered}"
    );
}

/// The rendering with every run of whitespace removed.
///
/// For an assertion that must not depend on how `prettyplease` spaces a line —
/// `ServiceRef :: new (…)` today, `ServiceRef::new(…)` tomorrow.
fn compact(rendered: &str) -> String {
    rendered.split_whitespace().collect()
}

/// Each optional block follows its own flag, in **all** eight combinations.
///
/// This is the contract the goldens cannot express — they show three sets, this
/// the other combination — and it costs a few milliseconds instead of one more
/// file.
///
/// One JSON flag, so the four blocks of that view travel together: the
/// `JsonInvoker` implementation, the converters, `provide_json` and the
/// `ProviderJson` mode. They are not split because a service cannot know which
/// process will call it; what decides whether a *binary* carries the host half is
/// that it never constructs a `ProviderJson` proxy, which the linker resolves.
#[test]
fn every_optional_block_follows_its_own_flag() {
    for monitoring in [false, true] {
        for json in [false, true] {
            let features = Features { monitoring, json };
            let rendered = render(features, quote! {}, CALCULATOR());

            assert_eq!(
                rendered.contains("CalculatorDecoder"),
                monitoring,
                "the observer decoder must follow `monitoring`, in {features}"
            );
            assert_eq!(
                rendered.contains("deserialize_request_to_value"),
                json,
                "the host converters must follow `json`, in {features}"
            );
            // The host mode is a whole variant and constructor, not only a
            // pair of converters: they cannot be split from each other.
            assert_eq!(
                rendered.contains("fn provide_json"),
                json,
                "`provide_json` must follow `json`, in {features}"
            );
            // The view a JSON transport dispatches to belongs to the same block.
            assert_eq!(
                rendered.contains("impl ice_rpc::codegen::JsonInvoker"),
                json,
                "the JsonInvoker implementation must follow `json`, in {features}"
            );
        }
    }
}

/// The `blocking` parameter decides, per service, whether the synchronous twin of
/// each method is generated — and nowhere else.
///
/// This is exactly why the twin is an attribute parameter rather than a Cargo
/// feature: the codegen is expanded in the **consumer's** crate and reads no
/// feature, so a service that declares `blocking` gets its twins whatever the
/// build, and a service that does not pays nothing.
#[test]
fn the_blocking_parameter_generates_the_sync_twin_only_when_asked() {
    let without = render(NONE, quote! { "Calc" }, CALCULATOR());
    assert!(
        !without.contains("add_blocking"),
        "a service without `blocking` must not carry the twin: {without}"
    );

    let with = render(NONE, quote! { "Calc", blocking }, CALCULATOR());
    assert_eq!(
        with.matches("fn add_blocking").count(),
        2,
        "the twin is generated on both the client and the proxy: {with}"
    );
    // The proxy twin reads the mode lock blocking instead of awaiting it, and
    // the client twin publishes through the already-synchronous call path.
    assert!(
        with.contains("read_blocking"),
        "the proxy twin must read the mode without an await: {with}"
    );
    assert!(
        with.contains("serialize_and_call"),
        "the client twin publishes through the synchronous call path: {with}"
    );
}
