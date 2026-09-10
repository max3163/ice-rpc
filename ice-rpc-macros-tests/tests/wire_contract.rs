//! Anti-drift guard for the **documented** wire contract.
//!
//! The `EventKind` discriminants and the `RpcHeader` field capacities are part
//! of the binary protocol: they travel in the iceoryx2 `user_header` of every
//! sample and are shared with third-party implementations (the Node.js
//! gateway, and any future reimplementation). A stale documentation table would
//! silently corrupt interoperability, so the tables are asserted against the
//! source of truth here.
//!
//! # Why this test lives in `ice-rpc-macros-tests`
//!
//! It reads the workspace-root `Readme.md`, which sits **outside** the
//! published `ice-rpc` package directory. An `include_str!` reaching outside
//! the package would compile locally but break `cargo package` / `cargo
//! publish` for `ice-rpc`. `ice-rpc-macros-tests` is never published, so the
//! check can live there safely alongside the other contract tests.

#![allow(clippy::unwrap_used)] // tests/examples/benches may panic; production libs keep the deny, see [workspace.lints]

/// The README must document the real `EventKind` values.
///
/// The expected strings include the alignment padding used by the README code
/// block (`Request` + 2 spaces, `Next` + 5 spaces, …) so that a mere
/// reformatting cannot silently satisfy the assertion.
#[test]
fn readme_documents_the_real_event_kind_discriminants() {
    let readme = include_str!("../../Readme.md");

    let expected = [
        "Request  = 0",
        "Next     = 1",
        "Complete = 2",
        "Error    = 3",
    ];
    for needle in expected {
        assert!(
            readme.contains(needle),
            "Readme.md must document `{needle}` (EventKind discriminants)"
        );
    }

    // The former, wrong mapping was `Next = 0`, `Complete = 1`, `Error = 2`
    // (without `Request`). Both spellings must stay out of the document.
    assert!(
        !readme.contains("Next     = 0"),
        "Readme.md reintroduced the stale EventKind mapping (Next = 0)"
    );
    assert!(
        !readme.contains("Next=0, Complete=1, Error=2"),
        "Readme.md reintroduced the stale EventKind mapping in the RpcHeader table"
    );
}

/// The README must document the real name capacities of `RpcHeader`.
#[test]
fn readme_documents_the_real_rpc_header_capacities() {
    let readme = include_str!("../../Readme.md");

    assert!(
        readme.contains("StaticString<64>"),
        "Readme.md must document the `RpcHeader` `StaticString<64>` capacities"
    );
    assert!(
        !readme.contains("StaticString<126>"),
        "Readme.md reintroduced the stale `StaticString<126>` capacity"
    );
    assert!(
        readme.contains("protocol_version"),
        "Readme.md must document the `protocol_version` field"
    );
    assert!(
        readme.contains("service_version"),
        "Readme.md must document the `service_version` field"
    );
}
