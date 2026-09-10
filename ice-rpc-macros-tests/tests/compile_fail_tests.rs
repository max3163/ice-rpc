//! Compile-time validation tests (trybuild).
//!
//! Each file in `compile_fail/` must fail to compile
//! with a specific error message.

#![allow(clippy::unwrap_used)] // tests/examples/benches may panic; production libs keep the deny, see [workspace.lints]
#[test]
fn compile_fail_validation() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/service_name_too_long.rs");
    t.compile_fail("tests/compile_fail/invalid_discovery_timeout.rs");
    t.compile_fail("tests/compile_fail/service_name_invalid_chars.rs");
    t.compile_fail("tests/compile_fail/service_name_underscore_start.rs");
    t.compile_fail("tests/compile_fail/method_name_too_long.rs");
    t.compile_fail("tests/compile_fail/service_name_collision.rs");
    // Method signature validation: reported as `compile_error!` on the method
    // (not as a panic inside the macro).
    t.compile_fail("tests/compile_fail/invalid_return_type.rs");
    t.compile_fail("tests/compile_fail/missing_return_type.rs");
    // `#[ice_rpc::main]` only accepts an `async fn main`.
    t.compile_fail("tests/compile_fail/ice_rpc_main_not_async.rs");
}
