//! Compile-time validation tests (trybuild).
//!
//! Each file in `compile_fail/` must fail to compile
//! with a specific error message.

#[test]
fn compile_fail_validation() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/service_name_too_long.rs");
    t.compile_fail("tests/compile_fail/service_name_invalid_chars.rs");
    t.compile_fail("tests/compile_fail/service_name_underscore_start.rs");
    t.compile_fail("tests/compile_fail/method_name_too_long.rs");
    t.compile_fail("tests/compile_fail/service_name_collision.rs");
}
