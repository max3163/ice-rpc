//! Build script for the Node.js gateway.
//!
//! `napi_build::setup()` configures the linker so the crate is produced as a
//! Node.js-loadable `cdylib` (correct symbol visibility and platform flags).

extern crate napi_build;

fn main() {
    napi_build::setup();
}
