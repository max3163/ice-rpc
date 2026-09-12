//! Compatibility shim — this crate is being absorbed into `ice-rpc`.
//!
//! Step 1 of the merge described in `plans/merge-rx.md`: the Rx implementation
//! now lives in `ice_rpc::rx`, and this crate only re-exports it so that the
//! examples keep compiling unchanged. It disappears at step 2, when those
//! examples move to `ice-rpc/examples` and the crate is deleted.

pub use ice_rpc::rx::*;
