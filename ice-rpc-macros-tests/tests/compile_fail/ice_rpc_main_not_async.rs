//! `#[ice_rpc::main]` must reject a synchronous `main`, because the bootstrap
//! and the shutdown are emitted around an awaited body.

#[ice_rpc::main]
fn main() {}
