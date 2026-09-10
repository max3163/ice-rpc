use ice_rpc::service;

// An RPC method must declare its return type: `#[service]` cannot infer the
// `(T, E)` pair of the generated request enum.
#[service("NoReturn")]
pub trait NoReturn: Send + Sync + 'static {
    async fn ping(&self);
}

fn main() {}
