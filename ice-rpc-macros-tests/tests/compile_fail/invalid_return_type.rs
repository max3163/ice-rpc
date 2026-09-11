use ice_rpc::service;

// `Observable` needs exactly two generic parameters: the value type and the
// business error type. The macro must report this on the method signature
// instead of panicking.
#[service("BadReturn")]
pub trait BadReturn: Send + Sync + 'static {
    async fn ping(&self) -> ice_rpc::Observable<()>;
}

fn main() {}
