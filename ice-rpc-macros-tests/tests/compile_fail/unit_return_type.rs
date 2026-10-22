use ice_rpc::service;

// `-> ()` is not an RPC return type: a method with no return type is a one-way
// notification, and a method that answers returns `Observable<T, E>`. The macro
// must name both alternatives instead of silently picking one.
#[service("UnitReturn")]
pub trait UnitReturn: Send + Sync + 'static {
    async fn notify(&self) -> ();
}

fn main() {}
