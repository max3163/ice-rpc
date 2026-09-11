use ice_rpc::{service, Observable};

#[service("BadTimeout", discovery_timeout = "abc")]
pub trait BadTimeout: Send + Sync + 'static {
    async fn ping(&self) -> Observable<(), String>;
}

fn main() {}
