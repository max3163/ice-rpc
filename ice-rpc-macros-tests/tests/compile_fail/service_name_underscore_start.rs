use ice_rpc::{service, Observable};

#[service("_UnderscoreStart")]
#[async_trait::async_trait]
pub trait UnderscoreService: Send + Sync + 'static {
    async fn ping(&self) -> Observable<(), String>;
}

fn main() {}
