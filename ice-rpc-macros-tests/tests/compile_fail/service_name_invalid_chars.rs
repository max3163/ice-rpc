use ice_rpc::{service, Observable};

#[service("My Service")]
#[async_trait::async_trait]
pub trait InvalidService: Send + Sync + 'static {
    async fn ping(&self) -> Observable<(), String>;
}

fn main() {}
