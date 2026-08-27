use ice_rpc::{service, Observable};

#[service("CollisionService")]
#[async_trait::async_trait]
pub trait ServiceA: Send + Sync + 'static {
    async fn ping_a(&self) -> Observable<(), String>;
}

#[service("CollisionService")]
#[async_trait::async_trait]
pub trait ServiceB: Send + Sync + 'static {
    async fn ping_b(&self) -> Observable<(), String>;
}

fn main() {}
