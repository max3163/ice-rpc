use ice_rpc::{service, Observable};

#[service("ThisServiceNameIsVeryLongVeryLongVeryLongVeryLongVeryLongVeryLongVeryLongVeryLongVeryLongVeryLongVeryLongVeryLongVeryLongVeryLongVeryLong")]
#[async_trait::async_trait]
pub trait LongNameService: Send + Sync + 'static {
    async fn ping(&self) -> Observable<(), String>;
}

fn main() {}
