use ice_rpc::{service, Observable};

#[service("MethodLen")]
#[async_trait::async_trait]
pub trait LongMethodService: Send + Sync + 'static {
    async fn this_method_name_is_extremely_long_and_deliberately_exceeds_the_maximum_allowed_length_of_126_characters_for_validation_purposes_aaaaaa(&self) -> Observable<(), String>;
}

fn main() {}
