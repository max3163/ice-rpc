#![allow(missing_docs)] // test/example target: documented by Readme.md, not part of a published API
#![allow(clippy::unwrap_used)] // tests/examples/benches may panic
#![allow(unexpected_cfgs)]
// Integration tests for the `#[service]` procedural macro.
//
// `#[service]` MUST be placed before `#[async_trait::async_trait]`, and the
// trait MUST have `Send + Sync + 'static` as supertraits.

use ice_rpc::gen::ServiceNamed;
use ice_rpc::{self, Observable, ServiceInit};
use ice_rpc_macros::service;

// Test 1: macro without parameter (name = the trait name in lowercase)

#[service]
#[async_trait::async_trait]
pub trait Calculator: Send + Sync + 'static {
    async fn add(&self, a: i32, b: i32) -> Observable<i32, String>;
}

#[test]
fn test_generated_types_exist() {
    let _proxy = CalculatorProxy::consume();
    assert_eq!(_proxy.service_name(), "calculator");
}

#[test]
fn test_request_enum_has_variant() {
    let req = CalculatorRequest::Add { a: 1, b: 2 };
    match req {
        CalculatorRequest::Add { a, b } => {
            assert_eq!(a, 1);
            assert_eq!(b, 2);
        }
    }
}

// Test 2: macro with an explicit name

#[service("CustomName")]
#[async_trait::async_trait]
pub trait NamedService: Send + Sync + 'static {
    async fn ping(&self) -> Observable<(), String>;
}

#[test]
fn test_custom_service_name() {
    let proxy = NamedServiceProxy::consume();
    assert_eq!(proxy.service_name(), "CustomName");
}

#[test]
fn test_custom_name_request_enum() {
    let req = NamedServiceRequest::Ping {};
    match req {
        NamedServiceRequest::Ping {} => {} // OK
    }
}

// Test 3: the proxy implements ServiceInit and ServiceNamed

#[test]
fn test_proxy_implements_service_init() {
    fn _assert_service_init<T: ServiceInit + ServiceNamed + Send + Sync + 'static>(_t: &T) {}
    let proxy = CalculatorProxy::consume();
    _assert_service_init(&*proxy);
}

// Test 4: Provider mode with provide()

struct CalcImpl;

#[async_trait::async_trait]
impl Calculator for CalcImpl {
    async fn add(&self, a: i32, b: i32) -> Observable<i32, String> {
        // Channel-free single-value response.
        ice_rpc::Observable::from_events([ice_rpc::Event::Next(a + b), ice_rpc::Event::Complete])
    }
}

#[test]
fn test_proxy_provide_creates_provider() {
    let _proxy = CalculatorProxy::provide(CalcImpl);
}

#[test]
fn test_proxy_provide_with_init_creates_provider_with_deps() {
    struct CalcWithInit(CalcImpl);

    #[async_trait::async_trait]
    impl Calculator for CalcWithInit {
        async fn add(&self, a: i32, b: i32) -> Observable<i32, String> {
            self.0.add(a, b).await
        }
    }

    #[async_trait::async_trait]
    impl ServiceInit for CalcWithInit {
        fn dependencies(&self) -> Vec<&'static str> {
            vec!["OtherService"]
        }
    }

    let _proxy = CalculatorProxy::provide_with_init(CalcWithInit(CalcImpl));
}

// Test 5: the client struct is Send + Sync

#[test]
fn test_client_struct_is_send_sync() {
    let client = CalculatorClient::new();
    let _: &dyn Send = &client;
    let _: &dyn Sync = &client;
}

// Test 7: a service name of exactly 64 bytes (= SERVICE_NAME_LEN) is accepted

/// Exactly 64 bytes: the maximum the macro accepts, and the capacity of the
/// `RpcHeader` `StaticString<64>`. A name at the limit must survive verbatim.
#[service("MaxLenServiceAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")]
#[async_trait::async_trait]
pub trait MaxLenNameService: Send + Sync + 'static {
    async fn ping(&self) -> Observable<(), String>;
}

#[test]
fn test_service_name_at_max_length_is_accepted_verbatim() {
    assert_eq!(
        <MaxLenNameServiceProxy as ServiceNamed>::SERVICE_NAME.len(),
        64,
        "a 64-byte service name must be accepted without truncation"
    );
    assert_eq!(MaxLenNameServiceProxy::consume().service_name().len(), 64);
}

// Test 8: `#[service(version = N)]`

#[service(version = 2)]
#[async_trait::async_trait]
pub trait VersionedService: Send + Sync + 'static {
    async fn ping(&self) -> Observable<(), String>;
}

#[test]
fn test_versioned_service_compiles() {
    let client = VersionedServiceClient::new();
    let _ = &client;
}

// Test 9: several `#[service]` parameters at once

#[service("AllParamsService", version = 3, group = "allparams")]
#[async_trait::async_trait]
pub trait AllParamsService: Send + Sync + 'static {
    async fn ping(&self) -> Observable<(), String>;
}

#[test]
fn test_all_service_parameters_compile() {
    let client = AllParamsServiceClient::new();
    let _ = &client;
}
