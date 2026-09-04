#![allow(unexpected_cfgs)]
// =============================================================================
// Integration tests for the `#[service]` procedural macro.
//
// RULES:
//   1. `#[service]` MUST be placed BEFORE `#[async_trait::async_trait]`
//      so that the macro sees the original `async fn` signatures.
//   2. The trait MUST have `Send + Sync + 'static` as supertraits
//      so that `Arc<dyn Trait>` (used in the generated Provider Mode)
//      is `Send + Sync` and compatible with `RwLock<Mode>`.
// =============================================================================

use ice_rpc::{self, cache, Event, Observable, ServiceInit, ServiceNamed};
use ice_rpc_macros::service;

// -----------------------------------------------------------------------------
// Test 1: Macro without parameter (name = the trait name in lowercase)
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// Test 2: Macro with explicit name
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// Test 3: Proxy provides ServiceInit and ServiceNamed
// -----------------------------------------------------------------------------

#[test]
fn test_proxy_implements_service_init() {
    fn _assert_service_init<T: ServiceInit + ServiceNamed + Send + Sync + 'static>(_t: &T) {}
    let proxy = CalculatorProxy::consume();
    _assert_service_init(&*proxy);
}

// -----------------------------------------------------------------------------
// Test 4: Provider Mode with provide()
// -----------------------------------------------------------------------------

struct CalcImpl;

#[async_trait::async_trait]
impl Calculator for CalcImpl {
    async fn add(&self, a: i32, b: i32) -> Observable<i32, String> {
        let (tx, rx) = ice_rpc::channel::<i32, String>(1);
        tx.send(Event::Next(a + b)).await.ok();
        drop(tx);
        Ok(rx)
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

// -----------------------------------------------------------------------------
// Test 5: Client struct is Send + Sync
// -----------------------------------------------------------------------------

#[test]
fn test_client_struct_is_send_sync() {
    let client = CalculatorClient::new();
    let _: &dyn Send = &client;
    let _: &dyn Sync = &client;
}

// -----------------------------------------------------------------------------
// Test 6: #[cache(ttl)] attribute on a method
// -----------------------------------------------------------------------------

#[service]
#[async_trait::async_trait]
pub trait CachedService: Send + Sync + 'static {
    #[cache(ttl = "60s")]
    async fn get(&self, key: String) -> Observable<String, String>;
}

#[test]
fn test_cache_attribute_compiles() {
    // Checks that the client is correctly generated (the method is annotated #[cache]).
    let _client = CachedServiceClient::new();
}

// -----------------------------------------------------------------------------
// Test 7: `allow_large_payload` parameter
// -----------------------------------------------------------------------------

#[service(allow_large_payload = true)]
#[async_trait::async_trait]
pub trait LargePayloadService: Send + Sync + 'static {
    async fn big(&self, data: String) -> Observable<String, String>;
}

#[service("DefaultPayloadService", allow_large_payload = false)]
#[async_trait::async_trait]
pub trait DefaultPayloadService: Send + Sync + 'static {
    async fn small(&self, data: String) -> Observable<String, String>;
}

#[test]
fn test_allow_large_payload_parameter_compiles() {
    // The `allow_large_payload = true` service must generate a working client
    // and enable the large-payload segment on the global hub.
    let _large = LargePayloadServiceClient::new();
    assert!(ice_rpc::ServiceLocator::global()
        .hub()
        .is_large_payload_enabled());

    // `allow_large_payload = false` (explicit default) still compiles.
    let _default = DefaultPayloadServiceClient::new();
}

// -----------------------------------------------------------------------------
// Test 8: `default_size_message` parameter (in KiB)
// -----------------------------------------------------------------------------

#[service(default_size_message = 4)]
#[async_trait::async_trait]
pub trait SizedMessageService: Send + Sync + 'static {
    async fn echo(&self, data: String) -> Observable<String, String>;
}

#[service("FullService", allow_large_payload = true, default_size_message = 8)]
#[async_trait::async_trait]
pub trait FullService: Send + Sync + 'static {
    async fn ping(&self) -> Observable<(), String>;
}

#[test]
fn test_default_size_message_parameter() {
    // Creating the client must configure the default segment size in bytes.
    let _sized = SizedMessageServiceClient::new();
    assert!(
        ice_rpc::ServiceLocator::global()
            .hub()
            .default_message_size_bytes()
            >= 4 * 1024
    );

    // A larger value wins (the hub keeps the maximum requested size).
    let _full = FullServiceClient::new();
    assert!(
        ice_rpc::ServiceLocator::global()
            .hub()
            .default_message_size_bytes()
            >= 8 * 1024
    );
}

// -----------------------------------------------------------------------------
// Test 9: #[service(version = N)]
// -----------------------------------------------------------------------------

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
