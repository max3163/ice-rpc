#![allow(missing_docs)] // test/example target: documented by Readme.md, not part of a published API
#![allow(clippy::unwrap_used)] // tests/examples/benches may panic; production libs keep the deny, see [workspace.lints]
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

use ice_rpc::gen::ServiceNamed;
use ice_rpc::{self, Observable, ServiceInit};
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
    // The attribute is accepted for source compatibility and ignored by the
    // native request/response transport; both services must still generate a
    // working client.
    let _large = LargePayloadServiceClient::new();
    let _default = DefaultPayloadServiceClient::new();
}

// -----------------------------------------------------------------------------
// Test 7b: a service name of exactly 64 bytes (= SERVICE_NAME_LEN) is accepted
// -----------------------------------------------------------------------------

/// Exactly 64 bytes: the maximum the macro accepts, and the capacity of both
/// the `RpcHeader` `StaticString<64>` and the discovery blackboard key. A
/// name at the limit must be preserved verbatim end to end, otherwise the
/// service would be published but never discoverable.
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
    // The attribute is accepted for source compatibility and ignored by the
    // native request/response transport; both services must still generate a
    // working client.
    let _sized = SizedMessageServiceClient::new();
    let _full = FullServiceClient::new();
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

// -----------------------------------------------------------------------------
// Test 10: #[service(discovery_timeout = "5s")] — service-wide parameter
// -----------------------------------------------------------------------------

#[service("DiscoveryTimeoutService", discovery_timeout = "5s")]
#[async_trait::async_trait]
pub trait DiscoveryTimeoutService: Send + Sync + 'static {
    async fn ping(&self) -> Observable<(), String>;

    async fn other(&self, value: i32) -> Observable<i32, String>;
}

#[service(
    "AllParamsService",
    allow_large_payload = true,
    default_size_message = 4,
    version = 3,
    discovery_timeout = "2m"
)]
#[async_trait::async_trait]
pub trait AllParamsService: Send + Sync + 'static {
    async fn ping(&self) -> Observable<(), String>;
}

#[service("HourTimeoutService", discovery_timeout = "1h")]
#[async_trait::async_trait]
pub trait HourTimeoutService: Send + Sync + 'static {
    async fn ping(&self) -> Observable<(), String>;
}

#[test]
fn test_discovery_timeout_parameter_compiles() {
    // The discovery timeout is declared once for the whole service: both
    // methods share it.
    let client = DiscoveryTimeoutServiceClient::new();
    let _ = &client;

    // All the `#[service]` parameters coexist.
    let all = AllParamsServiceClient::new();
    let _ = &all;

    // The `s`, `m` and `h` duration suffixes are accepted.
    let hour = HourTimeoutServiceClient::new();
    let _ = &hour;
}
