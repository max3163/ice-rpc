# ice-rpc-macros

Procedural macros for the [ice-rpc](https://crates.io/crates/ice-rpc) framework.

This crate provides the `#[service]` attribute macro: from a single annotated trait, it generates all the IPC code required by `ice-rpc`.

## Macros

| Macro | Role |
|---|---|
| `#[service]` / `#[service("Name")]` | Generates Request, Client, Server, Proxy, Mode and lifecycle implementations. |
| `#[cache(ttl = "60s")]` | Enables a local TTL cache on a client method. |
| `#[timeout("30s")]` | Defines a custom service-location timeout for a method. |

## Generated types

For a trait `DatabaseService` annotated with `#[service("DatabaseService")]`, the macro generates:

- `DatabaseServiceRequest` — rkyv-serializable enum (one variant per method);
- `DatabaseServiceClient` — IPC client with an atomic `NodeId` cache and automatic reconnection;
- `DatabaseServiceServer` — IPC server with a dispatch channel;
- `DatabaseServiceProxy` — smart proxy supporting `Provider`, `Consumer` and `ProviderNodeJs` modes;
- `DatabaseServiceMode` — the mode enum;
- `ServiceLifecycle`, `ServiceNamed` and `ServiceInit` implementations.

## Usage

Normally you do not depend on this crate directly: `ice-rpc` re-exports `service`, `cache` and `timeout`.

```rust,ignore
use ice_rpc::{service, Observable};

#[service("MyService")]
pub trait MyService: Send + Sync + 'static {
    async fn hello(&self, name: String) -> Observable<String, MyError>;
}
```

## Service parameters

`#[service]` accepts three optional parameters, combinable in any order:

```rust,ignore
#[service("MyService", allow_large_payload = true, default_size_message = 8, version = 1)]
pub trait MyService: Send + Sync + 'static {
    async fn hello(&self, name: String) -> Observable<String, MyError>;
}
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `allow_large_payload` | `bool` | `false` | Creates the second shared-memory segment (`_large`) used for payloads above `LARGE_PAYLOAD_THRESHOLD`. |
| `default_size_message` | integer (KiB) | `256` bytes | Initial slice size of the `_default` shared-memory segment publisher. |
| `version` | integer | `1` | Service interface version carried in the RPC header. An incompatible peer is rejected with `RpcError::IncompatibleVersion`. |

## Method attributes

```rust,ignore
#[service("CachedService")]
pub trait CachedService: Send + Sync + 'static {
    #[cache(ttl = "60s", max_entries = 256)]
    #[timeout("30s")]
    async fn get(&self, key: String) -> Observable<String, MyError>;
}
```

`#[cache]` accepts:

- `ttl = "60s"` (or `"5m"`, `"1h"`) — cache lifetime;
- `max_entries = 256` — maximum number of cached responses (default 1024).

`#[timeout]` accepts a duration string and overrides the default service-location timeout.

## Validation

At expansion time, the macro validates:

- service name length (`<= 64` characters) and allowed characters (ASCII alphanumeric, `_`, `-`);
- method name length (`<= 64` characters);
- service name uniqueness (a duplicate logical name in the same binary is a compile-time error).

## Requirements

The generated code references `log` directly (`::log::…`), and your own types derive `rkyv` traits, so the consuming crate must declare both:

```toml
[dependencies]
rkyv = { version = "0.8", features = ["std"] }
log = "0.4"
```

## License

Apache-2.0
