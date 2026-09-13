# ice-rpc-macros

Procedural macros for the [ice-rpc](https://crates.io/crates/ice-rpc) framework.

This crate provides the `#[service]` attribute macro: from a single annotated trait, it generates all the IPC code required by `ice-rpc`.

## Macros

| Macro | Role |
|---|---|
| `#[service]` / `#[service("Name")]` | Generates Request, Client, Server, Proxy, Mode and lifecycle implementations. |

## Generated types

For a trait `DatabaseService` annotated with `#[service("DatabaseService")]`, the macro generates:

- `DatabaseServiceRequest` — rkyv-serializable enum (one variant per method);
- `DatabaseServiceClient` — IPC client with an atomic `NodeId` cache and automatic reconnection;
- `DatabaseServiceServer` — IPC server with a dispatch channel;
- `DatabaseServiceProxy` — smart proxy supporting `Provider`, `Consumer` and `ProviderNodeJs` modes;
- `DatabaseServiceMode` — the mode enum;
- `ServiceLifecycle`, `ServiceNamed` and `ServiceInit` implementations.
- With the `monitoring` feature only: `impl Display for DatabaseServiceRequest`
  and `DatabaseServiceDecoder` (see below).

## Features

| Feature | Default | Effect |
|---|---|---|
| `monitoring` | off | Also generate `impl Display for {Trait}Request` and the `{Trait}Decoder` implementing [`ice_rpc::monitor::ServiceDecoder`]. This is the only part that forces `Display` on every method argument and return type, so a plain provider/consumer must not carry it. `ice-rpc` re-exports it as the `monitoring` feature. |

An out-of-band observer built with this feature registers the generated decoders
to render the observed messages in clear text instead of raw bytes.

## Usage

Normally you do not depend on this crate directly: `ice-rpc` re-exports `service`.

```rust,ignore
use ice_rpc::{service, Observable};

#[service("MyService")]
pub trait MyService: Send + Sync + 'static {
    async fn hello(&self, name: String) -> Observable<String, MyError>;
}
```

## Service parameters

`#[service]` accepts optional parameters, combinable in any order:

```rust,ignore
#[service("MyService", version = 1, group = "db")]
pub trait MyService: Send + Sync + 'static {
    async fn hello(&self, name: String) -> Observable<String, MyError>;
}
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `version` | integer | `1` | Service interface version carried in the RPC header. An incompatible peer is rejected with `RpcError::IncompatibleVersion`. |
| `group` | string | service name | **Channel** shared with the other services of the same group: one request channel, one response channel and one dispatch thread, with the samples routed by the service id. |

The macro does not bound the response wait: use the `Observable` operators
(`timeout`, `take_until`) to bound the latency, or the silence between two values
of a streaming response.

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
