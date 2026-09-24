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
- `DatabaseServiceProxy` — smart proxy supporting `Provider`, `Consumer` and `ProviderJson` modes;
- `DatabaseServiceMode` — the mode enum;
- `ServiceLifecycle`, `ServiceNamed` and `ServiceInit` implementations.
- With the `monitoring` feature only: `impl Display for DatabaseServiceRequest`,
  `DatabaseServiceDecoder` and one **link-time registration** of it into
  `ice_rpc::monitor::DECODERS` (see below); every argument and response value is
  rendered with its own `Display` implementation when it has one, with its
  `Debug` implementation otherwise;
- With the `json` feature only: the rkyv ↔ `serde_json::Value` converters
  (`deserialize_request_to_value`, `serialize_response_from_value`), the
  `ProviderJson` mode of the proxy and its `provide_json()` constructor;
- With the `json` **or** `http` feature: `impl ice_rpc::gen::JsonInvoker for
  DatabaseServiceProxy` — the single JSON view every JSON transport dispatches
  to. The reading mode (`ReadMode::First` / `ReadMode::All`) is an argument of
  `invoke_json`, so one match table serves the single-value and the streaming
  entry points alike, and one inherent helper per method (`json_view_of_{method}`)
  carries the decoding the table would otherwise repeat.

## Features

| Feature | Default | Effect |
|---|---|---|
| `monitoring` | off | Also generate `impl Display for {Trait}Request`, the `{Trait}Decoder` implementing [`ice_rpc::monitor::ServiceDecoder`], and its registration into [`ice_rpc::monitor::DECODERS`] — a `linkme` distributed slice the linker assembles, so an observer lists nothing and calls `Decoders::linked()`. It is the only part that adds code a provider/consumer never calls, so a plain provider/consumer must not carry it. Method arguments and return types only have to be `Debug` — the requirement the generated request enum already imposes: `Display` is preferred when the type provides it, `Debug` is the fallback. `ice-rpc` re-exports it as the `monitoring` feature. |
| `json` | off | Also generate the rkyv ↔ `serde_json::Value` converters and the `ProviderJson` mode. Only a JSON host calls them, so a Rust-only deployment must not carry them: they are about half of the generated code of a service. `gateway_nodejs` asks for them through `common`'s `napi` feature, which enables `ice-rpc/json`. |
| `http` | off | Also generate the JSON view (`impl JsonInvoker`) the HTTP gateway dispatches to — the same one the `json` feature emits, so the two transports cannot drift apart. It is also the block that keeps `serde_json`'s conversion machinery in a binary, so it follows `ice-rpc`'s `http` feature, which the gateway already needs. |

Three independent switches, read in exactly one place: `Features::from_cfg`. Every
generator receives the set as a value, so a build that asks for nothing generates
nothing optional — and a test can expand a trait in any combination without
depending on the features of the build it runs in.

**Always enable them through `ice-rpc`, never on this crate directly** (`ice-rpc/json`,
`ice-rpc/monitoring`): `Features::from_cfg` reads the features of the build that
compiles *this* crate, which is the one `ice-rpc`'s own features select. Asking a
service crate for `ice-rpc-macros/monitoring` while `ice-rpc` was built without it
produces an expansion calling into a runtime that is not there.

An out-of-band observer built with `monitoring` therefore lists nothing: the
decoders of every service **linked into its binary** are already in the slice, and
`ice_rpc::monitor::Decoders::linked()` reads them back to render the observed
messages in clear text instead of raw bytes.

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
