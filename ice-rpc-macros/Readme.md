# ice-rpc-macros

Procedural macros for the [ice-rpc](https://crates.io/crates/ice-rpc) framework.

This crate provides the `#[service]` attribute macro: from a single annotated trait, it generates all the IPC code required by `ice-rpc`.

## Macros

| Macro                               | Role                                                                          |
| ----------------------------------- | ----------------------------------------------------------------------------- |
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
- With the `json` feature: the whole JSON view of the service —
  `impl ice_rpc::codegen::JsonInvoker for DatabaseServiceProxy`, the single view
  every JSON transport dispatches to (the reading mode, `ReadMode::First` /
  `ReadMode::All`, is an argument of `invoke_json`, so one match table serves the
  single-value and the streaming entry points alike, and one inherent helper per
  method, `json_view_of_{method}`, carries the decoding the table would otherwise
  repeat) — plus the rkyv ↔ `serde_json::Value` converters
  (`deserialize_request_to_value`, `serialize_response_from_value`), the
  `ProviderJson` mode and its `provide_json()` constructor. The constructor builds
  the JSON dispatcher, so the host half is reachable only from a binary that calls
  it: everything else is dropped by the linker.

## Features

| Feature      | Default | Effect                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ------------ | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `monitoring` | off     | Also generate `impl Display for {Trait}Request`, the `{Trait}Decoder` implementing [`ice_rpc::monitor::ServiceDecoder`], and its registration into [`ice_rpc::monitor::DECODERS`] — a `linkme` distributed slice the linker assembles, so an observer lists nothing and calls `Decoders::linked()`. It is the only part that adds code a provider/consumer never calls, so a plain provider/consumer must not carry it. Method arguments and return types only have to be `Debug` — the requirement the generated request enum already imposes: `Display` is preferred when the type provides it, `Debug` is the fallback. `ice-rpc` re-exports it as the `monitoring` feature.                                                                            |
| `json`       | off     | Also generate the whole JSON view of the service: `impl JsonInvoker` (the `JsonInvoker` every JSON transport dispatches to — the HTTP gateway, the Node.js consumer), the rkyv ↔ `serde_json::Value` converters, and the `ProviderJson` mode with its `provide_json()` constructor. One switch, because only the program that opens an entry point knows how the service will be called; a service crate cannot. The host half is reachable only from `provide_json()`, so the link step drops it in a binary that never builds such a proxy — the gateway carries no converter. The `Value` is an in-memory tree, not JSON text; the Node.js bridge converts it to V8 objects. See [`docs/nodejs-json-bridge-adr.md`](../docs/nodejs-json-bridge-adr.md). |

Two switches (`monitoring`, `json`), read in exactly one place:
`Features::from_cfg`. Every generator receives the set as a value, so a build
that asks for nothing generates nothing optional — and a test can expand a trait
in every configuration without depending on the features of the build it runs in.
The golden matrix is therefore four combinations, not eight.

**Always enable them through `ice-rpc`, never on this crate directly** (`ice-rpc/json`,
`ice-rpc/monitoring`): `Features::from_cfg` reads the features of the build that
compiles _this_ crate, which is the one `ice-rpc`'s own features select. Asking a
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
#[service("MyService", version = 1, group = "db", blocking)]
pub trait MyService: Send + Sync + 'static {
    async fn hello(&self, name: String) -> Observable<String, MyError>;
}
```

| Parameter  | Type    | Default      | Description                                                                                                                                                                                                                                           |
| ---------- | ------- | ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `version`  | integer | `1`          | Service interface version carried in the RPC header. An incompatible peer is rejected with `RpcError::IncompatibleVersion`.                                                                                                                           |
| `group`    | string  | service name | **Channel** shared with the other services of the same group: one request channel, one response channel and one dispatch thread, with the samples routed by the service id.                                                                           |
| `blocking` | flag    | off          | Generates a synchronous twin `{method}_blocking` of **every** method, on both the `{Trait}Client` and the `{Trait}Proxy`. It returns the `Observable` directly, so a consumer with no async runtime calls the service without `.await` or `block_on`. |

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
