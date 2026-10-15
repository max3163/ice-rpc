# ice-rpc

High-performance RPC framework over [iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2) shared memory: **zero-copy header**, **rkyv payload**, **in-place decoding** when the payload is aligned.

From a single `#[service]`-annotated trait, the procedural macro generates the entire IPC code: client, server, proxy and lifecycle. Automatic reconnection after a provider crash (including `SIGKILL`) is provided by iceoryx2's **native node monitoring**: the OS releases the node's monitoring file lock when the process dies, and `Node::list` reports it as `NodeState::Dead`.

## Features

- **Zero-copy IPC transport** through iceoryx2 shared memory.
- **Code generation** with `#[service]`: Request enum, Client, Server, Proxy and lifecycle.
- **Lazy consumer proxies** (discovery and connection handled by iceoryx2 on the
  first call) and **dependency-aware topological initialization** of the local
  services — a dependency provided by another process never blocks the startup.
- **Crash detection & reconnection** without heartbeat (native iceoryx2 node monitoring).
- **Three proxy modes**: `Provider`, `Consumer`, `ProviderJson` — the last one
  only when the `json` feature is on.
- **Optional HTTP gateway** (`http` plus one `http-*` adapter) built on trillium.
  `http-tokio` runs the server on the application's tokio runtime; the
  `trillium-smol` adapters run it on the executor trillium embeds, while the
  ice-rpc tasks stay on the executor of the selected mode. The feature also
  generates the JSON view of every proxy (`impl JsonInvoker`) — the same one the
  `json` feature emits, so the two transports cannot drift apart — which is also
  what keeps `serde_json`'s conversion code out of a binary that never speaks JSON.
- **Optional JSON host surface** (`json` feature): `#[service]` also generates the
  rkyv ↔ `serde_json::Value` converters and the `ProviderJson` mode a JSON host
  dispatches to — the `ice-rpc-nodejs` bridge today. Off by default: a Rust-only
  deployment does not carry them, and they are about half of the generated code
  of a service.
- **Optional out-of-band monitoring** (`monitoring` feature): `#[service]` also
  generates a `{Service}Decoder` (and `Display` on the request enum) so an
  external observer renders the observed payloads in clear text — with each
  type's `Display` implementation when it has one, with its `Debug` one
  otherwise. Off by default: a plain provider/consumer never calls that code.
- **Optional call tracing** (`tracing` feature): the provider handler enters one
  `tracing` span per call, carrying the ids the caller propagated (correlation,
  trace, span, parent) plus the service **name** and the method. The implementation's own
  `tracing` / `log` events land in that span without it writing a line — and the
  ids are the framework's own, so no OpenTelemetry stack is needed to obtain
  them. A provider that delegates to another service of its **own process** is
  traced as well: the delegation gets a span of its own, marked `kind = "local"`,
  and the callee reads its own identity instead of the caller's — while with the
  feature off that hop stays the plain in-process call it was, with no context
  and no allocation. Off by default: a deployment that collects no spans pays
  neither the dependency nor the span. `examples/tracing-demo.rs` shows both
  halves across two hops, one transported and one direct.

## Installation

```toml
[dependencies]
ice-rpc = "0.3"
```

That is the whole list. The framework hides its own dependencies behind its
facade:

- a service payload is declared with `#[ice_rpc::payload]`, which supplies the
  `rkyv` derives — **and `Debug`**, unless the item derives it already or is
  annotated `no_debug` for a hand-written `impl Debug` — and points their
  expansion at the re-exported `rkyv`: no `rkyv` dependency of your own. `Debug`
  is what the `monitoring` feature needs to render a payload, so leaving it to the
  macro keeps a service observable without asking for the trait. An **error**
  payload propagated with `?` into a `Box<dyn std::error::Error>` also needs
  `Display`, which `std::error::Error` requires and no derive can produce:
  `#[ice_rpc::payload(display)]` generates one, delegating to the `Debug` form,
  for the payloads whose author does not write a better rendering by hand;
- a service implementation is annotated with `#[ice_rpc::async_trait]`, the
  re-exported `async-trait` attribute — no `async-trait` dependency of your own;
- everything `#[service]` generates names `log` (and the rest) through
  `ice_rpc::codegen`, so no `log` dependency of your own either.

**Backward compatible.** A crate that already depends on `rkyv` may keep
`#[derive(rkyv::Archive, rkyv::Deserialize, rkyv::Serialize)]`, and one that
already depends on `async-trait` may keep `#[async_trait::async_trait]`: both
spellings are recognized and produce the same code. The `json` and `http`
features additionally require `serde` (`Serialize` + `Deserialize`) on the
service types, exactly as before.

## Execution modes

The core names **no runtime** outside one façade (`ice_rpc::rt`). A façade
cannot be runtime-*neutral*: a detached task has to be polled by someone, and
"who polls" is the only thing a runtime is asked for here. So instead of one
lowest-common-denominator executor, there is one **full mode per host runtime**,
plus a fallback for a deployment that has none. "Full" means the process then
runs **one** pool, not three.

```toml
ice-rpc = { version = "0.3" }                              # rt-async (default)
ice-rpc = { version = "0.3", features = ["tokio"] }        # tokio only
ice-rpc = { version = "0.3", features = ["smol"] }         # smol only
```

| Mode | Task spawning, timers, blocking pool |
|---|---|
| `rt-async` (default) | an `async-executor` instance owned by the crate, run by `available_parallelism()` OS threads (`ICE_RPC_THREADS` overrides the count), `futures-timer` for `sleep`, `blocking` for the pool. No third-party runtime, and **no I/O reactor**: the core performs no async I/O. |
| `tokio` | `tokio::spawn`, `tokio::time::sleep`, tokio's blocking pool |
| `smol` | `smol::spawn` on smol's global executor — so the tasks share the pool of an application that already runs smol — `smol::Timer`, `smol::unblock`. Set `SMOL_THREADS`: that executor runs **one** thread by default. |

The modes are exclusive: `tokio` + `smol` is a compile error. Enabling a mode
does *not* remove the fallback from the dependency graph, because Cargo features
are additive; a build that carries one runtime and nothing else says so:

```toml
ice-rpc = { version = "0.3", default-features = false, features = ["tokio"] }
```

The two long-lived IPC loops own a `std::thread` and block on an iceoryx2
`WaitSet`: they never go through a runtime. `#[ice_rpc::main]` drives `main`
with `ice_rpc::rt::block_on`, which polls the body on the calling thread and
therefore executes on the mode selected above — `rt-async` (the default), or
whichever one the features pick. The macro bundles no runtime of its own, so
this is not a runtime-free mode; `#[ice_rpc::main(tokio)]` builds a multi-thread
tokio runtime, and any `fn(Future) -> T` can be given instead, e.g.
`#[ice_rpc::main(pollster::block_on)]`. Only `#[ice_rpc::main(rt-none)]` — the
`rt-none` mode behind the `client` feature — drops the runtime entirely, which
is why it is the sole mode that can truthfully be called *runtime-free*.

- Service methods return `ice_rpc::Observable<T, E>`. Build one with the
  stream constructors of the Rx layer (`of`, `from`, `throw_error`, `Subject`); an advanced
  provider may use the raw channel via `ice_rpc::codegen::channel::<T, E>(capacity)`.
- `ice_rpc::rt` exposes `spawn`, `spawn_blocking`, `blocking_call`, `sleep`,
  `block_on`, `oneshot` and `CancellationToken`. A **response deadline** is not
  one of them: it is the `timeout` operator on the stream a call returns.

## Quick start

A complete, runnable example lives in
[`examples/quickstart.rs`](examples/quickstart.rs): one file holding the service,
its provider and its consumer — the two roles picked by a command-line argument.
The steps below are that file, and step 5 runs it.

### 1. Define the service

```rust,ignore
use ice_rpc::{Observable, service};

/// The business error of the service.
///
/// `#[ice_rpc::payload]` supplies the rkyv derives (and `Debug`) — no `rkyv`
/// dependency of your own.
#[ice_rpc::payload]
#[derive(Debug, Clone)]
pub enum GreeterError {
    /// The name was empty.
    EmptyName,
}

#[service("GreeterService")]
pub trait GreeterService {
    /// Returns a greeting for `name`.
    async fn hello(&self, name: String) -> Observable<String, GreeterError>;
}
```

The macro generates `GreeterServiceRequest`, `GreeterServiceClient`,
`GreeterServiceServer`, `GreeterServiceProxy` and `GreeterServiceMode`. A service
method returns `Observable<T, E>` — never a `Result`: the error travels inside the
flux.

`#[service]` also accepts optional parameters:

- `version` (integer, default `1`) — service interface version carried in the RPC header. An incompatible peer is rejected with `RpcError::IncompatibleVersion`;
- `group` (string, default: the service name) — the **channel** shared with the other services of the same group: one request channel, one response channel and one dispatch thread, with the samples routed by the service id;
- `max_slice_len` and `blocking` — see *Configuration* below.

```rust,ignore
#[service("MyService", version = 1, group = "db")]
pub trait MyService {
    async fn hello(&self, name: String) -> Observable<String, GreeterError>;
}
```

### 2. The provider

`#[ice_rpc::main(tokio)]` owns the bootstrap (init + runtime) and the clean
shutdown; `run_provider!` starts the services and blocks until Ctrl+C.

```rust,ignore
struct Greeter;

#[ice_rpc::async_trait]
impl GreeterService for Greeter {
    async fn hello(&self, name: String) -> Observable<String, GreeterError> {
        if name.is_empty() {
            // A business error is an observable too: it travels inside the flux.
            return ice_rpc::throw_error(GreeterError::EmptyName);
        }
        // One value, then completion — no channel, no task. A long-lived push
        // stream would use `ice_rpc::codegen::channel`.
        ice_rpc::of(format!("Hello {name}!"))
    }
}

/// Hosts the service and blocks until Ctrl+C.
async fn run_provider() {
    ice_rpc::run_provider!(GreeterServiceProxy::provide(Greeter)).await
}
```

### 3. The consumer

The proxy is built lazily from its type — no registry, no explicit connection;
the IPC link opens on the first call. A call never fails at the call site: a
transport failure becomes an in-stream error that `first_value()` returns.

```rust,ignore
async fn run_consumer() {
    let proxy = ice_rpc::locator()
        .get::<GreeterServiceProxy>()
        .await
        .expect("GreeterService unknown");

    match proxy.hello("Alice".into()).await.first_value().await {
        Ok(greeting) => println!("{greeting}"),
        Err(error) => eprintln!("call failed: {error}"),
    }
}
```

### 4. Two roles, one binary

The example dispatches on its first argument, so the provider and the consumer
are the two runs of the same program:

```rust,ignore
#[ice_rpc::main(tokio)]
async fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("provider") | None => run_provider().await,
        Some("consumer") => run_consumer().await,
        Some(other) => eprintln!("usage: quickstart [provider|consumer] (got {other})"),
    }
}
```

### 5. Run it

Start the provider in one terminal and the consumer in another; both must run
from the same directory, so they resolve the same iceoryx2 configuration.

```bash
# terminal 1 — the provider (blocks until Ctrl+C)
cargo run -p ice-rpc --example quickstart --features tokio -- provider

# terminal 2 — the consumer (prints the greeting and exits)
cargo run -p ice-rpc --example quickstart --features tokio -- consumer
```

The consumer prints `Hello Alice!`. Stop the provider with Ctrl+C.

## Core concepts

| Concept | Description |
|---|---|
| `Observable<T, E>` | The composable stream, and the return type of service methods (no `Result`). |
| `Event<T, E>` | Consumer-facing: `Next(T)` / `Complete` / `Error(ObservableError<E>)` where `ObservableError` is `Business(E)` or `Technical(RpcError)`. |
| `ObservableError<E>` | Terminal error of `first_value()`: `Business(E)` / `Technical(RpcError)` / `Empty`. |
| `ServiceLocator` | Global registry, reached through `locator()`: `locator().get::<MyProxy>()`. |
| `ServiceInit` | The only trait a developer implements: `dependencies()` + the `on_init` hook. |
| `Proxy` | Single entry point with 3 modes (`Provider` / `Consumer` / `ProviderJson`). |

Internal concepts (`NodeId`, `WireEvent`, the transport entry points, …) are
exposed through the doc-hidden `ice_rpc::codegen` module and used by
`ice-rpc-macros`. The transport itself is a publish/subscribe pair of channels
per service, correlated by a 16-byte id: see `ice_rpc::transport`.

## Consumption

Consuming a stream is done natively on `ice_rpc::Observable`, whose operators are
inherent methods (nothing to import); a call never fails at the call site — a
connection/transport failure becomes an in-stream technical error:

```rust,ignore
let value = proxy.hello("Alice".into()).await.first_value().await?;
let all   = proxy.list().await.collect().await?; // Vec<T>
```

- `first_value() -> Result<T, ObservableError<E>>` (`Empty` when the stream completes
  without a value);
- `collect() -> Result<Vec<T>, ObservableError<E>>`;
- `last_value() -> Result<T, ObservableError<E>>` (the counterpart of `first_value`, with
  the same `Empty` shape and one slot held instead of the whole sequence);
- the `timeout` and `take_until` operators bound the response wait and cancel a stream;
- the operators documented on `Observable` are all available as inherent
  methods: transforming (`map`, `map_err`, `scan`, `switch_map`), filtering
  (`filter`, `take`, `distinct_until_changed`, `skip`, `first`, `first_with`),
  combining (`merge`, `start_with`), conditional (`take_until`), error handling
  (`catch_error`) and utility (`tap`, `finalize`, `delay`, `timeout`). Each
  method documents its semantics and ships a runnable example — see the
  `Observable` API documentation;
- `switch_map`'s projection accepts either an `Observable` or a `Future`
  producing one (`ObservableInput`), so an `async fn` call composes directly:
  `.switch_map(move |x| async move { proxy.get_other(x).await })`;
- dropping a response stream whose call is still in flight **cancels the call on
  the provider**: the handler's future is dropped, so the work stops instead of
  running on for a caller that no longer listens. A call whose terminal event was
  read is released silently — nothing is sent. The implementation honours the
  signal by reading `CallContext::cancellation()`;
  [`examples/remote_cancel.rs`](examples/remote_cancel.rs) shows both halves.

### Blocking consumer (no runtime)

A small consumer that only issues calls needs neither an executor nor an async
`main`: `#[ice_rpc::main(rt-none)]` bootstraps ice-rpc around a plain
**synchronous** `fn main` and shuts it down on exit. The blocking surface mirrors
the async one.

```rust,ignore
#[ice_rpc::main(rt-none)] // init + shutdown_blocking around a sync main
fn main() {
    let proxy = ice_rpc::locator().get_blocking::<MyServiceProxy>().unwrap();

    // `#[service(..., blocking)]` generated a synchronous twin of the method:
    // no `.await`, and no `block_on` at the call site.
    let value = proxy
        .hello_blocking("Alice".into())
        .first_value_blocking()
        .unwrap();
}
```

- `#[ice_rpc::main(rt-none)]` wraps a **synchronous** `fn main` (no `async`):
  `init()` before the body, `shutdown_blocking()` after — the same guarantees as
  `#[ice_rpc::main]`, without a runtime;
- `ServiceLocator::get_blocking` and
  `Observable::{recv,next,first_value,last_value,collect}_blocking` are the
  synchronous twins of the awaited ones;
- `#[service(..., blocking)]` also generates a synchronous twin
  `{method}_blocking` on both the client and the proxy, so the call itself needs
  neither `.await` nor `block_on`;
- `ShutdownGuard::shutdown_blocking()` is the synchronous twin of `shutdown()`,
  so the program keeps one shape from the call to the exit;
- `first_value_blocking` pulls the response with no executor;
- the only auxiliary thread is the transport's own response dispatcher;
- a full program lives in
  [`examples/blocking-consumer-app.rs`](examples/blocking-consumer-app.rs).

## Error semantics

Errors travel **inside** the flux. `Event::Error(ObservableError<E>)` carries
either a business error (`Business(E)`) or a technical one
(`Technical(RpcError)`), so a single `match` detects a failure and the caller
refines with the two variants.

`RpcError` classifies technical failures so callers can choose a policy
(`retry` / `fallback` / `log` / `fatal`) via
[`RpcError::is_retryable()`](../ice-rpc-rx/src/error.rs:49).

Every variant the enum declares, in declaration order:

| Variant | Meaning | Retryable |
|---|---|---|
| `SerializationError` | rkyv serialization/deserialization failure | no |
| `TransportError` | iceoryx2 I/O failure (loan, send, receive, port creation) | yes |
| `Timeout` | deadline exceeded | yes |
| `Cancelled` | cancelled by a global shutdown (SIGINT/SIGTERM or programmatic), a `take_until` token or a `timeout`; a remote Cancel published by a consumer that abandoned the call is handled by the provider and stays silent | no |
| `Internal` | unexpected internal error / invariant violation | no |
| `ProtocolMismatch` | the service on the bus was recorded with a different configuration — a peer resolving another `iceoryx2.toml`, or another build — or its state was left behind by a process killed while it held it; no amount of retrying resolves it — see [`docs/wire-compat.md`](../docs/wire-compat.md) | no |
| `UnknownMethod` | the provider has no method of that name on the requested service; answered at once, so a typo or a stale client is diagnosable instead of ending in a transport timeout | no |
| `UnknownService` | no service is registered under the requested id on that channel; answered at once too, and it names the id in hexadecimal because a hash cannot be inverted | no |
| `IncompatibleVersion` | the provider was built with another `#[service(version = N)]`; emitted before the payload is decoded, and no amount of retrying resolves it — both peers must be rebuilt with the same declared version | no |

Four names that appeared in an earlier version of this table — `DiscoveryError`,
`ProviderUnavailable`, `ServiceNotFound`, `PayloadTooLarge` — have **no** variant.
`ServiceNotFound` is not `UnknownService` above: the latter is real, and it names an
id with no dispatcher on the channel, not a service the locator failed to find.
Addressing is static: a provider that is not running is not a distinct error, it
surfaces as a `TransportError` reading `no subscriber connected` once the provider
wait (`ICE_RPC_PROVIDER_WAIT_MS`, 30 s) elapses, and an oversized payload as an
iceoryx2 failure. A `match` on `RpcError` is exhaustive, so the compiler settles the
question rather than this table.

## Service initialization

Implement `ServiceInit` on your service type to declare dependencies and run an
initialization hook:

```rust,ignore
#[ice_rpc::async_trait]
impl ice_rpc::ServiceInit for MyServiceImpl {
    fn dependencies(&self) -> Vec<&'static str> {
        vec!["ConfigService"]
    }

    async fn on_init(&self) -> bool {
        // Return false to abort the provider startup with an error naming this
        // service. There is no automatic retry.
        true
    }
}
```

`dependencies()` feeds **only** the topological sort of the services registered
in the **same** process: a service is initialized after the local services it
lists. A dependency provided by **another process** is *external* and never
blocks the startup — it is ignored by the sort, with no discovery, no connection
and no wait. There is **no iceoryx2 discovery check** at initialization, so a
provider reports itself ready even when a declared external dependency is not
running. The coupling is resolved lazily, **at call time**: the first RPC opens
the channel (creating it if no provider exists yet) and waits a **bounded**
provider wait (30 s by default, `ICE_RPC_PROVIDER_WAIT_MS` overrides it) for a
subscriber, then fails with a `TransportError` reading `no subscriber connected`.
To require an external dependency at startup, run the check yourself in
`on_init()` (a typed `ping` with a bounded retry) and return `false` when it is
unreachable.

For services that consume other services, register with `provide_with_init`.
`#[ice_rpc::main]` bootstraps ice-rpc (init + runtime) and owns the clean
shutdown, while `run_provider!` starts the services and waits for Ctrl+C:

```rust,ignore
ice_rpc::run_provider!(
    MyServiceProxy::provide_with_init(MyServiceImpl),
).await
```

### Provider and consumer in the same process

`run_provider!` **never returns before shutdown**: it awaits
`wait_for_shutdown()` and blocks the task that calls it, so code placed after its
`.await` runs only when the process stops. To serve a provider and drive a
consumer in the same `main`, **spawn** the provider and keep the caller free:

```rust,ignore
#[ice_rpc::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `rt::spawn` works in every execution mode; under
    // `#[ice_rpc::main(tokio)]`, `tokio::spawn` is the equivalent.
    ice_rpc::rt::spawn(async {
        if let Err(error) = ice_rpc::run_provider!(
            CoreServiceProxy::provide(Core),
        ).await {
            log::error!("provider stopped: {error}");
        }
    });

    // Consumer side, on the main task.
    let print = ice_rpc::locator().get::<PrintServiceProxy>().await.unwrap();
    print.print().await.first_value().await?;
    Ok(())
}
```

A `rt::block_on(run_provider!(..))` does **not** start the provider in the
background: `block_on` runs a future to completion on the calling thread, and
this future completes only on shutdown — such a call blocks until Ctrl+C.

## Clean shutdown

`#[ice_rpc::main]` shuts ice-rpc down for you, including on an early `return` or
`?`:

```rust,ignore
#[ice_rpc::main(tokio)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proxy = ice_rpc::locator().get::<MyServiceProxy>().await?;
    // ...
    Ok(())
}
```

For a hand-written `main` (one calling `std::process::exit`, the N-API gateway,
tests), the lifecycle lives in `ice_rpc::codegen`: `let guard = ice_rpc::codegen::init();`
… `guard.shutdown().await;`.

## HTTP gateway (optional)

Enable the `http` feature **and one adapter**, `http-<mode>`: trillium needs one,
and letting the bare feature pick would contradict the mode chosen above.

```toml
ice-rpc = { version = "0.3", features = ["tokio", "http-tokio"] }
ice-rpc = { version = "0.3", features = ["smol", "http-smol"] }
ice-rpc = { version = "0.3", features = ["http-threads"] }   # the default mode
```

```rust,ignore
#[ice_rpc::main(tokio)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ice_rpc::start_http_gateway!(8080, MyServiceProxy).await;
    Ok(())
}
```

Services are then reachable through `/{service}/{method}`.

## Configuration

`ice-rpc` does not own the iceoryx2 configuration and never writes a config
file: the effective configuration is the one iceoryx2 resolves itself. iceoryx2
looks for `./config/iceoryx2.toml` first, then the user config directory, then
the global config directory, and falls back to its compiled-in default when none
exists.

Two processes communicate only if they agree on the root path (the shared-memory
domain). Provide a `./config/iceoryx2.toml` — or a user/global one — when the
default is not what you want; every peer that discovers the same file shares the
domain. A partial file is enough: iceoryx2 merges it over its defaults.

### Every participant must share the same configuration

The service limits — publishers, subscribers, nodes, subscriber buffer size and
loaned samples — are **not** hard-coded by `ice-rpc`: they come from the iceoryx2
configuration, so the deployment owns them. This widens the coherence rule above:
**all participants of a channel must resolve the same iceoryx2 configuration.**
When they do not, `ice-rpc` reports a protocol mismatch that names the two cases
— a divergent configuration, or leftover state from a killed process — each with
its own remedy.

The same **protocol version** is required; the same **build** is not. A consumer
and a provider built from different revisions interoperate as long as they speak
the same protocol version and resolve the same configuration, which is what lets
one side be upgraded before the other.

iceoryx2's compiled-in defaults are conservative (`max-publishers = 2`,
`max-subscribers = 8`, `subscriber-max-buffer-size = 2`, …). When no
configuration file is present they apply, so ship a baseline that restores the
values `ice-rpc` used to pin. It must sit in `./config/iceoryx2.toml` relative to
the **working directory of each process** — iceoryx2 resolves it from there, so
`cargo test` and `cargo run` look in the package directory, not the workspace
root. This workspace ships one for its own tests and examples under
`ice-rpc/config/` and `ice-rpc-macros-tests/config/`:

```toml
[defaults.publish-subscribe]
max-publishers = 16
max-subscribers = 16
max-nodes = 32
subscriber-max-buffer-size = 1024
publisher-max-loaned-samples = 1024
```

### Slice length

The one publisher setting iceoryx2 does not expose in its configuration is the
initial slice length, and its builder default is `1` — with the default
`publisher-allocation-strategy = "Static"` that is a hard cap, so a 1-element
slice cannot carry an RPC payload. It is therefore declared by the service, per
channel:

```rust,ignore
#[service("DatabaseService", group = "db", max_slice_len = 4096)]
```

Every service of a group must declare the same `max_slice_len`; an omitted value
falls back to `DEFAULT_MAX_SLICE_LEN` (256). Setting
`publisher-allocation-strategy = "PowerOfTwo"` in the configuration lets a
publisher grow past that initial size instead of refusing the sample.

### Process allocator

A request that carries a **struct the service takes by value** (`async fn
search(&self, query: WorkloadQuery)`) is deserialized, and rkyv pays **one
allocation per variable field** to do it — the same for a `String` argument, or
for the list a caller builds before the call. On a request with ~130 variable
fields that is 130 `malloc`/`free` pairs per call, and the process allocator
becomes the dominant cost:

| one process, one line changed | system allocator | `mimalloc` |
|---|---|---|
| provider serving a struct of ~130 variable fields | 62 481 req/s | **138 032 req/s** |
| caller building 256 string fields | 83 717 req/s | **141 618 req/s** |

A **response** pays it twice — once by the provider that builds the value, once by
the caller that decodes it — and no signature can avoid that, since a stream hands
out owned values. The same 64-field structure runs at 19 434 req/s as a *return*
value, against 139 854 as a *request* and 283 668 for a scalar call (40 733 /
211 429 / 341 074 with `mimalloc`). A large structured response is therefore a
**design choice**: splitting it into several small ones costs a fraction of that.

ice-rpc never picks an allocator; the binary does:

```rust,ignore
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;
```

Both demo binaries carry it for that reason, and the three-way measurement
(provider only, caller only, both) is what the two rows above come from: the
allocator is a property each side decides for itself.

The allocations themselves cannot be avoided from the outside: an archive holds the
**archived** form of a value, so the provider rebuilds every owned field, and a
stream hands the caller owned values. What the numbers above price *is* that, and the
levers left are the allocator (above) and the **shape** of the exchange — a response
split into several small ones, or a request that carries fewer variable fields.

## License

Apache-2.0
