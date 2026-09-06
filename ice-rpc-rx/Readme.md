# ice-rpc-rx

Reactive extensions for [ice-rpc](https://crates.io/crates/ice-rpc) event
streams, inspired by RxJS.

This crate extends the native `ice_rpc::Stream` type with composable operators
and provides two multicast primitives (`Subject`, `ShareReplay`). It is
runtime-agnostic and depends only on `ice-rpc`.

## Modules

| Module | Role |
|---|---|
| [`RxStreamExt`](src/operators.rs) | `map`, `filter`, `take` operators on `ice_rpc::Stream` |
| [`Subject`](src/subject.rs) | multi-producer / multi-consumer multicast |
| [`ShareReplay`](src/share_replay.rs) | multicast with replay of the last value (`shareReplay(1)`) |

## Installation

```toml
[dependencies]
ice-rpc-rx = { path = "../ice-rpc-rx" }
```

## Operators

Operators chain directly on the native stream type and return the native type:

```rust,ignore
use ice_rpc_rx::RxStreamExt;

let stream: ice_rpc::Stream<i32, String> = proxy.list().await?;

let top = stream
    .filter(|v| *v > 0)
    .map(|v| v * 2)
    .take(3);
```

## Subject

A subject is the local equivalent of an RxJS `Subject`. Producers push values;
each subscriber receives every event emitted after it subscribed.

```rust,ignore
use ice_rpc_rx::Subject;

let subject = Subject::<i32, String>::new();
let rx = subject.subscribe().await;

subject.next(42).await;
subject.complete().await;
```

## ShareReplay

A share-replay multicasts a source and replays the **last** value to late
subscribers. It is the typical building block for an observable state.

```rust,ignore
use ice_rpc_rx::ShareReplay;

let shared = ShareReplay::new(source_stream);
let rx = shared.subscribe().await; // replays the last value, if any
```

## Example: an observable state over RPC

See [`examples/state_service.rs`](examples/state_service.rs) for a `StateService`
exposing:

- `get_state()` — subscribes to the state (replays the current value);
- `set_state(status)` — pushes a new `Status::Ok` / `Status::Nok` / `Status::Nc`.

The example runs two programs over ice-rpc: a **provider** that hosts the state
and is notified of every change, and a **consumer** that subscribes to
`get_state` and pushes changes with `set_state`.

Provider (terminal 1):

```bash
cargo run -p ice-rpc-rx --example state_service --features tokio -- provider
```

Consumer (terminal 2):

```bash
cargo run -p ice-rpc-rx --example state_service --features tokio -- consumer
```

## Normalization

ice-rpc can transport a single response as `ice_rpc::Event::CompleteWith`. All
operators in this crate treat `CompleteWith` exactly like `Next`, so consuming
code always observes a uniform stream of `Next` values followed by a terminal
event (`Complete`, `Error` or `RpcError`).
