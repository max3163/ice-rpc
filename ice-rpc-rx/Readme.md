# ice-rpc-rx

Reactive extensions for [ice-rpc](https://crates.io/crates/ice-rpc) event
streams, inspired by RxJS.

This crate is the **public, user-facing API** of ice-rpc. It extends the native
`ice_rpc::Stream` type with composable operators, provides multicast primitives
(`Subject`, `ShareReplay`), stream constructors (`from`, `of`) and terminal
consumption helpers. It is runtime-agnostic and depends only on `ice-rpc`.

## Modules

| Module | Role |
|---|---|
| [`transform`](src/transform.rs) | `RxStreamExt` trait: `map`, `filter`, `map_err`, `scan`, `take`, `first`, `first_with`, `start_with`, `tap`, `delay`, `finalize`, `catch_error` |
| [`join`](src/join.rs) | `merge`, `retry`, `retry_with`, `retry_with_delay` |
| [`creation`](src/creation.rs) | `from`, `of` |
| [`Subject`](src/subject.rs) | multi-producer / multi-consumer multicast |
| [`ShareReplay`](src/share_replay.rs) | multicast with replay of the last value (`shareReplay(1)`) |

## Installation

```toml
[dependencies]
ice-rpc-rx = { path = "../ice-rpc-rx" }
```

## Operators

Operators chain directly on the native `ice_rpc::Stream` type (or any poll-based
stream of `ice_rpc::Event`) and return pull-based combinator streams, so they
compose without any wrapper, channel allocation or spawned task:

```rust,ignore
use ice_rpc_rx::RxStreamExt;

let stream: ice_rpc::Stream<i32, String> = proxy.list().await?;

let top = stream
    .filter(|v| *v > 0)      // keeps only positive values
    .map(|v| v * 2)          // transforms each value
    .scan(0, |acc, v| acc + v) // emits a running sum
    .take(3);                // stops after 3 values
```

### Transformation

- `map(f)` — transforms every `Next` value.
- `filter(pred)` — keeps only the `Next` values matching a predicate.
- `map_err(f)` — maps the error type `E` to another one (`Fn(E) -> E2`); useful
  to adapt errors across layers.
- `scan(initial, f)` — emits a running accumulator state after each value.
- `switch_map(f)` — projects each value to an inner `Stream` and emits from the
  latest one, cancelling previous subscriptions (RxJS `switchMap`).
- `take(n)` — emits at most `n` values, then completes.
- `skip(n)` — ignores the first `n` values (symmetric of `take`).
- `first()` / `first_with(pred)` — emits only the first (matching) value.
- `start_with(v)` — prefixes the stream with an initial value.

### Utility

- `tap(f)` — runs a side effect per value without altering it.
- `delay(duration)` — delays every event.
- `timeout(duration)` — emits `RpcError::Timeout` when no event arrives in time.
- `finalize(f)` — runs a callback once the stream terminates.
- `catch_error(f)` — replaces an `Error` with a fallback value and completes.

### Join / resilience (free functions)

```rust,ignore
use ice_rpc_rx::{merge, retry, of};

// Merge several streams into one.
let merged = merge(vec![of(1), of(2)]);

// Retry the underlying call up to 3 times on a business error.
let resilient = retry(|| proxy.fetch().await, 3);
```

- `merge(streams)` — combines several `Stream<T, E>` into one.
- `retry(factory, n)` — re-invokes the factory on a business `Error`, up to `n`
  times.
- `retry_with(factory, n, pred)` — retry only when `pred(&error)` is `true`.
- `retry_with_delay(factory, n, delay)` — retry with a delay between attempts.

## Consuming the first value

The former `ice_rpc::take_one!` / `take_one_or_cancel!` macros have been removed.
Use the terminal methods of `ice_rpc::Stream` instead:

```rust,ignore
let value = proxy.get("my.key".into()).await?.first_value().await?;
let all = proxy.list().await?.collect().await?; // Vec<T>
```

[`Stream::first_value()`](../ice-rpc/src/types.rs) returns
`Result<T, StreamError<E>>`, where
`StreamError = Rpc(RpcError) | Business(E) | Empty`.
[`Stream::collect()`](../ice-rpc/src/types.rs) gathers every value into a
`Vec<T>`. For a composable first event inside a pipeline, use
`RxStreamExt::first()` followed by `recv()`.

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

## Event model

ice-rpc transports a single response as an internal `CompleteWith` sample.
`ice_rpc::Stream::recv()` normalizes it into `Next` + `Complete`, so consumers
only ever observe four events:

- `Next(T)` — an emitted value;
- `Complete` — normal end of the stream;
- `Error(E)` — business error;
- `RpcError(RpcError)` — technical error.

`CompleteWith` is therefore never exposed to user code.
