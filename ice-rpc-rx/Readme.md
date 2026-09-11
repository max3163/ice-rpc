# ice-rpc-rx

Reactive extensions for [ice-rpc](https://crates.io/crates/ice-rpc) event
streams, inspired by RxJS.

This crate is the **public, user-facing API** of ice-rpc. It extends the native
`ice_rpc::Observable` type with composable operators, provides multicast primitives
(`Subject`, `ShareReplay`), stream constructors (`from`, `of`) and terminal
consumption helpers. It is runtime-agnostic and depends only on `ice-rpc`.

## Modules

| Module | Role |
|---|---|
| [`transform`](src/transform/mod.rs) | `RxStreamExt` trait and its terminals (`first_value`, `collect`, `for_each`, `subscribe`, `subscribe_with`, `into_observable`). Operator implementations are split by ReactiveX category: [`transforming.rs`](src/transform/transforming.rs) (`map`, `scan`, `map_err`, `switch_map`), [`filtering.rs`](src/transform/filtering.rs) (`filter`, `take`, `skip`, `first`, `first_with`), [`combining.rs`](src/transform/combining.rs) (`start_with`, `merge`), [`error_handling.rs`](src/transform/error_handling.rs) (`catch_error`, `retry`, `retry_with`, `retry_with_delay`), [`utility.rs`](src/transform/utility.rs) (`tap`, `finalize`, `delay`, `timeout`), [`conditional.rs`](src/transform/conditional.rs) (`take_until`) |
| [`creation`](src/creation.rs) | `from`, `of`, `throw_error` (Creating: channel-free sources) |
| [`subscribe`](src/subscribe.rs) | `Observer`, `ObserverFns`, `Subscription` (push mode) |
| [`Subject`](src/subject.rs) | multi-producer / multi-consumer multicast |
| [`ShareReplay`](src/share_replay.rs) | multicast with replay of the last value (`shareReplay(1)`) |

## Installation

```toml
[dependencies]
ice-rpc-rx = { path = "../ice-rpc-rx" }
```

## Operators

Operators chain directly on the native `ice_rpc::Observable` type (or any poll-based
stream of `ice_rpc::Event`) and return pull-based combinator streams, so they
compose without any wrapper, channel allocation or spawned task:

```rust,ignore
use ice_rpc_rx::RxStreamExt;

let stream: ice_rpc::Observable<i32, String> = proxy.list().await;

let top = stream
    .filter(|v| *v > 0)      // keeps only positive values
    .map(|v| v * 2)          // transforms each value
    .scan(0, |acc, v| acc + v) // emits a running sum
    .take(3);                // stops after 3 values
```

### Operator classification (ReactiveX)

The operators are grouped using the official
[ReactiveX operator taxonomy](https://reactivex.io/documentation/operators.html).
Each item lists its ReactiveX counterpart in parentheses; `extension` marks an
operator that has no direct ReactiveX equivalent and exists for the ice-rpc model.

#### Creating Observables

- `of(value)` — single-value source (`Just` / RxJS `of`), channel-free.
- `from(iter)` — emits every item of an iterator, then completes (`From`).
- `throw_error(err)` — source that only emits a business `Error` (`Throw`).
- `Subject::new()` — multi-producer multicast source (`Subject`; see [Subject](#subject)).
- `ShareReplay::new(source)` — multicast source that replays the last value
  (`Replay` / Connectable Observable; RxJS `shareReplay(1)`, see [ShareReplay](#sharereplay)).

> The underlying `ice_rpc::Observable::from_events`, `channel()` and
> `unbounded_channel()` constructors live in `ice-rpc`, not in this crate.

#### Transforming Observables

- `map(f)` — transforms every `Next` value (`Map`).
- `scan(initial, f)` — emits a running accumulator state after each value (`Scan`).
- `switch_map(f)` — projects each value to an inner `Observable` and emits from the
  latest one, cancelling previous subscriptions (`FlatMap` family / RxJS `switchMap`;
  the *Switch* aspect also belongs to Combining, see below).
- `map_err(f)` — remaps a **business** error (`Fn(E) -> E2`); technical errors pass
  through unchanged (transformation of the error channel; see also Error Handling).

#### Filtering Observables

- `filter(pred)` — keeps only the `Next` values matching a predicate (`Filter`).
- `take(n)` — emits at most `n` values, then completes (`Take`).
- `skip(n)` — ignores the first `n` values (`Skip`).
- `first()` — emits only the first `Next` value (`First`).
- `first_with(pred)` — emits the first matching value (`First` with predicate).

#### Combining Observables

- `merge(streams)` — combines several `Observable<T, E>` into one (`Merge`).
- `start_with(v)` — prefixes the stream with an initial value (`StartWith`).
- `switch_map(f)` — listed under Transforming; its *Switch* behaviour (`CombineLatest`
  adjacent) is part of Combining on reactivex.io.

```rust,ignore
use ice_rpc_rx::{merge, of};

// Merge several streams into one.
let merged = merge(vec![of(1), of(2)]);
```

#### Error Handling Operators

- `catch_error(f)` — replaces a **business** `Error` with a fallback value and
  completes; a technical error stays fatal (`Catch`).
- `retry(factory, n)` — re-invokes the factory on a business `Error`, up to `n`
  times (`Retry`).
- `retry_with(factory, n, pred)` — retry only when `pred(&error)` is `true` (`Retry`).
- `retry_with_delay(factory, n, delay)` — retry with a delay between attempts (`Retry`).
- `map_err(f)` — remaps the error channel (listed under Transforming).

```rust,ignore
use ice_rpc_rx::retry;

// Retry the underlying call up to 3 times on a business error.
let resilient = retry(|| proxy.fetch().await, 3);
```

#### Observable Utility Operators

- `tap(f)` — runs a side effect per value without altering it (`Do`).
- `delay(duration)` — delays every event (`Delay`).
- `timeout(duration)` — emits a technical timeout error when no event arrives in
  time (`Timeout`).
- `finalize(f)` — runs a callback once the stream terminates (finalization; RxJS
  `finalize`).
- `subscribe(observer)` / `subscribe_with(n, e, c)` — push-mode subscription (`Subscribe`).
- `for_each(f)` — pull-based per-value consumption (`Subscribe` / `forEach`).
- `first_value()` — awaits the first value (terminal, async) — `extension`.
- `into_observable()` — freezes a pipeline into the concrete `ice_rpc::Observable`
  so a service method can return it — `extension`.
- `Observer`, `ObserverFns`, `Subscription` — observer / subscription machinery
  (`Observer`, `Subscribe`, and `unsubscribe` through `Subscription`).

#### Conditional and Boolean Operators

- `take_until(&token)` — mirrors events until the token fires, then emits a
  technical `Cancelled` error (`TakeUntil`; RxJS `takeUntil`).

#### Mathematical and Aggregate Operators

- `collect()` — gathers every value into a `Vec<T>` (`ToArray` / `ToList`, and an
  aggregate terminal). Also available as the inherent `ice_rpc::Observable::collect`.

#### ReactiveX operators not implemented

Not provided today (some are expressible with the existing set):

`buffer`, `group_by`, `window`, `flat_map` / `concat_map`, `element_at`,
`debounce`, `distinct`, `default_if_empty`, `skip_while`, `take_while`,
`combine_latest`, `zip`, `concat`, `count`, `reduce`, `min`, `max`, `sum`,
`average`. Multicast is covered by `Subject` / `ShareReplay`, and most aggregates
are reachable with `scan` plus a terminal.

## Consuming the first value

The former `ice_rpc::take_one!` / `take_one_or_cancel!` macros have been removed.
A service method returns the observable directly (no `Result`), so the terminal
methods of `ice_rpc::Observable` apply without any `?` at the call site:

```rust,ignore
let value = proxy.get("my.key".into()).await.first_value().await?;
let all = proxy.list().await.collect().await?; // Vec<T>
```

[`Observable::first_value()`](../ice-rpc/src/types/stream.rs) returns
`Result<T, StreamError<E>>`, where
`StreamError = Business(E) | Technical(RpcError) | Empty`.
[`Observable::collect()`](../ice-rpc/src/types/stream.rs) gathers every value into a
`Vec<T>` and returns `Result<Vec<T>, ObservableError<E>>` (no `Empty`).

`RxStreamExt::for_each(f)` is the pull-based equivalent (`Result<(), ObservableError<E>>`),
and `RxStreamExt::first()` returns a composable pipeline emitting only the first
event.

## Subscribing (push mode)

`subscribe` is the only operator that spawns a task: it pulls the pipeline and
pushes `next` / `error` / `complete` into an [`Observer`](src/subscribe.rs).

```rust,ignore
use ice_rpc_rx::RxStreamExt;

// A plain `FnMut(T)` observer: errors and completion are ignored.
let sub = proxy.get_state().await.subscribe(|status| println!("{status:?}"));

// A full observer with the three callbacks.
let sub = proxy.get_state().await.subscribe_with(
    |v| println!("next {v:?}"),
    |e| eprintln!("error: {e:?}"), // ObservableError: Business or Technical
    || println!("complete"),
);

// Wait for the end of the subscription (Complete, Error or unsubscribe).
sub.closed().await;
assert!(sub.is_closed());

// Dropping the Subscription cancels the task silently (Rx unsubscribe).
drop(sub);
```

A runnable demonstration lives in the **existing** examples (no extra binary):
the streaming `NotificationService` is provided by
[`provider-app.rs`](examples/provider-app.rs) and consumed by
[`consumer-app.rs`](examples/consumer-app.rs) with `--service notifications`.

The demo services are declared **once**, in the `common` crate
([`examples/common`](../examples/common/src/mod.rs)), and shared with the
`gateway_nodejs` examples: `ice-rpc-rx` pulls them as a `dev-dependency`, so there
is no local copy to keep in sync.

It shows, in order: `subscribe` with a closure, `subscribe_with` with a business
error (`throw_error`), a hand-written `Observer`, a single-value RPC read in pull
mode (`ping` + `first_value`), a full **streaming RPC** subscribed until
completion (`watch(5)` + `closed().await`), and an **early unsubscribe**
mid-stream (`watch(10)` + `drop`).

```bash
# terminal 1
cargo run -p ice-rpc-rx --example provider-app --features tokio

# terminal 2
cargo run -p ice-rpc-rx --example consumer-app --features tokio -- --service notifications
```

## Subject

A subject is the local equivalent of an RxJS `Subject`. Producers push values;
each subscriber receives every event emitted after it subscribed.

```rust,ignore
use ice_rpc_rx::Subject;

let subject = Subject::<i32, String>::new();
let mut rx = subject.subscribe().await;

subject.next(42).await;
subject.complete().await;
```

## ShareReplay

A share-replay multicasts a source and replays the **last** value to late
subscribers. It is the typical building block for an observable state.

```rust,ignore
use ice_rpc_rx::ShareReplay;

let shared = ShareReplay::new(source_stream);
let mut rx = shared.subscribe().await; // replays the last value, if any
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
`ice_rpc::Observable::recv()` normalizes it into `Next` + `Complete`, so consumers
only ever observe three events:

- `Next(T)` — an emitted value;
- `Complete` — normal end of the stream;
- `Error(ObservableError<E>)` — terminal error, either `Business(E)` (raised by
  the service) or `Technical(RpcError)` (raised by the framework: discovery,
  transport, protocol, timeout, cancellation).

A single `match` therefore detects any failure; the caller refines with the two
variants of `ObservableError`. Call-level failures (service not found, provider
unreachable) are reported **inside** the flux through
`Observable::from_technical_error`, so a proxy call never returns a `Result`.
`CompleteWith` is never exposed to user code.

## Pure (channel-free) sources

`of(value)`, `throw_error(err)` and `from(iter)` build observables backed by an
**inline** event buffer (the internal `Inline` variant of
`ice_rpc::Observable`): no channel, no spawned task, no `Arc` and no lock on the data
path. The queue is drained through `&mut self`, so `recv()` / `recv_wire()`
require a mutable binding (e.g. `let mut rx = ...`).

```rust,ignore
use ice_rpc_rx::{from, of, throw_error};

let single: ice_rpc::Observable<i32, MyError> = of(42);          // Next(42) + Complete
let failure: ice_rpc::Observable<i32, MyError> = throw_error(MyError::NotFound);
let many: ice_rpc::Observable<i32, MyError> = from([1, 2, 3]);
```

They make a single-response service trivial to implement on the provider side:

```rust,ignore
#[async_trait::async_trait]
impl DatabaseService for DatabaseServiceImpl {
    async fn get_user_age(&self, name: String) -> Observable<i32, DatabaseError> {
        match name.as_str() {
            "Alice" => of(30),
            _ => throw_error(DatabaseError::NotFound),
        }
    }
}
```

A single-response service still travels as **one** iceoryx2 sample: the server
relay folds the trailing `Next(v)` + `Complete` back into the transport-level
`CompleteWith(v)` optimization, which the client expands into `Next` +
`Complete`. The consumer therefore observes exactly `Next(v)` then `Complete`,
and the wire carries a single message.

See [`examples/provider-app.rs`](examples/provider-app.rs) for the full example.

## Returning a pipeline from a service

A service method must return `Observable<T, E>`, which is the **concrete**
stream type: the generated proxy needs a single return type shared by its
`Provider` (in-process implementation) and `Consumer` (IPC client) modes, so an
operator type (`Map<…>`, `Delay<…>`, …) cannot be returned directly.

`into_observable()` freezes the pipeline into an `Observable`:

```rust,ignore
async fn watch(&self, count: u32) -> Observable<u32, String> {
    // one value every 100 ms, then Complete — no channel, no task, no spawn
    from(1..=count)
        .delay(Duration::from_millis(100))
        .into_observable()
}
```

Any stream of `Event<T, E>` can be frozen this way
(`Subject::subscribe().delay(..)`, `merge(vec![..])`, …), and the
`CompleteWith` single-sample optimization is preserved.

Two points to keep in mind:

- a frozen pipeline is **single-subscription** (an `Observable` is not `Clone`);
  rebuild it from its constructor to get a second consumer, or fan out through
  `Subject` / `ShareReplay` for multicast;
- it cannot detect that the consumer unsubscribed, so it runs to completion. Use
  an explicit `channel()` + `send_next` (which fails on a closed receiver) when
  the producer must stop early.
