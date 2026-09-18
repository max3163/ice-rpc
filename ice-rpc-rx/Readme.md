# ice-rpc-rx

Reactive stream layer of [`ice-rpc`](../ice-rpc): the stream vocabulary, the
operators, the multicast primitive and the execution facade they need.

This crate is the foundation `ice-rpc` builds on. It owns `Observable`, `Event`,
`ObservableError`, `RpcError` and the runtime-agnostic `rt` facade, and it knows
**nothing about the wire**: no framing, no serialization format, no iceoryx2.

## Why a separate crate

The reactive layer is not separable from the stream type: in Rust an inherent
`impl` must live in the crate that defines the type, so the operators can only
follow `Observable`. Putting `Observable` here is therefore what lets the
operators stay **inherent methods** — one stream type, no extension trait to
import, no wrapper, no `into_observable`.

What this buys:

- `ice-rpc` keeps the protocol, the transport, the locator and the macros; this
  crate keeps the stream behaviour. Each half is documented, linted and tested
  on its own.
- The operators are tested without shared memory, so their suite runs anywhere.
- `ice-rpc` re-exports every item under its historical path, so a consumer of
  `ice-rpc` sees exactly the same API as before.

## Usage

As a consumer of `ice-rpc`, depend on `ice-rpc` and use `ice_rpc::Observable`:
that is this type, re-exported. Depend on `ice-rpc-rx` directly only when you
want the stream vocabulary without the RPC stack:

```toml
[dependencies]
ice-rpc-rx = { version = "0.2", default-features = false }
```

```rust
use ice_rpc_rx::{from, Observable};

let stream: Observable<i32, String> = from([1, 2, 3]);
let doubled = stream.map(|v| v * 2);
```

## Layout

| Module | Contents |
|--------|----------|
| `event` | `Event`, `ObservableError`, the producer-side `Sender` |
| `stream` | `Observable`, `channel`, `unbounded_channel` |
| `error` | `RpcError`, the technical error of the whole stack |
| `creation` | `from`, `of`, `throw_error` |
| `subject` | `Subject`, the multicast source |
| `subscribe` | `Subscription`, the cancellation handle |
| `transform` | the operators, carried by `Observable` |
| `rt` | execution facade (`spawn`, `sleep`, `block_on`) and `CancellationToken` |

## Runtime

The execution facade is runtime-agnostic by default (async-global-executor). The
`tokio` feature switches it to tokio; `ice-rpc` forwards its own feature to this
one, so the choice is made once, at the top of the dependency tree.

The facade lives here on purpose, and the switch is a **single** `cfg` at a
**single** level. A separate `ice-rpc-rt` crate would shed `rkyv` and
`async-channel` from the dependency set of someone who only wants
`CancellationToken` or `block_on` — a narrow audience, since `tokio-util` already
covers that need. For a consumer of `ice-rpc` it would change nothing, and it
would put the runtime choice on two levels (`ice-rpc-rx/tokio` **and**
`ice-rpc-rt/tokio`): enabling one without the other yields a silent mismatch, such
as an agnostic `sleep` next to a tokio `spawn`, which panics at run time.

Extracting the facade becomes worthwhile the day a third execution backend is
wanted, or the day this crate must stop depending on `async-global-executor`
(wasm, caller-provided executor). The right answer then is an executor trait
inside that new crate, not a plain move.

## License

Apache-2.0, like the rest of the workspace.
