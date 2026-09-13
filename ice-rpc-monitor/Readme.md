# ice-rpc-monitor

Out-of-band observer for [`ice-rpc`](../ice-rpc/Readme.md).

It attaches **in read-only mode** to the iceoryx2 services an ice-rpc process
already exposes (`{channel}_req`, `{channel}_resp` and their `_notify` event
services), reads the zero-copy `RpcHeader` and never decodes the rkyv payload.
It is a separate process, so its cost never runs on the observed processes.

## Two capture modes

Reading the payload is not free, so the observer has two modes:

| Mode | Reads/decodes the payload | Cost | Use case |
|---|---|---|---|
| `stats` (default) | **never** | zero copy per sample | high throughput: counts, throughput, error kinds, exact latency, loss |
| `detail` | yes, decoded | one copy + one decode per sample | debugging at moderate throughput: the message content |

`--detail` is the shorthand for `--mode detail` (full capture) and applies
everywhere; `--detail-channel <name>` (repeatable) forces detail mode on specific
channels while the rest stay in stats mode:

```bash
# Everything in detail.
cargo run -p ice-rpc-monitor -- --detail --trace-sample-rate 1

# Only DatabaseService in detail, the rest in stats.
cargo run -p ice-rpc-monitor -- \
    --channel DatabaseService --channel ConfigService \
    --detail-channel DatabaseService --trace-sample-rate 1
```

In detail mode, `TraceRecord` gains `request` and `response`, the decoded
messages; in stats mode those fields are omitted entirely.

## Decoding the messages

An observer is a separate process but it is linked against the **same service
contract** as the providers and consumers, so it can decode the payloads like any
client or provider. Building the service definitions with the `monitoring`
feature of `ice-rpc` makes `#[service]` also generate, per service:

- `impl Display for {Service}Request`;
- a `{Service}Decoder` implementing [`ice_rpc::monitor::ServiceDecoder`].

A crate that declares services (here `common`) can then expose an inventory:

```rust
// `common`, with the `monitoring` feature.
pub fn decoders() -> ice_rpc::monitor::Decoders { /* register every {Service}Decoder */ }
```

and the observer registers it before running:

```rust
let mut config = ice_rpc_monitor::config::Config::default();
config.decoders = std::sync::Arc::new(common::decoders());
```

Decoding is opt-in on purpose: without the `monitoring` feature a plain
provider/consumer carries no decoder, and is not forced to implement `Display`
on every argument and return type. Without a registered decoder for a service,
its messages are shown as `<N bytes, no decoder>`.

## What it produces

| Output | Detail |
|---|---|
| Prometheus endpoint | `GET /metrics` on `127.0.0.1:9898` by default |
| Trace stream | NDJSON correlated by `correlation_id`, off by default |

Metrics exposed:

- `ice_rpc_requests_total`, `ice_rpc_responses_total` (`kind=next|complete|error`)
- `ice_rpc_latency_seconds` — **exact** `response.timestamp_ns - request.timestamp_ns`
- `ice_rpc_payload_bytes`, `ice_rpc_inflight`
- `ice_rpc_sample_gaps_total` — samples missed, measured from `seq` holes
- `ice_rpc_unmatched_requests_total`, `ice_rpc_orphan_responses_total`
- `ice_rpc_clock_skew_total`, `ice_rpc_nodes_alive`, `ice_rpc_node_crashes_total`

## Usage

```bash
# Discover every channel on the machine.
cargo run -p ice-rpc-monitor

# Restrict to a set of channels and expose a trace stream.
cargo run -p ice-rpc-monitor -- \
    --channel DatabaseService --channel ConfigService \
    --trace-sample-rate 100 --trace-file traces.ndjson
```

Run it from the workspace root so it shares the generated
`config/iceoryx2.toml` (the root path) with the observed processes.

## Console example

[`examples/console-monitor.rs`](examples/console-monitor.rs) prints a live stats
block on the console instead of exposing Prometheus, and — with `--detail` — the
decoded messages (request and response of every completed call) as one `[msg] …`
line each. `--demo` makes it self-contained by hosting and calling a
`DatabaseService` in-process:

```bash
# Watch the existing channels, stats only (the default).
cargo run -p ice-rpc-monitor --example console-monitor

# Full mode: also print the decoded messages of every completed call.
cargo run -p ice-rpc-monitor --example console-monitor -- --detail

# Standalone demonstration: host and call DatabaseService in-process.
cargo run -p ice-rpc-monitor --example console-monitor -- --demo --detail
```

The same runners are available through cargo-make (from the workspace root) and
as Cargo aliases:

```bash
cargo make monitoring                 # console, stats only
cargo make monitoring-detail          # console, decoded messages
cargo make monitoring-demo-detail     # self-contained demo (recommended)
cargo make monitoring-metrics         # observer binary, Prometheus on :9898
cargo make monitoring-test            # observer tests

cargo monitoring-detail -- --channel DatabaseService --interval-ms 500
```

With `--detail` the messages are decoded, e.g.:

```
[msg] … method=get_user_age kind=complete request=get_user_age(name=Alice) response=30
```

In `--detail` the example switches the trace format to `human`; the default
`json` (NDJSON) remains available with `--trace-format human|json`.

## Why it cannot perturb the bus

`iceoryx2` natively supports several subscribers per pub/sub service. Because the
transport disables safe overflow and stops publishing as soon as one subscriber
received the sample, a saturated observer is simply **skipped** by the publisher
instead of blocking it. The loss is not silent: the `seq` field of the header
makes it countable.

## Limits

- The observer must be **rebuilt together with the processes it watches**: the
  `user_header` size is part of the iceoryx2 service definition, so a process
  running an older `ice-rpc` build cannot open the same services.
- Decoding requires the service definitions to be built with the `monitoring`
  feature (so `#[service]` generates the decoders) **and** registered with the
  observer. Without a decoder for a service, `--detail` shows its messages as
  `<N bytes, no decoder>`.
- Only the request and the response of a call are decoded. The payload is never
  interpreted beyond the service types, so no business value is extracted.
