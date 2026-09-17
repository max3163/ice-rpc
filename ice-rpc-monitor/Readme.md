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

Each rendered value uses its own `Display` implementation when it has one, and
falls back to its `Debug` implementation otherwise: `Debug` is the only
formatting requirement a service type has to meet.

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
provider/consumer carries no decoder at all. Without a registered decoder for a
service, its messages are shown as `<N bytes, no decoder>`.

## What it produces

| Output | Detail |
|---|---|
| Prometheus endpoint | `GET /metrics` on `127.0.0.1:9898` by default |
| Trace stream | NDJSON correlated by `correlation_id`, off by default |

Metrics exposed:

Bus metrics:

- `ice_rpc_requests_total`, `ice_rpc_responses_total` (`kind=next|complete|error`)
- `ice_rpc_latency_seconds` — **exact** `response.timestamp_ns - request.timestamp_ns`
- `ice_rpc_payload_bytes`, `ice_rpc_inflight`
- `ice_rpc_sample_gaps_total` — samples missed, measured from `seq` holes
- `ice_rpc_unmatched_requests_total`, `ice_rpc_orphan_responses_total`
- `ice_rpc_clock_skew_total`, `ice_rpc_nodes_alive`, `ice_rpc_node_crashes_total`

## Health of the network

An inventory of the machine's iceoryx2 state is refreshed at
`--health-interval-ms` (default 2000, `0` disables it). `Node::list` and
`Service::list` are expensive, so the scan is throttled and **conservative on
error**: a failed scan keeps the previous snapshot and never reports a live node
as dead.

| Metric | Meaning |
|---|---|
| `ice_rpc_nodes{state}` | nodes by native liveness (`alive`/`dead`/`inaccessible`/`undefined`) |
| `ice_rpc_node_info{pid,state,executable}` | one series per node (value 1) |
| `ice_rpc_services{service,pattern,role}` | one series per iceoryx2 service (`role`: `req`/`resp`/`req_notify`/`resp_notify`) |
| `ice_rpc_service_participants{service}` | nodes registered on a service |
| `ice_rpc_channel{channel,direction}` | whether the observer is attached (0/1) |
| `ice_rpc_channel_publishers` / `ice_rpc_channel_subscribers` | active ports, observer excluded |
| `ice_rpc_channel_capacity{kind}` | `max_publishers`, `max_subscribers`, `subscriber_buffer_samples` |
| `ice_rpc_process_cpu_percent{pid,name}` | CPU as a percentage of **one core** (can exceed 100), `process-metrics` feature |
| `ice_rpc_process_cpu_percent_total{pid,name}` | same usage normalised to `0..100` over every core — comparable to a task manager |
| `ice_rpc_process_rss_bytes` / `_virtual_bytes` / `_uptime_seconds` | memory and uptime per process |
| `ice_rpc_host_cpu_count` | logical CPUs of the host, the divisor of the normalised value |
| `ice_rpc_shm_scan_enabled`, `ice_rpc_shm_bytes`, `ice_rpc_shm_segments`, `ice_rpc_shm_files` | measured shared-memory footprint |
| `ice_rpc_health_scans_total` / `ice_rpc_health_errors_total` | inventory scans / failed partial scans |
| `ice_rpc_observer_dropped_traces_total` / `ice_rpc_discovery_errors_total` | observer's own health |

```bash
# Inventory every 1 s, with the process resources and the on-disk shm footprint.
cargo run -p ice-rpc-monitor --features process-metrics -- \
    --health-interval-ms 1000 --health-shm
```

```
 network : nodes=2 (alive 2, dead 0)  services=12  channels=3
 shm     : 6 segment(s), 1.2MiB
 node    : pid=1234 provider-app  cpu 3.1% core / 0.2% host  rss 18.4MiB  up 42s
```

Reading the block: `network` is a bus summary (nodes, services, channels);
`shm` is the measured shared-memory footprint; each `node` line describes **one
emitting process** (an iceoryx2 node) with its CPU, memory and uptime.

`cpu 3.1% core` follows the `sysinfo` convention: a percentage of a **single**
CPU, so it can exceed 100 on a multi-threaded process. The value after the slash
is the same usage divided by the number of logical CPUs
(`ice_rpc_host_cpu_count`), i.e. the figure a task manager shows.

In `--live` the frame is truncated to the terminal height (`LINES`, default 24)
so it can never scroll — the one thing that would garble an in-place redraw.

Platform notes:

- **Shared memory**: iceoryx2 exposes no public segment-size getter, so the
  footprint is *measured* by summing the segment files under the iceoryx2 root
  path (and `/dev/shm` on Unix). This only works where the segments are
  **file-backed** — on Windows they are OS named mappings, the walk finds nothing
  and the console prints `shared : n/a`. The per-process RSS remains available
  and includes the mapped pages.
- **Process metrics** need the `process-metrics` feature (`sysinfo`) and are off
  by default, so a plain bus observer pays nothing.

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
cargo make monitoring-live            # top-like live view
cargo make monitoring-live-release    # same, compiled in release
cargo make monitoring-metrics         # observer binary, Prometheus on :9898
cargo make monitoring-test            # observer tests

cargo monitoring-detail -- --channel DatabaseService --interval-ms 500
```

For a view left running, prefer the release variants: the same workload costs
about **1.5 %** of one core in release versus **5.5 %** in debug (provider and
consumer idle, health inventory every 2 s). `--demo` additionally runs the
provider and its traffic generator inside the observer process.

With `--detail` the messages are decoded, e.g.:

```
[msg] … method=get_user_age kind=complete request=get_user_age(name=Alice) response=30
```

In `--detail` the example switches the trace format to `human`; the default
`json` (NDJSON) remains available with `--trace-format human|json`.

## Live view, like `top`

`--live` redraws the stats **in place** using the terminal's alternate screen
buffer: nothing scrolls, and the previously displayed content is restored on
exit — including on `Ctrl+C`, through an RAII guard. In detail mode the messages
are kept in a bounded in-memory buffer (the last 10) and shown inside the frame
instead of being streamed, so the view stays stable.

```bash
cargo make monitoring-live
# or
cargo run -p ice-rpc-monitor --features process-metrics -- \
    --live --health-shm --channel DatabaseService
```

```
===== ice-rpc-monitor (console stats) =====
 requests        : 128
 latency (exact) : calls=128 avg=90us p50=80us p90=110us p99=400us
 network : nodes=2 (alive 2, dead 0)  services=12  channels=3
 node    : pid=4242 provider-app  cpu 3.1% core / 0.2% host  rss 18.4MiB  up 42s
------------------------------------------------------------
 recent messages
 [msg] … method=get_user_age kind=complete request=get_user_age(name=Alice) response=30
```

When stdout is **not** a terminal (a pipe, a file, an IDE without a TTY) the live
mode is silently disabled: the same frames are appended, so `>` redirection still
produces readable, greppable output.

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
