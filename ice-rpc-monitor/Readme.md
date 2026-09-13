# ice-rpc-monitor

Out-of-band observer for [`ice-rpc`](../ice-rpc/Readme.md).

It attaches **in read-only mode** to the iceoryx2 services an ice-rpc process
already exposes (`{channel}_req`, `{channel}_resp` and their `_notify` event
services), reads the zero-copy `RpcHeader` and never decodes the rkyv payload.
It is a separate process, so its cost never runs on the observed processes.

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
- It reads metadata only. Counting business values or error payloads would
  require decoding rkyv and is deliberately out of scope.
