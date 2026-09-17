# Service compatibility, and how to purge the iceoryx2 state

This document is the canonical version of the hint that
[`scripts/bench-load.sh`](../scripts/bench-load.sh) already prints when a provider
dies at startup, and of the remedy carried by `RpcError::ProtocolMismatch`.

## Why a service can refuse to open

iceoryx2 records the **static configuration** of a service when it is created, and
refuses to open it later if a process asks for a different one. For an ice-rpc
channel, that configuration includes:

| Recorded setting | Changed by |
|---|---|
| user header | a field added, removed or reordered in `RpcHeader` (the layout is pinned to 128 bytes by a unit test) |
| payload alignment | `PAYLOAD_ALIGNMENT` in [`transport/tuning.rs`](../ice-rpc/src/transport/tuning.rs) |
| payload type | the `Payload` generic of the service definition |
| safe overflow | the `enable_safe_overflow(false)` of the request and response services |
| buffer sizes, port limits, node limit | `SUBSCRIBER_BUFFER`, `MAX_PUBLISHERS`, `MAX_SUBSCRIBERS`, `MAX_NODES`, `MAX_LOANED_SAMPLES` |

Two facts follow:

- **Every process on a machine must be rebuilt together** after such a change. A
  binary from before the change and a binary from after it cannot share a channel;
- a process killed while it held a service (`SIGKILL`, a crash, a debugger stop)
  leaves its files behind. iceoryx2 then reports the service as corrupted, or —
  in the worst case — tries to remove a file whose shared memory is gone and
  recurses until `thread has overflowed its stack`.

## How it shows up

`ice_rpc::RpcError::ProtocolMismatch` — not a transport error, and deliberately
**not retryable**: no amount of retrying resolves it. Its message names the
iceoryx2 variant and the remedy below, so the log of a failing startup is enough
to act:

```
RPC error: incompatible service on the bus: open service: ServiceInCorruptedState.
the iceoryx2 state on this machine was created by another build of this service
(wire format, buffer sizes or port limits changed), or a process was killed while
it held it. Once no process still runs the previous build, remove the iceoryx2
root path: ...
```

The classification lives in [`transport/open.rs`](../ice-rpc/src/transport/open.rs),
which is also where the remedy text is written once.

## The procedure

1. **Stop every process of the previous build.** A purge while one is still
   running only recreates the state that is being removed. On Windows, check the
   process list; the liveness probe
   (`cargo make probe -- list`, [`node_liveness_probe.rs`](../ice-rpc/examples/node_liveness_probe.rs))
   lists the iceoryx2 nodes that are still alive with their PID and executable.
2. **Remove the iceoryx2 root path:**

   | OS | Path |
   |---|---|
   | Windows | `%APPDATA%\ice-rpc\iceoryx2` |
   | Linux and macOS | `$XDG_DATA_HOME/ice-rpc/iceoryx2`, or `~/.local/share/ice-rpc/iceoryx2` |

   [`scripts/purge-iceoryx2-root.sh`](../scripts/purge-iceoryx2-root.sh) resolves it
   per OS, shows what it holds, and removes it only with `--yes`:

   ```bash
   scripts/purge-iceoryx2-root.sh          # dry run: path, file count, size
   scripts/purge-iceoryx2-root.sh --yes    # remove it
   ```
3. **Rebuild everything** and restart the provider first, then the consumers.

## Two adjacent cases

- A service that simply **does not exist yet** is not this problem: `native_call`
  waits for its provider (`ICE_RPC_PROVIDER_WAIT_MS`, 30 s by default) and then
  reports `no subscriber connected (is the provider running?)`.
- A channel whose subscriber buffer stayed full is reported as
  `delivery refused: the subscriber buffer stayed full`. That one is backpressure:
  the publisher retries, and the call only fails if the whole timeout elapses.
