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

## A different failure: an interface version mismatch

Two builds of the same service can still open the same channel — the iceoryx2
service itself is compatible — while disagreeing on the **service interface**.
That case is not a transport failure and is not detected at open time.

The provider checks two versions, both read from the fixed-layout `RpcHeader` and
never from the payload, before dispatching anything:

- `protocol_version` — the framing version. A peer whose framing differs cannot be
  trusted to have filled the rest of the header, so it is checked first and
  answered with `RpcError::ProtocolMismatch`;
- `service_version` — the interface version. The service id (the FNV-1a hash of the
  logical name) and this version are declared together in a `ServiceRef`,
  generated once per service and shared by the client and the provider, so a call
  can never carry one without the other. A mismatch is answered with
  `RpcError::IncompatibleVersion { expected, actual }`.

Every rejection is framed as a **bare `RpcError`** labelled `EventKind::RpcError`
(not the service's `WireEvent<T, E>`), so the caller receives the diagnosis instead
of waiting for a timeout, and all of them are deliberately **not retryable**: the
peers must be rebuilt with the same protocol version, and with the same
`#[service(..., version = N)]`.

The same framing answers the two cases where the provider cannot produce a typed
response at all — a request whose `service_id` nobody registered, and a method the
service does not expose — with `RpcError::UnknownService` and
`RpcError::UnknownMethod`. A rejection must not depend on the service types: the
provider cannot name them for a method it does not have, so a generic framing would
leave those calls unanswered.

The checks read the header precisely because the payload layout is what changes
between versions: a guard that had to decode the divergent payload first would be
doing the very operation it is meant to protect against.

## The procedure

## A clean shutdown releases what the process created

The dispatch thread of a channel owns its iceoryx2 ports, and dropping those
ports is what unlinks the services — and, with them, their `*.shm_state` markers.
`shutdown_and_release` therefore **joins** those threads (through the shutdown
registry) before the process leaves `main`. Without that join, Ctrl+C leaves the
whole channel on the bus: the process exits, the system kills the threads, and no
destructor ever runs.

Same idea for the cache of consumed channels, which lives in a `static` — Rust
never drops a `static`, so it is released explicitly at shutdown.

## A provider reaps the dead nodes at startup

iceoryx2 can clean up after dead nodes itself, in three places, all enabled by
default. ice-rpc turns two of them off on purpose — a *reader* (the observer, a
one-shot client) must not delete a provider's resources on its way in or out —
and asks for the cleanup explicitly once, when a provider starts:

```
[ice-rpc] reaped 1 dead node(s) left by previous runs
```

That single call is what makes a machine self-healing in production, where nobody
runs a purge script: each restart absorbs the state left by the run that was
killed. Measured on a kill/restart loop, the leftover count stabilizes at one
run's worth (~20 `*.shm_state` markers) instead of growing by one run per kill.

The manual procedure below remains the answer when the state cannot be explained
by a dead process: a service whose *recorded configuration* differs from the
requested one — another build — is not a dead node, so no cleanup removes it.

## The procedure

1. **Stop every process of the previous build.** A purge while one is still
   running only recreates the state that is being removed. On Windows, check the
   process list; the liveness probe
   (`cargo make probe -- list`, [`node_liveness_probe.rs`](../ice-rpc/examples/node_liveness_probe.rs))
   lists the iceoryx2 nodes that are still alive with their PID and executable.
2. **Remove the iceoryx2 state.** There are two locations, and only removing
   both is a complete purge:

   | What | OS | Path |
   |---|---|---|
   | Root path: configuration, service registry, segments | Windows | `%APPDATA%\ice-rpc\iceoryx2` |
   | | Linux and macOS | `$XDG_DATA_HOME/ice-rpc/iceoryx2`, or `~/.local/share/ice-rpc/iceoryx2` |
   | Shared-memory markers (`iox2_*.shm_state`) | Windows | `C:\Temp` |
   | | Linux and macOS | `/tmp` |

   [`scripts/purge-iceoryx2-root.sh`](../scripts/purge-iceoryx2-root.sh) resolves
   both per OS, shows what they hold, and removes them only with `--yes`. It
   deletes the root path entirely and, in the marker directory, only the
   `iox2_*` entries — never the directory itself, which is a shared temporary
   directory on Unix:

   ```bash
   scripts/purge-iceoryx2-root.sh          # dry run: paths, file counts, sizes
   scripts/purge-iceoryx2-root.sh --yes    # remove them
   ```
3. **Rebuild everything** and restart the provider first, then the consumers.

## The other half of the state: the shared-memory markers

Windows has no `shm_open`, so `iceoryx2-pal-posix` emulates it with
memory-mapped files and keeps one small `<segment>.shm_state` marker per segment.
That marker lives in a directory of its own, taken from the PAL constant
`TEMP_DIRECTORY` — literally `C:\Temp` on Windows, `/tmp` on Unix — and **not**
under the root path. It is what `shm_unlink` deletes when a process releases its
last reference, which is why a process that is *killed* rather than exiting
leaves it behind: the segment is gone, its marker stays.

Three consequences worth knowing:

- a purge that only removes the root path leaves those markers behind, so the
  state of a machine that has seen many killed runs is never fully reset;
- each file is 8 bytes, but they accumulate one per segment per run, and nothing
  ages them out;
- every segment operation enumerates that directory, which is where the
  `< Win32 API error > ... FindNextFileA ... [ 18 ]` lines on Windows come from.
  Error 18 is *no more files*: that is the end of the scan, printed through a
  wrapper that cannot render the message. Their frequency tracks what the
  directory holds, and they are harmless.

Since the directory is shared with everything else on the machine, never delete
it wholesale: only the `iox2_*` entries belong to iceoryx2, and only when nothing
is running.

## Two adjacent cases

- A service that simply **does not exist yet** is not this problem: `native_call`
  waits for its provider (`ICE_RPC_PROVIDER_WAIT_MS`, 30 s by default) and then
  reports `no subscriber connected (is the provider running?)`.
- A channel whose subscriber buffer stayed full is reported as
  `delivery refused: the subscriber buffer stayed full`. That one is backpressure:
  the publisher retries, and the call only fails if the whole timeout elapses.
