# Service compatibility, and how to purge the iceoryx2 state

This document is the canonical version of the hint that
[`scripts/bench-load.sh`](../scripts/bench-load.sh) already prints when a provider
dies at startup, and of the two remedies carried by `RpcError::ProtocolMismatch`.

## Why a service can refuse to open

iceoryx2 records the **static configuration** of a service when it is created, and
refuses to open it later if a process asks for a different one. For an ice-rpc
channel, that configuration mixes two families — settings compiled into the
library, and settings the deployment provides:

| Recorded setting | Changed by | Family |
|---|---|---|
| user header | a field added, removed or reordered in `RpcHeader` (its size **and** every offset are pinned by a unit test; 120 bytes today) | compiled in |
| payload alignment | `PAYLOAD_ALIGNMENT` in [`transport/tuning.rs`](../ice-rpc/src/transport/tuning.rs) | compiled in |
| payload type | the `Payload` generic of the service definition | compiled in |
| safe overflow | the `enable_safe_overflow(false)` of the request and response services | compiled in |
| buffer sizes, port limits, node limit | the `[defaults.publish-subscribe]` of the iceoryx2 configuration: `max-publishers`, `max-subscribers`, `max-nodes`, `subscriber-max-buffer-size`, `publisher-max-loaned-samples` | configuration |

The compiled-in family is a **protocol** property: changing one of those requires
bumping `PROTOCOL_VERSION` and rebuilding every participant together. The
configuration family is a **deployment** property: `ice-rpc` pins none of it, so
each process opens the service with whatever it resolves — which is why all
participants of a channel must resolve the same `iceoryx2.toml` (see the
`Configuration` section of the [`ice-rpc` Readme](../ice-rpc/Readme.md)).

Two facts follow:

- **Every process on a machine must be rebuilt together** after a change of the
  compiled-in family; for the configuration family it is enough that they all
  resolve the same values. Otherwise a binary from before the change and a binary
  from after it cannot share a channel;
- a process killed while it held a service (`SIGKILL`, a crash, a debugger stop)
  leaves its files behind. iceoryx2 then reports the service as corrupted, or —
  in the worst case — tries to remove a file whose shared memory is gone and
  recurses until `thread has overflowed its stack`.

## How it shows up

`ice_rpc::RpcError::ProtocolMismatch` — not a transport error, and deliberately
**not retryable**: no amount of retrying resolves it. Its message names the
iceoryx2 variant and the matching remedy, so the log of a failing startup is
enough to act. There are **two** remedies, and confusing them sends the reader to
a purge that cannot help — the same divergent configuration comes back on the
next start.

A configuration mismatch: the peers do not resolve the same configuration.

```
RPC error: incompatible service on the bus: open service: IncompatibleTypes.
the iceoryx2 configuration resolved by this process differs from the one recorded
when the service was created: payload alignment, overflow behavior, buffer sizes
or port limits. Every participant of this channel must resolve the same iceoryx2
configuration (notably its `[defaults.publish-subscribe]`) and speak the same
protocol version
```

Leftover state: a process was killed while it held the service.

```
RPC error: incompatible service on the bus: open service: ServiceInCorruptedState.
the iceoryx2 state on this machine was left behind by a process that was killed
while it held this service. Once no process still runs, remove the iceoryx2 root
path: ...
```

The classification lives in [`transport/open.rs`](../ice-rpc/src/transport/open.rs),
which is also where the two remedy texts are written once.

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

## A third case: a new *value* in a recorded field

`EventKind` gained a variant for the remote cancellation of a call:

| Value | Variant | Direction | Terminal |
|---|---|---|---|
| 5 | `Cancel` | consumer → provider | no |

It is a new **value** of an existing field, not a new field: `RpcHeader` keeps its
layout, its size (120 bytes, pinned by a unit test), its offsets, the payload
alignment and every buffer size. Nothing of what iceoryx2 records when a service
is created changes, so **no purge is needed** and two builds of this table can
share a channel:

- a **new** consumer talking to an **older** provider: the provider reads the
  unknown value through `EventKind::from_u8`, which is deliberately fail-closed —
  an unknown value maps to `EventKind::Error`, never to a terminal `Complete` — so
  it logs `unexpected Error sample` and ignores the sample. The call is simply not
  cancelled, which is exactly what that build did before;
- an **older** consumer talking to a **new** provider: no Cancel is ever
  published, and nothing changes.

What a new consumer does expect is a provider that honours a Cancel: the in-flight
registry keyed by correlation id, and the token a handler reads with
`CallContext::cancellation()`. A Cancel names a **call**, not a service, so it is
routed before any dispatcher lookup — a Cancel for a method the provider does not
have still stops the call it names.

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

That call reaps the *service and port tags* a dead node left, and with them the
per-port segments. It does **not** reach the `_mgmt` dynamic storage of an event
service: a marker of that kind survives the reaping, which is why a second call
follows it, when a provider starts.

The pair is what makes a machine self-healing in production, where nobody runs a
purge script: each restart absorbs the state left by the runs that were killed.
Measured on a kill/restart loop, the leftover count fell back to zero instead of
growing by one run per kill.

The manual procedure below remains the answer when the state cannot be explained
by a dead process: a service whose *recorded configuration* differs from the
requested one — another build, or a peer that resolves a different `iceoryx2.toml`
— is not a dead node, so no cleanup removes it. Align the configuration first;
only leftover state calls for the purge.

## The call context and its trace ids

`RpcHeader` carries the call context an implementation reads through
`CallContext`: the correlation id, the service and the method the call was routed
to, and the **trace context** — `trace_id: [u8; 16]`, `parent_span_id: u64` and
W3C `flags`.

The trace ids are the framework's own. A call made outside any traced work mints
one (`pid ++ counter`, unique on the machine), and every hop continues it by
parenting on the span id of the hop that emitted the call. No OpenTelemetry stack
is needed to obtain them, and `trace_id` is already in the W3C shape, so an
exporter can be wired downstream later.

Three consequences worth keeping in mind:

- adding fields to `RpcHeader` changes its **size**, and that size is part of what
  iceoryx2 validates when a service is opened: every process on the machine must
  be rebuilt together, exactly like the other compiled-in settings above;
- the header is capped by iceoryx2's `user_header`, so the room for the trace
  context was found by reducing `METHOD_NAME_LEN` from 64 to 32 — the header did
  not grow, it shrank (128 → 120 bytes);
- a trace id is **per call**. It belongs in the trace records, never in a
  Prometheus label, where one series per call would explode the cardinality.

[`ice-rpc/examples/tracing-demo.rs`](../ice-rpc/examples/tracing-demo.rs) runs both
halves across two real hops — the ids an implementation reads with
`CallContext::current()` (available without the `tracing` feature, so ordinary logs
carry them) and the span per call the feature adds:

```bash
cargo run -p ice-rpc --example tracing-demo --features tokio,tracing -- provider
cargo run -p ice-rpc --example tracing-demo --features tokio,tracing -- consumer
```

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
   | Root path: configuration, service registry, segments | Windows | `C:\Temp\iceoryx2` (iceoryx2 default), or the `root-path` of the effective `iceoryx2.toml` |
   | | Linux and macOS | `/tmp/iceoryx2` (iceoryx2 default), or the `root-path` of the effective `iceoryx2.toml` |
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
3. **Align the configuration, or rebuild.** A configuration mismatch is fixed by
   making every participant resolve the same `iceoryx2.toml`; a compiled-in
   protocol change needs every participant rebuilt. Either way, restart the
   provider first, then the consumers.

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
- each file is 8 bytes, but they accumulate one per segment per run; the ones
  actually observed were all `*.event_mgmt.shm_state` — the `_mgmt` storage of an
  **event service**, the one resource the dead-node reaping does not remove
  (it stops at the service and port *tags*);
- every segment operation enumerates that directory, which is where the
  `< Win32 API error > ... FindNextFileA ... [ 18 ]` lines on Windows come from.
  Error 18 is *no more files*: that is the end of the scan, printed through a
  wrapper that cannot render the message. Their frequency tracks what the
  directory holds, and they are harmless.

Since the directory is shared with everything else on the machine, never delete
it wholesale: only the `iox2_*` entries belong to iceoryx2, and only when nothing
is running.

A provider start does exactly that, once, before it creates anything:
[`sweep_orphan_shm_markers`](../ice-rpc/src/transport/mod.rs) enumerates the
markers through `SharedMemory::list()` and asks `SharedMemory::does_exist()` for
each name. On Windows that call **is** the test — opening a name whose mapping is
gone unlinks its marker as a side effect — so a live segment is left untouched and
an orphan is removed. The script below therefore only remains the answer for the
state no running provider would sweep.

This is a **Windows-only** remedy, and that is a property of the primitive, not a
choice: the removal is a side effect of the `shm_open` emulation described above.
Where `shm_open` is the real one, the same call merely reports that the object
exists, so it can remove nothing; on Linux, `SharedMemory::list()` returns the
segments themselves rather than markers. The guard test of the sweep
([`orphan_shm_markers.rs`](../ice-rpc/tests/orphan_shm_markers.rs)) therefore
carries `#![cfg(windows)]`: on any other platform it would assert the opposite of
what the code promises, which is exactly what it did on the Ubuntu coverage job.

## Two adjacent cases

- A service that simply **does not exist yet** is not this problem: `native_call`
  waits for its provider (`ICE_RPC_PROVIDER_WAIT_MS`, 30 s by default) and then
  reports `no subscriber connected (is the provider running?)`.
- A channel whose subscriber buffer stayed full is reported as
  `delivery refused: the subscriber buffer stayed full`. That one is backpressure:
  the publisher retries, and the call only fails if the whole timeout elapses.
