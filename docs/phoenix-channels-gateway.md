# ADR — Phoenix Channels gateway on `trillium-websockets`

- **Status**: implemented
- **Scope**: the optional `channels-*` features,
  [`ice-rpc/src/channels_gateway.rs`](../ice-rpc/src/channels_gateway.rs:1) and
  [`ice-rpc/src/phoenix.rs`](../ice-rpc/src/phoenix.rs:1)
- **Client**: the standard JavaScript `phoenix` package, WebSocket transport
- **Replaces**: the abandoned Socket.IO plan
  ([`plans/socketio-gateway.md`](../plans/socketio-gateway.md:1))

## Context

The crate already exposes services over HTTP with the `http-*` features and over
N-API with `ice-rpc-nodejs`. A browser needs a push-capable transport for
services that stream several values, and the project wants to stay on the
trillium stack rather than add hyper/engine.io.

`trillium-channels` looked like the natural fit: it implements the Phoenix
Channels wire protocol as a trillium `Handler` and composes with the REST
handler. It cannot be used as published, however; the next section explains
why, and the decision records the smallest replacement that keeps the standard
Phoenix contract.

## Why `trillium-channels` 0.4.0 is not used

The published `trillium-channels` 0.4.0 depends on `trillium-websockets
^0.7.0`. In that version,
[`WebSocketConn::new`](../../.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/trillium-websockets-0.7.0/src/websocket_connection.rs:80)
stores `upgrade.path()`, and `Upgrade::path()` already removes the query string.
[`WebSocketConn::querystring()`](../../.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/trillium-websockets-0.7.0/src/websocket_connection.rs:151)
therefore always returns an empty string.

[`ChannelCentral::connect`](../../.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/trillium-channels-0.4.0/src/channel_central.rs:77)
reads `vsn` from that query and falls back to `Version::V1` when it is absent.
The official Phoenix JavaScript client always appends `vsn=2.0.0` and only emits
V2 array frames, so every client message fails V1 deserialization and is
silently dropped: joins and pushes never reach the application.

`trillium-websockets` 0.9.3 fixes this by storing the query string separately,
but no released `trillium-channels` depends on 0.9 yet. Forcing the newer
version is not possible without a fork:

- `[patch.crates-io]` and `[replace]` require the patched package to satisfy the
  dependency's `^0.7.0` requirement. No 0.7.x release contains the fix, and
  declaring a 0.9.3 source as `0.7.0` is exactly the vendored fork we refuse.
- A git pin of `trillium-channels` uses `trillium.workspace = true` and
  `trillium-websockets.workspace = true`. The upstream workspace resolves both
  through path dependencies, pulling a second `trillium` crate whose `Handler`
  trait is incompatible with the crates.io `trillium` used by `ice-rpc`.
- The crate exposes no version override: `ChannelCentral` is `pub(crate)`,
  `Channel::new` takes only the handler, and `ChannelClient::new` as well as
  `ChannelEvent::{serialize, deserialize}` are `pub(crate)`.
- Patches and path dependencies apply only to the workspace root. A published
  library cannot require its downstream users to reproduce them, so any
  patch-based gateway would be broken for consumers.

The dedicated codec in [`ice-rpc/src/phoenix.rs`](../ice-rpc/src/phoenix.rs:1)
is therefore a protocol adapter, not a replacement for the channel library: it
owns only version negotiation, frame parsing and frame encoding. Routing,
joins, heartbeats, call tracking and cancellation remain in
[`channels_gateway.rs`](../ice-rpc/src/channels_gateway.rs:1).

## Decision

### One server, two gateways

`channels-threads`, `channels-smol` and `channels-tokio` each imply the matching
`http-*` feature, because the HTTP feature supplies the trillium server adapter.
The gateway is mounted on the **same listener** as REST:

- `GET|POST /{service}/{method}` goes to the REST handler;
- `GET /socket/websocket?vsn=2.0.0` goes to the Phoenix handler.

The composition is a small `RpcGateway<Http, Channels>` handler that selects the
path before the REST handler can halt, and forwards `has_upgrade`/`upgrade` to
the channel handler.

### Feature graph

```toml
channels-threads = ["json", "http-threads", "dep:trillium-websockets"]
channels-smol = ["json", "http-smol", "dep:trillium-websockets"]
channels-tokio = ["json", "http-tokio", "dep:trillium-websockets"]
```

`full` includes `channels-tokio`: unlike the Node.js gateway, this is another
transport on the same server, not a separate N-API deployment. A build without
`channels-*` carries neither `trillium-websockets` nor the codec.

### Wire contract

Each service is its own topic, named after the service. The endpoint defaults to
`/socket`, so the client joins `socket.channel("Database", params)` and then
pushes one of three events:

| Client event | Payload            | Answer                           |
| ------------ | ------------------ | -------------------------------- |
| `call`       | `{ method, args }` | one `phx_reply`                  |
| `stream`     | `{ method, args }` | one `phx_reply` ack, then pushes |
| `cancel`     | `{ ref }`          | one `phx_reply` ack              |

The join reply carries `{"protocol":1,"service":"Database"}`. An unknown service
is refused at join time with `E_UNKNOWN_SERVICE`, so a client cannot keep a
channel open for a service the gateway does not expose. The `phoenix` heartbeat
topic is reserved.

### Correlation

The Phoenix `ref` is the correlation id. The client library assigns it to every
push, and `phx_reply` already echoes it, so a `call` needs no application-level
id. A `stream` reuses its push ref in the ack and in every server push: the
client can match `payload.ref` in `channel.on("next", payload => …)`, and
`cancel` names that ref. The client never invents an identifier.

`call` reads one value (`ReadMode::First`): the reply is
`{"status":"ok","response":{"kind":"value","data":…}}`,
`{"kind":"nothing"}` or `{"kind":"accepted","delivered":…}`. A failure is
`{"status":"error","response":{"kind":"E_…","message":…}}`.

`stream` is acknowledged immediately so a long stream does not trip the Phoenix
push timeout. The server then pushes `next`, and closes with `complete`,
`error` or `accepted`, each carrying the same `ref`. After the ack, an error is
always an event, never a second `phx_reply`.

`cancel` cancels the per-call token and drops the event receiver. The service's
`Observable` is dropped, which publishes the transport's `Cancel`; the server
pushes a best-effort `complete` when the socket is still open.

Refs are 1–64 bytes of `[A-Za-z0-9._:-]`, unique among the calls in flight on
one topic. A duplicate is `E_DUPLICATE_REF`, an invalid one is `E_INVALID_REF`,
and exceeding `max_calls_per_channel` is `E_LIMIT`.

### Streaming contract

The gateway is a third consumer of the JSON contract: `call` uses
`JsonInvoker::invoke_json`, `stream` uses the additive
`JsonInvoker::invoke_json_stream`. The macro generates one streaming helper per
method and the shared [`stream_json`](../ice-rpc/src/json.rs:1) loop maps
`Observable` events to `JsonStreamEvent`s. A bounded `JsonEventSink` is the
backpressure and cancellation boundary: a dropped receiver stops the producer.

A one-way method reports `accepted` and completes. A `call` on a multi-value
method cancels the remainder after the first value, exactly like the HTTP
gateway; `stream` is the transport for such methods.

### Authorization

`ChannelsAuthorizer::authorize_join` receives a `JoinContext` with the handshake
headers, the join parameters (`token` included), the peer address and the service
named by the topic. A token can therefore authorize `Database` without opening
`Billing`. It returns `Ok(())` or a client-facing reason, sent as an
`E_UNAUTHORIZED` reply.

An exact-match `allowed_origins` allowlist is checked first. Empty means any
origin; the gateway logs a warning at startup. Authentication in front of the
gateway is the deployment's responsibility when no authorizer is configured.

### Limits

- WebSocket only. The Phoenix JavaScript long-poll transport is not implemented;
  the client must use its WebSocket transport. In a browser the WebSocket
  transport is the default; the long-poll fallback is unreachable and fails.
- Services are compile-time: the `start_channels_gateway!` macro lists the
  proxies, exactly like `start_http_gateway!`.
- The client should pass a suitable push timeout when it uses `call` for a long
  unary operation; `stream` is immune because its ack is immediate.
- In-flight calls are lost on reconnect; the Phoenix client rejoins, the
  application reissues.
- The outbound per-connection channel is bounded by `stream_buffer`. A slow
  client therefore applies backpressure to the service through the
  `JsonEventSink`, instead of growing an unbounded queue on the server.
- `max_calls_per_channel` is a per-service-topic limit, and a `ref` is unique
  only inside its topic. Two services may therefore use the same `ref`
  concurrently on one socket, and one service cannot exhaust the in-flight
  budget of another.

## Testing

- [`ice-rpc/src/phoenix.rs`](../ice-rpc/src/phoenix.rs:1) tests V1/V2 parsing,
  malformed frames, both reply encodings, version negotiation and query parsing.
- [`ice-rpc/src/channels_gateway.rs`](../ice-rpc/src/channels_gateway.rs:1)
  tests ref validation, payload parsing, admission limits, duplicate refs,
  per-topic call tables, leave and rejoin isolation, per-call cancellation,
  shutdown, origin matching and the error code mapping.
- [`ice-rpc/tests/channels_gateway.rs`](../ice-rpc/tests/channels_gateway.rs:1)
  runs the real `start_channels_server_with_config` entry point and speaks the
  Phoenix V2 array format directly: per-service joins, a second service on the
  same socket, unary call, business error, streaming, cancellation, heartbeat,
  a call on a topic that was not joined, an unknown service refused at join, and
  the reserved `phoenix` topic. A raw client is used instead of the npm package
  so the test is deterministic and needs no JavaScript toolchain; the frames are
  exactly those the `phoenix` client emits.
- `channels-tokio`, `channels-smol` and `channels-threads` are checked
  separately; the WebSocket upgrade path is shared by all three adapters.

## Alternatives considered

| Alternative                                 | Why not                                                                                               |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| Single multiplexed `rpc` topic              | no per-service authorization at join, and every payload must repeat the service name                  |
| Application-level `id` field                | duplicates the Phoenix `ref` and makes the client invent an identifier                                |
| Socketioxide (Socket.IO)                    | tokio-only, hyper/engine.io dependency, second listener                                               |
| Raw WebSocket JSON                          | not the Phoenix Channels stack; the client would be bespoke                                           |
| Separate `ice-rpc-channels` crate           | the trillium server and invoker registry are already in `ice-rpc`                                     |
| Vendor or fork `trillium-websockets`        | local maintenance, rejected; also invisible to downstream users of the published crate                |
| Pin the whole trillium stack to a git rev   | pulls unreleased code, duplicates `trillium` unless every adapter follows, and patches do not publish |
| Wait for a released `trillium-channels` 0.5 | no release date; the gateway would be unusable in the meantime                                        |

## Consequences

- Browsers get push streaming from the existing `JsonInvoker` without a new wire
  format or a second serializer.
- The dependency is optional and standard: only `trillium-websockets` 0.9 from
  crates.io, with no local patch, vendored tree or git pin.
- The protocol surface maintained in-tree is small and covered by its own tests:
  version negotiation, five-field frame parsing and reply encoding.
- The gateway trusts the join authorizer for access control; without one, it is
  an unauthenticated network surface and must sit behind an authenticating
  reverse proxy.
- The upstream dependency problem is documented in a GitHub issue; the codec can
  be deleted once a released `trillium-channels` supports
  `trillium-websockets` 0.9.
