# ADR — Phoenix Channels gateway with `trillium-channels`

- **Status**: implemented
- **Scope**: the optional `channels-*` features and
  [`ice-rpc/src/channels_gateway.rs`](../ice-rpc/src/channels_gateway.rs:1)
- **Client**: the standard JavaScript `phoenix` package, WebSocket transport
- **Replaces**: the abandoned Socket.IO plan
  ([`plans/socketio-gateway.md`](../plans/socketio-gateway.md:1))

## Context

The crate already exposes services over HTTP with the `http-*` features and over
N-API with `ice-rpc-nodejs`. A browser needs a push-capable transport for
services that stream several values, and the project wants to stay on the
trillium stack rather than add hyper/engine.io.

`trillium-channels` is the natural fit: it implements the Phoenix Channels wire
protocol as a trillium `Handler`, composes with the REST handler, and owns the
V1/V2 framing, the heartbeat and the topic subscriptions.

## Dependency on `trillium-channels` 0.5

`trillium-channels` 0.5 depends on `trillium-websockets` 0.9, which preserves
the upgrade query string. `ChannelCentral::connect` therefore reads `vsn=2.0.0`
and selects the V2 array framing used by the official Phoenix JavaScript
client.

The gateway implements `ChannelHandler`. `ChannelEvent::reference()` supplies
the Phoenix `ref` used as the correlation id, and `ChannelClient` carries the
replies and server pushes. `Channel::allow_join`, `ChannelConn::reject`,
`ChannelEvent::build_reply` and `ChannelClient::send_event` cover the protocol
operations this gateway needs.

The previous 0.4 releases pinned `trillium-websockets` 0.7, which dropped that
query and made the crate negotiate V1; 0.5 removes the need for the temporary
in-tree codec used by the earlier revision.

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
channels-threads = ["json", "http-threads", "dep:trillium-channels"]
channels-smol = ["json", "http-smol", "dep:trillium-channels"]
channels-tokio = ["json", "http-tokio", "dep:trillium-channels"]
```

`full` includes `channels-tokio`: unlike the Node.js gateway, this is another
transport on the same server, not a separate N-API deployment. A build without
`channels-*` does not carry `trillium-channels`.

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

`call` reads one value (`first_json`, which cancels the rest of the call): the reply is
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
`Observable` events to `JsonStreamEvent`s. A bounded `JsonEventSink` limits how
far the service can run ahead of the forwarding task and is the cancellation
boundary: a dropped receiver stops the producer.

A one-way method reports `accepted` and completes. A `call` on a multi-value
method cancels the remainder after the first value, exactly like the HTTP
gateway; `stream` is the transport for such methods.

### Authorization

`ChannelsAuthorizer::authorize_join` receives a `JoinContext` with the handshake
headers, the join parameters (`token` included), the peer address and the service
named by the topic. A token can therefore authorize `Database` without opening
`Billing`. It returns `Ok(())` or a client-facing reason, sent as an
`E_UNAUTHORIZED` reply.

The WebSocket handshake itself applies the `trillium-websockets` same-origin
policy: a browser page served from another host than the socket is refused with
`403 Forbidden`, while a non-browser client that sends no `Origin` is accepted.
Two configuration points widen it:

- `allowed_origins` lists the exact origins admitted by the handshake. The same
  exact-match check runs again when a topic is joined, as defence in depth. An
  empty list keeps the same-origin default.
- `allow_any_origin` disables the handshake check entirely. It exists for
  tests, custom schemes and deployments whose public origin differs from the
  `Host` the gateway receives (TLS-terminating proxy, port mapping), and it
  takes precedence over `allowed_origins`. The gateway logs a warning at
  startup, because any page can then open the socket.

Authentication in front of the gateway is the deployment's responsibility when
no authorizer is configured.

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
- `trillium-channels` uses an unbounded per-client outbound queue, so a slow
  socket can buffer events. The bounded `JsonEventSink` still bounds the service
  side and cancellation; the socket-side queue is an upstream limitation,
  documented rather than hidden.
- `max_calls_per_channel` is a per-service-topic limit, and a `ref` is unique
  only inside its topic. Two services may therefore use the same `ref`
  concurrently on one socket, and one service cannot exhaust the in-flight
  budget of another.

## Testing

- [`ice-rpc/src/channels_gateway.rs`](../ice-rpc/src/channels_gateway.rs:1)
  tests ref validation, payload parsing, admission limits, duplicate refs,
  per-topic call tables, leave and rejoin isolation, per-call cancellation,
  shutdown, origin matching, outcome payloads and the error code mapping.
- [`ice-rpc/tests/channels_gateway.rs`](../ice-rpc/tests/channels_gateway.rs:1)
  runs the real `start_channels_server_with_config` entry point and speaks the
  Phoenix V2 array format directly: per-service joins, a second service on the
  same socket, unary call, business error, streaming, cancellation, heartbeat,
  a call on a topic that was not joined, an unknown service refused at join, and
  the reserved `phoenix` topic. A raw client is used instead of the npm package
  so the test is deterministic and needs no JavaScript toolchain; the frames are
  exactly those the `phoenix` client emits.
- [`ice-rpc/tests/channels_gateway_origin.rs`](../ice-rpc/tests/channels_gateway_origin.rs:1)
  drives the handshake with an explicit `Origin`: the default refuses a foreign
  page with `403 Forbidden`, `allow_any_origin` accepts it, and
  `allowed_origins` admits exactly the listed origin while a client without an
  `Origin` header stays accepted.
- `channels-tokio`, `channels-smol` and `channels-threads` are checked
  separately; the WebSocket upgrade path is shared by all three adapters.

## Alternatives considered

| Alternative                       | Why not                                                                                       |
| --------------------------------- | --------------------------------------------------------------------------------------------- |
| Single multiplexed `rpc` topic    | no per-service authorization at join, and every payload must repeat the service name          |
| Application-level `id` field      | duplicates the Phoenix `ref` and makes the client invent an identifier                        |
| Socketioxide (Socket.IO)          | tokio-only, hyper/engine.io dependency, second listener                                       |
| Raw WebSocket JSON                | not the Phoenix Channels stack; the client would be bespoke                                   |
| Separate `ice-rpc-channels` crate | the trillium server and invoker registry are already in `ice-rpc`                             |
| In-tree Phoenix codec             | unnecessary duplication now that `trillium-channels` 0.5 depends on `trillium-websockets` 0.9 |

## Consequences

- Browsers get push streaming from the existing `JsonInvoker` without a new wire
  format or a second serializer.
- The dependency is optional and standard: `trillium-channels` 0.5 from
  crates.io, with no local patch, vendored tree or git pin.
- The Phoenix envelope, heartbeat and subscriptions are maintained upstream; the
  gateway only owns the RPC semantics (join validation, call table, streaming
  and cancellation).
- The per-client outbound queue is unbounded upstream. This is documented; the
  service side remains bounded by `JsonEventSink`.
- The gateway trusts the join authorizer for access control; without one, it is
  an unauthenticated network surface and must sit behind an authenticating
  reverse proxy.
