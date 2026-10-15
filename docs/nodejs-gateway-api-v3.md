# ADR — Node.js gateway N-API contract v3

- **Status**: **implemented**. The provider-side envelope below is in place; the
  surface, the argument convention, the error codes and the lifecycle of v2 are
  unchanged.
- **Scope**: the provider-side envelope of the `gateway-nodejs` addon
  ([`gateway_nodejs/src/lib.rs`](../gateway_nodejs/src/lib.rs:1))
- **Supersedes**: [`nodejs-gateway-api-v2.md`](nodejs-gateway-api-v2.md) §3 (the
  event envelope, replaced here). v2 §2 (argument convention), §4 (error codes),
  §5 (lifecycle), §6 (async execution) and §7 (the JSON surface) still hold.
- **Breaking**: yes — a provider written against v2 must be migrated (see
  *Migration*). The consumer surface is untouched.

## Context

v2 gave `resolveNodejsCall` and `emitNodejsEvent` the **same** envelope,
`{ type, data }`, and let the caller state the event kind:

```js
gateway.resolveNodejsCall(cid, { type: 'next', data: v });      // v2
gateway.resolveNodejsCall(cid, { type: 'complete', data: v });  // v2
gateway.resolveNodejsCall(cid, { type: 'error', data: e });     // v2
```

Two defects follow from that:

1. **The terminality of a call is decided by a field the caller may get wrong.**
   `resolveNodejsCall` *closes* the call, but a caller passing `type: 'next'`
   emits a lone `Next` and no terminal event at all. The consumer reads the first
   value and then waits — for a stream it can never see end. The reference
   examples did exactly this
   ([`provider-context.js`](../gateway_nodejs/examples/provider-context.js:80),
   [`provider-notification.js`](../gateway_nodejs/examples/provider-notification.js:59)),
   which is the tell that the API invited the mistake.
2. **Two entry points with the same shape but different meanings.** `emit`
   *always* means "one more value, the call stays open"; `resolve` *always* means
   "this call ends here". Their signature said neither.

## Decision

The envelope no longer states the event kind: **the entry point does.**

| JavaScript | Wire | Meaning |
|---|---|---|
| `emitNodejsEvent(cid, { data: v })` | `Next(v)` | one intermediate value, call stays open |
| `resolveNodejsCall(cid, { data: v })` | `CompleteWith(v)` | closes with one last value, a single sample |
| `resolveNodejsCall(cid, {})` (or `null`) | `Complete` | closes without a value |
| `resolveNodejsCall(cid, { error: e })` | `Error(e)` | closes with a business error |

Rules:

- `resolveNodejsCall` **derives** the closing event; it is never told a `type`;
- `emitNodejsEvent` accepts `{ data }` only — an `error` is terminal, so it is
  refused with `E_INVALID_ARGS`;
- the v2 `{ type, data }` envelope is **removed** from both entry points and
  refused with `E_INVALID_ARGS`, so a caller that still uses it gets a
  diagnosable failure instead of a stream that never terminates.

The presence of a **key** decides, not its value: `{ data: null }` is the
`CompleteWith(())` of a unit method — `()` serializes to `null` — while `{}` is
the only way to close without a value.

### Where it is implemented

- [`bridge.rs`](../ice-rpc-nodejs/src/bridge.rs:258): `next_envelope` and
  `terminal_envelope` normalize the JavaScript envelope into the internal
  `{ "type", "data" }` form the generated converter reads
  (`serialize_response_from_value`, unchanged). Both entry points apply them —
  [`emit_event`](../ice-rpc-nodejs/src/bridge.rs:198) and
  [`resolve`](../ice-rpc-nodejs/src/bridge.rs:223) — `resolve` validating
  **before** it takes the call, so a malformed envelope never closes one
  unanswered;
- [`napi_api.rs`](../ice-rpc-nodejs/src/napi_api.rs:228) and
  [`macros.rs`](../ice-rpc-nodejs/src/macros.rs:1): the doc-comments of the two
  entry points follow the same contract.

The normalization sits at the JavaScript boundary, so neither the generated code
nor the HTTP gateway — which consume the same `{ "type", "data" }` internally —
is affected.

## Consequences

- **Breaking** for a provider written against v2; `callService` and
  `callServiceStream` are unchanged, since they read events rather than produce
  them.
- A failure on a value-less method stays expressible: `{ error }`.
- The examples, the tests, `types/gateway.d.ts` and every Readme are updated in
  the same change, so no shipped document describes the v2 envelope.

## Migration

| v2 | v3 |
|---|---|
| `resolveNodejsCall(cid, { type: 'next', data: v })` | `resolveNodejsCall(cid, { data: v })` |
| `resolveNodejsCall(cid, { type: 'complete', data: v })` | `resolveNodejsCall(cid, { data: v })` |
| `resolveNodejsCall(cid, { type: 'complete' })` | `resolveNodejsCall(cid, {})` |
| `resolveNodejsCall(cid, { type: 'error', data: e })` | `resolveNodejsCall(cid, { error: e })` |
| `emitNodejsEvent(cid, { type: 'next', data: v })` | `emitNodejsEvent(cid, { data: v })` |

Note the first two rows: in v2 a `next` and a `complete`-with-`data` were distinct
calls; in v3 they are the same one, because a `next` sent to `resolveNodejsCall`
was always meant to close the call.
