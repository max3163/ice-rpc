# ADR — Node.js JSON bridge: keep `serde_json::Value`

- **Status**: Accepted
- **Date**: 2026-10-19
- **Scope**: the JSON representation used by `ice-rpc-nodejs` between the IPC
  transport and the JavaScript host
- **Related**: [`nodejs-gateway-api-v3.md`](nodejs-gateway-api-v3.md),
  [`../ice-rpc/benches/json_value_cost.rs`](../ice-rpc/benches/json_value_cost.rs:1)

## Context

The Node.js gateway moves service payloads in both directions:

- **provider side** — an IPC request is decoded from rkyv, converted to a
  `serde_json::Value` by `deserialize_request_to_value`, handed to the JavaScript
  dispatcher through a `ThreadsafeFunction`, and converted to a native JS object
  by N-API;
- **consumer side** — JavaScript passes a value to `callService` /
  `callServiceStream`, or answers with `emitNodejsEvent` / `resolveNodejsCall`;
  N-API converts it to a `serde_json::Value`, then the generated converter decodes
  it into the Rust payload type and serializes it back to rkyv.

The bridge is therefore **not** doing JSON text serialization: there is no
`JSON.stringify` and no `JSON.parse`. [`ThreadsafeFunction<Value>`](../ice-rpc-nodejs/src/bridge.rs:45)
is converted to V8 values on the JavaScript thread by the `serde-json` feature of
[`napi`](../Cargo.toml:133). What the bridge does build is an intermediate
`serde_json::Value` tree in Rust, and that tree is dropped after the N-API
conversion.

The question this ADR settles: could a feature on `#[payload]` add
`#[napi(object)]` to the payload types and send the objects directly to Node.js,
removing the intermediate tree and simplifying the solution?

## Measurement

The tree cost is measured by [`json_value_cost.rs`](../ice-rpc/benches/json_value_cost.rs:1):

```bash
cargo bench -p ice-rpc --bench json_value_cost
```

The bench prices five operations on the same payload shapes:

- `to_value`: provider-side tree build (`serde_json::to_value(&payload)`);
- `from_value`: response-side tree read (`serde_json::from_value::<T>(value)`),
  with the input clone excluded from the timed section through
  `iter_with_setup`;
- `value_clone`: the clone of `data` the bridge used to perform in
  [`next_envelope`](../ice-rpc-nodejs/src/bridge.rs:266) and
  [`terminal_envelope`](../ice-rpc-nodejs/src/bridge.rs:294); the implementation
  now moves the value instead;
- `rkyv_encode` / `rkyv_decode`: the baseline the tree is added to.

Medians, on the machine used for this decision, with Criterion (1 s warm-up,
2 s measurement; the exact values move with the host, the orders of magnitude do
not):

| Shape                                | `to_value` | `from_value` | `Value::clone` | rkyv encode | rkyv decode |
| ------------------------------------ | ---------: | -----------: | -------------: | ----------: | ----------: |
| `u32`                                |    4.34 ns |     23.45 ns |        9.59 ns |    26.04 ns |     1.05 ns |
| `WorkloadScalars`                    |   228.7 ns |     136.5 ns |       160.4 ns |     30.4 ns |     2.30 ns |
| `String` 64 B                        |   11.14 ns |     23.10 ns |       23.65 ns |    109.6 ns |    21.14 ns |
| `String` 1 KiB                       |   17.91 ns |     22.99 ns |       29.52 ns |    114.0 ns |    37.83 ns |
| `String` 1 MiB                       |   22.29 µs |      23.0 ns |       21.55 µs |    21.96 µs |    35.39 µs |
| `PersonneInfo`                       |   204.4 ns |      94.6 ns |       133.9 ns |     96.8 ns |     64.5 ns |
| `Vec<ContextEntry>` × 4              |   417.6 ns |     221.5 ns |       281.4 ns |    196.5 ns |    105.3 ns |
| `Vec<ContextEntry>` × 64             |   11.47 µs |     4.233 µs |       10.17 µs |    940.9 ns |    4.250 µs |
| `Vec<ContextEntry>` × 1024           |   204.8 µs |     73.28 µs |       171.7 µs |    10.61 µs |    65.19 µs |
| `WorkloadQuery` 8 tags + 8 filters   |   1.540 µs |     662.1 ns |       1.373 µs |    405.8 ns |    543.3 ns |
| `WorkloadQuery` 64 tags + 64 filters |   14.43 µs |     5.789 µs |       12.98 µs |    1.541 µs |    6.389 µs |
| `HttpResponseParams`, 1 KiB body     |   9.630 µs |     6.817 µs |       3.881 µs |    214.3 ns |    124.2 ns |
| `HttpResponseParams`, 1 MiB body     |   25.99 ms |     9.846 ms |       20.78 ms |    22.76 µs |    23.12 µs |

Two readings matter:

1. **Small and scalar payloads are not dominated by the tree.** For `u32` and a
   short `String`, the tree costs nanoseconds; the IPC, the thread hop and the
   V8 object creation are the costs that matter. A napi-object fast path would
   not be visible there.
2. **Structured and binary-heavy payloads are dominated by the tree.** For a
   `Vec<ContextEntry>` of 1024 entries, `to_value` is ~19× the rkyv encode and
   ~3× the rkyv decode. For `HttpResponseParams` with a 1 MiB `Vec<u8>`, the
   tree is three orders of magnitude larger than rkyv: `serde_json` represents
   the bytes as an array of numbers. Before the follow-up optimization, the
   envelope cloned `data`, adding another 20.78 ms on the response path before
   the 9.85 ms parse; the bridge now moves it.

The `from_value` line for a top-level `String` is intentionally surprising:
`serde_json::from_value::<String>` **moves** the string out of
`Value::String`, so it is a 23 ns operation even for 1 MiB. The tree build
(`to_value`) was the real copy in that path; the envelope clone priced by the
bench is now avoided by moving `data`.

The measurement is an **upper bound of the conversion saving**, not an
end-to-end speedup: a napi-object path still creates the V8 objects, and the
current contract for a nested `Vec<u8>` is an array of numbers (only a top-level
`Vec<u8>` argument is base64-encoded by the generated converter). Mapping nested
bytes to a Buffer would be a contract change, not just a serialization
optimization.

## Decision

Keep `serde_json::Value` as the transport-neutral intermediate. Do **not** add a
`napi` feature to `#[payload]` and do **not** generate `#[napi(object)]` on the
service payload types.

## Why the payload-level napi feature is rejected

1. **It does not cover the payload language.** `#[napi(object)]` applies to
   structs; service payloads are also enums with data
   ([`DatabaseError`](../examples/common/src/database.rs:11),
   [`ContextError`](../examples/common/src/context.rs:27)). The JSON contract is
   the one `serde` defines (`rename_all`, `skip`, `flatten`, `with`, custom
   `Serialize`), and a napi projection would have to reproduce it or accept a
   divergent JS shape.
2. **A napi handle cannot cross the bridge.** `napi::Object` exists only on the
   JavaScript thread and is not `Send`. A `ThreadsafeFunction` can carry a
   **Rust** `#[napi(object)]` struct, whose conversion runs on the JS thread,
   but the bridge is generic over one payload type. Serving every method would
   require an erased converter, a generated enum, or one bridge type per addon —
   a redesign of [`NodeJsBridge`](../ice-rpc-nodejs/src/bridge.rs:119), not a
   simplification.
3. **The response direction needs the method at the JS boundary.** Today
   `emitNodejsEvent` / `resolveNodejsCall` receive a `Value` and the generated
   `serialize_response_from_value` knows the method later, on the dispatch
   thread. A typed napi conversion would have to look up the payload converter
   from the correlation id, store it in the pending table, and convert before
   the event enters the call stream.
4. **It breaks the transport-neutral layer.** [`ice-rpc/src/json.rs`](../ice-rpc/src/json.rs:16)
   deliberately names no transport; the HTTP gateway consumes the same
   [`JsonInvoker`](../ice-rpc/src/json.rs:87). Putting napi on [`#[payload]`](../ice-rpc-macros/src/lib.rs:551)
   would force every shared service crate — including Rust-only providers and
   the HTTP gateway — to depend on napi, or force duplicate mirror types. Cargo
   feature unification makes the feature leak across the workspace.
5. **The measured gain is concentrated in shapes the current contract would have
   to change to exploit.** The largest wins are nested `Vec<u8>` and long lists;
   for those, the right contract-compatible fix is not `#[napi(object)]` but a
   dedicated representation (base64 for nested bytes) or a custom serializer.
   For small payloads, the gain is in the noise of the bridge.

## Alternatives considered

- **Custom `serde::Serializer` writing directly to N-API**, fed by an erased
  `Serialize` value through the `ThreadsafeFunction`. This preserves every serde
  attribute and removes the tree without adding napi to the service crates. It
  is the technically sound long-term option, but it is a substantial piece of
  code (objects, arrays, number handling, error paths) and it still runs on the
  JS thread. Not adopted now.
- **A generated per-addon payload enum** implementing the conversion for every
  method. It removes the tree for the addon's own services, but makes the
  bridge addon-specific and duplicates the JSON contract. Not adopted.
- **Nested `Vec<u8>` as base64** in the generated converters. This is a small,
  contract-compatible change for the shape that the benchmark shows to be the
  worst; it is a candidate for a separate change, not for this decision.
- **`#[napi(object)]` on payloads**: rejected for the reasons above.

## Consequences

- The JS contract is unchanged; the bridge behavior is preserved.
- The `Value::clone` column above now prices the clone the bridge avoids, not
  the clone it performs. It remains the upper bound of what the move-based
  implementation saves.
- The JSON surface is still described as _no JSON text_, not _zero-copy_. The
  intermediate `Value` tree is part of the documented cost model.
- [`json_value_cost.rs`](../ice-rpc/benches/json_value_cost.rs:1) stays in the
  tree as the regression guard and as the evidence for this decision.
- The documentation in the root `Readme.md`, `ice-rpc-nodejs/Readme.md` and
  `ice-rpc/Readme.md` links here instead of repeating the argument.

## When to revisit

Reopen this decision if one of these happens:

- profiling of a real deployment shows the `Value` tree as a significant share
  of the bridge's p95/p99, not just of the conversion micro-benchmark;
- a service that crosses the Node.js bridge carries large `Vec<u8>` fields or
  many-entry lists, which the table above shows to be the expensive shapes;
- a contract-compatible representation for those shapes is agreed (for example
  base64 for nested bytes), with the JS surface and the HTTP gateway updated in
  the same change;
- the custom N-API serializer can be implemented and tested without adding napi
  to `ice-rpc` or `ice-rpc-macros`.

If it is revisited, the preferred path is the custom serializer in
`ice-rpc-nodejs`, **not** `#[napi(object)]` on the shared payload types.
