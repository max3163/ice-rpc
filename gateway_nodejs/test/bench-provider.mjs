/**
 * Node.js provider half of the Rust → Node.js bridge benchmark.
 *
 * The Rust consumer (`ice-rpc/examples/benchmark-app.rs`, service
 * `node-get` / `node-set` / `node-list`) drives this process through the
 * `ContextService` declaration. The payload sizes are fixed once at startup, so
 * the measurement prices the bridge and the transport, not JavaScript string
 * construction:
 *
 * - `BENCH_PAYLOAD_SIZE` — bytes returned by `ContextService.get`;
 * - `BENCH_ENTRIES`     — number of entries emitted by `ContextService.list`;
 * - `BENCH_ENTRY_SIZE`  — bytes of each entry's `value`.
 *
 * `GATEWAY_ADDON` selects the addon to load (default: `../index.js`), which is
 * what makes an A/B run possible: point it at a saved `index.js` from another
 * build, while the Rust consumer stays the same.
 *
 * Prints `PROVIDER_READY` on stdout once the gateway has had time to announce
 * its service. The caller waits for that marker before starting the consumer.
 */

import { createRequire } from 'node:module';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const require = createRequire(import.meta.url);

function envNumber(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`${name} must be a non-negative number, got '${raw}'`);
  }
  return Math.floor(value);
}

const addonPath = process.env.GATEWAY_ADDON
  ? resolve(process.env.GATEWAY_ADDON)
  : fileURLToPath(new URL('../index.js', import.meta.url));

const payloadSize = envNumber('BENCH_PAYLOAD_SIZE', 4096);
const entryCount = envNumber('BENCH_ENTRIES', 64);
const entrySize = envNumber('BENCH_ENTRY_SIZE', 256);
const readyDelayMs = envNumber('BENCH_READY_MS', 1500);

const gateway = require(addonPath);

// Built once: every call returns the same value, so the benchmark measures the
// bridge conversion and the IPC path, never the JS allocation of the payload.
const value = 'x'.repeat(payloadSize);
const entries = Array.from({ length: entryCount }, (_, index) => ({
  key: `key-${index}`,
  value: 'y'.repeat(entrySize),
}));

gateway.registerService('ContextService');
// The same workload shapes as `provider-app.rs`: this is what lets the Rust
// consumer run the classic `text`/`fields`/`blob`/`nested`/`mixed` cases
// against either a Rust or a Node.js provider.
gateway.registerService('WorkloadService');

function textLength(value) {
  return typeof value === 'string' ? value.length : 0;
}

function sumLengths(values) {
  return Array.isArray(values)
    ? values.reduce((total, value) => total + textLength(value), 0)
    : 0;
}

/** Single-value `ContextService` methods; `list` is streamed by the dispatcher. */
function handleContext(method) {
  switch (method) {
    case 'get':
      return { data: value };
    case 'set':
      // The request value is the benchmark's payload; the answer is the scalar
      // the declaration expects.
      return { data: true };
    default:
      return { error: 'KeyNotFound' };
  }
}

/** The `WorkloadService` semantics of `WorkloadServiceImpl` in Rust. */
function handleWorkload(method, args) {
  switch (method) {
    case 'echo_text':
      return { data: textLength(args) };
    case 'index_fields':
      return { data: sumLengths(args) };
    case 'upload':
      // A top-level `Vec<u8>` travels as base64; Rust sees the decoded bytes.
      return {
        data: typeof args === 'string' ? Buffer.from(args, 'base64').length : 0,
      };
    case 'search':
      return {
        data:
          textLength(args?.key) +
          sumLengths(args?.tags) +
          sumLengths(args?.filter_fields) +
          sumLengths(args?.filter_values),
      };
    case 'ping':
      return { data: Number(args) >>> 0 };
    case 'fetch': {
      const count = Math.max(0, Number(args) | 0);
      const tags = Array.from(
        { length: count },
        (_, index) => `tag${String(index).padStart(8, '0')}`,
      );
      const filters = Array.from({ length: count }, (_, index) => ({
        field: `field${String(index).padStart(6, '0')}`,
        value: `value${String(index).padStart(6, '0')}`,
      }));
      return { data: { key: 'reply-key', tags, filters } };
    }
    case 'mixed': {
      const scalars = args?.scalars ?? {};
      let total =
        (Number(args?.count) >>> 0) +
        Math.abs(Number(args?.total) | 0) +
        (Number(scalars.count) >>> 0) +
        Math.abs(Number(scalars.total) | 0) +
        (Number(scalars.optional ?? 0) >>> 0) +
        (args?.flag ? 1 : 0) +
        (Number(scalars.ratio) | 0) +
        (scalars.profile === 'Precise' ? 1 : 0);
      if (Array.isArray(args?.codes)) {
        total += args.codes.reduce((sum, code) => sum + (Number(code) >>> 0), 0);
      }
      if (Array.isArray(args?.filters)) {
        total += args.filters.length;
        total += args.filters.reduce(
          (sum, filter) => sum + textLength(filter?.field),
          0,
        );
      }
      return { data: total >>> 0 };
    }
    default:
      return { error: 'KeyNotFound' };
  }
}

gateway.init((err, call) => {
  if (err) {
    console.error(`[bench-provider] dispatch error: ${err.message ?? err}`);
    return;
  }

  if (call.service === 'ContextService' && call.method === 'list') {
    // One `Next` per entry, then a terminal `Complete`: this is the
    // multi-value path, so every event pays its own envelope conversion.
    for (const entry of entries) {
      gateway.emitNodejsEvent(call.correlationId, { data: entry });
    }
    gateway.resolveNodejsCall(call.correlationId, {});
    return;
  }

  const event =
    call.service === 'ContextService'
      ? handleContext(call.method)
      : call.service === 'WorkloadService'
        ? handleWorkload(call.method, call.args)
        : { error: 'KeyNotFound' };
  gateway.resolveNodejsCall(call.correlationId, event);
});

console.error(
  `[bench-provider] addon=${addonPath} payload=${payloadSize}B ` +
    `entries=${entryCount}x${entrySize}B`,
);

// `init` announces the registered services in the background and has no
// completion signal; this delay is the contract with the shell runner.
setTimeout(() => {
  process.stdout.write('PROVIDER_READY\n');
}, readyDelayMs);

let stopping = false;
async function stop() {
  if (stopping) {
    return;
  }
  stopping = true;
  clearInterval(keepAlive);
  // A benchmark helper must never leave a terminal blocked on shutdown: the
  // gateway is released best-effort, but the process exits after a short grace
  // period even if the N-API task does not settle.
  const forceExit = setTimeout(() => process.exit(0), 3000);
  forceExit.unref();
  try {
    await gateway.shutdown();
  } finally {
    clearTimeout(forceExit);
    process.exit(0);
  }
}

process.on('SIGTERM', stop);
process.on('SIGINT', stop);
process.on('unhandledRejection', (error) => {
  console.error(error);
  process.exit(1);
});

// Keep the event loop (and therefore the JS dispatcher) alive.
const keepAlive = setInterval(() => {}, 1 << 30);
