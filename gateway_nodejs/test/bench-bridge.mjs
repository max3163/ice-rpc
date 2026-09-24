/**
 * Latency and throughput of the bridge, measured on a real round trip.
 *
 * `plans/baseline/BASELINE.md` records what the bridge cost before this work
 * (a 30 s freeze per failed call, 0 timer ticks served meanwhile) and the
 * numbers of a served call. This script is how those numbers are reproduced and
 * how a regression would show: it drives `callService` against a provider in
 * another process, sequentially and then concurrently.
 *
 * Usage:
 *   npm run bench
 *   CALLS=500 CONCURRENCY=32 npm run bench
 *
 * Requires a build: `npm run build:debug` (or `npm run build`).
 */

import { createRequire } from 'node:module';
import { performance } from 'node:perf_hooks';

import { STEP_TIMEOUT_MS, startProvider, stopProvider, withTimeout } from './harness.mjs';

const require = createRequire(import.meta.url);
const gateway = require('../index.js');

const CALLS = Number(process.env.CALLS ?? 200);
const CONCURRENCY = Number(process.env.CONCURRENCY ?? 32);

/** Percentile of an already sorted array. */
function percentile(sorted, p) {
  const index = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[index];
}

function stats(durations) {
  const sorted = [...durations].sort((a, b) => a - b);
  const total = sorted.reduce((sum, value) => sum + value, 0);
  return {
    calls: sorted.length,
    min_ms: +sorted[0].toFixed(3),
    p50_ms: +percentile(sorted, 50).toFixed(3),
    p90_ms: +percentile(sorted, 90).toFixed(3),
    p99_ms: +percentile(sorted, 99).toFixed(3),
    max_ms: +sorted[sorted.length - 1].toFixed(3),
    mean_ms: +(total / sorted.length).toFixed(3),
    calls_per_second: +(1000 / (total / sorted.length)).toFixed(1),
  };
}

async function timeOne(key) {
  const started = performance.now();
  const value = await gateway.callService('ContextService', 'get', key);
  if (value === undefined) {
    throw new Error(`no answer for '${key}'`);
  }
  return performance.now() - started;
}

async function sequential() {
  const durations = [];
  for (let index = 0; index < CALLS; index += 1) {
    durations.push(await timeOne('app.name'));
  }
  return durations;
}

async function concurrent() {
  const batches = Math.ceil(CALLS / CONCURRENCY);
  const durations = [];
  for (let batch = 0; batch < batches; batch += 1) {
    const size = Math.min(CONCURRENCY, CALLS - durations.length);
    const started = performance.now();
    await Promise.all(
      Array.from({ length: size }, () => gateway.callService('ContextService', 'get', 'app.name')),
    );
    // Per-call cost of the batch: what one caller experiences under load.
    durations.push(...Array.from({ length: size }, () => (performance.now() - started) / size));
  }
  return durations;
}

async function main() {
  const provider = await startProvider();

  try {
    gateway.init(() => {});
    // One warm-up call: it pays the service discovery, which the baseline
    // measures at ~50 ms and which would otherwise land in the p99.
    await withTimeout(
      gateway.callService('ContextService', 'get', 'app.name'),
      STEP_TIMEOUT_MS,
      'the warm-up call',
    );

    const report = {
      node: process.version,
      gateway: gateway.version(),
      calls_per_batch: CALLS,
      concurrency: CONCURRENCY,
      sequential: stats(await sequential()),
      concurrent: stats(await concurrent()),
    };
    console.log(JSON.stringify(report, null, 2));
  } finally {
    await withTimeout(gateway.shutdown(), STEP_TIMEOUT_MS, 'shutdown').catch(() => {});
    await stopProvider(provider);
  }
}

main().catch((error) => {
  console.error(`fatal: ${error && error.message ? error.message : error}`);
  process.exitCode = 1;
});
