#!/usr/bin/env node
// Report of an interleaved `bench-node-provider-ab.sh` run.
//
// Within one pass the two addons ran back to back, so the paired ratio is free
// of the temporal drift that makes two separate runs incomparable. The table
// shows the median of those ratios, with the medians of each variant for scale.
//
// Usage:
//   node scripts/bench-node-report.mjs target/bench-node
//   A=old B=new node scripts/bench-node-report.mjs target/bench-node
//
// `rps B/A > 1` means the B addon is faster; `p50 B/A < 1` means it is faster
// in latency.

import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

const dir = process.argv[2] ?? "target/bench-node";
const a = process.env.A ?? "old";
const b = process.env.B ?? "new";

const median = (values) => {
  const sorted = [...values].sort((x, y) => x - y);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
};

const runs = new Map();
for (const file of readdirSync(dir)) {
  const match = /^([^-]+)-(.+)-pass(\d+)\.json$/.exec(file);
  if (!match) continue;
  const [, variant, name, pass] = match;
  if (variant !== a && variant !== b) continue;
  const data = JSON.parse(readFileSync(join(dir, file), "utf8"));
  if (!runs.has(name)) runs.set(name, new Map());
  runs.get(name).set(`${pass}:${variant}`, data);
}

const cases = [...runs.keys()].sort();
const width = Math.max(4, ...cases.map((name) => name.length));
console.log(`A = ${a}   B = ${b}`);
console.log(
  `${"case".padEnd(width)}  ${"A rps".padStart(9)}  ${"B rps".padStart(9)}  ` +
    `${"rps B/A".padStart(8)}  ${"A p50µs".padStart(8)}  ${"B p50µs".padStart(8)}  ` +
    `${"p50 B/A".padStart(8)}  ${"A p95µs".padStart(8)}  ${"B p95µs".padStart(8)}  ` +
    `${"p95 B/A".padStart(8)}`,
);

for (const name of cases) {
  const entries = runs.get(name);
  const passes = [...new Set([...entries.keys()].map((key) => key.split(":")[0]))].sort(
    (x, y) => Number(x) - Number(y),
  );

  const aRps = [];
  const bRps = [];
  const rpsRatios = [];
  const aP50 = [];
  const bP50 = [];
  const p50Ratios = [];
  const aP95 = [];
  const bP95 = [];
  const p95Ratios = [];
  let failed = false;

  for (const pass of passes) {
    const left = entries.get(`${pass}:${a}`);
    const right = entries.get(`${pass}:${b}`);
    if (!left || !right) continue;
    aRps.push(left.throughput_rps);
    bRps.push(right.throughput_rps);
    rpsRatios.push(right.throughput_rps / left.throughput_rps);
    aP50.push(left.latency_us.p50);
    bP50.push(right.latency_us.p50);
    p50Ratios.push(right.latency_us.p50 / left.latency_us.p50);
    aP95.push(left.latency_us.p95);
    bP95.push(right.latency_us.p95);
    p95Ratios.push(right.latency_us.p95 / left.latency_us.p95);
    failed ||= left.success_rate < 0.99 || right.success_rate < 0.99;
  }
  if (!rpsRatios.length) continue;

  console.log(
    `${name.padEnd(width)}  ${median(aRps).toFixed(0).padStart(9)}  ` +
      `${median(bRps).toFixed(0).padStart(9)}  ${median(rpsRatios).toFixed(3).padStart(8)}  ` +
      `${median(aP50).toFixed(0).padStart(8)}  ${median(bP50).toFixed(0).padStart(8)}  ` +
      `${median(p50Ratios).toFixed(3).padStart(8)}  ${median(aP95).toFixed(0).padStart(8)}  ` +
      `${median(bP95).toFixed(0).padStart(8)}  ${median(p95Ratios).toFixed(3).padStart(8)}` +
      `${failed ? "   !" : ""}`,
  );
}
