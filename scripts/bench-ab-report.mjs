#!/usr/bin/env node
// Report of an interleaved A/B run (`scripts/bench-ab.sh`).
//
// The statistic that matters is the **median of the paired ratios**: within one
// pass the two variants ran back to back, so their ratio is free of the temporal
// drift that makes two separate runs incomparable (±10-15 % on unchanged code).
// The median of the per-pass throughputs is printed next to it for scale only.
//
// Usage:
//   node scripts/bench-ab-report.mjs target/ab/results-view
//   A=c1 B=c4 node scripts/bench-ab-report.mjs target/ab/results-final

import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

const dir = process.argv[2] ?? "target/ab/results";
const a = process.env.A ?? "c4";
const b = process.env.B ?? "c5";

const median = (values) => {
  const sorted = [...values].sort((x, y) => x - y);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
};

// `<variant>-<case>-pass<N>.json` — the case name itself may contain dashes.
const runs = new Map();
for (const file of readdirSync(dir)) {
  const match = /^([^-]+)-(.+)-pass(\d+)\.json$/.exec(file);
  if (!match) continue;
  const [, variant, name, pass] = match;
  if (variant !== a && variant !== b) continue;
  const { throughput_rps: rps, success_rate: success } = JSON.parse(
    readFileSync(join(dir, file), "utf8"),
  );
  if (!runs.has(name)) runs.set(name, new Map());
  runs.get(name).set(`${pass}:${variant}`, { rps, success });
}

const cases = [...runs.keys()].sort();
const width = Math.max(4, ...cases.map((name) => name.length));
console.log(`A = ${a}   B = ${b}`);
console.log(
  `${"case".padEnd(width)}  ${`${a} rps`.padStart(11)}  ${`${b} rps`.padStart(11)}  ` +
    `${"B/A".padStart(6)}  per-pass ratios (success < 99 % flagged)`,
);

for (const name of cases) {
  const entries = runs.get(name);
  const passes = [...new Set([...entries.keys()].map((key) => key.split(":")[0]))].sort(
    (x, y) => Number(x) - Number(y),
  );

  const ratios = [];
  const aRps = [];
  const bRps = [];
  for (const pass of passes) {
    const left = entries.get(`${pass}:${a}`);
    const right = entries.get(`${pass}:${b}`);
    if (!left || !right) continue;
    aRps.push(left.rps);
    bRps.push(right.rps);
    ratios.push(right.rps / left.rps);
  }
  if (!ratios.length) continue;

  const flag = passes.some((pass) => {
    const left = entries.get(`${pass}:${a}`);
    const right = entries.get(`${pass}:${b}`);
    return (left?.success ?? 1) < 0.99 || (right?.success ?? 1) < 0.99;
  });
  console.log(
    `${name.padEnd(width)}  ${median(aRps).toFixed(0).padStart(11)}  ` +
      `${median(bRps).toFixed(0).padStart(11)}  ${median(ratios).toFixed(3).padStart(6)}  ` +
      `${ratios.map((r) => r.toFixed(3)).join(" ")}${flag ? "   !" : ""}`,
  );
}
