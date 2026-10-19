#!/usr/bin/env bash
# End-to-end benchmark of a Rust consumer calling a Node.js provider.
#
# The Node.js half is `gateway_nodejs/test/bench-provider.mjs`, which serves
# `ContextService` with a payload size fixed once at startup. The Rust half is
# `ice-rpc/examples/benchmark-app.rs` (`--service node-get|node-set|node-list`),
# so it reuses the same worker/pipeline/JSON harness as the Rust-to-Rust load
# benchmark.
#
# Usage:
#   scripts/bench-node-provider.sh
#   SERVICE=node-list BENCH_ENTRIES=256 PASSES=5 scripts/bench-node-provider.sh
#   SERVICE=node-get BENCH_PAYLOAD_SIZE=1048576 REQUESTS=200 scripts/bench-node-provider.sh
#
# A/B comparison of two addon builds (the Rust consumer is the current one):
#   LABEL=old GATEWAY_ADDON=/path/to/old/index.js OUT_DIR=target/bench-node scripts/bench-node-provider.sh
#   LABEL=new GATEWAY_ADDON=/path/to/new/index.js OUT_DIR=target/bench-node scripts/bench-node-provider.sh
#   A=old B=new node scripts/bench-ab-report.mjs target/bench-node
#
# The addon must be built first; from `gateway_nodejs/`:
#   npm ci && npm run build        # or `npm run build:debug` for the test profile
#
# Both processes run from the workspace root so they resolve the same
# `./config/iceoryx2.toml`, exactly like the other two-process benchmarks.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SERVICE="${SERVICE:-node-get}"
WORKERS="${WORKERS:-1}"
REQUESTS="${REQUESTS:-200}"
WARMUP="${WARMUP:-10}"
PIPELINE="${PIPELINE:-1}"
PASSES="${PASSES:-3}"
LABEL="${LABEL:-current}"
OUT_DIR="${OUT_DIR:-$ROOT/target/bench-node}"
WAIT_READY="${WAIT_READY:-10}"

# `node-set` sends a payload of this size; `node-get` returns the size set by
# BENCH_PAYLOAD_SIZE and ignores this one.
PAYLOAD_SIZE="${PAYLOAD_SIZE:-64}"
BENCH_PAYLOAD_SIZE="${BENCH_PAYLOAD_SIZE:-4096}"
BENCH_ENTRIES="${BENCH_ENTRIES:-64}"
BENCH_ENTRY_SIZE="${BENCH_ENTRY_SIZE:-256}"

GATEWAY_ADDON="${GATEWAY_ADDON:-$ROOT/gateway_nodejs/index.js}"
if [[ -d "$GATEWAY_ADDON" ]]; then
  GATEWAY_ADDON="$GATEWAY_ADDON/index.js"
fi
if [[ ! -f "$GATEWAY_ADDON" ]]; then
  echo "[bench-node] missing addon: $GATEWAY_ADDON" >&2
  echo "[bench-node] build it with: (cd gateway_nodejs && npm ci && npm run build)" >&2
  exit 1
fi

case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) EXE=".exe" ;;
  *) EXE="" ;;
esac

mkdir -p "$OUT_DIR"
PROVIDER_LOG="$OUT_DIR/$LABEL-provider.log"

echo "[bench-node] building the Rust consumer (release, tokio)..."
cargo build -p ice-rpc --release --example benchmark-app --features tokio
BENCH_BIN="$ROOT/target/release/examples/benchmark-app$EXE"

echo "[bench-node] starting Node.js provider (addon: $GATEWAY_ADDON)..."
GATEWAY_ADDON="$GATEWAY_ADDON" \
BENCH_PAYLOAD_SIZE="$BENCH_PAYLOAD_SIZE" \
BENCH_ENTRIES="$BENCH_ENTRIES" \
BENCH_ENTRY_SIZE="$BENCH_ENTRY_SIZE" \
  node "$ROOT/gateway_nodejs/test/bench-provider.mjs" > "$PROVIDER_LOG" 2>&1 &
PROVIDER_PID=$!

cleanup() {
  if [[ -z "$PROVIDER_PID" ]]; then
    return 0
  fi
  echo "[bench-node] stopping provider (pid $PROVIDER_PID)..."
  # `wait` reaps the child as soon as it exits. A detached killer bounds a
  # `gateway.shutdown()` that does not settle; polling `kill -0` would mistake a
  # zombie for a running process and always wait the full timeout.
  ( sleep 5; kill -9 "$PROVIDER_PID" 2>/dev/null || true ) &
  local killer=$!
  kill "$PROVIDER_PID" 2>/dev/null || true
  wait "$PROVIDER_PID" 2>/dev/null || true
  kill "$killer" 2>/dev/null || true
  wait "$killer" 2>/dev/null || true
  PROVIDER_PID=""
}
trap cleanup EXIT

ready=0
for _ in $(seq 1 "$WAIT_READY"); do
  if grep -q 'PROVIDER_READY' "$PROVIDER_LOG"; then
    ready=1
    break
  fi
  if ! kill -0 "$PROVIDER_PID" 2>/dev/null; then
    echo "[bench-node] provider died during startup (log: $PROVIDER_LOG)" >&2
    cat "$PROVIDER_LOG" >&2
    exit 1
  fi
  sleep 1
done
if [[ "$ready" -ne 1 ]]; then
  echo "[bench-node] provider not ready after ${WAIT_READY}s (log: $PROVIDER_LOG)" >&2
  cat "$PROVIDER_LOG" >&2
  exit 1
fi
echo "[bench-node] provider ready (pid $PROVIDER_PID, log: $PROVIDER_LOG)"

for pass in $(seq 1 "$PASSES"); do
  out="$OUT_DIR/$LABEL-$SERVICE-pass$pass.json"
  echo "[bench-node] pass $pass/$PASSES — $SERVICE — $LABEL"
  "$BENCH_BIN" \
    --service "$SERVICE" \
    --workers "$WORKERS" \
    --requests "$REQUESTS" \
    --warmup "$WARMUP" \
    --pipeline "$PIPELINE" \
    --payload-size "$PAYLOAD_SIZE" \
    --json > "$out"
  echo "[bench-node]   -> $out"
done

echo "[bench-node] done. Results in $OUT_DIR"
echo "[bench-node] report: A=old B=new node scripts/bench-ab-report.mjs $OUT_DIR"
