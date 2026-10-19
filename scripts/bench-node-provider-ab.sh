#!/usr/bin/env bash
# Interleaved A/B benchmark: Rust consumer → Node.js provider.
#
# Two frozen addons are alternated **within each pass**, and their order flips
# every pass, exactly like `scripts/bench-ab.sh`. That removes the temporal drift
# that makes two sequential runs incomparable on an unchanged machine.
#
# Defaults: `old` points at `target/bench-node/old/index.js`, `new` at
# `target/bench-node/new/index.js`. Both directories must hold the addon's
# `index.js` and its `.node` file.
#
# Usage:
#   PASSES=5 scripts/bench-node-provider-ab.sh
#   SERVICE=node-list BENCH_ENTRIES=1024 PASSES=5 scripts/bench-node-provider-ab.sh
#   SERVICE=node-get BENCH_PAYLOAD_SIZE=1048576 REQUESTS=200 scripts/bench-node-provider-ab.sh
#
# Then:
#   A=old B=new node scripts/bench-node-report.mjs target/bench-node
#
# The Rust consumer is built once and shared by both variants; only the addon
# changes, so the comparison isolates the Node.js bridge.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SERVICE="${SERVICE:-node-get}"
WORKERS="${WORKERS:-1}"
REQUESTS="${REQUESTS:-100}"
WARMUP="${WARMUP:-5}"
PIPELINE="${PIPELINE:-1}"
PASSES="${PASSES:-5}"
OUT_DIR="${OUT_DIR:-$ROOT/target/bench-node}"
WAIT_READY="${WAIT_READY:-10}"

PAYLOAD_SIZE="${PAYLOAD_SIZE:-64}"
BENCH_PAYLOAD_SIZE="${BENCH_PAYLOAD_SIZE:-4096}"
BENCH_ENTRIES="${BENCH_ENTRIES:-64}"
BENCH_ENTRY_SIZE="${BENCH_ENTRY_SIZE:-256}"

A_LABEL="${A_LABEL:-old}"
B_LABEL="${B_LABEL:-new}"
A_ADDON="${A_ADDON:-$ROOT/target/bench-node/old/index.js}"
B_ADDON="${B_ADDON:-$ROOT/target/bench-node/new/index.js}"

for addon in "$A_ADDON" "$B_ADDON"; do
  if [[ ! -f "$addon" ]]; then
    echo "[bench-node-ab] missing addon: $addon" >&2
    echo "[bench-node-ab] save an addon build as target/bench-node/{old,new}/index.js" >&2
    exit 1
  fi
done

case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) EXE=".exe" ;;
  *) EXE="" ;;
esac

mkdir -p "$OUT_DIR"

echo "[bench-node-ab] building the Rust consumer (release, tokio)..."
cargo build -p ice-rpc --release --example benchmark-app --features tokio
BENCH_BIN="$ROOT/target/release/examples/benchmark-app$EXE"

PROVIDER_PID=""

cleanup() {
  if [[ -z "$PROVIDER_PID" ]]; then
    return 0
  fi
  echo "[bench-node-ab] stopping provider (pid $PROVIDER_PID)..."
  # Reap the child with `wait`; the detached killer bounds a shutdown that does
  # not settle. Polling `kill -0` would count a zombie as still running.
  ( sleep 5; kill -9 "$PROVIDER_PID" 2>/dev/null || true ) &
  local killer=$!
  kill "$PROVIDER_PID" 2>/dev/null || true
  wait "$PROVIDER_PID" 2>/dev/null || true
  kill "$killer" 2>/dev/null || true
  wait "$killer" 2>/dev/null || true
  PROVIDER_PID=""
}
trap cleanup EXIT

run_variant() {
  local label="$1" addon="$2" pass="$3"
  local log="$OUT_DIR/$label-provider-pass$pass.log"

  echo "[bench-node-ab] pass $pass/$PASSES — $label"
  GATEWAY_ADDON="$addon" \
  BENCH_PAYLOAD_SIZE="$BENCH_PAYLOAD_SIZE" \
  BENCH_ENTRIES="$BENCH_ENTRIES" \
  BENCH_ENTRY_SIZE="$BENCH_ENTRY_SIZE" \
    node "$ROOT/gateway_nodejs/test/bench-provider.mjs" > "$log" 2>&1 &
  PROVIDER_PID=$!

  local ready=0
  for _ in $(seq 1 "$WAIT_READY"); do
    if grep -q 'PROVIDER_READY' "$log"; then
      ready=1
      break
    fi
    if ! kill -0 "$PROVIDER_PID" 2>/dev/null; then
      echo "[bench-node-ab] $label provider died during startup (log: $log)" >&2
      cat "$log" >&2
      exit 1
    fi
    sleep 1
  done
  if [[ "$ready" -ne 1 ]]; then
    echo "[bench-node-ab] $label provider not ready after ${WAIT_READY}s (log: $log)" >&2
    cat "$log" >&2
    exit 1
  fi

  "$BENCH_BIN" \
    --service "$SERVICE" \
    --workers "$WORKERS" \
    --requests "$REQUESTS" \
    --warmup "$WARMUP" \
    --pipeline "$PIPELINE" \
    --payload-size "$PAYLOAD_SIZE" \
    --json > "$OUT_DIR/$label-$SERVICE-pass$pass.json"

  cleanup
}

for pass in $(seq 1 "$PASSES"); do
  if (( pass % 2 == 1 )); then
    run_variant "$A_LABEL" "$A_ADDON" "$pass"
    run_variant "$B_LABEL" "$B_ADDON" "$pass"
  else
    run_variant "$B_LABEL" "$B_ADDON" "$pass"
    run_variant "$A_LABEL" "$A_ADDON" "$pass"
  fi
done

echo "[bench-node-ab] done. Results in $OUT_DIR"
echo "[bench-node-ab] report: A=$A_LABEL B=$B_LABEL node scripts/bench-node-report.mjs $OUT_DIR"
