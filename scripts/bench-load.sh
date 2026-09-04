#!/usr/bin/env bash
# ice-rpc end-to-end concurrent load benchmark runner.
#
# Starts the demo provider, then runs the consumer benchmark in several
# concurrency modes (sequential / pipeline / blast) and stores one JSON result
# per mode. The provider is stopped automatically at the end.
#
# Usage:
#   scripts/bench-load.sh
#   WORKERS=8 REQUESTS=500 OUT_DIR=target/bench-results scripts/bench-load.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The demo provider/consumer resolve `examples/config.toml` and the generated
# iceoryx2 config (`./config/iceoryx2.toml`) relative to the crate directory.
cd "$ROOT/ice-rpc"

WORKERS="${WORKERS:-3}"
REQUESTS="${REQUESTS:-2000}"
OUT_DIR="${OUT_DIR:-$ROOT/target/bench-results}"
WAIT_READY="${WAIT_READY:-6}"
FEATURES="tokio,cache"

case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) EXE=".exe" ;;
  *) EXE="" ;;
esac

PROVIDER_BIN="$ROOT/target/release/examples/provider-app$EXE"
BENCH_BIN="$ROOT/target/release/examples/benchmark-app$EXE"

mkdir -p "$OUT_DIR"

echo "[bench-load] building release examples (features: $FEATURES)..."
cargo build --release --example provider-app --example benchmark-app --features "$FEATURES"

echo "[bench-load] starting provider ($PROVIDER_BIN)..."
"$PROVIDER_BIN" &
PROVIDER_PID=$!

cleanup() {
  echo "[bench-load] stopping provider (pid $PROVIDER_PID)..."
  kill "$PROVIDER_PID" 2>/dev/null || true
  wait "$PROVIDER_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "[bench-load] waiting ${WAIT_READY}s for the provider to be ready..."
sleep "$WAIT_READY"

run_mode() {
  local key="$1"
  shift
  local out="$OUT_DIR/$key.json"
  echo "[bench-load] running mode '$key'..."
  "$BENCH_BIN" \
    --workers "$WORKERS" \
    --requests "$REQUESTS" \
    --min-success-rate "${MIN_SUCCESS_RATE:-0.95}" \
    "$@" \
    --json > "$out"
  echo "[bench-load]   -> $out"
}

run_mode sequential --pipeline 1
run_mode pipeline --pipeline 4
run_mode blast --blast

echo "[bench-load] done. Results in $OUT_DIR"
