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
# The demo provider/consumer examples live in the `ice-rpc-rx` crate, but the
# generated iceoryx2 config (`./config/iceoryx2.toml`) is resolved relative to
# the working directory. Run from the workspace root so both processes share the
# same config file, and select the crate explicitly with `-p ice-rpc-rx`.
cd "$ROOT"

WORKERS="${WORKERS:-4}"
REQUESTS="${REQUESTS:-20000}"
OUT_DIR="${OUT_DIR:-$ROOT/target/bench-results}"
WAIT_READY="${WAIT_READY:-6}"
FEATURES="tokio"

case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) EXE=".exe" ;;
  *) EXE="" ;;
esac

PROVIDER_BIN="$ROOT/target/release/examples/provider-app$EXE"
BENCH_BIN="$ROOT/target/release/examples/benchmark-app$EXE"

mkdir -p "$OUT_DIR"

echo "[bench-load] building release examples (features: $FEATURES)..."
cargo build -p ice-rpc-rx --release --example provider-app --example benchmark-app --features "$FEATURES"

echo "[bench-load] starting provider ($PROVIDER_BIN)..."
"$PROVIDER_BIN" > "$OUT_DIR/provider.log" 2>&1 &
PROVIDER_PID=$!

cleanup() {
  echo "[bench-load] stopping provider (pid $PROVIDER_PID)..."
  kill "$PROVIDER_PID" 2>/dev/null || true
  wait "$PROVIDER_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "[bench-load] waiting ${WAIT_READY}s for the provider to be ready..."
sleep "$WAIT_READY"

# A provider that dies during startup leaves the benchmark measuring nothing:
# the calls reach no subscriber and every request times out.
if ! kill -0 "$PROVIDER_PID" 2>/dev/null; then
  echo "[bench-load] ERROR: the provider died during startup (log: $OUT_DIR/provider.log)." >&2
  echo "[bench-load] hint: iceoryx2 refuses a service left behind by a build with a" >&2
  echo "[bench-load]       different wire format, and a half-created service makes it" >&2
  echo "[bench-load]       recurse until the stack overflows. Remove the root path:" >&2
  echo "[bench-load]         Windows: %APPDATA%\\ice-rpc\\iceoryx2" >&2
  echo "[bench-load]         Unix:    \$XDG_DATA_HOME/ice-rpc/iceoryx2 (or ~/.local/share/ice-rpc/iceoryx2)" >&2
  echo "[bench-load]       ...after making sure no process still runs the previous build." >&2
  exit 1
fi
echo "[bench-load] provider is alive (pid $PROVIDER_PID), log: $OUT_DIR/provider.log"

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
