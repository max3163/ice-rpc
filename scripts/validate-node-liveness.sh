#!/usr/bin/env bash
#
#
# Runs three checks:
#   T2  clean shutdown  -> no spurious `Dead`
#   T1  SIGKILL         -> `Dead` detected, latency measured
#   T3  cost            -> `Node::list` per iteration
#
# Usage: scripts/validate-node-liveness.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="node_liveness_probe"
ICE_ROOT="$ROOT/target/c7-iceoryx2-root"
SCRATCH="$ROOT/target/c7-scratch"

echo "[c7] building $BIN_NAME..."
cargo build -p ice-rpc --example "$BIN_NAME" --manifest-path "$ROOT/Cargo.toml"

BIN="$ROOT/target/debug/examples/$BIN_NAME"

# Isolate from the application's iceoryx2 root: the harness must not see (or
# clean up) the nodes of other runs.
rm -rf "$SCRATCH"
mkdir -p "$SCRATCH"
rm -rf "$ICE_ROOT"
mkdir -p "$ICE_ROOT"
export ICE_RPC_ROOT_PATH="$ICE_ROOT"
cd "$SCRATCH"

PROVIDER_PID=""
cleanup() {
  if [ -n "$PROVIDER_PID" ]; then
    kill -9 "$PROVIDER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

NODE_PID=""
start_provider() {
  local out="$1"; shift
  "$BIN" provider "$@" >"$out" 2>"$out.err" &
  PROVIDER_PID=$!
  for _ in $(seq 1 100); do
    if grep -q '^PID ' "$out" 2>/dev/null; then
      NODE_PID="$(awk '/^PID /{print $2; exit}' "$out")"
      return 0
    fi
    sleep 0.1
  done
  echo "[c7] provider failed to start:"
  cat "$out.err" || true
  return 1
}

echo
echo "=== T2: clean shutdown must NOT be reported as Dead ==="
# The provider owns its Node locally, so exiting really runs the cleanup.
start_provider "$SCRATCH/clean.out" 2
echo "[c7] provider os/iceoryx2 pid = $NODE_PID"
sleep 5
"$BIN" list | tee "$SCRATCH/clean.list"
if grep -q "^$NODE_PID Dead$" "$SCRATCH/clean.list"; then
  echo "[c7] T2 FAIL: clean shutdown reported Dead"
  exit 3
fi
echo "[c7] T2 OK (no spurious Dead)"
PROVIDER_PID=""

echo
echo "=== T1/T3: crash detection and cost ==="
# realistic provider path (Node also held by the global singleton)
start_provider "$SCRATCH/crash.out" 0
echo "[c7] provider os/iceoryx2 pid = $NODE_PID"
sleep 1
echo "[c7] nodes before kill:"
"$BIN" list | tee "$SCRATCH/before.list"
if ! grep -q "^$NODE_PID Alive$" "$SCRATCH/before.list"; then
  echo "[c7] FAIL: provider not seen Alive before kill"
  exit 4
fi

echo
echo "[c7] T3 cost (2000 iterations):"
"$BIN" bench 2000

echo
echo "[c7] sending SIGKILL to $PROVIDER_PID..."
kill -KILL "$PROVIDER_PID"
wait "$PROVIDER_PID" 2>/dev/null || true
PROVIDER_PID=""

"$BIN" watch "$NODE_PID" | tee "$SCRATCH/watch.out"

echo
echo "[c7] done. Raw outputs in $SCRATCH"
