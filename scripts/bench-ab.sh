#!/usr/bin/env bash
# Interleaved A/B benchmark: two frozen binary pairs, passes alternated.
#
# The sequential before/after harness cannot resolve the zero-copy work: whole
# runs drift by ±10-15 % on code that did not change at all. This script removes
# the temporal drift by alternating the two variants *within* each pass, and by
# flipping their order every pass (A→B, then B→A) so neither benefits from a warm
# machine.
#
# Prerequisites — both pairs must already be built and frozen, under
# `target/ab/<variant>/`:
#   target/ab/c1/{provider-app,benchmark-app}   (before)
#   target/ab/c2/{provider-app,benchmark-app}   (after)
#
# The variants compared are `A` and `B` (default `c1` and `c2`).
#
# The per-case table and the medians of the paired ratios are read back with
# `scripts/bench-ab-report.mjs <OUT_DIR>` (`A=`/`B=` name the variants, default
# `c4`/`c5`), so the comparison never has to be recomputed by hand.
#
# Usage:
#   PASSES=7 scripts/bench-ab.sh
#   OUT_DIR=target/ab/results A=c1 B=c4 PASSES=5 scripts/bench-ab.sh
#   A=c1 B=c4 node scripts/bench-ab-report.mjs target/ab/results

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

OUT_DIR="${OUT_DIR:-$ROOT/target/ab/results}"
PASSES="${PASSES:-7}"
WORKERS="${WORKERS:-4}"
REQUESTS="${REQUESTS:-2000}"
PIPELINE="${PIPELINE:-4}"
WAIT_READY="${WAIT_READY:-6}"

case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) EXE=".exe" ;;
  *) EXE="" ;;
esac

mkdir -p "$OUT_DIR"

# The shapes that matter: a witness (`db`, unchanged code), the two read-only
# leaf cases (`text`, `blob`) and the three many-field cases (`fields`, `nested`)
# where the number of variable fields, not the byte count, decides the cost.
# One case per **method**, so the comparison reads method by method:
#
#   db          — DatabaseService::get_user_age   (untouched code, the witness)
#   text-*      — WorkloadService::echo_text      (&str)
#   blob-32k    — WorkloadService::upload         (&[u8])
#   fields-*    — WorkloadService::index_fields   (&[&str])
#   nested-64   — WorkloadService::search         (flat: &str + 3 × &[&str])
#   mixed-64    — WorkloadService::mixed          (all owned: scalars, Vec<u32>,
#                                                  Vec<WorkloadFilter>)
CASES=(
  "db --service db"
  "text-4k --service text --payload-size 4096"
  "text-32k --service text --payload-size 32768"
  "blob-32k --service blob --payload-size 32768"
  "fields-64 --service fields --fields 64 --field-size 32"
  "fields-256 --service fields --fields 256 --field-size 32"
  "nested-64 --service nested --fields 64 --field-size 32"
  "mixed-64 --service mixed --fields 64 --field-size 32"
  "ping-64 --service ping --fields 64"
  "reply-64 --service reply --fields 64"
  "reply-256 --service reply --fields 256"
)

run_variant() {
  local variant="$1" pass="$2"
  local provider="$ROOT/target/ab/$variant/provider-app$EXE"
  local bench="$ROOT/target/ab/$variant/benchmark-app$EXE"

  if [[ ! -x "$provider" || ! -x "$bench" ]]; then
    echo "[bench-ab] missing the frozen $variant pair in target/ab/$variant/" >&2
    exit 1
  fi

  echo "[bench-ab] pass $pass/$PASSES — $variant"
  "$provider" > "$OUT_DIR/$variant-provider-pass$pass.log" 2>&1 &
  local pid=$!
  sleep "$WAIT_READY"

  local case_spec name
  for case_spec in "${CASES[@]}"; do
    name="${case_spec%% *}"
    # `CASES_FILTER` narrows the run to a few cases — useful to settle one method
    # with many passes instead of measuring eight of them.
    if [[ -n "${CASES_FILTER:-}" ]] && [[ ! "$name" =~ $CASES_FILTER ]]; then
      continue
    fi
    # shellcheck disable=SC2086
    "$bench" \
      --workers "$WORKERS" \
      --requests "$REQUESTS" \
      --pipeline "$PIPELINE" \
      --min-success-rate 0.95 \
      --json \
      ${case_spec#* } > "$OUT_DIR/$variant-$name-pass$pass.json"
  done

  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

A="${A:-c1}"
B="${B:-c2}"

for pass in $(seq 1 "$PASSES"); do
  if (( pass % 2 == 1 )); then
    run_variant "$A" "$pass"
    run_variant "$B" "$pass"
  else
    run_variant "$B" "$pass"
    run_variant "$A" "$pass"
  fi
done

echo "[bench-ab] done. Results in $OUT_DIR"
