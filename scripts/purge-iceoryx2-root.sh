#!/usr/bin/env bash
# Removes the iceoryx2 state left behind by another build of the service.
#
# iceoryx2 records the static configuration of every service it creates — the
# user header, the payload alignment, the buffer sizes, the port limits — and
# refuses to open a service whose recorded configuration differs from the
# requested one. A process killed while it held a service leaves the same kind of
# state behind, and sometimes a file whose shared memory is gone, which iceoryx2
# then tries to remove until the stack overflows.
#
# In both cases the fix is the same: remove the root path once no process still
# runs the previous build. The transport reports this situation as
# `RpcError::ProtocolMismatch`, whose message already says so; see
# docs/wire-compat.md for the full procedure.
#
# Usage:
#   scripts/purge-iceoryx2-root.sh            # dry run: path, file count, size
#   scripts/purge-iceoryx2-root.sh --yes      # remove it
#
# Before removing, make sure nothing is running: `cargo make probe -- list` lists
# the iceoryx2 nodes that are still alive, with their PID and executable.

set -euo pipefail

case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) ROOT="${APPDATA:-$HOME/AppData/Roaming}/ice-rpc/iceoryx2" ;;
  *) ROOT="${XDG_DATA_HOME:-$HOME/.local/share}/ice-rpc/iceoryx2" ;;
esac

# %APPDATA% carries backslashes; one separator everywhere keeps the printed path
# copy-pasteable and the commands below unambiguous.
ROOT="${ROOT//\\//}"

echo "[purge] iceoryx2 root path: $ROOT"

if [ ! -d "$ROOT" ]; then
  echo "[purge] nothing to remove: the path does not exist."
  exit 0
fi

FILES="$(find "$ROOT" -type f | wc -l | tr -d ' ')"
SIZE="$(du -sh "$ROOT" 2>/dev/null | cut -f1 || echo '?')"
echo "[purge] holds $FILES file(s), $SIZE."

if [ "${1:-}" != "--yes" ]; then
  echo "[purge] dry run. Re-run with --yes to remove it, after making sure no"
  echo "[purge] process still runs the previous build (cargo make probe -- list)."
  exit 0
fi

rm -rf "$ROOT"
echo "[purge] removed. Rebuild every process of the machine before restarting."
