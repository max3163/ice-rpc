#!/usr/bin/env bash
# Removes the iceoryx2 state left behind by another build of the service.
#
# iceoryx2 keeps **two** kinds of state, in two directories, and they have the
# same remedy — which is why this script handles both:
#
#   1. the **root path**: the configuration, the service registry and the shared
#      memory segments themselves;
#   2. the POSIX shared-memory **markers**. `iceoryx2-pal-posix` emulates
#      `shm_open` with memory-mapped files and keeps one small `<segment>.shm_state`
#      file per segment in a directory of its own, taken from the PAL constant
#      `TEMP_DIRECTORY` — hardcoded `C:\Temp` on Windows, `/tmp` elsewhere. That
#      directory is *not* under the root path, so step 1 leaves those markers
#      behind, and nothing removes them when a process is killed instead of
#      exiting (`shm_unlink`, which deletes them, never runs).
#
# They are tiny — 8 bytes each — but they accumulate run after run, and every
# iceoryx2 segment operation enumerates that directory, so a growing pile of them
# also grows the `FindNextFileA ... [ 18 ]` noise iceoryx2 prints on Windows
# (error 18 is "no more files": that is the end of its scan).
#
# This is the *fallback*, not the routine: a provider reaps the dead nodes of
# previous runs when it starts (`transport::cleanup_dead_nodes`), so a machine
# that only ever restarts providers cleans itself. What this script is for is the
# state a dead node does not explain — a service whose recorded configuration
# differs from the requested one, i.e. another build of the service.
#
# Usage:
#   scripts/purge-iceoryx2-root.sh            # dry run: paths, file counts, sizes
#   scripts/purge-iceoryx2-root.sh --yes      # remove them
#
# Before removing, make sure nothing is running: `cargo make probe -- list` lists
# the iceoryx2 nodes that are still alive, with their PID and executable. Removing
# the marker of a live segment is worse than leaving a stale one behind.

set -euo pipefail

case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    ROOT="${APPDATA:-$HOME/AppData/Roaming}/ice-rpc/iceoryx2"
    # Not `%TEMP%`: the PAL hardcodes its own directory and ignores the
    # environment, so pointing this at `%TEMP%` would clean the wrong place.
    SHM_DIR="C:/Temp"
    ;;
  *)
    ROOT="${XDG_DATA_HOME:-$HOME/.local/share}/ice-rpc/iceoryx2"
    SHM_DIR="/tmp"
    ;;
esac

# %APPDATA% carries backslashes; one separator everywhere keeps the printed path
# copy-pasteable and the commands below unambiguous.
ROOT="${ROOT//\\//}"
SHM_DIR="${SHM_DIR//\\//}"

REMOVE=no
if [ "${1:-}" = "--yes" ]; then
  REMOVE=yes
fi

# Counts the `$pattern` files directly inside `$dir`.
count_matching() {
  find "$1" -maxdepth 1 -name "$2" -type f 2>/dev/null | wc -l | tr -d ' '
}

# Total size of the `$pattern` files directly inside `$dir`, or `?`.
size_matching() {
  local total
  total="$(find "$1" -maxdepth 1 -name "$2" -type f -print0 2>/dev/null |
    xargs -0 du -ch 2>/dev/null | tail -1 | cut -f1)"
  echo "${total:-?}"
}

# Reports and, with `--yes`, deletes the `iox2_*` markers of one directory.
#
# Only `iox2_*`: that directory is a shared temporary directory on Unix, so it is
# never emptied, only relieved of what iceoryx2 owns.
purge_markers() {
  local dir="$1"
  local files
  files="$(count_matching "$dir" 'iox2_*')"

  if [ "$files" = "0" ]; then
    echo "[purge] shm state markers: nothing to remove in $dir."
    return
  fi

  echo "[purge] shm state markers: $files file(s), $(size_matching "$dir" 'iox2_*') in $dir."
  if [ "$REMOVE" = "yes" ]; then
    find "$dir" -maxdepth 1 -name 'iox2_*' -type f -delete
    echo "[purge] shm state markers: removed."
  fi
}

echo "[purge] iceoryx2 root path: $ROOT"

if [ -d "$ROOT" ]; then
  echo "[purge] root holds $(find "$ROOT" -type f | wc -l | tr -d ' ') file(s), \
$(du -sh "$ROOT" 2>/dev/null | cut -f1 || echo '?')."
else
  echo "[purge] root does not exist."
fi

purge_markers "$SHM_DIR"

if [ "$REMOVE" != "yes" ]; then
  echo "[purge] dry run. Re-run with --yes to remove them, after making sure no"
  echo "[purge] process still runs the previous build (cargo make probe -- list)."
  exit 0
fi

rm -rf "$ROOT"
echo "[purge] root removed. Rebuild every process of the machine before restarting."
