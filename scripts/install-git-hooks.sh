#!/bin/sh
# Enables the repository's versioned git hooks for this clone.
#
#     scripts/install-git-hooks.sh
#
# It points `core.hooksPath` at the committed `.githooks/` directory, which holds
# a pre-commit hook running `cargo fmt --all`. Idempotent: running it twice is
# harmless.
#
# Undo with:
#     git config --unset core.hooksPath

set -eu

if [ ! -e .git ]; then
    echo "install-git-hooks: run this from the repository root" >&2
    exit 1
fi

git config core.hooksPath .githooks
if [ -f .githooks/pre-commit ]; then
    chmod +x .githooks/pre-commit 2>/dev/null || true
fi
echo "core.hooksPath = $(git config --get core.hooksPath) (pre-commit: cargo fmt --all)"
