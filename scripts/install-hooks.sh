#!/usr/bin/env bash
# Point git at the versioned hooks in .githooks/ so pre-commit and pre-push run.
# One-time per clone. Undo with: git config --unset core.hooksPath
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
chmod +x "$root"/.githooks/*
git -C "$root" config core.hooksPath .githooks

echo "Installed git hooks: core.hooksPath -> .githooks"
echo "  pre-commit  format + lint + repo policy + web docs"
echo "  pre-push    clippy + all-target compile + type checking"
