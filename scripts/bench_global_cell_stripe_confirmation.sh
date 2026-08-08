#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ "${BORSUK_RUN_GLOBAL_CELL_STRIPE_CONFIRMATION:-0}" == "1" ]] || {
  echo "set BORSUK_RUN_GLOBAL_CELL_STRIPE_CONFIRMATION=1 for production execution" >&2
  exit 2
}

export BORSUK_GLOBAL_CELL_STRIPE_MANIFEST="$ROOT_DIR/docs/research/global-cell-stripe-confirmation.json"
export BORSUK_GLOBAL_CELL_STRIPE_PROTOCOL="read-stripe-confirmation"
export BORSUK_GLOBAL_CELL_STRIPE_COMPLETE_MARKER="GLOBAL_CELL_STRIPE_CONFIRMATION_COMPLETE"
export BORSUK_GLOBAL_CELL_STRIPE_FAILED_MARKER="GLOBAL_CELL_STRIPE_CONFIRMATION_FAILED"
export BORSUK_RUN_GLOBAL_CELL_STRIPES=1
exec bash "$ROOT_DIR/scripts/bench_global_cell_stripes.sh"
