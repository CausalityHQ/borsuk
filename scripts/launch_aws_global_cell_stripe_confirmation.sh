#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export BORSUK_GLOBAL_CELL_STRIPE_CAMPAIGN="$ROOT_DIR/docs/research/global-cell-stripe-confirmation.json"
export BORSUK_GLOBAL_CELL_STRIPE_RUNNER="scripts/bench_global_cell_stripe_confirmation.sh"
export BORSUK_GLOBAL_CELL_STRIPE_NAMESPACE="global-cell-stripe-confirmation"
export BORSUK_GLOBAL_CELL_STRIPE_SESSION_PREFIX="borsuk-global-cell-stripe-confirmation"
export BORSUK_RUN_GLOBAL_CELL_STRIPE_CONFIRMATION=1
exec bash "$ROOT_DIR/scripts/launch_aws_global_cell_stripes.sh"
