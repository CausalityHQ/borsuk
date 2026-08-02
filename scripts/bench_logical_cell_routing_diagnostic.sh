#!/usr/bin/env bash
set -euo pipefail

# Run one bounded, claim-ineligible remote write-path diagnostic. This protocol
# is deliberately separate from the frozen paired production campaign.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="$ROOT_DIR/docs/research/logical-cell-routing-diagnostic.json"
OUTPUT="${BORSUK_ROUTING_OUTPUT_ROOT:?set BORSUK_ROUTING_OUTPUT_ROOT}"
INDEX_URI="${BORSUK_ROUTING_INDEX_URI:?set BORSUK_ROUTING_INDEX_URI}"
RESULT_URI="${BORSUK_ROUTING_RESULT_URI:-}"
SOURCE_SHA256="${BORSUK_SOURCE_SHA256:?set BORSUK_SOURCE_SHA256}"
ARCHITECTURE="${BORSUK_ARCHITECTURE:?set BORSUK_ARCHITECTURE}"
INSTANCE_TYPE="${BORSUK_INSTANCE_TYPE:?set BORSUK_INSTANCE_TYPE}"
MANIFEST_SHA256="$(sha256sum "$MANIFEST" | awk '{print $1}')"
TIMEOUT_SECONDS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["cell_timeout_seconds"])' "$MANIFEST")"
TARGET_DIR="$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
BINARY="$TARGET_DIR/release/examples/logical_cell_routing_bench"
CELL_OUTPUT="$OUTPUT/cell"
COHORT_SHA256="$(printf '%s' '76412031:2000:8:1:5' | sha256sum | awk '{print $1}')"

sync_results() {
  if [[ -n "$RESULT_URI" ]]; then
    if [[ -n "${AWS_PROFILE:-}" ]]; then
      aws --profile "$AWS_PROFILE" s3 sync --only-show-errors "$OUTPUT" "$RESULT_URI"
    else
      aws s3 sync --only-show-errors "$OUTPUT" "$RESULT_URI"
    fi
  fi
}

failed() {
  status=$?
  if (( status != 0 )); then
    rm -f "$OUTPUT/LOGICAL_CELL_ROUTING_DIAGNOSTIC_COMPLETE"
    printf 'failed\n' > "$OUTPUT/LOGICAL_CELL_ROUTING_DIAGNOSTIC_FAILED"
    sync_results || true
  fi
  exit "$status"
}
trap failed EXIT

[[ ! -e "$OUTPUT" ]] || { echo "refusing to replace output $OUTPUT" >&2; exit 3; }
mkdir -p "$OUTPUT"
cp "$MANIFEST" "$OUTPUT/manifest.json"
printf '%s\n' \
  "source_sha256=$SOURCE_SHA256" \
  "manifest_sha256=$MANIFEST_SHA256" \
  "architecture=$ARCHITECTURE" \
  "instance_type=$INSTANCE_TYPE" \
  "claim_eligible=false" \
  > "$OUTPUT/environment.txt"

cargo build --locked --release -p borsuk --example logical_cell_routing_bench

env \
  BORSUK_ROUTING_SMOKE=0 \
  BORSUK_ROUTING_INDEX_URI="$INDEX_URI" \
  BORSUK_ROUTING_CELL_COUNT=2000 \
  BORSUK_ROUTING_DIMENSIONS=96 \
  "$BINARY" build

/usr/bin/time -v -o "$OUTPUT/cell.resources.txt" \
  timeout --signal=TERM --kill-after=30s "$TIMEOUT_SECONDS" env \
  BORSUK_ROUTING_SMOKE=0 \
  BORSUK_ROUTING_DIAGNOSTIC=1 \
  BORSUK_ROUTING_INDEX_URI="$INDEX_URI" \
  BORSUK_ROUTING_OUTPUT="$CELL_OUTPUT" \
  BORSUK_ROUTING_MODE=flat \
  BORSUK_ROUTING_CELL_COUNT=2000 \
  BORSUK_ROUTING_WRITERS=8 \
  BORSUK_ROUTING_REPETITION=1 \
  BORSUK_ROUTING_OPERATIONS_PER_WRITER=5 \
  BORSUK_ROUTING_WARMUP_OPERATIONS_PER_WRITER=2 \
  BORSUK_ROUTING_DIMENSIONS=96 \
  BORSUK_ROUTING_MASTER_SEED=76412031 \
  BORSUK_SOURCE_SHA256="$SOURCE_SHA256" \
  BORSUK_ROUTING_MANIFEST_SHA256="$MANIFEST_SHA256" \
  BORSUK_ROUTING_COHORT_SHA256="$COHORT_SHA256" \
  BORSUK_ARCHITECTURE="$ARCHITECTURE" \
  BORSUK_INSTANCE_TYPE="$INSTANCE_TYPE" \
  "$BINARY" run

sync_results
printf 'complete\n' > "$OUTPUT/LOGICAL_CELL_ROUTING_DIAGNOSTIC_COMPLETE"
sync_results
trap - EXIT
printf '%s\n' "$OUTPUT"
