#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="$ROOT_DIR/docs/research/group-commit-diagnostic.json"
OUTPUT="${BORSUK_GROUP_COMMIT_OUTPUT_ROOT:?set BORSUK_GROUP_COMMIT_OUTPUT_ROOT}"
INDEX_URI="${BORSUK_GROUP_COMMIT_INDEX_URI:?set BORSUK_GROUP_COMMIT_INDEX_URI}"
RESULT_URI="${BORSUK_GROUP_COMMIT_RESULT_URI:-}"
SOURCE_SHA256="${BORSUK_SOURCE_SHA256:?set BORSUK_SOURCE_SHA256}"
MANIFEST_SHA256="$(sha256sum "$MANIFEST" | awk '{print $1}')"
TARGET_DIR="$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
ROUTING_BINARY="$TARGET_DIR/release/examples/logical_cell_routing_bench"
GROUP_BINARY="$TARGET_DIR/release/examples/group_commit_bench"

sync_results() {
  if [[ -n "$RESULT_URI" ]]; then
    aws s3 sync --only-show-errors "$OUTPUT" "$RESULT_URI"
  fi
}

failed() {
  status=$?
  if (( status != 0 )); then
    rm -f "$OUTPUT/GROUP_COMMIT_DIAGNOSTIC_COMPLETE"
    printf 'failed\n' > "$OUTPUT/GROUP_COMMIT_DIAGNOSTIC_FAILED"
    sync_results || true
  fi
  exit "$status"
}
trap failed EXIT

[[ ! -e "$OUTPUT" ]] || { echo "refusing to replace output $OUTPUT" >&2; exit 3; }
mkdir -p "$OUTPUT"
cp "$MANIFEST" "$OUTPUT/manifest.json"
printf '%s\n' "source_sha256=$SOURCE_SHA256" "manifest_sha256=$MANIFEST_SHA256" \
  > "$OUTPUT/environment.txt"

cargo build --locked --release -p borsuk \
  --example logical_cell_routing_bench --example group_commit_bench

env BORSUK_ROUTING_SMOKE=0 BORSUK_ROUTING_INDEX_URI="$INDEX_URI" \
  BORSUK_ROUTING_CELL_COUNT=2000 BORSUK_ROUTING_DIMENSIONS=96 \
  "$ROUTING_BINARY" build

/usr/bin/time -v -o "$OUTPUT/resources.txt" \
  timeout --signal=TERM --kill-after=30s 900 env \
  BORSUK_GROUP_COMMIT_INDEX_URI="$INDEX_URI" \
  BORSUK_GROUP_COMMIT_OUTPUT="$OUTPUT/cell" \
  BORSUK_SOURCE_SHA256="$SOURCE_SHA256" \
  BORSUK_GROUP_COMMIT_MANIFEST_SHA256="$MANIFEST_SHA256" \
  BORSUK_GROUP_COMMIT_WRITERS=8 \
  BORSUK_GROUP_COMMIT_OPERATIONS_PER_WRITER=20 \
  BORSUK_GROUP_COMMIT_DIMENSIONS=96 \
  BORSUK_GROUP_COMMIT_MAX_DELAY_MS=5 \
  BORSUK_GROUP_COMMIT_MAX_RECORDS=64 \
  "$GROUP_BINARY"

printf 'complete\n' > "$OUTPUT/GROUP_COMMIT_DIAGNOSTIC_COMPLETE"
sync_results
trap - EXIT
printf '%s\n' "$OUTPUT"
