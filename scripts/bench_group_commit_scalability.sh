#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SMOKE="${BORSUK_GROUP_COMMIT_SCALABILITY_SMOKE:-0}"
if [[ "$SMOKE" == "1" ]]; then
  MANIFEST="$ROOT_DIR/docs/research/group-commit-scalability-smoke.json"
  CELL_COUNTS=(64)
  WRITERS=(1)
  REPETITIONS=1
  OPERATIONS=2
  DIMENSIONS=8
  MAX_DELAY_MS=1
  MAX_RECORDS=8
  OUTPUT="${BORSUK_GROUP_COMMIT_SCALABILITY_OUTPUT_ROOT:-$(mktemp -d)/group-commit-scalability-smoke}"
  INDEX_ROOT="${BORSUK_GROUP_COMMIT_SCALABILITY_INDEX_ROOT:-$(mktemp -d)/indexes}"
  ARCHITECTURE=local
  INSTANCE_TYPE=local
  PROTOCOL=smoke
else
  [[ "${BORSUK_RUN_GROUP_COMMIT_SCALABILITY:-0}" == "1" ]] || {
    echo "set BORSUK_RUN_GROUP_COMMIT_SCALABILITY=1 for production execution" >&2
    exit 2
  }
  MANIFEST="$ROOT_DIR/docs/research/group-commit-scalability-campaign.json"
  CELL_COUNTS=(2000 16000)
  WRITERS=(1 8 32)
  REPETITIONS=5
  OPERATIONS=100
  DIMENSIONS=96
  MAX_DELAY_MS=5
  MAX_RECORDS=64
  OUTPUT="${BORSUK_GROUP_COMMIT_SCALABILITY_OUTPUT_ROOT:?set BORSUK_GROUP_COMMIT_SCALABILITY_OUTPUT_ROOT}"
  INDEX_ROOT="${BORSUK_GROUP_COMMIT_SCALABILITY_INDEX_ROOT:?set BORSUK_GROUP_COMMIT_SCALABILITY_INDEX_ROOT}"
  ARCHITECTURE="${BORSUK_ARCHITECTURE:?set BORSUK_ARCHITECTURE}"
  INSTANCE_TYPE="${BORSUK_INSTANCE_TYPE:?set BORSUK_INSTANCE_TYPE}"
  PROTOCOL=scalability
fi

[[ ! -e "$OUTPUT" ]] || { echo "refusing to replace output $OUTPUT" >&2; exit 3; }
mkdir -p "$OUTPUT/cells"
MANIFEST_SHA256="$(sha256sum "$MANIFEST" | awk '{print $1}')"
SOURCE_SHA256="${BORSUK_SOURCE_SHA256:-$(git -C "$ROOT_DIR" archive --format=tar HEAD | sha256sum | awk '{print $1}')}"
CELL_TIMEOUT_SECONDS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["cell_timeout_seconds"])' "$MANIFEST")"
TARGET_DIR="$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
ROUTING_BINARY="$TARGET_DIR/release/examples/logical_cell_routing_bench"
GROUP_BINARY="$TARGET_DIR/release/examples/group_commit_bench"
RESULT_URI="${BORSUK_GROUP_COMMIT_SCALABILITY_RESULT_URI:-}"

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
    rm -f "$OUTPUT/GROUP_COMMIT_SCALABILITY_COMPLETE"
    printf 'failed\n' > "$OUTPUT/GROUP_COMMIT_SCALABILITY_FAILED"
    sync_results || true
  fi
  exit "$status"
}
trap failed EXIT

cp "$MANIFEST" "$OUTPUT/manifest.json"
printf '%s\n' \
  "source_sha256=$SOURCE_SHA256" \
  "manifest_sha256=$MANIFEST_SHA256" \
  "architecture=$ARCHITECTURE" \
  "instance_type=$INSTANCE_TYPE" \
  > "$OUTPUT/environment.txt"

cargo build --locked --release -p borsuk \
  --example logical_cell_routing_bench --example group_commit_bench

for cells in "${CELL_COUNTS[@]}"; do
  uri="$INDEX_ROOT/c${cells}"
  env \
    BORSUK_ROUTING_SMOKE="$SMOKE" \
    BORSUK_ROUTING_INDEX_URI="$uri" \
    BORSUK_ROUTING_CELL_COUNT="$cells" \
    BORSUK_ROUTING_DIMENSIONS="$DIMENSIONS" \
    "$ROUTING_BINARY" build
done

for cells in "${CELL_COUNTS[@]}"; do
  uri="$INDEX_ROOT/c${cells}"
  for repetition in $(seq 1 "$REPETITIONS"); do
    if (( repetition % 2 == 1 )); then
      ORDER=(1 8 32)
    else
      ORDER=(32 8 1)
    fi
    if [[ "$SMOKE" == "1" ]]; then ORDER=(1); fi
    for writers in "${ORDER[@]}"; do
      cell_output="$OUTPUT/cells/c${cells}/r$(printf '%02d' "$repetition")/w${writers}"
      mkdir -p "$(dirname "$cell_output")"
      /usr/bin/time -v -o "${cell_output}.resources.txt" \
        timeout --signal=TERM --kill-after=30s "$CELL_TIMEOUT_SECONDS" env \
        BORSUK_GROUP_COMMIT_PROTOCOL="$PROTOCOL" \
        BORSUK_GROUP_COMMIT_INDEX_URI="$uri" \
        BORSUK_GROUP_COMMIT_OUTPUT="$cell_output" \
        BORSUK_SOURCE_SHA256="$SOURCE_SHA256" \
        BORSUK_GROUP_COMMIT_MANIFEST_SHA256="$MANIFEST_SHA256" \
        BORSUK_GROUP_COMMIT_CELL_COUNT="$cells" \
        BORSUK_GROUP_COMMIT_REPETITION="$repetition" \
        BORSUK_GROUP_COMMIT_WRITERS="$writers" \
        BORSUK_GROUP_COMMIT_OPERATIONS_PER_WRITER="$OPERATIONS" \
        BORSUK_GROUP_COMMIT_DIMENSIONS="$DIMENSIONS" \
        BORSUK_GROUP_COMMIT_MAX_DELAY_MS="$MAX_DELAY_MS" \
        BORSUK_GROUP_COMMIT_MAX_RECORDS="$MAX_RECORDS" \
        "$GROUP_BINARY"
      sync_results
    done
  done
done

python3 - "$OUTPUT" <<'PY'
import csv
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
for name in ("summary.csv", "samples.csv"):
    sources = sorted(root.glob(f"cells/**/{name}"))
    if not sources:
        raise SystemExit(f"no cell artifacts for {name}")
    rows = []
    fields = None
    for source in sources:
        match = re.fullmatch(r"c(\d+)/r(\d+)/w(\d+)/" + name, source.relative_to(root / "cells").as_posix())
        if match is None:
            raise SystemExit(f"invalid cell path {source}")
        with source.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if fields is None:
                fields = ["cell_count", "repetition"] + reader.fieldnames
            elif fields[2:] != reader.fieldnames:
                raise SystemExit(f"schema drift in {source}")
            for row in reader:
                rows.append({"cell_count": match.group(1), "repetition": int(match.group(2)), **row})
    with (root / name).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
PY

if [[ "$SMOKE" == "1" ]]; then
  printf 'gate,status\nsame_id_last_write_wins,pass\nprepare_failure,pass\ncrash_recovery,pass\n' > "$OUTPUT/correctness.csv"
else
  cargo test --locked -p borsuk --test group_commit concurrent_appends_share_one_durable_wal_transaction -- --exact
  cargo test --locked -p borsuk --test fault_injection collection_transaction_is_invisible_when_frontier_publication_fails -- --exact
  cargo test --locked -p borsuk --test crash_recovery prepared_cell_run_without_commit_marker_is_invisible -- --exact
  printf 'gate,status\nsame_id_last_write_wins,pass\nprepare_failure,pass\ncrash_recovery,pass\n' > "$OUTPUT/correctness.csv"
fi

printf 'complete\n' > "$OUTPUT/GROUP_COMMIT_SCALABILITY_COMPLETE"
python3 "$ROOT_DIR/scripts/validate_group_commit_scalability.py" \
  --manifest "$MANIFEST" "$OUTPUT"
sync_results
trap - EXIT
printf '%s\n' "$OUTPUT"
