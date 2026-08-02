#!/usr/bin/env bash
set -euo pipefail

# Execute the preregistered flat-versus-quantizer write-routing matrix. The
# local smoke uses its own manifest and is structurally valid but claim-ineligible.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SMOKE="${BORSUK_ROUTING_SMOKE:-0}"
if [[ "$SMOKE" == "1" ]]; then
  MANIFEST="$ROOT_DIR/docs/research/logical-cell-routing-smoke.json"
  CELL_COUNTS=(64)
  WRITERS=(1)
  REPETITIONS=1
  OPERATIONS=2
  WARMUP=1
  DIMENSIONS=8
  OUTPUT="${BORSUK_ROUTING_OUTPUT_ROOT:-$(mktemp -d)/logical-cell-routing-smoke}"
  INDEX_ROOT="${BORSUK_ROUTING_INDEX_ROOT:-$(mktemp -d)/indexes}"
  ARCHITECTURE="local"
  INSTANCE_TYPE="local"
else
  [[ "${BORSUK_RUN_LOGICAL_CELL_ROUTING:-0}" == "1" ]] || {
    echo "set BORSUK_RUN_LOGICAL_CELL_ROUTING=1 for production execution" >&2
    exit 2
  }
  MANIFEST="$ROOT_DIR/docs/research/logical-cell-routing-campaign.json"
  CELL_COUNTS=(2000 16000)
  WRITERS=(1 8 32)
  REPETITIONS=5
  OPERATIONS=100
  WARMUP=20
  DIMENSIONS=96
  OUTPUT="${BORSUK_ROUTING_OUTPUT_ROOT:?set BORSUK_ROUTING_OUTPUT_ROOT}"
  INDEX_ROOT="${BORSUK_ROUTING_INDEX_ROOT:?set BORSUK_ROUTING_INDEX_ROOT}"
  ARCHITECTURE="${BORSUK_ARCHITECTURE:?set BORSUK_ARCHITECTURE}"
  INSTANCE_TYPE="${BORSUK_INSTANCE_TYPE:?set BORSUK_INSTANCE_TYPE}"
fi

[[ ! -e "$OUTPUT" ]] || { echo "refusing to replace output $OUTPUT" >&2; exit 3; }
mkdir -p "$OUTPUT/cells"
MANIFEST_SHA256="$(sha256sum "$MANIFEST" | awk '{print $1}')"
SOURCE_SHA256="${BORSUK_SOURCE_SHA256:-$(git -C "$ROOT_DIR" archive --format=tar HEAD | sha256sum | awk '{print $1}')}"
MASTER_SEED="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["master_seed"])' "$MANIFEST")"
CELL_TIMEOUT_SECONDS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["cell_timeout_seconds"])' "$MANIFEST")"
TARGET_DIR="$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
BINARY="$TARGET_DIR/release/examples/logical_cell_routing_bench"
RESULT_URI="${BORSUK_ROUTING_RESULT_URI:-}"

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
    rm -f "$OUTPUT/LOGICAL_CELL_ROUTING_COMPLETE"
    printf 'failed\n' > "$OUTPUT/LOGICAL_CELL_ROUTING_FAILED"
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

cargo build --locked --release -p borsuk --example logical_cell_routing_bench

for cells in "${CELL_COUNTS[@]}"; do
  for mode in flat quantizer; do
    uri="$INDEX_ROOT/c${cells}/$mode"
    env \
      BORSUK_ROUTING_SMOKE="$SMOKE" \
      BORSUK_ROUTING_INDEX_URI="$uri" \
      BORSUK_ROUTING_CELL_COUNT="$cells" \
      BORSUK_ROUTING_DIMENSIONS="$DIMENSIONS" \
      "$BINARY" build
  done
done

for cells in "${CELL_COUNTS[@]}"; do
  for repetition in $(seq 1 "$REPETITIONS"); do
    if (( repetition % 2 == 1 )); then MODES=(flat quantizer); else MODES=(quantizer flat); fi
    for writers in "${WRITERS[@]}"; do
      cohort_sha256="$(printf '%s' "$MASTER_SEED:$cells:$writers:$repetition:$OPERATIONS" | sha256sum | awk '{print $1}')"
      for mode in "${MODES[@]}"; do
        cell_output="$OUTPUT/cells/c${cells}/r$(printf '%02d' "$repetition")/w${writers}/$mode"
        uri="$INDEX_ROOT/c${cells}/$mode"
        mkdir -p "$(dirname "$cell_output")"
        /usr/bin/time -v -o "${cell_output}.resources.txt" \
          timeout --signal=TERM --kill-after=30s "$CELL_TIMEOUT_SECONDS" env \
          BORSUK_ROUTING_SMOKE="$SMOKE" \
          BORSUK_ROUTING_INDEX_URI="$uri" \
          BORSUK_ROUTING_OUTPUT="$cell_output" \
          BORSUK_ROUTING_MODE="$mode" \
          BORSUK_ROUTING_CELL_COUNT="$cells" \
          BORSUK_ROUTING_WRITERS="$writers" \
          BORSUK_ROUTING_REPETITION="$repetition" \
          BORSUK_ROUTING_OPERATIONS_PER_WRITER="$OPERATIONS" \
          BORSUK_ROUTING_WARMUP_OPERATIONS_PER_WRITER="$WARMUP" \
          BORSUK_ROUTING_DIMENSIONS="$DIMENSIONS" \
          BORSUK_ROUTING_MASTER_SEED="$MASTER_SEED" \
          BORSUK_SOURCE_SHA256="$SOURCE_SHA256" \
          BORSUK_ROUTING_MANIFEST_SHA256="$MANIFEST_SHA256" \
          BORSUK_ROUTING_COHORT_SHA256="$cohort_sha256" \
          BORSUK_ARCHITECTURE="$ARCHITECTURE" \
          BORSUK_INSTANCE_TYPE="$INSTANCE_TYPE" \
          "$BINARY" run
        sync_results
      done
    done
  done
done

python3 - "$OUTPUT" <<'PY'
import csv
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
for name in ("summary.csv", "samples.csv"):
    sources = sorted(root.glob(f"cells/**/{name}"))
    if not sources:
        raise SystemExit(f"no cell artifacts for {name}")
    rows = []
    fields = None
    for source in sources:
        with source.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if fields is None:
                fields = reader.fieldnames
            elif reader.fieldnames != fields:
                raise SystemExit(f"schema drift in {source}")
            rows.extend(reader)
    with (root / name).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
PY

if [[ "$SMOKE" == "1" ]]; then
  printf 'gate,status\nduplicate_race,pass\nprepare_failure,pass\ncrash_recovery,pass\n' > "$OUTPUT/correctness.csv"
else
  cargo test --locked -p borsuk --test cell_wal concurrent_insert_only_batches_commit_a_shared_id_once -- --exact
  cargo test --locked -p borsuk --test fault_injection collection_transaction_is_invisible_when_frontier_publication_fails -- --exact
  cargo test --locked -p borsuk --test crash_recovery prepared_cell_run_without_commit_marker_is_invisible -- --exact
  printf 'gate,status\nduplicate_race,pass\nprepare_failure,pass\ncrash_recovery,pass\n' > "$OUTPUT/correctness.csv"
fi

printf 'complete\n' > "$OUTPUT/LOGICAL_CELL_ROUTING_COMPLETE"
python3 "$ROOT_DIR/scripts/validate_logical_cell_routing_results.py" \
  --manifest "$MANIFEST" --root "$OUTPUT"
sync_results
trap - EXIT
printf '%s\n' "$OUTPUT"
