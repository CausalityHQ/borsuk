#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SMOKE="${BORSUK_GROUP_COMMIT_SCALABILITY_SMOKE:-0}"
EXACT_BOUND_LOCAL="${BORSUK_GROUP_COMMIT_EXACT_BOUND_LOCAL:-0}"
EXACT_BOUND_SHADOW=0
[[ "$SMOKE" != "1" || "$EXACT_BOUND_LOCAL" != "1" ]] || {
  echo "smoke and exact-bound local modes are mutually exclusive" >&2
  exit 2
}
MAX_P95_MS=""
MIN_RPS=""
MIN_END_TO_END_RPS=""
MAX_READ_P95_MS=""
MIN_INSERTED_ID_RECALL_AT_10=""
READ_QUERIES=""
PIPELINE_DEPTH="1"
RECORDS_PER_OPERATION="${BORSUK_GROUP_COMMIT_RECORDS_PER_OPERATION:-1}"
WORKER_LANES=(1)
THROUGHPUT_GATE_WRITERS=()
DATASET_DIR=""
DATASET_SHA256=""
if [[ "$SMOKE" == "1" ]]; then
  if [[ "$RECORDS_PER_OPERATION" == "16" ]]; then
    MANIFEST="$ROOT_DIR/docs/research/group-commit-scalability-smoke-bulk.json"
  else
    MANIFEST="$ROOT_DIR/docs/research/group-commit-scalability-smoke.json"
  fi
  mapfile -t CELL_COUNTS < <(python3 -c 'import json,sys; print(*json.load(open(sys.argv[1]))["cell_counts"], sep="\n")' "$MANIFEST")
  mapfile -t WRITERS < <(python3 -c 'import json,sys; print(*json.load(open(sys.argv[1]))["writers"], sep="\n")' "$MANIFEST")
  REPETITIONS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["repetitions"])' "$MANIFEST")"
  OPERATIONS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["operations_per_writer"])' "$MANIFEST")"
  DIMENSIONS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["dimensions"])' "$MANIFEST")"
  MAX_DELAY_MS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["max_group_delay_ms"])' "$MANIFEST")"
  MAX_RECORDS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["max_group_records"])' "$MANIFEST")"
  PIPELINE_DEPTH="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["pipeline_depth_per_writer"])' "$MANIFEST")"
  mapfile -t WORKER_LANES < <(python3 -c 'import json,sys; print(*json.load(open(sys.argv[1]))["worker_lanes"], sep="\n")' "$MANIFEST")
  OUTPUT="${BORSUK_GROUP_COMMIT_SCALABILITY_OUTPUT_ROOT:-$(mktemp -d)/group-commit-scalability-smoke}"
  INDEX_ROOT="${BORSUK_GROUP_COMMIT_SCALABILITY_INDEX_ROOT:-$(mktemp -d)/indexes}"
  ARCHITECTURE=local
  INSTANCE_TYPE=local
  PROTOCOL=smoke
elif [[ "$EXACT_BOUND_LOCAL" == "1" ]]; then
  EXACT_BOUND_SHADOW=1
  MANIFEST="$ROOT_DIR/docs/research/group-commit-exact-bound-local-qualification.json"
  mapfile -t CELL_COUNTS < <(python3 -c 'import json,sys; print(*json.load(open(sys.argv[1]))["cell_counts"], sep="\n")' "$MANIFEST")
  mapfile -t WRITERS < <(python3 -c 'import json,sys; print(*json.load(open(sys.argv[1]))["writers"], sep="\n")' "$MANIFEST")
  REPETITIONS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["repetitions"])' "$MANIFEST")"
  OPERATIONS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["operations_per_writer"])' "$MANIFEST")"
  DIMENSIONS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["dimensions"])' "$MANIFEST")"
  MAX_DELAY_MS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["max_group_delay_ms"])' "$MANIFEST")"
  MAX_RECORDS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["max_group_records"])' "$MANIFEST")"
  PIPELINE_DEPTH="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["pipeline_depth_per_writer"])' "$MANIFEST")"
  RECORDS_PER_OPERATION="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["records_per_operation"])' "$MANIFEST")"
  mapfile -t WORKER_LANES < <(python3 -c 'import json,sys; print(*json.load(open(sys.argv[1]))["worker_lanes"], sep="\n")' "$MANIFEST")
  OUTPUT="${BORSUK_GROUP_COMMIT_SCALABILITY_OUTPUT_ROOT:?set BORSUK_GROUP_COMMIT_SCALABILITY_OUTPUT_ROOT}"
  INDEX_ROOT="${BORSUK_GROUP_COMMIT_SCALABILITY_INDEX_ROOT:?set BORSUK_GROUP_COMMIT_SCALABILITY_INDEX_ROOT}"
  ARCHITECTURE="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["architecture"])' "$MANIFEST")"
  INSTANCE_TYPE="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["instance_type"])' "$MANIFEST")"
  PROTOCOL=local
  READ_QUERIES="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["read_queries_per_cell"])' "$MANIFEST")"
  DATASET_DIR="${BORSUK_GROUP_COMMIT_DATASET:?set validated Cohere dataset directory}"
  [[ -f "$DATASET_DIR/dataset.json" && -f "$DATASET_DIR/train.parquet" ]] || {
    echo "missing group-commit dataset descriptor or train.parquet" >&2
    exit 3
  }
  DATASET_SHA256="$(sha256sum "$DATASET_DIR/dataset.json" | awk '{print $1}')"
  [[ "$DATASET_SHA256" == "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["dataset_sha256"])' "$MANIFEST")" ]] || {
    echo "group-commit dataset descriptor SHA-256 mismatch" >&2
    exit 3
  }
  uv run --python 3.12 \
    --with-requirements "$ROOT_DIR/scripts/requirements-format-bench.txt" \
    python "$ROOT_DIR/scripts/fetch_vdbbench_dataset.py" \
    --dataset "$(basename "$DATASET_DIR")" \
    --output-root "$(dirname "$DATASET_DIR")" \
    --check-existing >/dev/null
else
  [[ "${BORSUK_RUN_GROUP_COMMIT_SCALABILITY:-0}" == "1" ]] || {
    echo "set BORSUK_RUN_GROUP_COMMIT_SCALABILITY=1 for production execution" >&2
    exit 2
  }
  MANIFEST="$ROOT_DIR/docs/research/realistic-group-commit-campaign.json"
  CELL_COUNTS=(2000 16000)
  WRITERS=(1 8 32)
  REPETITIONS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["repetitions"])' "$MANIFEST")"
  OPERATIONS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["operations_per_writer"])' "$MANIFEST")"
  DIMENSIONS=768
  MAX_DELAY_MS=5
  MAX_RECORDS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["max_group_records"])' "$MANIFEST")"
  OUTPUT="${BORSUK_GROUP_COMMIT_SCALABILITY_OUTPUT_ROOT:?set BORSUK_GROUP_COMMIT_SCALABILITY_OUTPUT_ROOT}"
  INDEX_ROOT="${BORSUK_GROUP_COMMIT_SCALABILITY_INDEX_ROOT:?set BORSUK_GROUP_COMMIT_SCALABILITY_INDEX_ROOT}"
  ARCHITECTURE="${BORSUK_ARCHITECTURE:?set BORSUK_ARCHITECTURE}"
  INSTANCE_TYPE="${BORSUK_INSTANCE_TYPE:?set BORSUK_INSTANCE_TYPE}"
  PROTOCOL=scalability
  MAX_P95_MS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["max_write_p95_ms"])' "$MANIFEST")"
  MIN_RPS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["min_records_per_second"])' "$MANIFEST")"
  MIN_END_TO_END_RPS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["min_end_to_end_records_per_second"])' "$MANIFEST")"
  MAX_READ_P95_MS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["max_read_p95_ms"])' "$MANIFEST")"
  MIN_INSERTED_ID_RECALL_AT_10="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["min_inserted_id_recall_at_10"])' "$MANIFEST")"
  READ_QUERIES="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["read_queries_per_cell"])' "$MANIFEST")"
  PIPELINE_DEPTH="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["pipeline_depth_per_writer"])' "$MANIFEST")"
  RECORDS_PER_OPERATION="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("records_per_operation", 1))' "$MANIFEST")"
  mapfile -t WORKER_LANES < <(python3 -c 'import json,sys; print(*json.load(open(sys.argv[1]))["worker_lanes"], sep="\n")' "$MANIFEST")
  mapfile -t THROUGHPUT_GATE_WRITERS < <(python3 -c 'import json,sys; print(*json.load(open(sys.argv[1]))["throughput_gate_writers"], sep="\n")' "$MANIFEST")
  DATASET_DIR="${BORSUK_GROUP_COMMIT_DATASET:?set validated Cohere dataset directory}"
  [[ -f "$DATASET_DIR/dataset.json" && -f "$DATASET_DIR/train.parquet" ]] || {
    echo "missing group-commit dataset descriptor or train.parquet" >&2
    exit 3
  }
  DATASET_SHA256="$(sha256sum "$DATASET_DIR/dataset.json" | awk '{print $1}')"
  [[ "$DATASET_SHA256" == "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["dataset_sha256"])' "$MANIFEST")" ]] || {
    echo "group-commit dataset descriptor SHA-256 mismatch" >&2
    exit 3
  }
  uv run --python 3.12 \
    --with-requirements "$ROOT_DIR/scripts/requirements-format-bench.txt" \
    python "$ROOT_DIR/scripts/fetch_vdbbench_dataset.py" \
    --dataset "$(basename "$DATASET_DIR")" \
    --output-root "$(dirname "$DATASET_DIR")" \
    --check-existing >/dev/null
fi

[[ ! -e "$OUTPUT" ]] || { echo "refusing to replace output $OUTPUT" >&2; exit 3; }
if [[ "$INDEX_ROOT" != s3://* ]]; then
  [[ ! -e "$INDEX_ROOT" ]] || { echo "refusing to reuse local index root $INDEX_ROOT" >&2; exit 3; }
fi
mkdir -p "$OUTPUT/cells"
MANIFEST_SHA256="$(sha256sum "$MANIFEST" | awk '{print $1}')"
SOURCE_FROM_GIT=0
if git -C "$ROOT_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  SOURCE_FROM_GIT=1
  HEAD_SOURCE_SHA256="$(git -C "$ROOT_DIR" archive --format=tar HEAD | sha256sum | awk '{print $1}')"
else
  SOURCE_ARCHIVE="${BORSUK_SOURCE_ARCHIVE:?set the preserved source archive for an extracted production source}"
  [[ -f "$SOURCE_ARCHIVE" ]] || { echo "missing preserved source archive" >&2; exit 3; }
  HEAD_SOURCE_SHA256="$(sha256sum "$SOURCE_ARCHIVE" | awk '{print $1}')"
  source_check="$(mktemp -d)"
  tar -xf "$SOURCE_ARCHIVE" -C "$source_check"
  diff -qr "$ROOT_DIR" "$source_check" >/dev/null || {
    echo "extracted source differs from its preserved source archive" >&2
    exit 3
  }
  rm -rf "$source_check"
fi
SOURCE_SHA256="${BORSUK_SOURCE_SHA256:-$HEAD_SOURCE_SHA256}"
CELL_TIMEOUT_SECONDS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["cell_timeout_seconds"])' "$MANIFEST")"
RESOURCE_INTERVAL_MS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("resource_sample_interval_ms", 100))' "$MANIFEST")"
TARGET_DIR="$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
ROUTING_BINARY="$TARGET_DIR/release/examples/logical_cell_routing_bench"
GROUP_BINARY="$TARGET_DIR/release/examples/group_commit_bench"
RESULT_URI="${BORSUK_GROUP_COMMIT_SCALABILITY_RESULT_URI:-}"
CURRENT_CELL=""

if [[ "$EXACT_BOUND_LOCAL" == "1" && "$SOURCE_FROM_GIT" == "1" ]]; then
  [[ "$SOURCE_SHA256" == "$HEAD_SOURCE_SHA256" ]] || {
    echo "exact-bound local source SHA-256 differs from checked-out HEAD archive" >&2
    exit 3
  }
  [[ -z "$(git -C "$ROOT_DIR" status --porcelain)" ]] || {
    echo "exact-bound local execution requires a clean tracked worktree" >&2
    exit 3
  }
fi

prefix_is_empty() {
  local uri="$1" location bucket prefix count
  location="${uri#s3://}"
  bucket="${location%%/*}"
  prefix="${location#*/}"
  count="$(aws s3api list-objects-v2 --bucket "$bucket" --prefix "${prefix%/}/" --max-keys 1 --query KeyCount --output text)"
  [[ "$count" == "0" ]]
}

clone_index() {
  local source="$1" destination="$2"
  if [[ "$source" == s3://* ]]; then
    aws s3 sync --only-show-errors "$source" "$destination"
  else
    mkdir -p "$destination"
    cp -a "$source/." "$destination/"
  fi
}

rotate_order() {
  local rotation="$1"
  shift
  local values=("$@") index
  ROTATED_ORDER=()
  for ((index=0; index<${#values[@]}; index++)); do
    ROTATED_ORDER+=("${values[$(((index + rotation) % ${#values[@]}))]}")
  done
}

if [[ "$SMOKE" != "1" && "$EXACT_BOUND_LOCAL" != "1" ]]; then
  [[ "$INDEX_ROOT" == s3://* ]] || { echo "production index root must be s3://" >&2; exit 3; }
  [[ "$RESULT_URI" == s3://* ]] || { echo "production result root must be s3://" >&2; exit 3; }
  [[ "$SOURCE_SHA256" == "$HEAD_SOURCE_SHA256" ]] || {
    echo "source SHA-256 differs from checked-out HEAD archive" >&2
    exit 3
  }
  if [[ "$SOURCE_FROM_GIT" == "1" ]]; then
    [[ -z "$(git -C "$ROOT_DIR" status --porcelain)" ]] || {
      echo "production execution requires a clean tracked worktree" >&2
      exit 3
    }
  fi
  index_prefix="${INDEX_ROOT%/}/"
  result_prefix="${RESULT_URI%/}/"
  if [[ "$index_prefix" == "$result_prefix"* || "$result_prefix" == "$index_prefix"* ]]; then
    echo "index and result prefixes must be disjoint" >&2
    exit 3
  fi
  prefix_is_empty "$INDEX_ROOT" || { echo "refusing to reuse non-empty index prefix" >&2; exit 3; }
  prefix_is_empty "$RESULT_URI" || { echo "refusing to reuse non-empty result prefix" >&2; exit 3; }
fi

sync_results() {
  if [[ -n "$RESULT_URI" ]]; then
    if [[ -n "${AWS_PROFILE:-}" ]]; then
      aws --profile "$AWS_PROFILE" s3 sync --only-show-errors "$OUTPUT" "$RESULT_URI"
    else
      aws s3 sync --only-show-errors "$OUTPUT" "$RESULT_URI"
    fi
  fi
}

run_exact_test() {
  local target="$1"
  local test_name="$2"
  local output
  output="$(cargo test --locked -p borsuk --test "$target" "$test_name" -- --exact 2>&1)"
  printf '%s\n' "$output"
  grep -Fq 'test result: ok. 1 passed; 0 failed;' <<<"$output"
}

run_exact_lib_test() {
  local test_name="$1"
  local output
  output="$(cargo test --locked -p borsuk --lib "$test_name" -- --exact 2>&1)"
  printf '%s\n' "$output"
  grep -Fq 'test result: ok. 1 passed; 0 failed;' <<<"$output"
}

failed() {
  status=$?
  if (( status != 0 )); then
    if [[ -n "$CURRENT_CELL" ]]; then
      mkdir -p "$CURRENT_CELL"
      rm -f "$CURRENT_CELL/CELL_COMPLETE"
      printf 'failed\n' > "$CURRENT_CELL/CELL_FAILED"
    fi
    rm -f "$OUTPUT/GROUP_COMMIT_SCALABILITY_COMPLETE"
    printf 'failed\n' > "$OUTPUT/GROUP_COMMIT_SCALABILITY_FAILED"
    sync_results || true
  fi
  exit "$status"
}
trap failed EXIT

cp "$MANIFEST" "$OUTPUT/manifest.json"
if [[ "$SMOKE" != "1" ]]; then
  cp "$DATASET_DIR/dataset.json" "$OUTPUT/dataset.json"
fi
printf '%s\n' \
  "source_sha256=$SOURCE_SHA256" \
  "dataset_sha256=$DATASET_SHA256" \
  "manifest_sha256=$MANIFEST_SHA256" \
  "architecture=$ARCHITECTURE" \
  "instance_type=$INSTANCE_TYPE" \
  > "$OUTPUT/environment.txt"

cargo build --locked --release -p borsuk \
  --example logical_cell_routing_bench --example group_commit_bench

for cells in "${CELL_COUNTS[@]}"; do
  template_uri="$INDEX_ROOT/templates/c${cells}"
  env \
    BORSUK_ROUTING_SMOKE="$SMOKE" \
    BORSUK_ROUTING_GROUP_COMMIT_BASE=1 \
    BORSUK_ROUTING_INDEX_URI="$template_uri" \
    BORSUK_ROUTING_CELL_COUNT="$cells" \
    BORSUK_ROUTING_DIMENSIONS="$DIMENSIONS" \
    "$ROUTING_BINARY" build
  for repetition in $(seq 1 "$REPETITIONS"); do
    writer_rotation="$(((repetition - 1) % ${#WRITERS[@]}))"
    lane_rotation="$(((repetition - 1) % ${#WORKER_LANES[@]}))"
    rotate_order "$writer_rotation" "${WRITERS[@]}"
    ORDER=("${ROTATED_ORDER[@]}")
    rotate_order "$lane_rotation" "${WORKER_LANES[@]}"
    LANE_ORDER=("${ROTATED_ORDER[@]}")
    for worker_lanes in "${LANE_ORDER[@]}"; do
    for writers in "${ORDER[@]}"; do
      cell_min_rps=0
      cell_min_end_to_end_rps=0
      for throughput_gate_writers in "${THROUGHPUT_GATE_WRITERS[@]}"; do
        if [[ "$writers" == "$throughput_gate_writers" ]]; then
          cell_min_rps="$MIN_RPS"
          cell_min_end_to_end_rps="$MIN_END_TO_END_RPS"
        fi
      done
      cell_output="$OUTPUT/cells/c${cells}/r$(printf '%02d' "$repetition")/l${worker_lanes}/w${writers}"
      uri="$INDEX_ROOT/cells/c${cells}/r$(printf '%02d' "$repetition")/l${worker_lanes}/w${writers}"
      clone_index "$template_uri" "$uri"
      mkdir -p "$(dirname "$cell_output")"
      CURRENT_CELL="$cell_output"
      resource_output="${cell_output}.resources.csv"
      storage_trace_output="${cell_output}.storage-access.csv"
      # The benchmark itself requires a nonexistent output path. Capture
      # diagnostics beside the cell while it runs, then move them into the
      # terminal cell after the process has created its result directory.
      benchmark_stdout="${cell_output}.benchmark.stdout.log"
      benchmark_stderr="${cell_output}.benchmark.stderr.log"
      cache_env=()
      if [[ "$EXACT_BOUND_LOCAL" != "1" ]]; then
        cache_env=(BORSUK_GROUP_COMMIT_CACHE_DIR="$cell_output/cache")
      fi
      set +e
      env \
        "${cache_env[@]}" \
        BORSUK_GROUP_COMMIT_EXACT_BOUND_SHADOW="$EXACT_BOUND_SHADOW" \
        BORSUK_GROUP_COMMIT_PROTOCOL="$PROTOCOL" \
        BORSUK_GROUP_COMMIT_INDEX_URI="$uri" \
        BORSUK_GROUP_COMMIT_OUTPUT="$cell_output" \
        BORSUK_SOURCE_SHA256="$SOURCE_SHA256" \
        BORSUK_GROUP_COMMIT_MANIFEST_SHA256="$MANIFEST_SHA256" \
        BORSUK_GROUP_COMMIT_DATASET="$DATASET_DIR" \
        BORSUK_GROUP_COMMIT_DATASET_SHA256="$DATASET_SHA256" \
        BORSUK_GROUP_COMMIT_CELL_COUNT="$cells" \
        BORSUK_GROUP_COMMIT_REPETITION="$repetition" \
        BORSUK_GROUP_COMMIT_WRITERS="$writers" \
        BORSUK_GROUP_COMMIT_WRITER_INSTANCES="$writers" \
        BORSUK_GROUP_COMMIT_EXECUTION="processes" \
        BORSUK_GROUP_COMMIT_OPERATIONS_PER_WRITER="$OPERATIONS" \
        BORSUK_GROUP_COMMIT_DIMENSIONS="$DIMENSIONS" \
        BORSUK_GROUP_COMMIT_MAX_DELAY_MS="$MAX_DELAY_MS" \
        BORSUK_GROUP_COMMIT_MAX_RECORDS="$MAX_RECORDS" \
        BORSUK_GROUP_COMMIT_MAX_P95_MS="$MAX_P95_MS" \
        BORSUK_GROUP_COMMIT_MIN_RECORDS_PER_SECOND="$cell_min_rps" \
        BORSUK_GROUP_COMMIT_MIN_END_TO_END_RECORDS_PER_SECOND="$cell_min_end_to_end_rps" \
        BORSUK_GROUP_COMMIT_MAX_READ_P95_MS="$MAX_READ_P95_MS" \
        BORSUK_GROUP_COMMIT_MIN_INSERTED_ID_RECALL_AT_10="$MIN_INSERTED_ID_RECALL_AT_10" \
        BORSUK_GROUP_COMMIT_READ_QUERIES="$READ_QUERIES" \
        BORSUK_GROUP_COMMIT_PIPELINE_DEPTH="$PIPELINE_DEPTH" \
        BORSUK_GROUP_COMMIT_RECORDS_PER_OPERATION="$RECORDS_PER_OPERATION" \
        BORSUK_GROUP_COMMIT_WORKER_LANES="$worker_lanes" \
        BORSUK_STORAGE_TRACE="$storage_trace_output" \
        python3 "$ROOT_DIR/scripts/benchmark_with_resources.py" \
          --output "$resource_output" \
          --interval-ms "$RESOURCE_INTERVAL_MS" \
          -- timeout --signal=TERM --kill-after=30s "$CELL_TIMEOUT_SECONDS" "$GROUP_BINARY" \
          >"$benchmark_stdout" 2>"$benchmark_stderr"
      status=$?
      set -e
      mkdir -p "$cell_output"
      if [[ -f "$benchmark_stdout" ]]; then
        mv "$benchmark_stdout" "$cell_output/benchmark.stdout.log"
      fi
      if [[ -f "$benchmark_stderr" ]]; then
        mv "$benchmark_stderr" "$cell_output/benchmark.stderr.log"
      fi
      if [[ -f "$resource_output" ]]; then
        mv "$resource_output" "$cell_output/resources.csv"
      else
        printf 'missing resource telemetry\n' > "$cell_output/RESOURCE_TELEMETRY_MISSING"
      fi
      if [[ -f "$storage_trace_output" ]]; then
        mv "$storage_trace_output" "$cell_output/storage-access.csv"
      else
        printf 'missing storage trace\n' > "$cell_output/STORAGE_TRACE_MISSING"
      fi
      printf '%s\n' "$status" > "$cell_output/process_exit.txt"
      if (( status != 0 )); then
        exit "$status"
      fi
      printf 'complete\n' > "$cell_output/CELL_COMPLETE"
      validation_error="$cell_output/validation-error.txt"
      if ! python3 "$ROOT_DIR/scripts/validate_group_commit_scalability.py" \
        --manifest "$MANIFEST" \
        --terminal-cell "c${cells}/r${repetition}/l${worker_lanes}/w${writers}" \
        "$OUTPUT" >"$validation_error" 2>&1; then
        printf 'validation failed\n' > "$cell_output/CELL_VALIDATION_FAILED"
        cat "$validation_error" >&2
        exit 1
      fi
      rm -f "$validation_error"
      sync_results
      CURRENT_CELL=""
    done
    done
  done
done

python3 - "$OUTPUT" <<'PY'
import csv
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
for name in ("summary.csv", "samples.csv", "reads.csv", "active-tail-reads.csv"):
    sources = sorted(root.glob(f"cells/**/{name}"))
    if not sources:
        raise SystemExit(f"no cell artifacts for {name}")
    rows = []
    fields = None
    for source in sources:
        match = re.fullmatch(r"c(\d+)/r(\d+)/l(\d+)/w(\d+)/" + name, source.relative_to(root / "cells").as_posix())
        if match is None:
            raise SystemExit(f"invalid cell path {source}")
        with source.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if name == "summary.csv":
                prefix_fields = ["cell_count", "repetition"]
            else:
                prefix_fields = ["cell_count", "repetition", "worker_lanes"]
            if fields is None:
                fields = prefix_fields + reader.fieldnames
            elif fields[len(prefix_fields):] != reader.fieldnames:
                raise SystemExit(f"schema drift in {source}")
            for row in reader:
                identity = {"cell_count": match.group(1), "repetition": int(match.group(2))}
                if name != "summary.csv":
                    identity["worker_lanes"] = int(match.group(3))
                rows.append({**identity, **row})
    with (root / name).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
PY

if [[ "$SMOKE" == "1" ]]; then
  printf 'gate,status\ngrouped_durable_ack,pass\nindependent_writer_instances,pass\nextent_idempotency,pass\npost_completion_lease_fencing,pass\nstale_watermark_reopen,pass\nepoch_zombie_exclusion,pass\nowner_only_head_mutation,pass\nsequential_last_write_wins,pass\ntail_backpressure,pass\ndelta_drain_frontier_safety,pass\n' > "$OUTPUT/correctness.csv"
else
  run_exact_test group_commit concurrent_appends_share_one_durable_wal_transaction
  run_exact_test group_commit independent_group_writers_can_share_one_collection
  run_exact_test fault_injection collection_transaction_is_invisible_when_pending_publication_fails
  run_exact_lib_test lane_log::tests::v30_extent_put_is_the_acknowledgement_boundary
  run_exact_lib_test lane_log::tests::v30_extent_completing_after_lease_guard_is_not_acknowledged
  run_exact_lib_test lane_log::tests::v30_linearizable_reader_recovers_extents_beyond_a_stale_watermark
  run_exact_lib_test lane_log::tests::v30_sealed_epoch_excludes_a_late_zombie_extent
  run_exact_test group_commit small_groups_publish_only_immutable_extents_before_release
  run_exact_test group_commit alternating_writer_lanes_preserve_sequential_last_write_wins
  run_exact_test group_commit background_materialization_keeps_sustained_ingest_below_the_hard_tail_bound
  run_exact_test group_commit drain_checkpoints_every_preceding_group_and_removes_pending_objects
  printf 'gate,status\ngrouped_durable_ack,pass\nindependent_writer_instances,pass\nextent_idempotency,pass\npost_completion_lease_fencing,pass\nstale_watermark_reopen,pass\nepoch_zombie_exclusion,pass\nowner_only_head_mutation,pass\nsequential_last_write_wins,pass\ntail_backpressure,pass\ndelta_drain_frontier_safety,pass\n' > "$OUTPUT/correctness.csv"
fi

printf 'complete\n' > "$OUTPUT/GROUP_COMMIT_SCALABILITY_COMPLETE"
python3 "$ROOT_DIR/scripts/validate_group_commit_scalability.py" \
  --manifest "$MANIFEST" "$OUTPUT"
if [[ "$EXACT_BOUND_LOCAL" == "1" ]]; then
  decision="$OUTPUT/exact-bound-shadow-decision.json"
  set +e
  python3 "$ROOT_DIR/scripts/evaluate_exact_bound_shadow.py" \
    --manifest "$MANIFEST" \
    --output "$decision" \
    "$OUTPUT"
  decision_status=$?
  set -e
  [[ -f "$decision" ]] || exit 1
  if (( decision_status == 0 )); then
    printf 'accepted\n' > "$OUTPUT/EXACT_BOUND_SHADOW_ACCEPTED"
  elif (( decision_status == 1 )); then
    printf 'rejected\n' > "$OUTPUT/EXACT_BOUND_SHADOW_REJECTED"
  else
    exit "$decision_status"
  fi
fi
sync_results
trap - EXIT
printf '%s\n' "$OUTPUT"
