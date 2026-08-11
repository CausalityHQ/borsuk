#!/usr/bin/env bash
set -euo pipefail

# Execute the preregistered paired positioned-V12 logical-cell routing matrix.
# Every measurement arm starts from a fresh clone of one immutable base.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SMOKE="${BORSUK_ROUTING_SMOKE:-0}"
if [[ "$SMOKE" == "1" ]]; then
  MANIFEST="$ROOT_DIR/docs/research/logical-cell-routing-positioned-v12-smoke.json"
else
  MANIFEST="$ROOT_DIR/docs/research/logical-cell-routing-positioned-v12-campaign.json"
fi
if [[ "${BORSUK_ROUTING_PLAN_ONLY:-0}" == "1" ]]; then
  exec python3 "$ROOT_DIR/scripts/bench_logical_cell_routing.py" --manifest "$MANIFEST"
fi
if [[ "$SMOKE" == "1" ]]; then
  OUTPUT="${BORSUK_ROUTING_OUTPUT_ROOT:-$(mktemp -d)/logical-cell-routing-v12-smoke}"
  INDEX_ROOT="${BORSUK_ROUTING_INDEX_ROOT:-$(mktemp -d)/logical-cell-routing-v12-indexes}"
  ARCHITECTURE="local"
  INSTANCE_TYPE="local"
  INSTANCE_ID="local"
  AVAILABILITY_ZONE="local"
  PURCHASE_OPTION="local"
else
  [[ "${BORSUK_RUN_LOGICAL_CELL_ROUTING:-0}" == "1" ]] || {
    echo "set BORSUK_RUN_LOGICAL_CELL_ROUTING=1 for production execution" >&2
    exit 2
  }
  OUTPUT="${BORSUK_ROUTING_OUTPUT_ROOT:?set BORSUK_ROUTING_OUTPUT_ROOT}"
  INDEX_ROOT="${BORSUK_ROUTING_INDEX_ROOT:?set BORSUK_ROUTING_INDEX_ROOT}"
  ARCHITECTURE="${BORSUK_ARCHITECTURE:?set BORSUK_ARCHITECTURE}"
  INSTANCE_TYPE="${BORSUK_INSTANCE_TYPE:?set BORSUK_INSTANCE_TYPE}"
  INSTANCE_ID="${BORSUK_INSTANCE_ID:?set BORSUK_INSTANCE_ID}"
  AVAILABILITY_ZONE="${BORSUK_AVAILABILITY_ZONE:?set BORSUK_AVAILABILITY_ZONE}"
  PURCHASE_OPTION="${BORSUK_PURCHASE_OPTION:?set BORSUK_PURCHASE_OPTION}"
  [[ "$PURCHASE_OPTION" == "spot" ]] || {
    echo "production logical-cell routing requires EC2 Spot" >&2
    exit 2
  }
  if [[ -z "${BORSUK_SOURCE_ARCHIVE:-}" ]]; then
    [[ -z "$(git -C "$ROOT_DIR" status --porcelain)" ]] || {
      echo "production execution requires a clean tracked worktree" >&2
      exit 2
    }
  fi
fi

json_value() {
  python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$MANIFEST" "$1"
}
read_json_array() {
  local -n destination="$1"
  local field="$2"
  local values
  values="$(python3 -c 'import json,sys; print(*json.load(open(sys.argv[1]))[sys.argv[2]], sep="\n")' "$MANIFEST" "$field")" || return
  [[ -n "$values" ]] || { echo "manifest array $field is empty" >&2; return 1; }
  mapfile -t destination <<<"$values"
}
read_json_array CELL_COUNTS cell_counts
read_json_array WRITERS writers
read_json_array MODES_FROZEN routing_modes
REPETITIONS="$(json_value repetitions)"
OPERATIONS="$(json_value operations_per_writer)"
WARMUP="$(json_value warmup_operations_per_writer)"
DIMENSIONS="$(json_value dimensions)"
METRIC="$(json_value metric)"
MUTATION_PROTOCOL="$(json_value mutation_protocol)"
CELL_TIMEOUT_SECONDS="$(json_value cell_timeout_seconds)"
CLONE_TIMEOUT_SECONDS="$(json_value clone_timeout_seconds)"
SETUP_TIMEOUT_SECONDS="$(json_value setup_timeout_seconds)"
RESOURCE_INTERVAL_MS="$(json_value resource_sample_interval_ms)"
MAX_WRITE_P95_MS="$(json_value max_write_p95_ms)"
MASTER_SEED="$(json_value master_seed)"

[[ "$DIMENSIONS" == 768 && "$METRIC" == cosine && "$MUTATION_PROTOCOL" == positioned-v12 ]] || {
  echo "manifest is not the realistic positioned V12 protocol" >&2
  exit 2
}
[[ "${MODES_FROZEN[*]}" == "flat quantizer" ]] || {
  echo "manifest routing modes are not the paired protocol" >&2
  exit 2
}
[[ ! -e "$OUTPUT" ]] || { echo "refusing to replace output $OUTPUT" >&2; exit 3; }
RESULT_URI="${BORSUK_ROUTING_RESULT_URI:-}"
CURRENT_CELL=""

sync_results() {
  if [[ -n "$RESULT_URI" ]]; then
    profile_args=()
    if [[ -n "${AWS_PROFILE:-}" ]]; then profile_args=(--profile "$AWS_PROFILE"); fi
    python3 "$ROOT_DIR/scripts/benchmark_s3.py" publish \
      --source "$OUTPUT" --destination "$RESULT_URI" \
      --timeout-seconds "$CLONE_TIMEOUT_SECONDS" "${profile_args[@]}"
  fi
}

failed() {
  status=$?
  if (( status != 0 )); then
    mkdir -p "$OUTPUT"
    rm -f "$OUTPUT/LOGICAL_CELL_ROUTING_COMPLETE"
    printf 'failed\n' > "$OUTPUT/LOGICAL_CELL_ROUTING_FAILED"
    if [[ -n "$CURRENT_CELL" ]]; then
      mkdir -p "$CURRENT_CELL"
      printf 'failed\n' > "$CURRENT_CELL/CELL_FAILED"
    fi
    sync_results || true
  fi
  exit "$status"
}
trap failed EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -p "$OUTPUT/cells"
python3 "$ROOT_DIR/scripts/bench_logical_cell_routing.py" \
  --manifest "$MANIFEST" > "$OUTPUT/execution-plan.json"
python3 - "$OUTPUT/execution-plan.json" > "$OUTPUT/arms.tsv" <<'PY'
import json
import sys

plan = json.load(open(sys.argv[1], encoding="utf-8"))
for arm in plan["arms"]:
    print(
        arm["cell_count"],
        arm["writers"],
        arm["repetition"],
        arm["routing_mode"],
        sep="\t",
    )
PY
[[ -s "$OUTPUT/arms.tsv" ]] || { echo "execution plan has no arms" >&2; exit 3; }
MANIFEST_SHA256="$(sha256sum "$MANIFEST" | awk '{print $1}')"
if [[ -n "${BORSUK_SOURCE_ARCHIVE:-}" ]]; then
  SOURCE_SHA256="$(sha256sum "$BORSUK_SOURCE_ARCHIVE" | awk '{print $1}')"
  [[ -z "${BORSUK_SOURCE_SHA256:-}" || "$BORSUK_SOURCE_SHA256" == "$SOURCE_SHA256" ]] || {
    echo "preserved source archive SHA-256 mismatch" >&2
    exit 3
  }
else
  if [[ -n "${BORSUK_SOURCE_SHA256:-}" ]]; then
    SOURCE_SHA256="$BORSUK_SOURCE_SHA256"
  else
    SOURCE_SHA256="$(cd "$ROOT_DIR" && {
      git archive --format=tar HEAD
      git diff --binary HEAD
      git ls-files --others --exclude-standard -z \
        | sort -z \
        | xargs -0 -r sha256sum
    } | sha256sum | awk '{print $1}')"
  fi
fi
TARGET_DIR="$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
BINARY="$TARGET_DIR/release/examples/logical_cell_routing_bench"

capture_epoch_ns() {
  local output_name="$1"
  local LC_ALL=C
  local now seconds micros
  now="$EPOCHREALTIME"
  seconds="${now%%.*}"
  micros="${now#*.}"
  printf -v "$output_name" '%s%s000' "$seconds" "$micros"
}

clone_index() {
  local source="$1"
  local destination="$2"
  local evidence="$3"
  local started_ns
  if [[ "$source" == s3://* && "$destination" == s3://* ]]; then
    python3 "$ROOT_DIR/scripts/benchmark_s3.py" assert-empty \
      --uri "$destination" --timeout-seconds "$CLONE_TIMEOUT_SECONDS"
    capture_epoch_ns started_ns
    timeout --signal=TERM --kill-after=30s "$CLONE_TIMEOUT_SECONDS" \
      aws s3 sync --only-show-errors "$source/" "$destination/"
  else
    capture_epoch_ns started_ns
    mkdir -p "$(dirname "$destination")"
    [[ ! -e "$destination" ]] || { echo "refusing to reuse index $destination" >&2; return 1; }
    cp -a "$source" "$destination"
  fi
  local completed_ns shape
  capture_epoch_ns completed_ns
  shape="$(template_object_shape "$destination")"
  python3 - "$shape" "$started_ns" "$completed_ns" "$evidence" <<'PY'
import json
import pathlib
import sys

shape = json.loads(sys.argv[1])
duration_ms = (int(sys.argv[3]) - int(sys.argv[2])) / 1_000_000
pathlib.Path(sys.argv[4]).write_text(
    json.dumps(
        {
            "copy_count": shape["object_count"],
            "copied_bytes": shape["object_bytes"],
            "duration_ms": duration_ms,
        },
        sort_keys=True,
    )
    + "\n",
    encoding="utf-8",
)
PY
}

template_object_shape() {
  local uri="$1"
  python3 - "$uri" "$CLONE_TIMEOUT_SECONDS" <<'PY'
import json
import os
import pathlib
import subprocess
import sys
from urllib.parse import urlparse

uri = sys.argv[1]
timeout_seconds = int(sys.argv[2])
if uri.startswith("s3://"):
    parsed = urlparse(uri)
    prefix = parsed.path.strip("/")
    if not prefix:
        raise SystemExit("template S3 URI must include a non-empty key prefix")
    command = [
        "aws", "s3api", "list-objects-v2", "--bucket", parsed.netloc,
        "--prefix", prefix + "/", "--output", "json",
    ]
    profile = os.environ.get("AWS_PROFILE")
    if profile:
        command.extend(["--profile", profile])
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
    contents = json.loads(result.stdout).get("Contents", [])
    print(json.dumps({
        "object_count": len(contents),
        "object_bytes": sum(int(item["Size"]) for item in contents),
    }, sort_keys=True))
else:
    root = pathlib.Path(uri.removeprefix("file://"))
    files = [path for path in root.rglob("*") if path.is_file()]
    print(json.dumps({
        "object_count": len(files),
        "object_bytes": sum(path.stat().st_size for path in files),
    }, sort_keys=True))
PY
}

run_exact_test() {
  local gate="$1"
  local target="$2"
  local test_name="$3"
  local log="$OUTPUT/correctness-${gate}.log"
  cargo test --locked -p borsuk --test "$target" "$test_name" -- --exact --nocapture >"$log" 2>&1
  grep -Fq 'test result: ok. 1 passed; 0 failed;' "$log" || {
    cat "$log" >&2
    return 1
  }
  printf '%s,pass\n' "$gate" >> "$OUTPUT/correctness.csv"
}

validate_terminal_arm() {
  local cell_output="$1"
  python3 - "$cell_output" "$OPERATIONS" "$MAX_WRITE_P95_MS" <<'PY'
import csv
import math
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
operations_per_writer = int(sys.argv[2])
hard_cap = float(sys.argv[3])
with (root / "summary.csv").open(newline="", encoding="utf-8") as handle:
    rows = list(csv.DictReader(handle))
if len(rows) != 1:
    raise SystemExit("terminal arm must contain exactly one summary row")
row = rows[0]
expected = int(row["writers"]) * operations_per_writer
if int(row["operations"]) != expected:
    raise SystemExit("terminal arm operation count mismatch")
p95 = float(row["p95_ms"])
if not math.isfinite(p95) or p95 > hard_cap:
    raise SystemExit(f"terminal arm write p95 hard cap exceeded: {p95} > {hard_cap}")
with (root / "samples.csv").open(newline="", encoding="utf-8") as handle:
    samples = list(csv.DictReader(handle))
if len(samples) != expected:
    raise SystemExit("terminal arm raw sample count mismatch")
PY
}

cp "$MANIFEST" "$OUTPUT/manifest.json"
printf '%s\n' \
  "source_sha256=$SOURCE_SHA256" \
  "manifest_sha256=$MANIFEST_SHA256" \
  "architecture=$ARCHITECTURE" \
  "instance_type=$INSTANCE_TYPE" \
  "instance_id=$INSTANCE_ID" \
  "availability_zone=$AVAILABILITY_ZONE" \
  "purchase_option=$PURCHASE_OPTION" \
  "mutation_protocol=$MUTATION_PROTOCOL" \
  "dimensions=$DIMENSIONS" \
  "metric=$METRIC" \
  > "$OUTPUT/environment.txt"

printf 'gate,status\n' > "$OUTPUT/correctness.csv"
run_exact_test multi_writer_reopen group_commit one_eight_and_thirty_two_producers_converge_after_reopen
run_exact_test sequential_last_write_wins group_commit grouped_upsert_is_last_write_wins_and_reopens
run_exact_test delete_reopen group_commit delete_receipts_are_request_local_across_stale_writers_and_reopen
run_exact_test cross_modality_reopen group_commit late_interaction_replacement_and_delete_reopen_from_one_positioned_log
run_exact_test shard_head_conflict_rebase positioned_log two_writers_rebase_one_shard_without_duplicate_visibility
run_exact_test lost_head_response_reconciled positioned_log lost_successful_head_response_is_reconciled_by_exact_receipt
run_exact_test publication_failure_invisible fault_injection collection_transaction_is_invisible_when_positioned_head_publication_fails
run_exact_test materializer_race positioned_log concurrent_checkpoint_generation_race_converges_on_one_materialized_prefix

cargo build --locked --release -p borsuk --example logical_cell_routing_bench

mkdir -p "$OUTPUT/templates"
setup_started_seconds=$SECONDS
for cells in "${CELL_COUNTS[@]}"; do
  template_uri="$INDEX_ROOT/templates/c${cells}"
  build_evidence="$OUTPUT/templates/c${cells}.build.json"
  shape_evidence="$OUTPUT/templates/c${cells}.shape.json"
  setup_remaining_seconds=$((SETUP_TIMEOUT_SECONDS - (SECONDS - setup_started_seconds)))
  (( setup_remaining_seconds > 0 )) || {
    echo "logical-cell template setup exceeded its frozen timeout" >&2
    exit 124
  }
  timeout --signal=TERM --kill-after=30s "$setup_remaining_seconds" env \
    BORSUK_ROUTING_SMOKE="$SMOKE" \
    BORSUK_ROUTING_INDEX_URI="$template_uri" \
    BORSUK_ROUTING_CELL_COUNT="$cells" \
    BORSUK_ROUTING_DIMENSIONS="$DIMENSIONS" \
    "$BINARY" build > "$build_evidence"
  template_object_shape "$template_uri" > "$shape_evidence"
  python3 - "$build_evidence" "$shape_evidence" "$OUTPUT/templates/c${cells}.json" <<'PY'
import json
import pathlib
import sys

build = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
shape = json.loads(pathlib.Path(sys.argv[2]).read_text(encoding="utf-8"))
build.update(shape)
pathlib.Path(sys.argv[3]).write_text(json.dumps(build, sort_keys=True) + "\n", encoding="utf-8")
PY
  rm "$build_evidence" "$shape_evidence"
done
(( SECONDS - setup_started_seconds <= SETUP_TIMEOUT_SECONDS )) || {
  echo "logical-cell template setup exceeded its frozen timeout" >&2
  exit 124
}

while IFS=$'\t' read -r cells writers repetition mode; do
        template_uri="$INDEX_ROOT/templates/c${cells}"
        cell_output="$OUTPUT/cells/c${cells}/r$(printf '%02d' "$repetition")/w${writers}/$mode"
        uri="$INDEX_ROOT/cells/c${cells}/r$(printf '%02d' "$repetition")/w${writers}/$mode"
        mkdir -p "$(dirname "$cell_output")"
        clone_evidence="${cell_output}.clone.json"
        clone_index "$template_uri" "$uri" "$clone_evidence"
        CURRENT_CELL="$cell_output"
        resource_output="${cell_output}.resources.csv"
        storage_trace_output="${cell_output}.storage-access.csv"
        stdout_output="${cell_output}.stdout.log"
        stderr_output="${cell_output}.stderr.log"
        set +e
        env \
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
          BORSUK_ROUTING_METRIC="$METRIC" \
          BORSUK_ROUTING_MASTER_SEED="$MASTER_SEED" \
          BORSUK_SOURCE_SHA256="$SOURCE_SHA256" \
          BORSUK_ROUTING_MANIFEST_SHA256="$MANIFEST_SHA256" \
          BORSUK_ARCHITECTURE="$ARCHITECTURE" \
          BORSUK_INSTANCE_TYPE="$INSTANCE_TYPE" \
          BORSUK_INSTANCE_ID="$INSTANCE_ID" \
          BORSUK_AVAILABILITY_ZONE="$AVAILABILITY_ZONE" \
          BORSUK_PURCHASE_OPTION="$PURCHASE_OPTION" \
          BORSUK_MUTATION_PROTOCOL="$MUTATION_PROTOCOL" \
          BORSUK_STORAGE_TRACE="$storage_trace_output" \
          python3 "$ROOT_DIR/scripts/benchmark_with_resources.py" \
            --output "$resource_output" \
            --interval-ms "$RESOURCE_INTERVAL_MS" \
            -- timeout --signal=TERM --kill-after=30s "$CELL_TIMEOUT_SECONDS" "$BINARY" run \
            >"$stdout_output" 2>"$stderr_output"
        status=$?
        set -e
        mkdir -p "$cell_output"
        mv "$clone_evidence" "$cell_output/clone.json"
        mv "$resource_output" "$cell_output/resources.csv" 2>/dev/null || true
        mv "$storage_trace_output" "$cell_output/storage-access.csv" 2>/dev/null || true
        mv "$stdout_output" "$cell_output/benchmark.stdout.log" 2>/dev/null || true
        mv "$stderr_output" "$cell_output/benchmark.stderr.log" 2>/dev/null || true
        printf '%s\n' "$status" > "$cell_output/process_exit.txt"
        if (( status != 0 )); then
          exit "$status"
        fi
        for artifact in clone.json resources.csv storage-access.csv summary.csv samples.csv; do
          [[ -s "$cell_output/$artifact" ]] || {
            echo "missing terminal arm artifact: $cell_output/$artifact" >&2
            exit 1
          }
        done
        for artifact in benchmark.stdout.log benchmark.stderr.log; do
          [[ -f "$cell_output/$artifact" ]] || {
            echo "missing terminal arm log: $cell_output/$artifact" >&2
            exit 1
          }
        done
        validate_terminal_arm "$cell_output"
        printf 'complete\n' > "$cell_output/CELL_COMPLETE"
        sync_results
        CURRENT_CELL=""
done < "$OUTPUT/arms.tsv"

python3 - "$OUTPUT" <<'PY'
import csv
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
for name in ("summary.csv", "samples.csv"):
    sources = sorted(root.glob(f"cells/**/{name}"))
    if not sources:
        raise SystemExit(f"no terminal arm artifacts for {name}")
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

printf 'complete\n' > "$OUTPUT/LOGICAL_CELL_ROUTING_COMPLETE"
python3 "$ROOT_DIR/scripts/validate_logical_cell_routing_positioned_v12_results.py" \
  --manifest "$MANIFEST" --root "$OUTPUT"
sync_results
trap - EXIT INT TERM
printf '%s\n' "$OUTPUT"
