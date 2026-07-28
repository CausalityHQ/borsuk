#!/usr/bin/env bash
# Fresh-index end-to-end qualification for the role-based storage layout.
set -euo pipefail

cd "$(dirname "$0")/.."
EXECUTE="${BORSUK_LAYOUT_EXECUTE:-0}"
REGION="${AWS_REGION:-eu-central-1}"
QUALIFICATION_PROTOCOL="docs/research/storage-layout-qualification-protocol.json"
PROTOCOL_RUN_ID="$(
  python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["campaign_id"])' \
    "$QUALIFICATION_PROTOCOL"
)"
if [[ "$EXECUTE" == "1" && "${BORSUK_RUN_LAYOUT_QUALIFICATION:-0}" != "1" ]]; then
  echo "paid execution requires BORSUK_RUN_LAYOUT_QUALIFICATION=1" >&2
  exit 2
fi

RUN_ID="${BORSUK_LAYOUT_RUN_ID:-$PROTOCOL_RUN_ID}"
if [[ "$RUN_ID" != "$PROTOCOL_RUN_ID" ]]; then
  echo "protocol campaign_id mismatch: run=$RUN_ID protocol=$PROTOCOL_RUN_ID" >&2
  exit 3
fi

REPETITIONS="${BORSUK_LAYOUT_REPETITIONS:-5}"
QUERIES="${BORSUK_LAYOUT_QUERIES:-100}"
SEGMENT_MAX=4096
MIXED_VORTEX_MIN_ROWS=4096
DATASET_NAMES="${BORSUK_LAYOUT_DATASET_NAMES:-fashion-mnist-784 glove-100}"
BACKENDS="${BORSUK_LAYOUT_BACKENDS:-local_disk s3}"
ARMS=(
  fixed-parquet
  fixed-vortex-full
  fixed-vortex-range
  mixed-vortex-full
  mixed-vortex-range
)

python3 - \
  "$QUALIFICATION_PROTOCOL" \
  "$REPETITIONS" \
  "$QUERIES" \
  "$SEGMENT_MAX" \
  "$MIXED_VORTEX_MIN_ROWS" \
  "$DATASET_NAMES" \
  "$BACKENDS" \
  "${ARMS[*]}" \
  "$REGION" <<'PY'
import json
import sys

(
    protocol_path,
    repetitions,
    queries,
    segment_max,
    vortex_minimum_rows,
    datasets,
    backends,
    arms,
    region,
) = sys.argv[1:]
with open(protocol_path) as handle:
    protocol = json.load(handle)

expected_arms = [protocol["baseline_arm"], *protocol["candidate_arms"]]
checks = (
    ("repetitions", int(repetitions), int(protocol["repetitions"])),
    (
        "queries_per_repetition",
        int(queries),
        int(protocol["queries_per_repetition"]),
    ),
    (
        "segment_max_rows",
        int(segment_max),
        int(protocol["adaptive_layout_contract"]["segment_max_rows"]),
    ),
    (
        "vortex_minimum_rows",
        int(vortex_minimum_rows),
        int(protocol["adaptive_layout_contract"]["vortex_minimum_rows"]),
    ),
    ("datasets", datasets.split(), protocol["datasets"]),
    ("backends", backends.split(), protocol["backends"]),
    ("arms", arms.split(), expected_arms),
    ("aws_region", region, protocol["hardware_contract"]["aws_region"]),
)
for field, actual, expected in checks:
    if actual != expected:
        raise SystemExit(
            f"protocol requires {field}={expected}; got {actual}"
        )
PY

ROOT="${BORSUK_LAYOUT_ROOT:-/home/ec2-user/borsuk-layout-qualification/$RUN_ID}"
if [[ -e "$ROOT" ]]; then
  echo "refusing to overwrite layout qualification root: $ROOT" >&2
  exit 3
fi
mkdir -p "$ROOT"
cp "$QUALIFICATION_PROTOCOL" "$ROOT/qualification-protocol.json"

cpu_model="$(
  lscpu 2>/dev/null |
    sed -n 's/^Model name:[[:space:]]*//p' |
    head -n 1 ||
    true
)"
cpu_model="${cpu_model:-unavailable}"
memory_bytes="$(
  sysctl -n hw.memsize 2>/dev/null ||
    awk '/^MemTotal:/ {printf "%.0f\n", $2 * 1024}' /proc/meminfo
)"
{
  printf '%s\n' \
    "created_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "run_id=$RUN_ID" \
    "execute=$EXECUTE" \
    "source_sha256=${BORSUK_SOURCE_SHA256:-not-provided}" \
    "aws_region=$REGION" \
    "instance_id=${BORSUK_INSTANCE_ID:-not-provided}" \
    "instance_type=${BORSUK_INSTANCE_TYPE:-not-provided}" \
    "ami_id=${BORSUK_AMI_ID:-not-provided}" \
    "local_disk_class=${BORSUK_LOCAL_DISK_CLASS:-not-provided}" \
    "architecture=$(uname -m)" \
    "cpu_model=$cpu_model" \
    "repetitions=$REPETITIONS" \
    "queries=$QUERIES" \
    "segment_max=$SEGMENT_MAX" \
    "mixed_vortex_min_rows=$MIXED_VORTEX_MIN_ROWS" \
    "datasets=$DATASET_NAMES" \
    "backends=$BACKENDS" \
    "arms=${ARMS[*]}" \
    "keep_case_data=${BORSUK_LAYOUT_KEEP_CASE_DATA:-0}" \
    "qualification_scope=forced-normal-segment-path" \
    "global_pq_chunks_required=0" \
    "segments_searched_required=positive" \
    "segment_schema=packed-header-v12" \
    "segment_header_codec=bsh1-little-endian-blake3" \
    "wal_control_codec=bwh1-bwn1-bwd1-bwc1-bmm1-btm1-bid1-bcn1" \
    "wal_write_sharding=cell-lane-no-explicit-id-global-cas" \
    "parquet_corruption_boundary=unwind-to-invalid-storage" \
    "byte_guard_source=query-scoped-backing-bytes" \
    "request_guard_source=local-backing-reads-or-s3-network-gets" \
    "uncached_phase=application-payload-cache-cold-kernel-page-cache-not-evicted" \
    "kernel=$(uname -srmo)" \
    "logical_cpus=$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.logicalcpu)" \
    "memory_bytes=$memory_bytes" \
    "rustc_version=$(rustc --version 2>/dev/null || printf unavailable)"
} > "$ROOT/environment.txt"

printf '%s\n' \
  "repetition_id,query_seed,dataset,backend,arm,arm_position,case_id" \
  > "$ROOT/schedule.csv"
for repetition in $(seq 1 "$REPETITIONS"); do
  repetition_id="$(printf 'r%02d' "$repetition")"
  query_seed="$((20260726 + repetition))"
  for dataset in $DATASET_NAMES; do
    for backend in $BACKENDS; do
      for position in 0 1 2 3 4; do
        arm_index="$(((position + repetition - 1) % ${#ARMS[@]}))"
        arm="${ARMS[$arm_index]}"
        case_id="$repetition_id/$dataset/$backend/$arm"
        printf '%s\n' \
          "$repetition_id,$query_seed,$dataset,$backend,$arm,$position,$case_id" \
          >> "$ROOT/schedule.csv"
      done
    done
  done
done

if [[ "$EXECUTE" != "1" ]]; then
  echo "layout qualification dry run: $ROOT/schedule.csv"
  exit 0
fi

: "${BORSUK_SOURCE_SHA256:?set the exact source archive SHA-256}"
: "${BORSUK_LAYOUT_BUCKET:?set the plain existing S3 bucket}"
: "${BORSUK_LAYOUT_DATASETS:?set the prepared standard-dataset root}"
: "${BORSUK_INSTANCE_ID:?set the fixed EC2 instance identity}"
: "${BORSUK_INSTANCE_TYPE:?set the fixed EC2 instance type}"
: "${BORSUK_AMI_ID:?set the EC2 image identity}"
: "${BORSUK_LOCAL_DISK_CLASS:?set the EBS volume class}"
[[ "$BORSUK_SOURCE_SHA256" =~ ^[[:xdigit:]]{64}$ ]] || {
  echo "BORSUK_SOURCE_SHA256 must be a 64-character digest" >&2
  exit 2
}
python3 - "$QUALIFICATION_PROTOCOL" "$BORSUK_INSTANCE_TYPE" "$(uname -m)" <<'PY'
import json
import sys

protocol_path, instance_type, architecture = sys.argv[1:]
with open(protocol_path) as handle:
    contract = json.load(handle)["hardware_contract"]
for field, actual in (
    ("instance_type", instance_type),
    ("architecture", architecture),
):
    expected = contract[field]
    if actual != expected:
        raise SystemExit(f"protocol requires {field}={expected}; got {actual}")
PY
RESULT_PREFIX="${BORSUK_LAYOUT_RESULT_PREFIX:-layout-qualification/results/$RUN_ID}"
INDEX_PREFIX="${BORSUK_LAYOUT_INDEX_PREFIX:-layout-qualification/indexes/$RUN_ID}"
for prefix in "$RESULT_PREFIX" "$INDEX_PREFIX"; do
  existing="$(aws --region "$REGION" s3api list-objects-v2 \
    --bucket "$BORSUK_LAYOUT_BUCKET" \
    --prefix "${prefix%/}/" \
    --max-keys 1 \
    --query KeyCount \
    --output text)"
  [[ "$existing" == "0" ]] || {
    echo "refusing to reuse non-empty S3 prefix: $prefix" >&2
    exit 3
  }
done

sync_results() {
  aws --region "$REGION" s3 sync \
    "$ROOT" \
    "s3://$BORSUK_LAYOUT_BUCKET/$RESULT_PREFIX" \
    --exclude '*/cache/*' \
    --exclude '*/scratch/*' \
    --exclude '*/index/*' \
    --only-show-errors
}
trap sync_results EXIT

python3 scripts/freeze_layout_dataset_identity.py \
  --dataset-root "$BORSUK_LAYOUT_DATASETS" \
  --protocol "$ROOT/qualification-protocol.json" \
  --output "$ROOT/dataset-identities.json"
dataset_identity_sha256="$(
  sha256sum "$ROOT/dataset-identities.json" | awk '{print $1}'
)"
printf '%s\n' \
  "dataset_identity_sha256=$dataset_identity_sha256" \
  >> "$ROOT/environment.txt"
sync_results

cargo build --locked --release -p borsuk --example production_bench

tail -n +2 "$ROOT/schedule.csv" |
while IFS=, read -r repetition_id query_seed dataset backend arm arm_position case_id; do
  dataset_dir="$BORSUK_LAYOUT_DATASETS/$dataset"
  [[ -d "$dataset_dir" ]] || {
    echo "missing prepared dataset: $dataset_dir" >&2
    exit 4
  }
  case_root="$ROOT/$case_id"
  result_root="$case_root/results"
  cache_root="$case_root/cache"
  scratch_root="$case_root/scratch"
  mkdir -p "$result_root" "$cache_root" "$scratch_root"
  if [[ "$backend" == "s3" ]]; then
    index_uri="s3://$BORSUK_LAYOUT_BUCKET/$INDEX_PREFIX/$case_id"
  else
    index_uri="$case_root/index"
  fi

  segment_format=parquet
  vortex_range_reads=1
  vortex_min_rows=""
  case "$arm" in
    fixed-parquet) ;;
    fixed-vortex-full)
      segment_format=vortex
      vortex_range_reads=0
      ;;
    fixed-vortex-range)
      segment_format=vortex
      ;;
    mixed-vortex-full)
      vortex_range_reads=0
      vortex_min_rows="$MIXED_VORTEX_MIN_ROWS"
      ;;
    mixed-vortex-range)
      vortex_min_rows="$MIXED_VORTEX_MIN_ROWS"
      ;;
    *)
      echo "unknown layout arm: $arm" >&2
      exit 2
      ;;
  esac
  arm_env=()
  if [[ -n "$vortex_min_rows" ]]; then
    arm_env+=("BORSUK_SEGMENT_VORTEX_MIN_ROWS=$vortex_min_rows")
  fi

  env -u BORSUK_SEGMENT_VORTEX_MIN_ROWS \
    "${arm_env[@]}" \
    AWS_REGION="$REGION" \
    AWS_DEFAULT_REGION="$REGION" \
    BORSUK_BENCH_URI="$index_uri" \
    BORSUK_BENCH_DATASET="$dataset_dir" \
    BORSUK_BENCH_CACHE="$cache_root" \
    BORSUK_BENCH_OUTPUT_DIR="$result_root" \
    BORSUK_BENCH_QUERY_SEED="$query_seed" \
    BORSUK_BENCH_REPETITION_ID="$repetition_id" \
    BORSUK_BENCH_QUERIES="$QUERIES" \
    BORSUK_BENCH_UNCACHED_QUERIES="$QUERIES" \
    BORSUK_BENCH_SEGMENT_MAX="$SEGMENT_MAX" \
    BORSUK_SEGMENT_TABLE_FORMAT="$segment_format" \
    BORSUK_VORTEX_RANGE_READS="$vortex_range_reads" \
    BORSUK_BENCH_GLOBAL_SCAN_CODEC=srht-pq-scan \
    BORSUK_BENCH_RECALL_LEAF_MODE=srht-pq-scan \
    BORSUK_BENCH_SERVING_LEAF_MODE=srht-pq-scan \
    BORSUK_BENCH_NPROBES=8 \
    BORSUK_BENCH_CANDIDATES=320 \
    BORSUK_BENCH_CACHE_EXECUTION=scan \
    BORSUK_BENCH_FORCE_SEGMENT_PATH=1 \
    BORSUK_BENCH_RECALL_ONLY=1 \
    BORSUK_BENCH_SKIP_EXACT_RECALL=1 \
    BORSUK_BENCH_LEAF_CAPABILITY=pq-scan-only \
    BORSUK_BENCH_MAX_CONCURRENT_SEARCHES=4 \
    BORSUK_BENCH_MAX_CONCURRENT_CELL_DECODES=24 \
    BORSUK_BENCH_RAM_BUDGET_BYTES=536870912 \
    BORSUK_BENCH_READ_ONLY=1 \
    python3 scripts/benchmark_with_resources.py \
      --output "$result_root/resources.csv" \
      --cache-dir "$cache_root" \
      --scratch-dir "$scratch_root" \
      -- target/release/examples/production_bench

  python3 scripts/validate_benchmark_artifacts.py \
    --directory "$result_root" \
    --expected-codec srht-pq-scan \
    --required bench_build.csv,bench_recall_latency.csv,bench_query_samples.csv,resources.csv
  if [[ "$backend" == "s3" ]]; then
    read -r segment_parquet_objects segment_vortex_objects < <(
      aws --region "$REGION" s3api list-objects-v2 \
        --bucket "$BORSUK_LAYOUT_BUCKET" \
        --prefix "$INDEX_PREFIX/$case_id/segments/" \
        --query '[length(Contents[?ends_with(Key, `.parquet`)]), length(Contents[?ends_with(Key, `.vortex`)])]' \
        --output text
    )
  else
    segment_parquet_objects="$(
      find "$case_root/index/segments" -type f -name '*.parquet' | wc -l | tr -d '[:space:]'
    )"
    segment_vortex_objects="$(
      find "$case_root/index/segments" -type f -name '*.vortex' | wc -l | tr -d '[:space:]'
    )"
  fi
  printf '%s\n' \
    "segment_parquet_objects=$segment_parquet_objects" \
    "segment_vortex_objects=$segment_vortex_objects" \
    > "$case_root/segment-layout.txt"
  case "$arm" in
    fixed-parquet)
      if ((segment_parquet_objects == 0 || segment_vortex_objects != 0)); then
        echo "fixed Parquet layout emitted the wrong segment formats" >&2
        exit 5
      fi
      ;;
    fixed-vortex-*)
      if ((segment_vortex_objects == 0 || segment_parquet_objects != 0)); then
        echo "fixed Vortex layout emitted the wrong segment formats" >&2
        exit 5
      fi
      ;;
    mixed-vortex-*)
      if ((segment_parquet_objects == 0 || segment_vortex_objects == 0)); then
        echo "mixed layout did not emit both Parquet and Vortex" >&2
        exit 5
      fi
      ;;
  esac
  printf '%s\n' \
    "source_sha256=$BORSUK_SOURCE_SHA256" \
    "dataset_identity_sha256=$dataset_identity_sha256" \
    "repetition_id=$repetition_id" \
    "query_seed=$query_seed" \
    "dataset=$dataset" \
    "backend=$backend" \
    "arm=$arm" \
    "arm_position=$arm_position" \
    "index_uri=$index_uri" \
    "segment_parquet_objects=$segment_parquet_objects" \
    "segment_vortex_objects=$segment_vortex_objects" \
    > "$case_root/protocol.txt"
  printf 'complete\n' > "$case_root/CASE_COMPLETE"
  sync_results
  if [[ "${BORSUK_LAYOUT_KEEP_CASE_DATA:-0}" != "1" ]]; then
    rm -rf "$cache_root" "$scratch_root"
    if [[ "$backend" == "local_disk" ]]; then
      rm -rf "$case_root/index"
    fi
  fi
done

python3 scripts/assemble_storage_layout_qualification.py \
  --root "$ROOT" \
  --output "$ROOT/qualification_samples.csv" \
  --minimum-samples "$QUERIES"

python3 scripts/analyze_storage_layout_qualification.py \
  --samples "$ROOT/qualification_samples.csv" \
  --output "$ROOT/layout-decisions.csv" \
  --minimum-samples "$QUERIES"
printf 'complete\n' > "$ROOT/LAYOUT_QUALIFICATION_COMPLETE"
sync_results
