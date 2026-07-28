#!/usr/bin/env bash
# Paired end-to-end AWS qualification for the adaptive cell-WAL table layout.
set -euo pipefail

cd "$(dirname "$0")/.."

PROTOCOL="${BORSUK_WAL_LAYOUT_PROTOCOL:-docs/research/wal-layout-qualification-protocol.json}"
EXECUTE="${BORSUK_WAL_LAYOUT_EXECUTE:-0}"
REGION="${AWS_REGION:-eu-central-1}"
RUN_ID="$(
  python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["campaign_id"])' \
    "$PROTOCOL"
)"
if [[ -n "${BORSUK_FORMAT_RUN_ID:-}" && "$BORSUK_FORMAT_RUN_ID" != "$RUN_ID" ]]; then
  echo "launcher run id $BORSUK_FORMAT_RUN_ID does not match protocol campaign $RUN_ID" >&2
  exit 2
fi
ROOT="${BORSUK_WAL_LAYOUT_ROOT:-/home/ec2-user/borsuk-wal-layout-qualification/$RUN_ID}"

if [[ "$EXECUTE" == "1" && "${BORSUK_RUN_WAL_LAYOUT_QUALIFICATION:-0}" != "1" ]]; then
  echo "paid execution requires BORSUK_RUN_WAL_LAYOUT_QUALIFICATION=1" >&2
  exit 2
fi
if [[ -e "$ROOT" ]]; then
  echo "refusing to overwrite WAL qualification root: $ROOT" >&2
  exit 3
fi
mkdir -p "$ROOT"
cp "$PROTOCOL" "$ROOT/qualification-protocol.json"

read -r REPETITIONS QUERIES EXPECTED_CASES < <(
  python3 - "$PROTOCOL" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    protocol = json.load(handle)
schema = protocol.get("wal_schema_contract", {})
required_columns = [
    "record_id",
    "metadata",
    "vector",
    "wal_record_extras",
    "wal_vector_element_type",
    "wal_vector_dimensions",
]
if schema.get("table_format_version") != 16:
    raise SystemExit("protocol must freeze WAL table_format_version=16")
if schema.get("required_columns") != required_columns:
    raise SystemExit("protocol WAL required_columns do not match record-only v16")
if schema.get("omitted_segment_columns") != [
    "segment_header",
    "routing_code",
    "pq_code",
]:
    raise SystemExit("protocol WAL omitted_segment_columns do not match record-only v16")

candidate = protocol["candidate_contract"]
if candidate.get("decision_cardinality") != "actual-wal-object-rows-at-write-time":
    raise SystemExit(
        "candidate decision_cardinality must use actual WAL object rows at write time"
    )
minimum_rows = int(candidate["minimum_rows"])
minimum_dimensions = int(candidate["minimum_dimensions"])
vortex_types = set(candidate["vortex_element_types"])
parquet_types = set(candidate["parquet_element_types"])
if vortex_types & parquet_types:
    raise SystemExit("candidate element-type contracts overlap")
for workload in protocol["workloads"]:
    rows = int(workload["rows"])
    batch_rows = int(workload["batch_rows"])
    dimensions = int(workload["dimensions"])
    element_type = workload["element_type"]
    if rows <= 0 or batch_rows <= 0 or rows % batch_rows:
        raise SystemExit(
            f"{workload['name']}: rows must be a positive multiple of batch_rows"
        )
    if element_type not in vortex_types | parquet_types:
        raise SystemExit(
            f"{workload['name']}: element_type is absent from candidate contract"
        )
    expected = (
        "vortex"
        if batch_rows >= minimum_rows
        and dimensions >= minimum_dimensions
        and element_type in vortex_types
        else "parquet"
    )
    if workload["expected_candidate_format"] != expected:
        raise SystemExit(
            f"{workload['name']}: expected_candidate_format="
            f"{workload['expected_candidate_format']} but runtime rule selects {expected}"
        )
expected = (
    int(protocol["repetitions"])
    * len(protocol["workloads"])
    * len(protocol["backends"])
    * 2
)
required = int(protocol["promotion_gates"]["required_complete_cases"])
if expected != required:
    raise SystemExit(
        f"protocol case count is inconsistent: schedule={expected} gate={required}"
    )
real_datasets = {
    workload.get("dataset", "")
    for workload in protocol["workloads"]
    if workload.get("dataset", "")
}
required_real = int(protocol["promotion_gates"].get("required_real_datasets", 0))
if len(real_datasets) != required_real:
    raise SystemExit(
        f"protocol has {len(real_datasets)} real datasets; gate requires {required_real}"
    )
if not real_datasets.issubset(protocol.get("dataset_contracts", {})):
    raise SystemExit("real workload dataset is absent from dataset_contracts")
print(protocol["repetitions"], protocol["queries_per_case"], expected)
PY
)

printf '%s\n' \
  "repetition_id,workload,backend,arm,arm_position,rows,dimensions,batch_rows,element_type,metric,dataset,expected_candidate_format,case_id" \
  > "$ROOT/schedule.csv"
python3 - "$PROTOCOL" >> "$ROOT/schedule.csv" <<'PY'
import csv
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    protocol = json.load(handle)
arms = [protocol["baseline_arm"], protocol["candidate_arm"]]
writer = csv.writer(sys.stdout, lineterminator="\n")
for repetition in range(1, int(protocol["repetitions"]) + 1):
    repetition_id = f"r{repetition:02d}"
    for workload_index, workload in enumerate(protocol["workloads"]):
        for backend_index, backend in enumerate(protocol["backends"]):
            offset = (repetition - 1 + workload_index + backend_index) % len(arms)
            for position in range(len(arms)):
                arm = arms[(position + offset) % len(arms)]
                case_id = "/".join(
                    [repetition_id, workload["name"], backend, arm]
                )
                writer.writerow(
                    [
                        repetition_id,
                        workload["name"],
                        backend,
                        arm,
                        position,
                        workload["rows"],
                        workload["dimensions"],
                        workload["batch_rows"],
                        workload["element_type"],
                        workload["metric"],
                        workload.get("dataset", ""),
                        workload["expected_candidate_format"],
                        case_id,
                    ]
                )
PY

actual_cases="$(( $(wc -l < "$ROOT/schedule.csv") - 1 ))"
if [[ "$actual_cases" != "$EXPECTED_CASES" ]]; then
  echo "schedule has $actual_cases cases; expected $EXPECTED_CASES" >&2
  exit 4
fi
if [[ "$EXECUTE" != "1" ]]; then
  echo "WAL layout qualification dry run: $ROOT/schedule.csv ($actual_cases cases)"
  exit 0
fi

: "${BORSUK_SOURCE_SHA256:?set the exact source archive SHA-256}"
: "${BORSUK_WAL_LAYOUT_BUCKET:?set the existing S3 bucket}"
: "${BORSUK_WAL_LAYOUT_DATASETS:?set the prepared standard-dataset root}"
: "${BORSUK_INSTANCE_ID:?set the EC2 instance identity}"
: "${BORSUK_INSTANCE_TYPE:?set the EC2 instance type}"
: "${BORSUK_AMI_ID:?set the EC2 image identity}"
: "${BORSUK_LOCAL_DISK_CLASS:?set the local disk contract}"
[[ "$BORSUK_SOURCE_SHA256" =~ ^[[:xdigit:]]{64}$ ]] || {
  echo "BORSUK_SOURCE_SHA256 must be a 64-character digest" >&2
  exit 2
}

python3 - \
  "$PROTOCOL" \
  "$BORSUK_INSTANCE_TYPE" \
  "$(uname -m)" \
  "$REGION" \
  "$BORSUK_LOCAL_DISK_CLASS" <<'PY'
import json
import sys

protocol_path, instance_type, architecture, region, disk_class = sys.argv[1:]
with open(protocol_path, encoding="utf-8") as handle:
    contract = json.load(handle)["hardware_contract"]
for field, actual in (
    ("instance_type", instance_type),
    ("architecture", architecture),
    ("aws_region", region),
    ("local_disk_class", disk_class),
):
    expected = contract[field]
    if actual != expected:
        raise SystemExit(f"protocol requires {field}={expected}; got {actual}")
PY

RESULT_PREFIX="${BORSUK_WAL_LAYOUT_RESULT_PREFIX:-layout-qualification/wal-results/$RUN_ID}"
INDEX_PREFIX="${BORSUK_WAL_LAYOUT_INDEX_PREFIX:-layout-qualification/wal-indexes/$RUN_ID}"
for prefix in "$RESULT_PREFIX" "$INDEX_PREFIX"; do
  existing="$(
    aws --region "$REGION" s3api list-objects-v2 \
      --bucket "$BORSUK_WAL_LAYOUT_BUCKET" \
      --prefix "${prefix%/}/" \
      --max-keys 1 \
      --query KeyCount \
      --output text
  )"
  if [[ "$existing" != "0" ]]; then
    echo "refusing to reuse non-empty S3 prefix: $prefix" >&2
    exit 3
  fi
done

cpu_model="$(
  lscpu 2>/dev/null |
    sed -n 's/^Model name:[[:space:]]*//p' |
    head -n 1 ||
    true
)"
memory_bytes="$(
  awk '/^MemTotal:/ {printf "%.0f\n", $2 * 1024}' /proc/meminfo
)"
printf '%s\n' \
  "created_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  "run_id=$RUN_ID" \
  "source_sha256=$BORSUK_SOURCE_SHA256" \
  "aws_region=$REGION" \
  "instance_id=$BORSUK_INSTANCE_ID" \
  "instance_type=$BORSUK_INSTANCE_TYPE" \
  "ami_id=$BORSUK_AMI_ID" \
  "local_disk_class=$BORSUK_LOCAL_DISK_CLASS" \
  "architecture=$(uname -m)" \
  "cpu_model=${cpu_model:-unavailable}" \
  "logical_cpus=$(getconf _NPROCESSORS_ONLN)" \
  "memory_bytes=$memory_bytes" \
  "kernel=$(uname -srmo)" \
  "rustc_version=$(rustc --version)" \
  "repetitions=$REPETITIONS" \
  "queries_per_case=$QUERIES" \
  "expected_cases=$EXPECTED_CASES" \
  "result_prefix=$RESULT_PREFIX" \
  "index_prefix=$INDEX_PREFIX" \
  > "$ROOT/environment.txt"

python3 scripts/freeze_layout_dataset_identity.py \
  --dataset-root "$BORSUK_WAL_LAYOUT_DATASETS" \
  --protocol "$ROOT/qualification-protocol.json" \
  --output "$ROOT/dataset-identities.json"
dataset_identity_sha256="$(
  sha256sum "$ROOT/dataset-identities.json" | awk '{print $1}'
)"
printf '%s\n' \
  "dataset_identity_sha256=$dataset_identity_sha256" \
  >> "$ROOT/environment.txt"

sync_results() {
  aws --region "$REGION" s3 sync \
    "$ROOT" \
    "s3://$BORSUK_WAL_LAYOUT_BUCKET/$RESULT_PREFIX" \
    --exclude '*/cache/*' \
    --exclude '*/scratch/*' \
    --exclude '*/index/*' \
    --only-show-errors
}
trap sync_results EXIT
sync_results

cargo build --locked --release -p borsuk --example wal_layout_bench

tail -n +2 "$ROOT/schedule.csv" |
while IFS=, read -r repetition_id workload backend arm arm_position rows dimensions batch_rows element_type metric dataset expected_candidate_format case_id; do
  case_root="$ROOT/$case_id"
  mkdir -p "$case_root"
  output="$case_root/result.csv"
  cache_root="$case_root/cache"
  scratch_root="$case_root/scratch"
  mkdir -p "$cache_root" "$scratch_root"

  if [[ "$backend" == "s3" ]]; then
    index_uri="s3://$BORSUK_WAL_LAYOUT_BUCKET/$INDEX_PREFIX/$case_id"
    uri_env=("BORSUK_WAL_AB_URI=$index_uri")
  else
    index_uri="$case_root/index"
    uri_env=("BORSUK_WAL_AB_ROOT=$index_uri")
  fi
  if [[ "$arm" == "fixed-parquet" ]]; then
    policy=parquet
  elif [[ "$arm" == "adaptive-candidate" ]]; then
    policy=adaptive
  else
    echo "unknown WAL qualification arm: $arm" >&2
    exit 5
  fi
  dataset_env=()
  if [[ -n "$dataset" ]]; then
    dataset_dir="$BORSUK_WAL_LAYOUT_DATASETS/$dataset"
    [[ -d "$dataset_dir" ]] || {
      echo "missing prepared dataset: $dataset_dir" >&2
      exit 4
    }
    dataset_env=("BORSUK_WAL_AB_DATASET=$dataset_dir")
  fi

  env -u BORSUK_WAL_AB_DATASET \
    "${dataset_env[@]}" \
    "${uri_env[@]}" \
    AWS_REGION="$REGION" \
    AWS_DEFAULT_REGION="$REGION" \
    BORSUK_WAL_AB_FORMAT="$policy" \
    BORSUK_WAL_AB_ELEMENT_TYPE="$element_type" \
    BORSUK_WAL_AB_METRIC="$metric" \
    BORSUK_WAL_AB_REPETITION="$repetition_id" \
    BORSUK_WAL_AB_ROWS="$rows" \
    BORSUK_WAL_AB_DIMENSIONS="$dimensions" \
    BORSUK_WAL_AB_BATCH_ROWS="$batch_rows" \
    BORSUK_WAL_AB_QUERIES="$QUERIES" \
    BORSUK_WAL_AB_OUTPUT="$output" \
    python3 scripts/benchmark_with_resources.py \
      --output "$case_root/resources.csv" \
      --cache-dir "$cache_root" \
      --scratch-dir "$scratch_root" \
      -- target/release/examples/wal_layout_bench

  python3 scripts/validate_wal_layout_qualification.py \
    --case "$output" \
    --arm "$arm" \
    --expected-candidate-format "$expected_candidate_format"

  printf '%s\n' \
    "source_sha256=$BORSUK_SOURCE_SHA256" \
    "dataset_identity_sha256=$dataset_identity_sha256" \
    "queries_per_case=$QUERIES" \
    "repetition_id=$repetition_id" \
    "workload=$workload" \
    "backend=$backend" \
    "arm=$arm" \
    "arm_position=$arm_position" \
    "rows=$rows" \
    "dimensions=$dimensions" \
    "batch_rows=$batch_rows" \
    "element_type=$element_type" \
    "metric=$metric" \
    "dataset=$dataset" \
    "expected_candidate_format=$expected_candidate_format" \
    "index_uri=$index_uri" \
    > "$case_root/protocol.txt"
  printf 'complete\n' > "$case_root/CASE_COMPLETE"
  sync_results

  if [[ "${BORSUK_WAL_LAYOUT_KEEP_CASE_DATA:-0}" != "1" ]]; then
    rm -rf "$cache_root" "$scratch_root"
    if [[ "$backend" == "local-disk" ]]; then
      rm -rf "$case_root/index"
    fi
  fi
done

python3 scripts/validate_wal_layout_qualification.py \
  --root "$ROOT" \
  --protocol "$ROOT/qualification-protocol.json" \
  --output "$ROOT/qualification-cases.csv"
python3 scripts/analyze_wal_layout_qualification.py \
  --cases "$ROOT/qualification-cases.csv" \
  --protocol "$ROOT/qualification-protocol.json" \
  --output "$ROOT/wal-layout-decisions.csv"
printf 'complete\n' > "$ROOT/WAL_LAYOUT_QUALIFICATION_COMPLETE"
sync_results
