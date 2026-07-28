#!/usr/bin/env bash
# Stage-one physical-format qualification. It deliberately stops before the
# full BORSUK publication matrix so an unresolved format choice cannot
# invalidate expensive results.
set -euo pipefail

: "${BORSUK_S3_BUCKET:?set BORSUK_S3_BUCKET to the bucket name, without s3://}"
: "${BORSUK_FORMAT_RUN_ID:?set BORSUK_FORMAT_RUN_ID}"

REGION="${AWS_REGION:-eu-central-1}"
ROOT="${BORSUK_FORMAT_ROOT:-/home/ec2-user/borsuk-format-results/$BORSUK_FORMAT_RUN_ID}"
S3_RESULT_PREFIX="${BORSUK_FORMAT_RESULT_PREFIX:-format-qualification/results/$BORSUK_FORMAT_RUN_ID}"
S3_DATA_PREFIX="${BORSUK_FORMAT_DATA_PREFIX:-format-qualification/data/$BORSUK_FORMAT_RUN_ID}"
PYTHON="${BORSUK_FORMAT_PYTHON:-.venv-format/bin/python}"
REPETITIONS="${BORSUK_FORMAT_REPETITIONS:-30}"
WARMUPS="${BORSUK_FORMAT_WARMUPS:-3}"
METRIC_PROPAGATION_SECONDS="${BORSUK_S3_METRIC_PROPAGATION_SECONDS:-900}"

cd "$(dirname "$0")/.."
mkdir -p "$ROOT"
exec > >(tee -a "$ROOT/campaign.log") 2>&1
{
  printf '%s\n' \
    "captured_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "source_sha256=${BORSUK_SOURCE_SHA256:-unknown}" \
    "instance_type=${BORSUK_INSTANCE_TYPE:-unknown}" \
    "local_disk_class=${BORSUK_LOCAL_DISK_CLASS:-unknown}" \
    "kernel=$(uname -srmo)" \
    "logical_cpus=$(getconf _NPROCESSORS_ONLN)" \
    "memory_kib=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
  lsblk -o NAME,TYPE,SIZE,ROTA,MOUNTPOINTS,FSTYPE
} > "$ROOT/environment.txt"
aws --region "$REGION" s3 cp \
  "$ROOT/environment.txt" \
  "s3://$BORSUK_S3_BUCKET/$S3_RESULT_PREFIX/environment.txt" \
  --only-show-errors
S3_METRIC_REGISTRY="$ROOT/s3-metric-windows.csv"
if [[ ! -f "$S3_METRIC_REGISTRY" ]]; then
  printf '%s\n' 'case,filter_id,prefix,start_epoch,end_epoch' > "$S3_METRIC_REGISTRY"
fi

printf '%s\n' "run_id=$BORSUK_FORMAT_RUN_ID" "region=$REGION" "root=$ROOT"
printf '%s\n' "source_sha256=${BORSUK_SOURCE_SHA256:-unknown}"

if [[ ! -x "$PYTHON" ]]; then
  command -v uv >/dev/null 2>&1 || {
    echo "uv is required to install the pinned Python 3.13 format environment" >&2
    exit 2
  }
  uv venv .venv-format --python 3.13
fi
uv pip install --python "$PYTHON" -r scripts/requirements-format-bench.txt

echo "running physical-format dependency preflight"
"$PYTHON" - <<'PY'
import tempfile
from pathlib import Path
import sys

sys.path.insert(0, "scripts")
from benchmark_table_formats import create_table, load_dependencies, write_format

_, _, _, parquet = load_dependencies()
source = create_table(10_000, 64, 0xB05, "variable")
with tempfile.TemporaryDirectory(prefix="borsuk-format-preflight-") as root:
    root = Path(root)
    for format_name in ("parquet", "vortex-default", "vortex-compact"):
        suffix = "parquet" if format_name == "parquet" else "vortex"
        path = root / f"{format_name}.{suffix}"
        write_format(format_name, source, path, 8_192)
        if format_name == "parquet":
            decoded = parquet.read_table(path)
        else:
            import vortex as vx

            decoded = vx.open(str(path)).scan().read_all().to_arrow_table()
            # Vortex deliberately exposes variable Binary/UTF-8 as Arrow view
            # types. Cast back to the declared source schema before comparing
            # values; the dedicated compatibility gate records type changes.
            decoded = decoded.cast(source.schema)
        if not decoded.equals(source):
            raise SystemExit(
                f"dependency preflight corrupted {format_name}; "
                "format qualification refused"
            )
print("physical-format dependency preflight passed")
PY

metric_filter_id() {
  local family="$1"
  local profile="$2"
  local format_name="$3"
  printf 'borsuk-%s' \
    "$(printf '%s' "$BORSUK_FORMAT_RUN_ID-$family-$profile-$format_name" \
      | sha256sum | cut -c1-20)"
}

metric_prefix() {
  local family="$1"
  local profile="$2"
  local format_name="$3"
  printf '%s/%s/%s/%s' "$S3_DATA_PREFIX" "$family" "$profile" "$format_name"
}

install_metric_filter() {
  local family="$1"
  local profile="$2"
  local format_name="$3"
  local filter_id
  local prefix
  local configuration
  filter_id="$(metric_filter_id "$family" "$profile" "$format_name")"
  prefix="$(metric_prefix "$family" "$profile" "$format_name")"
  configuration="$(printf '{"Id":"%s","Filter":{"Prefix":"%s"}}' \
    "$filter_id" "$prefix")"
  aws --region "$REGION" s3api put-bucket-metrics-configuration \
    --bucket "$BORSUK_S3_BUCKET" \
    --id "$filter_id" \
    --metrics-configuration "$configuration"
}

cleanup_metric_filters() {
  local format_name
  local profile
  local filter_id
  for format_name in parquet vortex-default vortex-compact; do
    filter_id="$(metric_filter_id table table-1m "$format_name")"
    aws --region "$REGION" s3api delete-bucket-metrics-configuration \
      --bucket "$BORSUK_S3_BUCKET" \
      --id "$filter_id" >/dev/null 2>&1 || true
  done
  for profile in vector-250k-128 vector-25k-960; do
    for format_name in arrow-ipc vortex-default vortex-compact; do
      filter_id="$(metric_filter_id ann "$profile" "$format_name")"
      aws --region "$REGION" s3api delete-bucket-metrics-configuration \
        --bucket "$BORSUK_S3_BUCKET" \
        --id "$filter_id" >/dev/null 2>&1 || true
    done
  done
}
trap cleanup_metric_filters EXIT

metric_filters_installed=0
for format_name in parquet vortex-default vortex-compact; do
  if [[ ! -f "$ROOT/table/table-1m/s3/$format_name/validated.ok" ]]; then
    install_metric_filter table table-1m "$format_name"
    metric_filters_installed=1
  fi
done
for profile in vector-250k-128 vector-25k-960; do
  for format_name in arrow-ipc vortex-default vortex-compact; do
    if [[ ! -f "$ROOT/ann/$profile/s3/$format_name/validated.ok" ]]; then
      install_metric_filter ann "$profile" "$format_name"
      metric_filters_installed=1
    fi
  done
done
if [[ "$metric_filters_installed" == "1" && "$METRIC_PROPAGATION_SECONDS" -gt 0 ]]; then
  echo "waiting ${METRIC_PROPAGATION_SECONDS}s for S3 request-metric filters to propagate"
  sleep "$METRIC_PROPAGATION_SECONDS"
fi

run_case() {
  local family="$1"
  local profile="$2"
  local backend="$3"
  local format_name="$4"
  shift 4
  local case_root="$ROOT/$family/$profile/$backend/$format_name"
  local result_root="$case_root/results"
  local marker="$case_root/validated.ok"
  if [[ -f "$marker" ]]; then
    echo "skip validated case $family/$profile/$backend/$format_name"
    return
  fi
  if [[ -e "$case_root" ]]; then
    echo "partial case exists; refusing to mix repetitions: $case_root" >&2
    exit 3
  fi
  mkdir -p "$result_root" "$case_root/scratch"
  local backend_args=(--backend "$backend")
  local metric_id=""
  local metric_prefix=""
  local metric_start=""
  if [[ "$backend" == "s3" ]]; then
    metric_prefix="$(metric_prefix "$family" "$profile" "$format_name")"
    metric_id="$(metric_filter_id "$family" "$profile" "$format_name")"
    metric_start="$(date -u +%s)"
    backend_args+=(
      --s3-bucket "$BORSUK_S3_BUCKET"
      --s3-prefix "$metric_prefix"
      --aws-region "$REGION"
      --cache-profile uncached
      --vortex-without-segment-cache
    )
  else
    backend_args+=(--cache-profile disk_cached)
  fi
  "$PYTHON" scripts/benchmark_with_resources.py \
    --output "$result_root/resources.csv" \
    --scratch-dir "$case_root/scratch" \
    --interval-ms 100 \
    -- "$PYTHON" "$@" \
      --output-dir "$result_root" \
      --formats "$format_name" \
      --repetitions "$REPETITIONS" \
      --warmups "$WARMUPS" \
      "${backend_args[@]}"
  if [[ "$backend" == "s3" ]]; then
    printf '%s\n' \
      "$family/$profile/$format_name,$metric_id,$metric_prefix,$metric_start,$(date -u +%s)" \
      >> "$S3_METRIC_REGISTRY"
  fi
  "$PYTHON" scripts/validate_format_qualification.py \
    --directory "$result_root" \
    --expected-samples "$REPETITIONS"
  "$PYTHON" scripts/render_resource_charts.py \
    --experiment-root "$result_root" \
    --output-dir "$case_root/charts" \
    --prefix "$family-$profile-$backend-$format_name"
  printf '%s\n' "validated $(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$marker"
  aws --region "$REGION" s3 sync \
    "$case_root" \
    "s3://$BORSUK_S3_BUCKET/$S3_RESULT_PREFIX/$family/$profile/$backend/$format_name" \
    --only-show-errors
}

for backend in local_disk s3; do
  for format_name in parquet vortex-default vortex-compact; do
    run_case table table-1m "$backend" "$format_name" \
      scripts/benchmark_table_formats.py \
      --rows 1000000 \
      --code-width 64 \
      --code-type variable \
      --row-group-rows 8192
  done

  for format_name in arrow-ipc vortex-default vortex-compact; do
    run_case ann vector-250k-128 "$backend" "$format_name" \
      scripts/benchmark_vector_formats.py \
      --rows 250000 \
      --dimensions 128 \
      --element-type float32 \
      --batch-rows 32768 \
      --selected-rows 10,100,1000 \
      --patterns clustered,scattered
    run_case ann vector-25k-960 "$backend" "$format_name" \
      scripts/benchmark_vector_formats.py \
      --rows 25000 \
      --dimensions 960 \
      --element-type float32 \
      --batch-rows 4369 \
      --selected-rows 10,100,1000 \
      --patterns clustered,scattered
  done
done

collect_metric_sum() {
  local metric="$1"
  local filter_id="$2"
  local start_epoch="$3"
  local end_epoch="$4"
  aws --region "$REGION" cloudwatch get-metric-statistics \
    --namespace AWS/S3 \
    --metric-name "$metric" \
    --dimensions \
      "Name=BucketName,Value=$BORSUK_S3_BUCKET" \
      "Name=FilterId,Value=$filter_id" \
    --start-time "$(date -u -d "@$((start_epoch - 120))" +%Y-%m-%dT%H:%M:%SZ)" \
    --end-time "$(date -u -d "@$((end_epoch + 300))" +%Y-%m-%dT%H:%M:%SZ)" \
    --period 60 \
    --statistics Sum \
    --query 'sum(Datapoints[].Sum)' \
    --output text
}

metrics_ready=0
for _ in $(seq 1 30); do
  metrics_ready=1
  while IFS=, read -r _case filter_id _prefix start_epoch end_epoch; do
    [[ "$filter_id" == "filter_id" ]] && continue
    all_requests="$(collect_metric_sum AllRequests "$filter_id" "$start_epoch" "$end_epoch")"
    if [[ "$all_requests" == "0" || "$all_requests" == "0.0" || "$all_requests" == "None" ]]; then
      metrics_ready=0
      break
    fi
  done < "$S3_METRIC_REGISTRY"
  [[ "$metrics_ready" == "1" ]] && break
  sleep 60
done
if [[ "$metrics_ready" != "1" ]]; then
  echo "S3 CloudWatch request metrics did not arrive; decision checkpoint withheld" >&2
  exit 5
fi

S3_METRIC_OUTPUT="$ROOT/s3-request-metrics.csv"
printf '%s\n' \
  'case,filter_id,prefix,get_requests,head_requests,all_requests,bytes_downloaded' \
  > "$S3_METRIC_OUTPUT"
while IFS=, read -r case_name filter_id metric_prefix start_epoch end_epoch; do
  [[ "$filter_id" == "filter_id" ]] && continue
  get_requests="$(collect_metric_sum GetRequests "$filter_id" "$start_epoch" "$end_epoch")"
  head_requests="$(collect_metric_sum HeadRequests "$filter_id" "$start_epoch" "$end_epoch")"
  all_requests="$(collect_metric_sum AllRequests "$filter_id" "$start_epoch" "$end_epoch")"
  bytes_downloaded="$(collect_metric_sum BytesDownloaded "$filter_id" "$start_epoch" "$end_epoch")"
  printf '%s\n' \
    "$case_name,$filter_id,$metric_prefix,$get_requests,$head_requests,$all_requests,$bytes_downloaded" \
    >> "$S3_METRIC_OUTPUT"
  aws --region "$REGION" s3api delete-bucket-metrics-configuration \
    --bucket "$BORSUK_S3_BUCKET" \
    --id "$filter_id"
done < "$S3_METRIC_REGISTRY"
aws --region "$REGION" s3 cp \
  "$S3_METRIC_OUTPUT" \
  "s3://$BORSUK_S3_BUCKET/$S3_RESULT_PREFIX/s3-request-metrics.csv" \
  --only-show-errors

compat_root="$ROOT/table/type-compatibility/local_disk/all"
if [[ ! -f "$compat_root/validated.ok" ]]; then
  if [[ -e "$compat_root" ]]; then
    echo "partial compatibility case exists: $compat_root" >&2
    exit 3
  fi
  "$PYTHON" scripts/probe_table_format_compatibility.py \
    --output-dir "$compat_root/results" \
    --rows 128 \
    --formats parquet,vortex-default,vortex-compact
  "$PYTHON" - "$compat_root/results/compatibility.csv" <<'PY'
import csv
import sys

with open(sys.argv[1], newline="") as handle:
    rows = list(csv.DictReader(handle))
cases = {row["case"] for row in rows}
formats = {row["format"] for row in rows}
if len(cases) != 15 or formats != {"parquet", "vortex-default", "vortex-compact"}:
    raise SystemExit(f"incomplete compatibility matrix: cases={len(cases)}, formats={formats}")
if len(rows) != len(cases) * len(formats):
    raise SystemExit(f"duplicate or missing compatibility cells: {len(rows)}")
parquet_failures = [
    row for row in rows if row["format"] == "parquet" and row["status"] != "complete"
]
if parquet_failures:
    raise SystemExit(f"Parquet control failed compatibility: {parquet_failures}")
PY
  printf '%s\n' "validated $(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    > "$compat_root/validated.ok"
  aws --region "$REGION" s3 sync \
    "$compat_root" \
    "s3://$BORSUK_S3_BUCKET/$S3_RESULT_PREFIX/table/type-compatibility" \
    --only-show-errors
fi

vector_compat_root="$ROOT/ann/type-compatibility/local_disk/all"
if [[ ! -f "$vector_compat_root/validated.ok" ]]; then
  if [[ -e "$vector_compat_root" ]]; then
    echo "partial vector compatibility case exists: $vector_compat_root" >&2
    exit 3
  fi
  "$PYTHON" scripts/probe_vector_format_compatibility.py \
    --output-dir "$vector_compat_root/results" \
    --rows 128 \
    --dimensions 64 \
    --formats arrow-ipc,vortex-default,vortex-compact
  "$PYTHON" - "$vector_compat_root/results/compatibility.csv" <<'PY'
import csv
import sys

with open(sys.argv[1], newline="") as handle:
    rows = list(csv.DictReader(handle))
cases = {row["case"] for row in rows}
formats = {row["format"] for row in rows}
expected_cases = {"float32", "float16", "bfloat16", "int8", "binary"}
expected_formats = {"arrow-ipc", "vortex-default", "vortex-compact"}
if cases != expected_cases or formats != expected_formats:
    raise SystemExit(f"incomplete vector compatibility matrix: cases={cases}, formats={formats}")
if len(rows) != len(cases) * len(formats):
    raise SystemExit(f"duplicate or missing vector compatibility cells: {len(rows)}")
arrow_failures = [
    row for row in rows if row["format"] == "arrow-ipc" and row["status"] != "complete"
]
if arrow_failures:
    raise SystemExit(f"Arrow IPC control failed vector compatibility: {arrow_failures}")
PY
  printf '%s\n' "validated $(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    > "$vector_compat_root/validated.ok"
  aws --region "$REGION" s3 sync \
    "$vector_compat_root" \
    "s3://$BORSUK_S3_BUCKET/$S3_RESULT_PREFIX/ann/type-compatibility" \
    --only-show-errors
fi

printf '%s\n' \
  "Format qualification completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)." \
  "Review Parquet versus Vortex table evidence and Arrow versus Vortex ANN evidence." \
  "No full BORSUK publication benchmark was started." \
  > "$ROOT/FORMAT_DECISION_REQUIRED"
aws --region "$REGION" s3 cp \
  "$ROOT/FORMAT_DECISION_REQUIRED" \
  "s3://$BORSUK_S3_BUCKET/$S3_RESULT_PREFIX/FORMAT_DECISION_REQUIRED" \
  --only-show-errors
echo "FORMAT_DECISION_REQUIRED"
