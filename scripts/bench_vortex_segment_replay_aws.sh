#!/usr/bin/env bash
# Corrected real-BORSUK Parquet/Vortex replay. Latency is publishable only
# under the materialized_arrow contract; compressed-native len-only timing is
# deliberately unavailable here.
set -euo pipefail

: "${BORSUK_S3_BUCKET:?set BORSUK_S3_BUCKET to the existing bucket name}"
: "${BORSUK_VORTEX_RUN_ID:?set BORSUK_VORTEX_RUN_ID}"
: "${BORSUK_VORTEX_SOURCE_URI:?set BORSUK_VORTEX_SOURCE_URI to an existing s3:// index prefix}"
: "${BORSUK_VORTEX_RESULT_PREFIX:?set a fresh BORSUK_VORTEX_RESULT_PREFIX}"
: "${BORSUK_VORTEX_DATA_PREFIX:?set a fresh BORSUK_VORTEX_DATA_PREFIX}"
: "${BORSUK_SOURCE_SHA256:?set BORSUK_SOURCE_SHA256 to the exact source archive digest}"

REGION="${AWS_REGION:-eu-central-1}"
ROOT="${BORSUK_VORTEX_ROOT:-/home/ec2-user/borsuk-vortex-segment-replay/$BORSUK_VORTEX_RUN_ID}"
PYTHON="${BORSUK_VORTEX_PYTHON:-.venv-vortex-replay/bin/python}"
LAUNCHED_INSTANCE="${BORSUK_VORTEX_LAUNCHED_INSTANCE:-0}"
SHUTDOWN="${BORSUK_VORTEX_SHUTDOWN:-0}"

cd "$(dirname "$0")/.."

case "$BORSUK_VORTEX_SOURCE_URI" in
  s3://*/*) ;;
  *)
    echo "BORSUK_VORTEX_SOURCE_URI must be a non-empty s3://bucket/prefix" >&2
    exit 2
    ;;
esac
if [[ ! "$BORSUK_SOURCE_SHA256" =~ ^[[:xdigit:]]{64}$ ]]; then
  echo "BORSUK_SOURCE_SHA256 must be a 64-character SHA-256 digest" >&2
  exit 2
fi
if [[ -e "$ROOT" ]]; then
  echo "refusing to overwrite existing local result root: $ROOT" >&2
  exit 3
fi

source_location="${BORSUK_VORTEX_SOURCE_URI#s3://}"
source_bucket="${source_location%%/*}"
source_prefix="${source_location#*/}"
aws --region "$REGION" s3api head-bucket --bucket "$BORSUK_S3_BUCKET"
aws --region "$REGION" s3api head-bucket --bucket "$source_bucket"
source_parquet_count="$(aws --region "$REGION" s3api list-objects-v2 \
  --bucket "$source_bucket" \
  --prefix "$source_prefix/segments/" \
  --query 'length(Contents[?ends_with(Key, `.parquet`)])' \
  --output text)"
if [[ "$source_parquet_count" == "None" || "$source_parquet_count" -lt 1 ]]; then
  echo "source prefix has no segment Parquet objects: $BORSUK_VORTEX_SOURCE_URI" >&2
  exit 4
fi

for fresh_prefix in "$BORSUK_VORTEX_RESULT_PREFIX" "$BORSUK_VORTEX_DATA_PREFIX"; do
  existing="$(aws --region "$REGION" s3api list-objects-v2 \
    --bucket "$BORSUK_S3_BUCKET" \
    --prefix "${fresh_prefix%/}/" \
    --max-keys 1 \
    --query 'KeyCount' \
    --output text)"
  if [[ "$existing" != "0" ]]; then
    echo "refusing to overwrite non-empty S3 prefix: s3://$BORSUK_S3_BUCKET/$fresh_prefix" >&2
    exit 3
  fi
done

mkdir -p "$ROOT"
exec > >(tee -a "$ROOT/campaign.log") 2>&1

finish() {
  local status=$?
  aws --region "$REGION" s3 sync \
    "$ROOT" \
    "s3://$BORSUK_S3_BUCKET/$BORSUK_VORTEX_RESULT_PREFIX" \
    --only-show-errors || true
  if [[ "$LAUNCHED_INSTANCE" == "1" && "$SHUTDOWN" == "1" ]]; then
    sudo shutdown -h now >/dev/null 2>&1 || true
  fi
  return "$status"
}
trap finish EXIT

{
  printf '%s\n' \
    "captured_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "run_id=$BORSUK_VORTEX_RUN_ID" \
    "source_sha256=$BORSUK_SOURCE_SHA256" \
    "source_uri=$BORSUK_VORTEX_SOURCE_URI" \
    "source_segment_parquet_objects=$source_parquet_count" \
    "execution_mode=materialized_arrow" \
    "formats=parquet,vortex-default,vortex-compact" \
    "families=segments" \
    "warmups=3" \
    "samples=30" \
    "resource_scope=process-tree_cpu_ram_disk;exclusive-worker_network" \
    "tmux_version=${BORSUK_TMUX_VERSION:-unknown}" \
    "tmux_provisioning=${BORSUK_TMUX_PROVISIONING:-unknown}" \
    "uv_version=${BORSUK_UV_VERSION:-unknown}" \
    "uv_provisioning=${BORSUK_UV_PROVISIONING:-unknown}" \
    "pip_provisioning=${BORSUK_PIP_PROVISIONING:-unknown}" \
    "instance_type=${BORSUK_INSTANCE_TYPE:-unknown}" \
    "local_disk_class=${BORSUK_LOCAL_DISK_CLASS:-unknown}" \
    "kernel=$(uname -srmo)" \
    "logical_cpus=$(getconf _NPROCESSORS_ONLN)" \
    "memory_kib=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
  lsblk -o NAME,TYPE,SIZE,ROTA,MOUNTPOINTS,FSTYPE
} > "$ROOT/environment.txt"
printf '%s\n' \
  "Only value-consuming materialized_arrow samples are valid in this campaign." \
  "No compressed_native or len-only Vortex latency is produced or published." \
  > "$ROOT/measurement-contract.txt"

command -v uv >/dev/null 2>&1 || {
  echo "uv is required to install the pinned Python 3.13 environment" >&2
  exit 2
}
uv venv --clear .venv-vortex-replay --python 3.13
uv pip install --python "$PYTHON" \
  numpy==2.4.2 \
  pyarrow==24.0.0 \
  vortex-data==0.81.0

"$PYTHON" - <<'PY' | tee "$ROOT/dependency-preflight.txt"
from importlib.metadata import version
import sys

if sys.version_info[:2] != (3, 13):
    raise SystemExit(f"expected Python 3.13, got {sys.version}")
import pyarrow as pa
import vortex as vx

versions = {
    "pyarrow": pa.__version__,
    "vortex-data": version("vortex-data"),
}
if versions != {"pyarrow": "24.0.0", "vortex-data": "0.81.0"}:
    raise SystemExit(f"dependency pin mismatch: {versions}")
if not hasattr(vx.io.VortexWriteOptions, "default"):
    raise SystemExit("Vortex 0.81 default writer API unavailable")
if not hasattr(vx.io.VortexWriteOptions, "compact"):
    raise SystemExit("Vortex 0.81 compact writer API unavailable")
print(f"python={sys.version.split()[0]}")
for name, value in versions.items():
    print(f"{name}={value}")
print("materialized_arrow_only=true")
PY

result_root="$ROOT/replay"
mkdir -p "$result_root" "$ROOT/scratch"
"$PYTHON" scripts/benchmark_with_resources.py \
  --output "$ROOT/resources.csv" \
  --scratch-dir "$ROOT/scratch" \
  --interval-ms 100 \
  --cache-interval-ms 1000 \
  -- "$PYTHON" scripts/benchmark_borsuk_table_formats.py \
    "$BORSUK_VORTEX_SOURCE_URI" \
    --output-dir "$result_root" \
    --aws-region "$REGION" \
    --families segments \
    --formats parquet,vortex-default,vortex-compact \
    --execution-modes materialized_arrow \
    --s3-materialized-prefix "$BORSUK_VORTEX_DATA_PREFIX" \
    --vortex-without-segment-cache \
    --warmups 3 \
    --repetitions 30

"$PYTHON" - "$result_root/samples.csv" "$result_root/summary.csv" "$ROOT/resources.csv" <<'PY'
import csv
from pathlib import Path
import sys

samples_path, summary_path, resources_path = map(Path, sys.argv[1:])
for path in (samples_path, summary_path, resources_path):
    if not path.is_file() or path.stat().st_size == 0:
        raise SystemExit(f"missing or empty required CSV: {path}")

with samples_path.open(newline="") as handle:
    samples = list(csv.DictReader(handle))
if not samples:
    raise SystemExit("samples.csv is empty")
if {row["execution_mode"] for row in samples} != {"materialized_arrow"}:
    raise SystemExit("non-materialized samples are forbidden")
if any(row["status"] != "complete" for row in samples):
    raise SystemExit("blocked or failed replay cells cannot be published")
if {row["family"] for row in samples} != {"segments"}:
    raise SystemExit("the first campaign must contain only segment artifacts")
variants = {(row["format"], row["layout"]) for row in samples}
expected = {("parquet", "source"), ("vortex", "default"), ("vortex", "compact")}
if variants != expected:
    raise SystemExit(f"incomplete format matrix: {variants}")

with summary_path.open(newline="") as handle:
    summaries = list(csv.DictReader(handle))
if not summaries:
    raise SystemExit("summary.csv is empty")
if any(int(row["samples"]) != 30 for row in summaries):
    raise SystemExit("every summary cell must contain exactly 30 timed samples")
for field in ("mean_ms", "stddev_ms", "p50_ms", "p95_ms", "p99_ms"):
    if any(row[field] == "" for row in summaries):
        raise SystemExit(f"missing required latency distribution field: {field}")

with resources_path.open(newline="") as handle:
    resources = list(csv.DictReader(handle))
required_resource_fields = {
    "cpu_percent",
    "rss_bytes",
    "process_read_bytes",
    "process_write_bytes",
    "cache_disk_bytes",
    "scratch_disk_bytes",
    "network_receive_bytes",
    "network_transmit_bytes",
}
if not resources or not required_resource_fields.issubset(resources[0]):
    raise SystemExit("resource CSV does not cover CPU/RAM/disk/network")
if max(float(row["cpu_percent"]) for row in resources) <= 0:
    raise SystemExit("resource CSV did not observe CPU usage")
if max(int(row["rss_bytes"]) for row in resources) <= 0:
    raise SystemExit("resource CSV did not observe RSS")
if max(
    int(row["process_read_bytes"]) + int(row["process_write_bytes"])
    for row in resources
) <= 0:
    raise SystemExit("resource CSV did not observe process-tree disk I/O")
if max(int(row["network_receive_bytes"]) for row in resources) <= 0:
    raise SystemExit("resource CSV did not observe S3 receive traffic")
print(
    f"validated {len(samples)} samples, {len(summaries)} distributions, "
    f"{len(resources)} resource observations"
)
PY

"$PYTHON" scripts/render_resource_charts.py \
  --experiment-root "$ROOT" \
  --output-dir "$ROOT/charts" \
  --prefix vortex-segment-replay-materialized-arrow

"$PYTHON" scripts/render_borsuk_table_format_charts.py \
  --input "$result_root/summary.csv" \
  --output "$ROOT/charts/vortex-segment-replay-table-formats.svg" \
  --title "Corrected real BORSUK segment replay"

resource_chart="$ROOT/charts/vortex-segment-replay-materialized-arrow-experiment.svg"
if [[ ! -s "$resource_chart" ]]; then
  echo "resource chart was not generated: $resource_chart" >&2
  exit 5
fi
for panel in "CPU utilization" "Process memory" "Disk and cache footprint" "Network I/O"; do
  if ! grep -Fq "$panel" "$resource_chart"; then
    echo "resource chart is missing panel: $panel" >&2
    exit 5
  fi
done

table_format_chart="$ROOT/charts/vortex-segment-replay-table-formats.svg"
if [[ ! -s "$table_format_chart" ]]; then
  echo "table-format chart was not generated: $table_format_chart" >&2
  exit 5
fi
for panel in "Storage footprint" "Latency distributions by workload" \
  "materialized_arrow only" "mean ±1 sample SD" "p50" "p95" "p99"; do
  if ! grep -Fq "$panel" "$table_format_chart"; then
    echo "table-format chart is missing evidence: $panel" >&2
    exit 5
  fi
done

aws --region "$REGION" s3 sync \
  "$ROOT" \
  "s3://$BORSUK_S3_BUCKET/$BORSUK_VORTEX_RESULT_PREFIX" \
  --only-show-errors
if aws --region "$REGION" s3api head-object \
  --bucket "$BORSUK_S3_BUCKET" \
  --key "$BORSUK_VORTEX_RESULT_PREFIX/VORTEX_SEGMENT_REPLAY_COMPLETE" \
  >/dev/null 2>&1; then
  echo "refusing to overwrite completion checkpoint" >&2
  exit 3
fi
printf '%s\n' \
  "completed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  "source_sha256=$BORSUK_SOURCE_SHA256" \
  "execution_mode=materialized_arrow" \
  > "$ROOT/VORTEX_SEGMENT_REPLAY_COMPLETE"
aws --region "$REGION" s3 cp \
  "$ROOT/VORTEX_SEGMENT_REPLAY_COMPLETE" \
  "s3://$BORSUK_S3_BUCKET/$BORSUK_VORTEX_RESULT_PREFIX/VORTEX_SEGMENT_REPLAY_COMPLETE" \
  --only-show-errors
echo "VORTEX_SEGMENT_REPLAY_COMPLETE"
