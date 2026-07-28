#!/usr/bin/env bash
# Fresh dense and real-data hybrid qualification on the dedicated AWS worker.
# The runner owns one immutable result/index prefix, syncs evidence on success
# or failure, and shuts the worker down when the detached campaign exits.
set -euo pipefail

: "${BORSUK_PUBLICATION_BUCKET:?set the plain S3 bucket name}"
: "${BORSUK_PUBLICATION_RUN_ID:?set the publication run id}"
: "${BORSUK_SOURCE_SHA256:?set the packaged source SHA-256}"

REGION="${AWS_REGION:-eu-central-1}"
DATASETS="${BORSUK_PUBLICATION_DATASETS:-/home/ec2-user/borsuk-datasets}"
BEIR_SOURCE_ROOT="${BORSUK_PUBLICATION_BEIR_SOURCE_ROOT:-/home/ec2-user/borsuk-beir-source}"
HYBRID_DATASETS="${BORSUK_PUBLICATION_HYBRID_DATASETS:-/home/ec2-user/borsuk-hybrid-datasets/$BORSUK_PUBLICATION_RUN_ID}"
HYBRID_VENV="${BORSUK_PUBLICATION_HYBRID_VENV:-/home/ec2-user/borsuk-hybrid-venv}"
ROOT="${BORSUK_PUBLICATION_ROOT:-/home/ec2-user/borsuk-publication-results/$BORSUK_PUBLICATION_RUN_ID}"
RESULT_PREFIX="${BORSUK_PUBLICATION_RESULT_PREFIX:-publication/results/$BORSUK_PUBLICATION_RUN_ID}"
INDEX_PREFIX="${BORSUK_PUBLICATION_INDEX_PREFIX:-publication/indexes/$BORSUK_PUBLICATION_RUN_ID}"
STATUS="failed"
TOOLCHAIN_BIN="${BORSUK_RUST_TOOLCHAIN_BIN:-/home/ec2-user/.rustup/toolchains/stable-aarch64-unknown-linux-gnu/bin}"
export PATH="$TOOLCHAIN_BIN:/home/ec2-user/.cargo/bin:$PATH"

cd "$(dirname "$0")/.."
if [[ -e "$ROOT" ]]; then
  echo "refusing to overwrite existing publication root: $ROOT" >&2
  exit 3
fi
mkdir -p "$ROOT"
exec > >(tee -a "$ROOT/campaign.log") 2>&1

sync_results() {
  aws --region "$REGION" s3 sync \
    "$ROOT" \
    "s3://$BORSUK_PUBLICATION_BUCKET/$RESULT_PREFIX" \
    --exclude '*/cache/*' \
    --exclude '*/scratch/*' \
    --exclude '*/target/*' \
    --exclude '*/.venv/*' \
    --only-show-errors
}

publish_checkpoint() {
  local checkpoint="$1"
  local checkpoint_temp
  shift
  if aws --region "$REGION" s3api head-object \
    --bucket "$BORSUK_PUBLICATION_BUCKET" \
    --key "$RESULT_PREFIX/$checkpoint" \
    >/dev/null 2>&1; then
    echo "refusing to overwrite completion checkpoint: $checkpoint" >&2
    return 3
  fi
  checkpoint_temp="$(mktemp "/tmp/borsuk-${checkpoint}.XXXXXX")"
  printf '%s\n' \
    "source_sha256=$BORSUK_SOURCE_SHA256" \
    "completed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "$@" \
    > "$checkpoint_temp"
  aws --region "$REGION" s3 cp \
    "$checkpoint_temp" \
    "s3://$BORSUK_PUBLICATION_BUCKET/$RESULT_PREFIX/$checkpoint" \
    --only-show-errors
  mv "$checkpoint_temp" "$ROOT/$checkpoint"
}

finalize() {
  local exit_code=$?
  trap - EXIT
  {
    printf '%s\n' \
      "finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      "status=$STATUS" \
      "exit_code=$exit_code"
  } >> "$ROOT/environment.txt"
  sync_results || true
  sudo shutdown -h +1 >/dev/null 2>&1 || true
  exit "$exit_code"
}
trap finalize EXIT

if [[ ! -x "$TOOLCHAIN_BIN/cargo" || ! -x "$TOOLCHAIN_BIN/rustc" ]]; then
  echo "complete preinstalled Rust toolchain is missing at $TOOLCHAIN_BIN" >&2
  exit 2
fi

{
  printf '%s\n' \
    "started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "source_sha256=$BORSUK_SOURCE_SHA256" \
    "run_id=$BORSUK_PUBLICATION_RUN_ID" \
    "region=$REGION" \
    "instance_type=${BORSUK_INSTANCE_TYPE:-unknown}" \
    "local_disk_class=${BORSUK_LOCAL_DISK_CLASS:-unknown}" \
    "durable_table_format=parquet" \
    "ann_container=arrow-ipc" \
    "hybrid_dense_model=BAAI/bge-small-en-v1.5" \
    "hybrid_dense_revision=5c38ec7c405ec4b44b94cc5a9bb96e735b38267a" \
    "hybrid_repetitions=5" \
    "hybrid_search_points=128:32,256:64,512:128" \
    "hybrid_hot_fractions=0,0.25,0.5,0.75,1" \
    "hybrid_rrf_k=1,5,10,30,60" \
    "arrow_max_gap_bytes=1048576" \
    "arrow_max_physical_range_bytes=4194304" \
    "arrow_max_parallel_ranges=10" \
    "rustc_version=$(rustc --version)" \
    "cargo_version=$(cargo --version)" \
    "kernel=$(uname -srmo)" \
    "logical_cpus=$(getconf _NPROCESSORS_ONLN)" \
    "memory_kib=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
  lsblk -o NAME,TYPE,SIZE,ROTA,MOUNTPOINTS,FSTYPE
} > "$ROOT/environment.txt"

for dataset in \
  fashion-mnist-784 glove-100 sift-128 nytimes-256 gist-960 deep-image-96
do
  if [[ ! -d "$DATASETS/$dataset" ]]; then
    echo "missing required source dataset: $DATASETS/$dataset" >&2
    exit 3
  fi
done

for prefix in "$RESULT_PREFIX" "$INDEX_PREFIX"; do
  existing="$(aws --region "$REGION" s3api list-objects-v2 \
    --bucket "$BORSUK_PUBLICATION_BUCKET" \
    --prefix "${prefix%/}/" \
    --max-keys 1 \
    --query KeyCount \
    --output text)"
  if [[ "$existing" != "0" ]]; then
    echo "refusing to overwrite non-empty S3 prefix: s3://$BORSUK_PUBLICATION_BUCKET/$prefix" >&2
    exit 3
  fi
done
printf '%s\n' \
  "started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  "source_sha256=$BORSUK_SOURCE_SHA256" \
  > "$ROOT/RUNNER_STARTED"

DATASETS="$DATASETS" \
OUT="$ROOT/dense-default" \
BORSUK_S3_BUCKET="s3://$BORSUK_PUBLICATION_BUCKET/$INDEX_PREFIX" \
BORSUK_FULL_RUN_ID="$BORSUK_PUBLICATION_RUN_ID" \
BORSUK_RUN_FULL_S3=1 \
RUN_DEEP_IMAGE=1 \
  bash scripts/bench_s3_full.sh

sync_results
publish_checkpoint DENSE_DEFAULT_COMPLETE \
  "datasets=fashion-mnist-784,glove-100,sift-128,nytimes-256,gist-960,deep-image-96" \
  "scan_codec=srht-pq-scan"

if [[ ! -x "$HYBRID_VENV/bin/python3" ]]; then
  python3 -m venv "$HYBRID_VENV"
fi
"$HYBRID_VENV/bin/python3" -m pip install \
  --disable-pip-version-check \
  --requirement scripts/requirements-hybrid-bench.txt
"$HYBRID_VENV/bin/python3" -m pip freeze \
  > "$ROOT/hybrid-python-packages.txt"
export PATH="$HYBRID_VENV/bin:$PATH"

mkdir -p "$BEIR_SOURCE_ROOT" "$HYBRID_DATASETS" "$ROOT/hybrid-inputs"
for dataset in fiqa scifact nfcorpus; do
  python3 scripts/fetch_beir_dataset.py \
    --dataset "$dataset" \
    --output "$BEIR_SOURCE_ROOT"
  python3 scripts/prepare_hybrid_dataset.py \
    --source "$BEIR_SOURCE_ROOT/$dataset" \
    --output "$HYBRID_DATASETS/$dataset" \
    --dataset "$dataset" \
    --split test \
    --dense-backend sentence-transformers \
    --dense-model BAAI/bge-small-en-v1.5 \
    --dense-revision 5c38ec7c405ec4b44b94cc5a9bb96e735b38267a \
    --dense-query-prefix "Represent this sentence for searching relevant passages: " \
    --publication
  python3 scripts/validate_hybrid_dataset.py "$HYBRID_DATASETS/$dataset" \
    > "$ROOT/hybrid-inputs/$dataset-validation.json"
  cp "$HYBRID_DATASETS/$dataset/manifest.json" \
    "$ROOT/hybrid-inputs/$dataset-manifest.json"
  cp "$BEIR_SOURCE_ROOT/$dataset/.borsuk-source.json" \
    "$ROOT/hybrid-inputs/$dataset-source.json"
done

while IFS='|' read -r dataset modes; do
  dataset_out="$ROOT/hybrid-retrieval/$dataset"
  BORSUK_HYBRID_DATASETS_ROOT="$HYBRID_DATASETS" \
  BORSUK_HYBRID_RUN_ID="$BORSUK_PUBLICATION_RUN_ID" \
  BORSUK_HYBRID_MATRIX_DATASETS="$dataset" \
  BORSUK_HYBRID_MATRIX_PROFILES=srht \
  BORSUK_HYBRID_MATRIX_MODES="$modes" \
  BORSUK_HYBRID_SEARCH_POINTS='128:32 256:64 512:128' \
  BORSUK_HYBRID_HOT_FRACTIONS='0 0.25 0.5 0.75 1' \
  BORSUK_HYBRID_RRF_KS='1 5 10 30 60' \
  BORSUK_HYBRID_REPETITIONS=5 \
  BORSUK_HYBRID_MATRIX_EXECUTE=1 \
  BORSUK_RUN_HYBRID_MATRIX=1 \
  BORSUK_S3_BUCKET="s3://$BORSUK_PUBLICATION_BUCKET/$INDEX_PREFIX" \
  OUT="$dataset_out" \
    bash scripts/bench_hybrid_retrieval_matrix.sh

  python3 - "$dataset_out/coverage.csv" "$dataset" <<'PY'
import csv
from pathlib import Path
import sys

coverage = Path(sys.argv[1])
dataset = sys.argv[2]
with coverage.open(newline="") as handle:
    rows = list(csv.DictReader(handle))
if len(rows) != 106:
    raise SystemExit(f"{dataset}: expected 106 coverage rows, got {len(rows)}")
if {row["status"] for row in rows} != {"measured"}:
    raise SystemExit(f"{dataset}: hybrid coverage contains non-measured rows")
if {row["dataset"] for row in rows} != {dataset}:
    raise SystemExit(f"{dataset}: hybrid coverage leaked another dataset")
print(f"validated {dataset}: {len(rows)} measured build/query rows")
PY
  python3 scripts/render_hybrid_retrieval_charts.py \
    --experiment-root "$dataset_out" \
    --output-dir "$dataset_out/charts"
  sync_results
done <<'MATRIX'
fiqa|dense sparse dense+sparse
scifact|dense text dense+text
nfcorpus|sparse text sparse+text
MATRIX

publish_checkpoint HYBRID_RETRIEVAL_COMPLETE \
  "datasets=fiqa,scifact,nfcorpus" \
  "workloads=dense+sparse,dense+text,sparse+text" \
  "coverage_rows=318"
sync_results
publish_checkpoint PUBLICATION_FULL_COMPLETE \
  "phases=dense-default,hybrid-retrieval" \
  "dense_datasets=6" \
  "hybrid_datasets=3"

STATUS="complete"
