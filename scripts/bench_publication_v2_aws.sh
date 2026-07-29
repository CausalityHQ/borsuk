#!/usr/bin/env bash
# Frozen confirmatory publication runner. Dry-run mode performs no cloud writes.
set -euo pipefail

cd "$(dirname "$0")/.."
MANIFEST="${BORSUK_PUBLICATION_V2_MANIFEST:-docs/research/publication-v2-manifest.json}"
EXECUTE="${BORSUK_PUBLICATION_V2_EXECUTE:-0}"
HYBRID_PYTHON_VERSION="${BORSUK_PUBLICATION_HYBRID_PYTHON_VERSION:-3.12}"
HYBRID_VENV="${BORSUK_PUBLICATION_HYBRID_VENV:-/home/ec2-user/borsuk-hybrid-venv-py312}"
MIN_FREE_BYTES="${BORSUK_PUBLICATION_MIN_FREE_BYTES:-34359738368}"

# This must precede every `aws` invocation and every paid side effect.
python3 scripts/publication_protocol.py validate "$MANIFEST"

manifest_field() {
  python3 - "$MANIFEST" "$1" <<'PY'
import json
from pathlib import Path
import sys

value = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))[sys.argv[2]]
if not isinstance(value, str) or not value or "\n" in value:
    raise SystemExit(f"invalid manifest string field: {sys.argv[2]}")
print(value)
PY
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

CAMPAIGN_ID="$(manifest_field campaign_id)"
MANIFEST_RESULT_PREFIX="$(manifest_field result_prefix)"
MANIFEST_INDEX_PREFIX="$(manifest_field index_prefix)"
CACHE_PREFIX="$(manifest_field cache_prefix)"
RUN_ID="${BORSUK_PUBLICATION_V2_RUN_ID:-$CAMPAIGN_ID}"
RESULT_PREFIX="${BORSUK_PUBLICATION_V2_RESULT_PREFIX:-$MANIFEST_RESULT_PREFIX}"
INDEX_PREFIX="${BORSUK_PUBLICATION_V2_INDEX_PREFIX:-$MANIFEST_INDEX_PREFIX}"
[[ "$RUN_ID" == "$CAMPAIGN_ID" ]] || {
  echo "campaign id mismatch: runner=$RUN_ID manifest=$CAMPAIGN_ID" >&2
  exit 3
}
[[ "$RESULT_PREFIX" == "$MANIFEST_RESULT_PREFIX" ]] || {
  echo "result prefix mismatch: runner=$RESULT_PREFIX manifest=$MANIFEST_RESULT_PREFIX" >&2
  exit 3
}
[[ "$INDEX_PREFIX" == "$MANIFEST_INDEX_PREFIX" ]] || {
  echo "index prefix mismatch: runner=$INDEX_PREFIX manifest=$MANIFEST_INDEX_PREFIX" >&2
  exit 3
}

if [[ "$EXECUTE" == "1" && "${BORSUK_RUN_PUBLICATION_V2:-0}" != "1" ]]; then
  echo "paid execution requires BORSUK_RUN_PUBLICATION_V2=1" >&2
  exit 2
fi

ROOT="${BORSUK_PUBLICATION_V2_ROOT:-/home/ec2-user/borsuk-publication-v2/$RUN_ID}"
if [[ -e "$ROOT" ]]; then
  echo "refusing to overwrite publication-v2 root: $ROOT" >&2
  exit 3
fi
mkdir -p "$ROOT"
cp "$MANIFEST" "$ROOT/manifest.json"
python3 scripts/publication_protocol.py schedule \
  "$ROOT/manifest.json" --output "$ROOT/schedule.csv"
{
  printf '%s\n' \
    "created_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "run_id=$RUN_ID" \
    "execute=$EXECUTE" \
    "source_sha256=${BORSUK_SOURCE_SHA256:-not-provided}" \
    "manifest_sha256=$(sha256_file "$ROOT/manifest.json")" \
    "kernel=$(uname -srmo)" \
    "logical_cpus=$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.logicalcpu)" \
    "ram_bytes=$(($(sysctl -n hw.memsize 2>/dev/null || awk '/^MemTotal:/ {print $2 * 1024}' /proc/meminfo)))" \
    "instance_type=${BORSUK_INSTANCE_TYPE:-not-provided}" \
    "local_disk_class=${BORSUK_LOCAL_DISK_CLASS:-not-provided}" \
    "minimum_free_bytes=$MIN_FREE_BYTES" \
    "accelerator=${BORSUK_ACCELERATOR:-not-provided}" \
    "index_storage_class=${BORSUK_INDEX_STORAGE_CLASS:-not-provided}" \
    "hybrid_python_version=$HYBRID_PYTHON_VERSION" \
    "client_compute_boundary=same-instance-for-borsuk-and-amazon-s3-vectors-client" \
    "managed_service_compute=undisclosed" \
    "rustc_version=$(rustc --version 2>/dev/null || printf unavailable)"
} > "$ROOT/environment.txt"
if [[ -n "${BORSUK_SOURCE_ARCHIVE:-}" ]]; then
  cp "$BORSUK_SOURCE_ARCHIVE" "$ROOT/source-archive.tar.gz"
fi

if [[ "$EXECUTE" != "1" ]]; then
  echo "publication-v2 dry run: frozen schedule at $ROOT/schedule.csv"
  exit 0
fi

: "${BORSUK_PUBLICATION_BUCKET:?set the plain result bucket}"
: "${BORSUK_PUBLICATION_DATASETS:?set the six-dataset root}"
: "${BORSUK_PUBLICATION_HYBRID_DATASETS:?set the prepared hybrid dataset root}"
: "${BORSUK_INSTANCE_TYPE:?set the measured client instance type}"
: "${BORSUK_LOCAL_DISK_CLASS:?set the measured local cache disk class}"
: "${BORSUK_ACCELERATOR:?set the measured client accelerator class}"
: "${BORSUK_INDEX_STORAGE_CLASS:?set the BORSUK index storage class}"
BEIR_SOURCE_ROOT="${BORSUK_PUBLICATION_BEIR_SOURCE_ROOT:-/home/ec2-user/borsuk-beir-source}"
REGION="${AWS_REGION:-eu-central-1}"
if [[ "$RESULT_PREFIX" == *pilot* || "$INDEX_PREFIX" == *pilot* ]]; then
  echo "confirmatory prefixes must not contain pilot" >&2
  exit 3
fi

for prefix in "$RESULT_PREFIX" "$INDEX_PREFIX"; do
  existing="$(aws --region "$REGION" s3api list-objects-v2 \
    --bucket "$BORSUK_PUBLICATION_BUCKET" --prefix "${prefix%/}/" \
    --max-keys 1 --query KeyCount --output text)"
  [[ "$existing" == "0" ]] || {
    echo "refusing to reuse non-empty confirmatory prefix: $prefix" >&2
    exit 3
  }
done

sync_results() {
  aws --region "$REGION" s3 sync "$ROOT" \
    "s3://$BORSUK_PUBLICATION_BUCKET/$RESULT_PREFIX" \
    --exclude '*/cache/*' --exclude '*/scratch/*' --only-show-errors
}

require_free_disk() {
  local phase="$1"
  python3 scripts/check_publication_disk.py \
    --root "$ROOT" \
    --minimum-free-bytes "$MIN_FREE_BYTES" \
    --phase "$phase"
}

cleanup_publication_ephemera() {
  local cleanup_root="$1"
  [[ -d "$cleanup_root" ]] || return 0
  find "$cleanup_root" -type d \
    \( -name cache -o -name scratch \) \
    -prune -exec rm -rf {} +
}

publication_exit() {
  local status=$?
  trap - EXIT
  # Exit traps inherit `set -e`. Cleanup and evidence sync must continue even
  # when the original failure was ENOSPC or one cleanup target is damaged.
  set +e
  if ((status != 0)); then
    # Cache and build scratch are never evidence. Reclaim them before the final
    # marker write and S3 sync so an exhausted client has room to persist and
    # upload failure evidence.
    cleanup_publication_ephemera "$ROOT" || \
      echo "failed to fully clean publication ephemera" >&2
    rm -f "$ROOT/PUBLICATION_V2_COMPLETE" || true
    printf 'failed\n' > "$ROOT/PUBLICATION_V2_FAILED" || \
      echo "failed to write publication failure marker" >&2
  fi
  if ! sync_results; then
    echo "failed to synchronize publication evidence" >&2
    ((status != 0)) && exit "$status"
    exit 1
  fi
  exit "$status"
}
trap publication_exit EXIT
trap 'exit 130' INT TERM HUP

if [[ ! -x "$HYBRID_VENV/bin/python3" ]]; then
  uv python install "$HYBRID_PYTHON_VERSION"
  uv venv --python "$HYBRID_PYTHON_VERSION" "$HYBRID_VENV"
fi
actual_hybrid_python="$("$HYBRID_VENV/bin/python3" -c \
  'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
[[ "$actual_hybrid_python" == "$HYBRID_PYTHON_VERSION" ]] || {
  echo "hybrid Python version mismatch: got $actual_hybrid_python, expected $HYBRID_PYTHON_VERSION" >&2
  exit 3
}
uv pip install --python "$HYBRID_VENV/bin/python3" \
  --requirement scripts/requirements-hybrid-bench.txt
uv pip freeze --python "$HYBRID_VENV/bin/python3" \
  > "$ROOT/hybrid-python-packages.txt"
export PATH="$HYBRID_VENV/bin:$PATH"
"$HYBRID_VENV/bin/python3" - "$REGION" <<'PY'
import boto3
import sys

client = boto3.client("s3vectors", region_name=sys.argv[1])
client.list_vector_buckets(maxResults=1)
print(f"S3 Vectors preflight valid in {sys.argv[1]}")
PY
mkdir -p "$BEIR_SOURCE_ROOT" "$BORSUK_PUBLICATION_HYBRID_DATASETS" \
  "$ROOT/hybrid-inputs"
for dataset in scifact nfcorpus fiqa; do
  target="$BORSUK_PUBLICATION_HYBRID_DATASETS/$dataset"
  if [[ ! -f "$target/manifest.json" ]]; then
    preparing="$target.preparing-$RUN_ID"
    if [[ -e "$preparing" ]]; then
      echo "refusing to overwrite partial hybrid preparation: $preparing" >&2
      exit 3
    fi
    python3 scripts/fetch_beir_dataset.py \
      --dataset "$dataset" \
      --output "$BEIR_SOURCE_ROOT"
    python3 scripts/prepare_hybrid_dataset.py \
      --source "$BEIR_SOURCE_ROOT/$dataset" \
      --output "$preparing" \
      --dataset "$dataset" \
      --split test \
      --dense-backend sentence-transformers \
      --dense-model BAAI/bge-small-en-v1.5 \
      --dense-revision 5c38ec7c405ec4b44b94cc5a9bb96e735b38267a \
      --dense-query-prefix "Represent this sentence for searching relevant passages: " \
      --publication
    mv "$preparing" "$target"
  fi
  python3 scripts/validate_hybrid_dataset.py "$target" \
    > "$ROOT/hybrid-inputs/$dataset-validation.json"
  python3 - "$target/manifest.json" <<'PY'
import json
from pathlib import Path
import sys

manifest = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
dense = manifest.get("dense", {})
expected = {
    "backend": "sentence-transformers",
    "model": "BAAI/bge-small-en-v1.5",
    "revision": "5c38ec7c405ec4b44b94cc5a9bb96e735b38267a",
    "query_prefix": "Represent this sentence for searching relevant passages: ",
    "normalized": True,
    "publication_valid": True,
}
for field, value in expected.items():
    if dense.get(field) != value:
        raise SystemExit(
            f"{manifest.get('dataset', 'unknown')}: dense {field} is not frozen"
        )
PY
  cp "$target/manifest.json" "$ROOT/hybrid-inputs/$dataset-manifest.json"
  cp "$BEIR_SOURCE_ROOT/$dataset/.borsuk-source.json" \
    "$ROOT/hybrid-inputs/$dataset-source.json"
done

cleanup_repetition_data() {
  local repetition_root="$1"
  cleanup_publication_ephemera "$repetition_root"
}

run_borsuk_direct() {
  local repetition_id="$1"
  local query_seed="$2"
  DATASETS="$BORSUK_PUBLICATION_DATASETS" \
  OUT="$ROOT/$repetition_id/borsuk-direct" \
  BORSUK_S3_BUCKET="s3://$BORSUK_PUBLICATION_BUCKET/$INDEX_PREFIX/$repetition_id/borsuk-direct" \
  BORSUK_FULL_RUN_ID="$RUN_ID-$repetition_id-direct" \
  BORSUK_FULL_DATASET_NAMES=fashion-mnist-784 \
  BORSUK_BENCH_QUERIES=1000 \
  BORSUK_BENCH_UNCACHED_QUERIES=1000 \
  BORSUK_BENCH_QUERY_SEED="$query_seed" \
  BORSUK_BENCH_REPETITION_ID="$repetition_id" \
  BORSUK_BENCH_NPROBES=8 \
  BORSUK_BENCH_CANDIDATES=320 \
  BORSUK_FULL_RECALL_ONLY=1 \
  BORSUK_FULL_SKIP_EXACT_RECALL=1 \
  BORSUK_SEGMENT_TABLE_FORMAT=parquet \
  BORSUK_RUN_FULL_S3=1 \
    bash scripts/bench_s3_full.sh
}

run_borsuk_remaining() {
  local repetition_id="$1"
  local query_seed="$2"
  local dense_order="$3"
  local remaining=()
  local dataset
  for dataset in $dense_order; do
    [[ "$dataset" == "fashion-mnist-784" ]] || remaining+=("$dataset")
  done
  ((${#remaining[@]} > 0)) || {
    echo "publication dense schedule has no non-direct datasets" >&2
    return 2
  }
  dense_order="${remaining[*]}"
  DATASETS="$BORSUK_PUBLICATION_DATASETS" \
  OUT="$ROOT/$repetition_id/borsuk-dense" \
  BORSUK_S3_BUCKET="s3://$BORSUK_PUBLICATION_BUCKET/$INDEX_PREFIX/$repetition_id/borsuk-dense" \
  BORSUK_FULL_RUN_ID="$RUN_ID-$repetition_id-dense" \
  BORSUK_FULL_DATASET_NAMES="$dense_order" \
  BORSUK_BENCH_QUERIES=1000 \
  BORSUK_BENCH_UNCACHED_QUERIES=1000 \
  BORSUK_BENCH_QUERY_SEED="$query_seed" \
  BORSUK_BENCH_REPETITION_ID="$repetition_id" \
  BORSUK_BENCH_NPROBES=8 \
  BORSUK_BENCH_CANDIDATES=320 \
  BORSUK_FULL_RECALL_ONLY=1 \
  BORSUK_FULL_SKIP_EXACT_RECALL=1 \
  BORSUK_SEGMENT_TABLE_FORMAT=parquet \
  BORSUK_RUN_FULL_S3=1 \
    bash scripts/bench_s3_full.sh
}

run_s3_vectors() {
  local repetition_id="$1"
  local query_seed="$2"
  OUT="$ROOT/$repetition_id/amazon-s3-vectors" \
  DATASETS="$BORSUK_PUBLICATION_DATASETS" \
  BORSUK_S3V_DATASETS=fashion-mnist-784 \
  BORSUK_S3V_RUN_ID="$RUN_ID-$repetition_id" \
  BORSUK_S3V_REPETITIONS=1 \
  BORSUK_S3V_REPETITION_ID="$repetition_id" \
  BORSUK_S3V_MASTER_SEED="$((query_seed - 1))" \
  BORSUK_S3V_QUERIES=1000 \
  BORSUK_S3V_EXECUTE=1 \
  BORSUK_RUN_S3V_MATRIX=1 \
    bash scripts/bench_s3_vectors_matrix.sh
  test -s "$ROOT/$repetition_id/amazon-s3-vectors/fashion-mnist-784/$repetition_id/query_samples.csv"
}

run_hybrid() {
  local repetition_id="$1"
  local query_seed="$2"
  local hybrid_dataset_order="$3"
  OUT="$ROOT/$repetition_id/hybrid" \
  BORSUK_HYBRID_DATASETS_ROOT="$BORSUK_PUBLICATION_HYBRID_DATASETS" \
  BORSUK_HYBRID_MATRIX_DATASETS="$hybrid_dataset_order" \
  BORSUK_HYBRID_MATRIX_PROFILES=srht \
  BORSUK_HYBRID_SEARCH_POINTS=256:64 \
  BORSUK_HYBRID_HOT_FRACTIONS="0 0.5 1" \
  BORSUK_HYBRID_RRF_KS=60 \
  BORSUK_HYBRID_REPETITIONS=1 \
  BORSUK_HYBRID_MASTER_SEED="$((query_seed - 1))" \
  BORSUK_HYBRID_MATRIX_EXECUTE=1 \
  BORSUK_RUN_HYBRID_MATRIX=1 \
  BORSUK_S3_BUCKET="s3://$BORSUK_PUBLICATION_BUCKET/$INDEX_PREFIX/$repetition_id/hybrid" \
    bash scripts/bench_hybrid_retrieval_matrix.sh
}

tail -n +2 "$ROOT/schedule.csv" |
while IFS=, read -r repetition_id query_seed dense_dataset_order hybrid_dataset_order system_order result_key index_key cache_key; do
  [[ "$result_key" == "$RESULT_PREFIX/$repetition_id" ]] || {
    echo "schedule result prefix drift for $repetition_id: $result_key" >&2
    exit 3
  }
  [[ "$index_key" == "$INDEX_PREFIX/$repetition_id" ]] || {
    echo "schedule index prefix drift for $repetition_id: $index_key" >&2
    exit 3
  }
  [[ "$cache_key" == "$CACHE_PREFIX-$repetition_id" ]] || {
    echo "schedule cache key drift for $repetition_id: $cache_key" >&2
    exit 3
  }
  require_free_disk "$repetition_id-start"
  mkdir -p "$ROOT/$repetition_id"
  printf '%s\n' \
    "repetition_id=$repetition_id" \
    "query_seed=$query_seed" \
    "dense_dataset_order=$dense_dataset_order" \
    "hybrid_dataset_order=$hybrid_dataset_order" \
    "system_order=$system_order" \
    "result_key=$result_key" \
    "index_key=$index_key" \
    "cache_key=$cache_key" \
    > "$ROOT/$repetition_id/protocol.txt"
  if [[ "$system_order" == "borsuk amazon-s3-vectors" ]]; then
    run_borsuk_direct "$repetition_id" "$query_seed"
    run_s3_vectors "$repetition_id" "$query_seed"
  else
    run_s3_vectors "$repetition_id" "$query_seed"
    run_borsuk_direct "$repetition_id" "$query_seed"
  fi
  sync_results
  run_borsuk_remaining "$repetition_id" "$query_seed" "$dense_dataset_order"
  sync_results
  require_free_disk "$repetition_id-before-hybrid"
  run_hybrid "$repetition_id" "$query_seed" "$hybrid_dataset_order"
  printf 'complete\n' > "$ROOT/$repetition_id/REPETITION_COMPLETE"
  sync_results
  cleanup_repetition_data "$ROOT/$repetition_id"
done

python3 - "$ROOT" <<'PY'
import csv
from pathlib import Path
import sys

root = Path(sys.argv[1])
fieldnames = [
    "repetition_id",
    "query_position",
    "query_source_index",
    "latency_ms",
    "recall_at_10",
    "status",
]

for borsuk_phase, s3_pass, suffix in [
    ("uncached", "first_pass", "first"),
    ("disk_cached", "repeated_pass", "repeated"),
]:
    borsuk_rows = []
    for source in sorted(
        root.glob("r*/borsuk-direct/fashion-mnist-784/bench_query_samples.csv")
    ):
        with source.open(newline="") as handle:
            for row in csv.DictReader(handle):
                if (
                    row["phase"] == borsuk_phase
                    and row["mode"] == "srht-pq-scan"
                    and row["nprobe"] == "8"
                    and row["max_candidates"] == "320"
                ):
                    borsuk_rows.append(
                        {
                            "repetition_id": row["repetition_id"],
                            "query_position": row["sample_index"],
                            "query_source_index": row["query_source_index"],
                            "latency_ms": row["latency_ms"],
                            "recall_at_10": row["recall_at_10"],
                            "status": "ok",
                        }
                    )
    s3_rows = []
    for source in sorted(
        root.glob("r*/amazon-s3-vectors/fashion-mnist-784/r*/query_samples.csv")
    ):
        with source.open(newline="") as handle:
            for row in csv.DictReader(handle):
                if row["pass"] == s3_pass:
                    s3_rows.append({field: row[field] for field in fieldnames})
    for engine, rows in [("borsuk", borsuk_rows), ("s3-vectors", s3_rows)]:
        output = root / f"direct-{engine}-{suffix}.csv"
        with output.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
PY

python3 scripts/analyze_publication_claims.py \
  --borsuk "$ROOT/direct-borsuk-first.csv" \
  --s3-vectors "$ROOT/direct-s3-vectors-first.csv" \
  --cache-pair uncached-vs-first-pass \
  --expected-repetitions 5 \
  --expected-queries 1000 \
  --output "$ROOT/direct-claim-first.csv"
python3 scripts/analyze_publication_claims.py \
  --borsuk "$ROOT/direct-borsuk-repeated.csv" \
  --s3-vectors "$ROOT/direct-s3-vectors-repeated.csv" \
  --cache-pair disk-cached-vs-repeated-pass \
  --expected-repetitions 5 \
  --expected-queries 1000 \
  --output "$ROOT/direct-claim-repeated.csv"

sync_results
printf 'complete\n' > "$ROOT/PUBLICATION_V2_COMPLETE"
sync_results
trap - EXIT
