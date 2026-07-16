#!/usr/bin/env bash
# Full-corpus SOTA benchmark run against real S3 — the paper-grade pass.
#
# Why S3: (1) BORSUK is object-store-native + near-zero resident RAM, so even a
# laptop can build/query million-to-billion-vector indexes hosted on S3;
# (2) byte / request / $ accounting only fires on the object-store layer, so the
# TCO / $-per-query numbers (the headline metrics) are ONLY measurable here — a
# local-filesystem run reports bytes_read=0, $/query=0.
#
# Full corpus (BORSUK_BENCH_LIMIT unset = 0) uses each dataset's shipped
# neighbors.i32 ground truth, so there is NO O(queries*corpus*dim) brute-force
# recompute — a full run is cheaper on the query side than a capped one.
#
# Prereqs:
#   - datasets fetched under $DATASETS (scripts/fetch_ann_dataset.py)
#   - AWS creds in the environment (or an instance role):
#       AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
#   - a writable S3 bucket/prefix you own (this WRITES index objects there)
#
# Usage:
#   BORSUK_S3_BUCKET=s3://my-bucket/borsuk-bench \
#   AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
#     scripts/bench_s3_full.sh
#
# Cost note: this writes and reads real S3 objects and issues real GET/PUT
# requests; you pay AWS for storage + requests + (cross-region) transfer. The
# whole point is to MEASURE that cost — the bench emits $/query from it.
set -euo pipefail

: "${BORSUK_S3_BUCKET:?set BORSUK_S3_BUCKET to an s3:// prefix you own, e.g. s3://my-bucket/borsuk-bench}"
DATASETS="${DATASETS:-/tmp/borsuk-datasets}"
OUT="${OUT:-docs/web/assets/benchmarks}"
QUERIES="${BORSUK_BENCH_QUERIES:-1000}"
# Datasets in ascending build cost. deep-image (10M) is optional — set
# RUN_DEEP_IMAGE=1 to include it; it is a multi-hour build.
DATASET_DIRS=(fashion-mnist-784 glove-100 sift-128 nytimes-256 gist-960)
[[ "${RUN_DEEP_IMAGE:-0}" == "1" ]] && DATASET_DIRS+=(deep-image-96)

cd "$(dirname "$0")/.."
echo "Building the release binary once..."
cargo build -p borsuk --release --example production_bench

for ds in "${DATASET_DIRS[@]}"; do
  dir="$DATASETS/$ds"
  if [[ ! -d "$dir" ]]; then
    echo "SKIP $ds — not found at $dir (fetch it first)"; continue
  fi
  echo "=================================================================="
  echo "== $ds  (FULL corpus -> $BORSUK_S3_BUCKET/$ds)"
  echo "=================================================================="
  # BORSUK_BENCH_LIMIT intentionally UNSET (0) => full corpus, shipped GT.
  BORSUK_BENCH_URI="$BORSUK_S3_BUCKET/$ds" \
  BORSUK_BENCH_DATASET="$dir" \
  BORSUK_BENCH_QUERIES="$QUERIES" \
  BORSUK_BENCH_OUTPUT_DIR="$OUT" \
    cargo run -p borsuk --release --example production_bench \
    || echo "!! $ds run failed (see output above) — continuing"
done

echo "Done. Regenerated production_*.csv are in $OUT (recall, bytes/query, \$/query)."
echo "Note: nytimes-256 aborts if its slice contains zero-norm vectors under cosine."
