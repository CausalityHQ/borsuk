#!/usr/bin/env bash
# Fresh Amazon S3 Vectors direct comparison on the standard datasets.
set -euo pipefail

DATASETS="${DATASETS:-/tmp/borsuk-datasets}"
RUN_ID="${BORSUK_S3V_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT="${OUT:-docs/web/assets/benchmarks/s3-vectors/$RUN_ID}"
DATASET_NAMES="${BORSUK_S3V_DATASETS:-fashion-mnist-784 glove-100 sift-128 nytimes-256 gist-960 deep-image-96}"
REGION="${AWS_REGION:-eu-central-1}"
PROFILE="${AWS_PROFILE:-}"
EXECUTE="${BORSUK_S3V_EXECUTE:-0}"
REPETITIONS="${BORSUK_S3V_REPETITIONS:-5}"
MASTER_SEED="${BORSUK_S3V_MASTER_SEED:-20260726}"

if [[ "$EXECUTE" == '1' && "${BORSUK_RUN_S3V_MATRIX:-0}" != '1' ]]; then
  echo "paid execution requires BORSUK_RUN_S3V_MATRIX=1" >&2
  exit 2
fi
if [[ "$EXECUTE" == '1' && -e "$OUT/coverage.csv" ]]; then
  echo "refusing to overwrite an existing run: $OUT" >&2
  exit 3
fi

mkdir -p "$OUT"
COVERAGE="$OUT/coverage.csv"
printf '%s\n' 'dataset,status,repetition_id,query_seed,vector_bucket,index_name,output_dir,resource_path' > "$COVERAGE"
safe_run="$(printf '%s' "$RUN_ID" | tr '[:upper:]_' '[:lower:]-' | tr -cd 'a-z0-9-')"
safe_run="${safe_run:0:20}"

failures=0
for dataset_name in $DATASET_NAMES; do
  dataset="$DATASETS/$dataset_name"
  [[ -d "$dataset" ]] || { echo "missing dataset directory: $dataset" >&2; exit 4; }
  short_dataset="$(printf '%s' "$dataset_name" | tr -cd 'a-z0-9-')"
  for campaign_repetition in $(seq 1 "$REPETITIONS"); do
    if [[ -n "${BORSUK_S3V_REPETITION_ID:-}" ]]; then
      [[ "$REPETITIONS" == "1" ]] || {
        echo "BORSUK_S3V_REPETITION_ID requires one repetition" >&2
        exit 4
      }
      repetition_id="$BORSUK_S3V_REPETITION_ID"
    else
      repetition_id="$(printf 'r%02d' "$campaign_repetition")"
    fi
    query_seed=$((MASTER_SEED + campaign_repetition))
    vector_bucket="borsuk-${safe_run}-${short_dataset}-${repetition_id}"
    vector_bucket="${vector_bucket:0:63}"
    index_name='vectors'
    output="$OUT/$dataset_name/$repetition_id"
    resources="$output/resources.csv"
    status=planned
    if [[ "$EXECUTE" == '1' ]]; then
      mkdir -p "$output/scratch"
      command=(python3 scripts/benchmark_s3_vectors.py --dataset "$dataset" --bucket "$vector_bucket" --index "$index_name" --output-dir "$output" --region "$REGION" --queries "${BORSUK_S3V_QUERIES:-100}" --workers "${BORSUK_S3V_WORKERS:-5}" --query-seed "$query_seed" --repetition-id "$repetition_id")
      if [[ -n "$PROFILE" ]]; then
        command+=(--profile "$PROFILE")
      fi
      if AWS_REGION="$REGION" AWS_DEFAULT_REGION="$REGION" \
        python3 scripts/benchmark_with_resources.py \
          --output "$resources" \
          --scratch-dir "$output/scratch" \
          -- "${command[@]}"; then
        python3 scripts/validate_benchmark_artifacts.py \
          --directory "$output" \
          --required build.csv,query.csv,query_samples.csv,resources.csv
        status=measured
      else
        status=failed
        failures=$((failures + 1))
      fi
    fi
    printf '%s\n' "$dataset_name,$status,$repetition_id,$query_seed,$vector_bucket,$index_name,$output,$resources" >> "$COVERAGE"
  done
done

echo "wrote $COVERAGE"
if [[ "$EXECUTE" != '1' ]]; then
  echo "dry run only; set BORSUK_S3V_EXECUTE=1 and BORSUK_RUN_S3V_MATRIX=1 for paid execution"
fi
if ((failures > 0)); then
  exit 1
fi
