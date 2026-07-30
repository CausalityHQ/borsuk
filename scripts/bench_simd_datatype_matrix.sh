#!/usr/bin/env bash
# Same-source, same-host end-to-end SIMD/scalar-control datatype campaign.
set -euo pipefail

cd "$(dirname "$0")/.."

RUN_ID="${BORSUK_SIMD_RUN_ID:?set BORSUK_SIMD_RUN_ID}"
ARCHITECTURE="${BORSUK_SIMD_ARCHITECTURE:?set BORSUK_SIMD_ARCHITECTURE}"
EXECUTE="${BORSUK_SIMD_MATRIX_EXECUTE:-0}"
MANIFEST_SOURCE="${BORSUK_SIMD_MANIFEST:-docs/research/simd-e2e-manifest.json}"
ROOT="${BORSUK_SIMD_ROOT:-/home/ec2-user/borsuk-simd-datatype/$RUN_ID/$ARCHITECTURE}"
CELL_RUNNER="${BORSUK_SIMD_CELL_RUNNER:-scripts/run_simd_datatype_cell.sh}"
VALIDATOR="${BORSUK_SIMD_VALIDATOR:-scripts/validate_simd_datatype_results.py}"
REGION="${AWS_REGION:-eu-central-1}"
MIN_FREE_BYTES="${BORSUK_SIMD_MIN_FREE_BYTES:-34359738368}"

if [[ "$EXECUTE" == "1" && "${BORSUK_RUN_SIMD_MATRIX:-0}" != "1" ]]; then
  echo "paid execution requires BORSUK_RUN_SIMD_MATRIX=1" >&2
  exit 2
fi
if [[ "$EXECUTE" != "0" && "$EXECUTE" != "1" ]]; then
  echo "BORSUK_SIMD_MATRIX_EXECUTE must be 0 or 1" >&2
  exit 2
fi
if [[ ! -f "$MANIFEST_SOURCE" ]]; then
  echo "missing SIMD manifest: $MANIFEST_SOURCE" >&2
  exit 2
fi
if [[ -e "$ROOT/schedule.csv" ]]; then
  echo "refusing to overwrite existing SIMD run: $ROOT" >&2
  exit 3
fi

mkdir -p "$ROOT"
cp "$MANIFEST_SOURCE" "$ROOT/manifest.json"

actual_manifest_sha256="$(sha256sum "$ROOT/manifest.json" | awk '{print $1}')"
expected_architecture="$(
  python3 - "$ROOT/manifest.json" "$ARCHITECTURE" <<'PY'
import json
import sys

manifest = json.load(open(sys.argv[1], encoding="utf-8"))
matches = [row for row in manifest["architectures"] if row["name"] == sys.argv[2]]
if len(matches) != 1:
    raise SystemExit(f"unknown or duplicate architecture {sys.argv[2]!r}")
row = matches[0]
print(f"{row['uname_machine']},{row['instance_type']},{row['region']}")
PY
)"
IFS=, read -r expected_uname expected_instance_type expected_region <<< "$expected_architecture"

python3 - "$ROOT/manifest.json" "$ARCHITECTURE" "$RUN_ID" "$ROOT/schedule.csv" <<'PY'
import csv
import json
import sys

manifest_path, architecture, run_id, output_path = sys.argv[1:]
manifest = json.load(open(manifest_path, encoding="utf-8"))
fields = (
    "architecture",
    "build",
    "path",
    "kind",
    "element_type",
    "dataset",
    "repetition",
    "cache_state",
    "target_cache_coverage_percent",
    "client_concurrency",
    "query_seed",
    "index_key",
    "status",
)
with open(output_path, "w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=fields)
    writer.writeheader()
    for build in manifest["builds"]:
        for path in manifest["paths"]:
            for repetition in range(1, manifest["repetitions"] + 1):
                index_key = (
                    f"{run_id}/{architecture}/{build['name']}/"
                    f"{path['name']}/r{repetition:02d}"
                )
                seed = manifest["query_cohort"]["master_seed"] + repetition
                for cache_state in manifest["cache_states"]:
                    for concurrency in manifest["client_concurrency"]:
                        writer.writerow(
                            {
                                "architecture": architecture,
                                "build": build["name"],
                                "path": path["name"],
                                "kind": path["kind"],
                                "element_type": path["element_type"],
                                "dataset": path["dataset"],
                                "repetition": repetition,
                                "cache_state": cache_state["name"],
                                "target_cache_coverage_percent": cache_state[
                                    "coverage_percent"
                                ],
                                "client_concurrency": concurrency,
                                "query_seed": seed,
                                "index_key": index_key,
                                "status": "planned",
                            }
                        )
PY

{
  printf '%s\n' \
    "run_id=$RUN_ID" \
    "architecture=$ARCHITECTURE" \
    "expected_uname_machine=$expected_uname" \
    "expected_instance_type=$expected_instance_type" \
    "expected_region=$expected_region" \
    "manifest_sha256=$actual_manifest_sha256" \
    "execution_requested=$EXECUTE" \
    "minimum_free_bytes=$MIN_FREE_BYTES" \
    "source_sha256=${BORSUK_SOURCE_SHA256:-not-set-in-dry-run}" \
    "captured_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "$ROOT/environment.txt"

if [[ "$EXECUTE" != "1" ]]; then
  echo "wrote $ROOT/schedule.csv"
  echo "dry run only; set BORSUK_SIMD_MATRIX_EXECUTE=1 and BORSUK_RUN_SIMD_MATRIX=1 for paid execution"
  exit 0
fi

: "${BORSUK_SOURCE_SHA256:?paid execution requires BORSUK_SOURCE_SHA256}"
: "${BORSUK_SIMD_MANIFEST_SHA256:?paid execution requires BORSUK_SIMD_MANIFEST_SHA256}"
: "${BORSUK_S3_BUCKET:?paid execution requires BORSUK_S3_BUCKET}"
: "${BORSUK_SIMD_RESULT_PREFIX:?paid execution requires BORSUK_SIMD_RESULT_PREFIX}"
: "${BORSUK_SIMD_INDEX_PREFIX:?paid execution requires BORSUK_SIMD_INDEX_PREFIX}"

if [[ ! "$BORSUK_SOURCE_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
  echo "BORSUK_SOURCE_SHA256 must be a lowercase SHA-256 digest" >&2
  exit 2
fi
if [[ "$actual_manifest_sha256" != "$BORSUK_SIMD_MANIFEST_SHA256" ]]; then
  echo "SIMD manifest SHA-256 mismatch" >&2
  exit 2
fi
if [[ "$(uname -m)" != "$expected_uname" ]]; then
  echo "architecture drift: expected $expected_uname, got $(uname -m)" >&2
  exit 2
fi
if [[ "$REGION" != "$expected_region" ]]; then
  echo "region drift: expected $expected_region, got $REGION" >&2
  exit 2
fi

actual_instance_type="${BORSUK_INSTANCE_TYPE:-}"
if [[ -z "$actual_instance_type" ]]; then
  imds_token="$(
    curl -fsS -X PUT \
      -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
      http://169.254.169.254/latest/api/token
  )"
  actual_instance_type="$(
    curl -fsS \
      -H "X-aws-ec2-metadata-token: $imds_token" \
      http://169.254.169.254/latest/meta-data/instance-type
  )"
fi
if [[ "$actual_instance_type" != "$expected_instance_type" ]]; then
  echo "instance type drift: expected $expected_instance_type, got $actual_instance_type" >&2
  exit 2
fi

for prefix in "$BORSUK_SIMD_RESULT_PREFIX" "$BORSUK_SIMD_INDEX_PREFIX"; do
  key_count="$(
    aws --region "$REGION" s3api list-objects-v2 \
      --bucket "$BORSUK_S3_BUCKET" \
      --prefix "${prefix%/}/" \
      --max-keys 1 \
      --query KeyCount \
      --output text
  )"
  if [[ "$key_count" != "0" ]]; then
    echo "refusing to overwrite non-empty S3 prefix: s3://$BORSUK_S3_BUCKET/$prefix" >&2
    exit 3
  fi
done

if [[ ! -x "$CELL_RUNNER" ]]; then
  echo "missing executable SIMD cell runner: $CELL_RUNNER" >&2
  exit 2
fi
if [[ ! -f "$VALIDATOR" ]]; then
  echo "missing SIMD result validator: $VALIDATOR" >&2
  exit 2
fi

campaign_complete=0
finish() {
  exit_status=$?
  if [[ "$campaign_complete" != "1" ]]; then
    printf '%s\n' "status=failed" "exit_status=$exit_status" \
      > "$ROOT/SIMD_DATATYPE_MATRIX_FAILED"
  fi
  aws --region "$REGION" s3 sync \
    "$ROOT" \
    "s3://$BORSUK_S3_BUCKET/${BORSUK_SIMD_RESULT_PREFIX%/}" \
    --only-show-errors || true
  return "$exit_status"
}
trap finish EXIT

python3 scripts/check_publication_disk.py \
  --path "$ROOT" \
  --minimum-free-bytes "$MIN_FREE_BYTES"

simd_target="$ROOT/build/simd"
scalar_target="$ROOT/build/scalar-control"
mkdir -p "$simd_target" "$scalar_target"

CARGO_INCREMENTAL=0 CARGO_TARGET_DIR="$simd_target" \
  cargo build --locked --release -p borsuk \
    --example production_bench \
    --example hybrid_retrieval_bench \
    --example market_workload_bench

CARGO_INCREMENTAL=0 CARGO_TARGET_DIR="$scalar_target" \
  RUSTFLAGS="-C llvm-args=-vectorize-loops=false -C llvm-args=-vectorize-slp=false" \
  cargo build --locked --release -p borsuk --features scalar-control \
    --example production_bench \
    --example hybrid_retrieval_bench \
    --example market_workload_bench

printf '%s\n' 'build,binary,sha256' > "$ROOT/builds.csv"
for build in simd scalar-control; do
  if [[ "$build" == "simd" ]]; then
    target="$simd_target"
  else
    target="$scalar_target"
  fi
  for binary in production_bench hybrid_retrieval_bench market_workload_bench; do
    digest="$(sha256sum "$target/release/examples/$binary" | awk '{print $1}')"
    printf '%s\n' "$build,$binary,$digest" >> "$ROOT/builds.csv"
  done
done

python3 - "$ROOT/builds.csv" <<'PY'
import csv
import sys

with open(sys.argv[1], newline="", encoding="utf-8") as handle:
    rows = list(csv.DictReader(handle))
hashes = {(row["build"], row["binary"]): row["sha256"] for row in rows}
for binary in ("production_bench", "hybrid_retrieval_bench", "market_workload_bench"):
    simd = hashes.get(("simd", binary))
    scalar = hashes.get(("scalar-control", binary))
    if not simd or not scalar or simd == scalar:
        raise SystemExit(f"missing or equal build hashes for {binary}")
PY

tail -n +2 "$ROOT/schedule.csv" |
while IFS=, read -r row_architecture build path kind element_type dataset repetition \
  cache_state target_coverage concurrency query_seed index_key row_status; do
  if [[ "$row_architecture" != "$ARCHITECTURE" || "$row_status" != "planned" ]]; then
    echo "invalid schedule row identity" >&2
    exit 4
  fi
  python3 scripts/check_publication_disk.py \
    --path "$ROOT" \
    --minimum-free-bytes "$MIN_FREE_BYTES"
  env \
    AWS_REGION="$REGION" \
    BORSUK_SIMD_RUN_ID="$RUN_ID" \
    BORSUK_SIMD_ARCHITECTURE="$ARCHITECTURE" \
    BORSUK_SIMD_BUILD="$build" \
    BORSUK_SIMD_PATH="$path" \
    BORSUK_SIMD_KIND="$kind" \
    BORSUK_SIMD_ELEMENT_TYPE="$element_type" \
    BORSUK_SIMD_DATASET="$dataset" \
    BORSUK_SIMD_REPETITION="$repetition" \
    BORSUK_SIMD_CACHE_STATE="$cache_state" \
    BORSUK_SIMD_TARGET_CACHE_COVERAGE_PERCENT="$target_coverage" \
    BORSUK_SIMD_CLIENT_CONCURRENCY="$concurrency" \
    BORSUK_SIMD_QUERY_SEED="$query_seed" \
    BORSUK_SIMD_INDEX_URI="s3://$BORSUK_S3_BUCKET/${BORSUK_SIMD_INDEX_PREFIX%/}/$index_key" \
    BORSUK_SIMD_OUTPUT_ROOT="$ROOT/cells/$build/$path/r$(printf '%02d' "$repetition")/$cache_state/c$concurrency" \
    BORSUK_SIMD_SIMD_TARGET="$simd_target" \
    BORSUK_SIMD_SCALAR_TARGET="$scalar_target" \
    BORSUK_SOURCE_SHA256="$BORSUK_SOURCE_SHA256" \
    BORSUK_SIMD_MANIFEST_SHA256="$BORSUK_SIMD_MANIFEST_SHA256" \
    "$CELL_RUNNER"
done

python3 "$VALIDATOR" \
  --manifest "$ROOT/manifest.json" \
  --schedule "$ROOT/schedule.csv" \
  --root "$ROOT" \
  --architecture "$ARCHITECTURE" \
  --source-sha256 "$BORSUK_SOURCE_SHA256" \
  --manifest-sha256 "$BORSUK_SIMD_MANIFEST_SHA256"

printf '%s\n' \
  "status=complete" \
  "run_id=$RUN_ID" \
  "architecture=$ARCHITECTURE" \
  "source_sha256=$BORSUK_SOURCE_SHA256" \
  "manifest_sha256=$BORSUK_SIMD_MANIFEST_SHA256" \
  > "$ROOT/SIMD_DATATYPE_MATRIX_COMPLETE"
campaign_complete=1
