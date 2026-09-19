#!/bin/bash
set -u

case "${1:-}" in
  --describe)
    exec python3 "$(dirname "$0")/v85_qualification.py" --print-matrix
    ;;
  "") ;;
  *)
    echo "usage: $0 [--describe]" >&2
    exit 2
    ;;
esac

: "${V85_SOURCE_ARCHIVE_URI:?}"
: "${V85_SOURCE_ARCHIVE_SHA256:?}"
: "${V85_SOURCE_COMMIT:?}"
: "${V85_OUTPUT_URI:?}"
: "${V85_MODE:?}"

if [ "$V85_MODE" != preflight ]; then
  echo "V85_MODE must be preflight until the fail-fast gate passes" >&2
  exit 2
fi

root=/mnt/v85-delta-screen
phase=bootstrap

publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  cd "$root" 2>/dev/null || true
  for name in build.log hashes.log prepare.log truth.time incremental.time fresh.time summary.json preflight.time preflight-receipt.json preflight-result.json; do
    [ -f "$name" ] && aws s3 cp "$name" "$V85_OUTPUT_URI/evidence/$name" --only-show-errors
  done
  for name in result-*.json; do
    [ -f "$name" ] && aws s3 cp "$name" "$V85_OUTPUT_URI/evidence/$name" --only-show-errors
  done
  for variant in incremental fresh; do
    if [ -d "$variant" ]; then
      for name in generation.json receipt.json router.arrow mutations.arrow base-000.arrow delta-000.arrow; do
        [ -f "$variant/$name" ] && aws s3 cp "$variant/$name" "$V85_OUTPUT_URI/$variant/$name" --only-show-errors
      done
    fi
  done
  printf '{"exit_code":%d,"phase":"%s","source_commit":"%s"}\n' \
    "$code" "$phase" "$V85_SOURCE_COMMIT" >/tmp/v85-terminal.json
  aws s3 cp /tmp/v85-terminal.json "$V85_OUTPUT_URI/terminal.json" --only-show-errors
  unlink /tmp/v85-terminal.json
  exit "$code"
}
trap publish_terminal EXIT

mkdir -p "$root" && cd "$root" || exit 90
phase=toolchain
export HOME=${HOME:-/root}
dnf install -y -q gcc zstd python3-devel >build.log 2>&1 || exit 91
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |
    sh -s -- -y --profile minimal --default-toolchain stable >>build.log 2>&1 || exit 92
fi
export PATH="$HOME/.cargo/bin:$PATH"
python3 -m venv .venv >>build.log 2>&1 || exit 93
.venv/bin/python -m pip install --disable-pip-version-check --quiet \
  'numpy==1.26.4' 'pyarrow==17.0.0' >>build.log 2>&1 || exit 93

phase=source
aws s3 cp "$V85_SOURCE_ARCHIVE_URI" source.tar.zst --only-show-errors || exit 94
printf '%s  source.tar.zst\n' "$V85_SOURCE_ARCHIVE_SHA256" | sha256sum -c - >hashes.log 2>&1 || exit 95
mkdir repo
tar --zstd -xf source.tar.zst -C repo || exit 96
grep -q 'name = "borsuk-v71"' repo/crates/borsuk-v71/Cargo.toml || exit 96
(cd repo && cargo build --release --locked -p borsuk-v71 --bin v85_delta_reader) >>build.log 2>&1 || exit 97
binary="$root/repo/target/release/v85_delta_reader"

phase=inputs
base=s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000
aws s3 cp "$base/source.parquet" source.parquet --only-show-errors || exit 98
aws s3 cp "$base/development-query.parquet" query-source.parquet --only-show-errors || exit 98
aws s3 cp "$base/development-gt100.parquet" gt-source.parquet --only-show-errors || exit 98
sha256sum -c >>hashes.log 2>&1 <<'HASHES' || exit 99
2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86  source.parquet
310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54  query-source.parquet
fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11  gt-source.parquet
HASHES

phase=preflight-prepare
export PYTHONPATH="$root/repo" OMP_NUM_THREADS=16 OPENBLAS_NUM_THREADS=16
.venv/bin/python - <<'PY' >prepare.log 2>&1 || exit 100
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from scripts.v85_build_delta import canonicalize_evaluation, compute_exact_truth

source = pq.ParquetFile("source.parquet")
batches = []
rows = 0
for batch in source.iter_batches(batch_size=10_000):
    take = min(batch.num_rows, 10_000 - rows)
    batches.append(batch.slice(0, take))
    rows += take
    if rows == 10_000:
        break
if rows != 10_000:
    raise ValueError("preflight source prefix row count differs")
pq.write_table(pa.Table.from_batches(batches), "source-10k.parquet")
canonicalize_evaluation(
    Path("query-source.parquet"), Path("gt-source.parquet"),
    Path("queries.parquet"), Path("truth-placeholder.parquet"),
    dimensions=768, neighbors=100, query_limit=1,
)
compute_exact_truth(
    Path("source-10k.parquet"), Path("query-source.parquet"), Path("truth.parquet"),
    dimensions=768, corpus_rows=10_000, neighbors=100, query_limit=1,
)
print("prepared", rows)
PY

phase=preflight-build
artifact_prefix="$V85_OUTPUT_URI/preflight/artifacts"
/usr/bin/time -v -o preflight.time .venv/bin/python repo/scripts/v85_build_delta.py \
  --source source-10k.parquet --output preflight --uri-prefix "$artifact_prefix" \
  --base-rows 9000 --delta-rows 1000 --dimensions 768 --page-rows 256 \
  --router-cells 256 --base-runs 1 --delta-runs 1 --seed 85 >>build.log 2>&1 || exit 101

for path in preflight/generation.json preflight/router.arrow preflight/mutations.arrow preflight/base-*.arrow preflight/delta-*.arrow; do
  [ -f "$path" ] || exit 102
  aws s3 cp "$path" "$artifact_prefix/$(basename "$path")" --only-show-errors || exit 102
done
aws s3 cp queries.parquet "$V85_OUTPUT_URI/preflight/queries.parquet" --only-show-errors || exit 102
aws s3 cp truth.parquet "$V85_OUTPUT_URI/preflight/truth.parquet" --only-show-errors || exit 102
aws s3 cp "$artifact_prefix/generation.json" generation-remote.json --only-show-errors || exit 102
cmp preflight/generation.json generation-remote.json || exit 103

artifact_args() {
  for spec in \
    "generation generation.json" "router router.arrow" "mutations mutations.arrow"; do
    set -- $spec
    role=$1
    name=$2
    printf -- '--%s %q %q %s %s ' "$role" "$root/preflight/$name" "$artifact_prefix/$name" \
      "$(sha256sum "preflight/$name" | cut -d' ' -f1)" "$(stat -c %s "preflight/$name")"
  done
  for path in preflight/base-*.arrow preflight/delta-*.arrow; do
    name=$(basename "$path")
    printf -- '--run-s3 %q %s %s ' "$artifact_prefix/$name" \
      "$(sha256sum "$path" | cut -d' ' -f1)" "$(stat -c %s "$path")"
  done
  printf -- '--queries %q %q %s %s ' "$root/queries.parquet" "$V85_OUTPUT_URI/preflight/queries.parquet" \
    "$(sha256sum queries.parquet | cut -d' ' -f1)" "$(stat -c %s queries.parquet)"
  printf -- '--truth %q %q %s %s ' "$root/truth.parquet" "$V85_OUTPUT_URI/preflight/truth.parquet" \
    "$(sha256sum truth.parquet | cut -d' ' -f1)" "$(stat -c %s truth.parquet)"
}

phase=preflight-cas
bucket_and_prefix=${V85_OUTPUT_URI#s3://}
bucket=${bucket_and_prefix%%/*}
prefix=${bucket_and_prefix#*/}
printf '{"schema":"borsuk-v85-preflight-cas-v1"}\n' >cas-head.json
aws s3api put-object --bucket "$bucket" --key "$prefix/preflight/cas-head.json" \
  --body cas-head.json --if-none-match '*' --no-cli-pager >/dev/null || exit 104
if aws s3api put-object --bucket "$bucket" --key "$prefix/preflight/cas-head.json" \
  --body cas-head.json --if-none-match '*' --no-cli-pager >cas-conflict.log 2>&1; then
  exit 105
fi
grep -Eq 'PreconditionFailed|412' cas-conflict.log || exit 105

phase=preflight-query
args=$(artifact_args)
eval "/usr/bin/time -v -o reader.time \"$binary\" $args --page-budget 8 --range-concurrency 16 --region eu-central-1" \
  >preflight-result.json || exit 106

phase=preflight-validate
export V85_SOURCE_ARCHIVE_SHA256 V85_SOURCE_COMMIT
.venv/bin/python - <<'PY' || exit 107
import hashlib
import json
import os
from pathlib import Path
from scripts.v85_qualification import frozen_matrix, validate_preflight_receipt

result_body = Path("preflight-result.json").read_bytes()
result = json.loads(result_body)
time_fields = {}
for line in Path("reader.time").read_text().splitlines():
    if ":" in line:
        key, value = line.rsplit(":", 1)
        time_fields[key.strip()] = value.strip()
peak_rss_bytes = int(time_fields["Maximum resident set size (kbytes)"]) * 1024
receipt = {
    "authenticated_inputs": 4,
    "binary_authenticated": True,
    "binary_sha256": hashlib.sha256(Path("repo/target/release/v85_delta_reader").read_bytes()).hexdigest(),
    "built_rows": 10_000,
    "cas_conflict_observed": True,
    "failed_queries": 0,
    "manifest_drift": False,
    "max_bytes_per_query": max(sample["bytes"] for sample in result["samples"]),
    "max_gets_per_query": max(sample["requests"] for sample in result["samples"]),
    "peak_rss_bytes": peak_rss_bytes,
    "query_count": len(result["samples"]),
    "result_sha256": hashlib.sha256(result_body).hexdigest(),
    "schema": "borsuk-v85-preflight-receipt-v1",
    "source_archive_sha256": os.environ["V85_SOURCE_ARCHIVE_SHA256"],
    "source_commit": os.environ["V85_SOURCE_COMMIT"],
}
validate_preflight_receipt(receipt, frozen_matrix())
Path("preflight-receipt.json").write_text(
    json.dumps(receipt, separators=(",", ":"), sort_keys=True) + "\n"
)
PY
aws s3 cp preflight-result.json "$V85_OUTPUT_URI/preflight/result.json" --only-show-errors || exit 108
aws s3 cp preflight-receipt.json "$V85_OUTPUT_URI/preflight/receipt.json" --only-show-errors || exit 108
phase=complete
exit 0

phase=prepare
export PYTHONPATH="$root/repo" OMP_NUM_THREADS=16 OPENBLAS_NUM_THREADS=16
.venv/bin/python - <<'PY' >prepare.log 2>&1 || exit 100
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from scripts.v85_build_delta import canonicalize_evaluation

source = pq.ParquetFile("source.parquet")
batches = []
rows = 0
for batch in source.iter_batches(batch_size=100_000):
    take = min(batch.num_rows, 100_000 - rows)
    batches.append(batch.slice(0, take))
    rows += take
    if rows == 100_000:
        break
if rows != 100_000:
    raise ValueError("source prefix row count differs")
pq.write_table(pa.Table.from_batches(batches), "source-100k.parquet")
canonicalize_evaluation(
    Path("query-source.parquet"), Path("gt-source.parquet"),
    Path("queries.parquet"), Path("truth-placeholder.parquet"),
    dimensions=768, neighbors=100, query_limit=32,
)
print("prepared", rows)
PY
/usr/bin/time -v -o truth.time .venv/bin/python - <<'PY' >>prepare.log 2>&1 || exit 101
from pathlib import Path
from scripts.v85_build_delta import compute_exact_truth
compute_exact_truth(
    Path("source-100k.parquet"), Path("query-source.parquet"), Path("truth.parquet"),
    dimensions=768, corpus_rows=100_000, neighbors=100, query_limit=32,
)
PY

phase=build
incremental_uri="$V85_OUTPUT_URI/incremental"
fresh_uri="$V85_OUTPUT_URI/fresh"
/usr/bin/time -v -o incremental.time .venv/bin/python repo/scripts/v85_build_delta.py \
  --source source-100k.parquet --output incremental --uri-prefix "$incremental_uri" \
  --base-rows 90000 --delta-rows 10000 --dimensions 768 --page-rows 256 \
  --router-cells 256 --base-runs 1 --delta-runs 1 --seed 85 >>build.log 2>&1 || exit 102
/usr/bin/time -v -o fresh.time .venv/bin/python repo/scripts/v85_build_delta.py \
  --source source-100k.parquet --output fresh --uri-prefix "$fresh_uri" \
  --base-rows 99999 --delta-rows 1 --dimensions 768 --page-rows 256 \
  --router-cells 256 --base-runs 1 --delta-runs 1 --seed 85 >>build.log 2>&1 || exit 103

artifact_args() {
  variant=$1
  prefix=$2
  for spec in \
    "generation generation.json" "router router.arrow" "mutations mutations.arrow"; do
    set -- $spec
    role=$1
    name=$2
    printf -- '--%s %q %q %s %s ' "$role" "$root/$variant/$name" "$prefix/$name" \
      "$(sha256sum "$variant/$name" | cut -d' ' -f1)" "$(stat -c %s "$variant/$name")"
  done
  for path in "$variant"/base-*.arrow "$variant"/delta-*.arrow; do
    [ -f "$path" ] || continue
    name=$(basename "$path")
    printf -- '--run %q %q %s %s ' "$root/$path" "$prefix/$name" \
      "$(sha256sum "$path" | cut -d' ' -f1)" "$(stat -c %s "$path")"
  done
  printf -- '--queries %q %q %s %s ' "$root/queries.parquet" "$V85_OUTPUT_URI/inputs/queries.parquet" \
    "$(sha256sum queries.parquet | cut -d' ' -f1)" "$(stat -c %s queries.parquet)"
  printf -- '--truth %q %q %s %s ' "$root/truth.parquet" "$V85_OUTPUT_URI/inputs/truth.parquet" \
    "$(sha256sum truth.parquet | cut -d' ' -f1)" "$(stat -c %s truth.parquet)"
}

phase=screen
for variant in incremental fresh; do
  if [ "$variant" = incremental ]; then prefix=$incremental_uri; else prefix=$fresh_uri; fi
  args=$(artifact_args "$variant" "$prefix")
  for budget in 64 128 256 512; do
    eval "\"$binary\" $args --page-budget $budget" >"result-$variant-p$budget.json" || exit 104
  done
done

phase=validate
.venv/bin/python - <<'PY' >summary.json || exit 105
import json
from pathlib import Path

out = {"cells": [], "gate": "reject"}
for budget in (64, 128, 256, 512):
    incremental = json.loads(Path(f"result-incremental-p{budget}.json").read_bytes())
    fresh = json.loads(Path(f"result-fresh-p{budget}.json").read_bytes())
    delta_ppm = fresh["aggregate_recall_ppm"] - incremental["aggregate_recall_ppm"]
    out["cells"].append({
        "budget": budget,
        "incremental_recall_ppm": incremental["aggregate_recall_ppm"],
        "incremental_worst_ppm": incremental["worst_recall_ppm"],
        "fresh_proxy_recall_ppm": fresh["aggregate_recall_ppm"],
        "fresh_proxy_worst_ppm": fresh["worst_recall_ppm"],
        "incremental_requests": incremental["total_requests"],
        "incremental_bytes": incremental["total_bytes"],
        "recall_delta_ppm": delta_ppm,
    })
if any(cell["recall_delta_ppm"] <= 2_000 and cell["incremental_recall_ppm"] >= 990_000
       for cell in out["cells"]):
    out["gate"] = "pass"
print(json.dumps(out, separators=(",", ":"), sort_keys=True))
PY

aws s3 cp queries.parquet "$V85_OUTPUT_URI/inputs/queries.parquet" --only-show-errors || exit 106
aws s3 cp truth.parquet "$V85_OUTPUT_URI/inputs/truth.parquet" --only-show-errors || exit 106
phase=complete
exit 0
