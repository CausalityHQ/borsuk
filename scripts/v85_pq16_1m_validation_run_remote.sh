#!/bin/bash
set -uo pipefail

case "${1:-}" in
  --describe)
    printf '%s\n' '{"blas_threads":16,"instance_type":"c7i.8xlarge","max_wall_seconds":1800,"page_budget":32,"queries":1000,"query_parallelism":1,"shortlist_rows":2048,"split":"validation","spot_only":true}'
    exit 0
    ;;
  "") ;;
  *) echo "usage: $0 [--describe]" >&2; exit 2 ;;
esac

: "${V85_SOURCE_ARCHIVE_URI:?}"
: "${V85_SOURCE_ARCHIVE_SHA256:?}"
: "${V85_SOURCE_COMMIT:?}"
: "${V85_OUTPUT_URI:?}"

root=/mnt/v85-pq16-1m-validation
phase=bootstrap

publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  cd "$root" 2>/dev/null || true
  for name in install.log hashes.log prepare.log build.time pq16.time pq16-summary.json result.json; do
    [ -f "$name" ] && aws s3 cp "$name" "$V85_OUTPUT_URI/evidence/$name" --only-show-errors
  done
  instance_id=unknown
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
    http://169.254.169.254/latest/api/token 2>/dev/null)
  [ -n "$token" ] && instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" \
    http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null)
  printf '{"exit_code":%d,"instance_id":"%s","phase":"%s","source_commit":"%s"}\n' \
    "$code" "$instance_id" "$phase" "$V85_SOURCE_COMMIT" >terminal.json
  aws s3 cp terminal.json "$V85_OUTPUT_URI/terminal.json" --only-show-errors
  (sleep 5; shutdown -h now) >/dev/null 2>&1 &
  exit "$code"
}
trap publish_terminal EXIT

mkdir -p "$root" && cd "$root" || exit 90
ulimit -v $((24 * 1024 * 1024))
export HOME=${HOME:-/root}
export OMP_NUM_THREADS=16 OPENBLAS_NUM_THREADS=16 MKL_NUM_THREADS=16

phase=toolchain
dnf install -y -q zstd python3-devel >install.log 2>&1 || exit 91
python3 -m venv .venv >>install.log 2>&1 || exit 92
.venv/bin/python -m pip install --disable-pip-version-check --quiet \
  'numpy==1.26.4' 'pyarrow==17.0.0' >>install.log 2>&1 || exit 92

phase=source
aws s3 cp "$V85_SOURCE_ARCHIVE_URI" source.tar.zst --only-show-errors || exit 93
printf '%s  source.tar.zst\n' "$V85_SOURCE_ARCHIVE_SHA256" | sha256sum -c - >hashes.log 2>&1 || exit 94
mkdir repo
tar --zstd -xf source.tar.zst -C repo || exit 95

phase=inputs
input_prefix=s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000
source_uri=$input_prefix/source.parquet
query_source_uri=$input_prefix/validation-query.parquet
truth_source_uri=$input_prefix/validation-gt100.parquet
aws s3 cp "$source_uri" source.parquet --only-show-errors || exit 96
aws s3 cp "$query_source_uri" query-source.parquet --only-show-errors || exit 96
aws s3 cp "$truth_source_uri" truth-source.parquet --only-show-errors || exit 96
sha256sum -c >>hashes.log 2>&1 <<'HASHES' || exit 97
2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86  source.parquet
869e225181f7d01a972d8faa144eaff4838c7c1f8f7c0c55091487e234f0bd5e  query-source.parquet
bf0fb0c934c986d05282e3d1c63dc351c553976ea05bfcab0cd3f06d2979e871  truth-source.parquet
HASHES

phase=prepare
export PYTHONPATH="$root/repo"
.venv/bin/python - <<'PY' >prepare.log 2>&1 || exit 98
from pathlib import Path
from scripts.v85_build_delta import canonicalize_evaluation

canonicalize_evaluation(
    Path("query-source.parquet"), Path("truth-source.parquet"),
    Path("queries.parquet"), Path("truth.parquet"),
    dimensions=768, neighbors=100, query_limit=1_000,
)
PY
queries_uri=$V85_OUTPUT_URI/inputs/queries.parquet
truth_uri=$V85_OUTPUT_URI/inputs/truth.parquet
aws s3 cp queries.parquet "$queries_uri" --only-show-errors || exit 99
aws s3 cp truth.parquet "$truth_uri" --only-show-errors || exit 99

phase=build
artifact_prefix=$V85_OUTPUT_URI/artifacts
/usr/bin/time -v -o build.time timeout 900 .venv/bin/python repo/scripts/v85_build_delta.py \
  --source source.parquet --output build --uri-prefix "$artifact_prefix" \
  --base-rows 900000 --delta-rows 100000 --dimensions 768 --page-rows 256 \
  --router-cells 256 --base-runs 1 --delta-runs 1 --seed 85 >>prepare.log 2>&1 || exit 100
for path in build/generation.json build/router.arrow build/mutations.arrow build/base-*.arrow build/delta-*.arrow; do
  [ -f "$path" ] || exit 101
  aws s3 cp "$path" "$artifact_prefix/$(basename "$path")" --only-show-errors || exit 101
done

generation_sha=$(sha256sum build/generation.json | cut -d' ' -f1)
query_sha=$(sha256sum queries.parquet | cut -d' ' -f1)
truth_sha=$(sha256sum truth.parquet | cut -d' ' -f1)
base_path=$(printf '%s\n' build/base-*.arrow)
delta_path=$(printf '%s\n' build/delta-*.arrow)

phase=pq16
/usr/bin/time -v -o pq16.time timeout 1200 .venv/bin/python repo/scripts/v85_pq16_page_nomination.py \
  --source source.parquet --source-uri "$source_uri" --source-sha256 2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86 \
  --queries queries.parquet --queries-uri "$queries_uri" --queries-sha256 "$query_sha" \
  --truth truth.parquet --truth-uri "$truth_uri" --truth-sha256 "$truth_sha" \
  --generation build/generation.json --generation-uri "$artifact_prefix/generation.json" --generation-sha256 "$generation_sha" \
  --base "$base_path" --delta "$delta_path" --output result.json \
  --dimensions 768 --neighbors 100 --queries-count 1000 --shortlist-rows 2048 --gap-pages 0 --seed 7216 >pq16-summary.json || exit 102

phase=validate
.venv/bin/python - <<'PY' || exit 103
import hashlib, json
from pathlib import Path

body = Path("result.json").read_bytes()
result = json.loads(body)
if (
    result["schema"] != "borsuk-v85-pq16-page-nomination-result-v2"
    or result["queries"] != 1_000
    or len(result["samples"]) != 1_000
    or result["max_gets_per_query"] > 32
    or result["max_bytes_per_query"] > 16 * 1024 * 1024
):
    raise ValueError("1M validation evidence differs")
print(hashlib.sha256(body).hexdigest())
PY

phase=complete
exit 0
