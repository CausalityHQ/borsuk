#!/bin/bash
set -uo pipefail

case "${1:-}" in
  --describe)
    printf '%s\n' '{"arms":["pq16-identity","sparse-residual-pq8","exact-f32"],"blas_threads":16,"development_end_exclusive":584,"development_start":456,"instance_type":"c7i.8xlarge","max_wall_seconds":1800,"page_budget":32,"queries":128,"query_parallelism":1,"residual_fraction_ppm":250000,"reuse_frozen_artifacts":true,"shortlist_rows":2048,"spot_only":true,"validation_or_holdout_reads":false}'
    exit 0
    ;;
  "") ;;
  *) echo "usage: $0 [--describe]" >&2; exit 2 ;;
esac

: "${V85_SOURCE_ARCHIVE_URI:?}"
: "${V85_SOURCE_ARCHIVE_SHA256:?}"
: "${V85_SOURCE_COMMIT:?}"
: "${V85_OUTPUT_URI:?}"

root=/mnt/v85-sparse-residual-1m-development
phase=bootstrap

publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  cd "$root" 2>/dev/null || true
  for name in install.log hashes.log screen.log screen.time result.json summary.json validate.log; do
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
ulimit -v $((48 * 1024 * 1024))
export HOME=${HOME:-/root}
export OMP_NUM_THREADS=16 OPENBLAS_NUM_THREADS=16 MKL_NUM_THREADS=16

phase=install
dnf install -y -q python3-pip tar zstd time >install.log 2>&1 || exit 91
python3 -m venv .venv >>install.log 2>&1 || exit 91
.venv/bin/pip install -q numpy==1.26.4 pyarrow==17.0.0 >>install.log 2>&1 || exit 91
aws s3 cp "$V85_SOURCE_ARCHIVE_URI" source.tar.zst --only-show-errors || exit 92
printf '%s  source.tar.zst\n' "$V85_SOURCE_ARCHIVE_SHA256" >hashes.log
sha256sum -c hashes.log >>install.log 2>&1 || exit 92
mkdir repo && tar --zstd -xf source.tar.zst -C repo || exit 92

phase=inputs
dataset_prefix=s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000
frozen_prefix=s3://borsuk-bench-453182569524-euc1/research/v85-pq16-1m-validation/da42b3da7a78a7a44ab259bb06f12c42527e8800/runs/v85-1m-validation-20260920T100638Z-da42b3d/a0001
source_uri=$dataset_prefix/source.parquet
query_uri=$dataset_prefix/development-query.parquet
truth_uri=$dataset_prefix/development-gt100.parquet
generation_uri=$frozen_prefix/artifacts/generation.json
aws s3 cp "$source_uri" source.parquet --only-show-errors || exit 96
aws s3 cp "$query_uri" queries.parquet --only-show-errors || exit 96
aws s3 cp "$truth_uri" truth.parquet --only-show-errors || exit 96
aws s3 cp "$generation_uri" generation.json --only-show-errors || exit 96
aws s3 cp "$frozen_prefix/artifacts/base-000.arrow" base-000.arrow --only-show-errors || exit 96
aws s3 cp "$frozen_prefix/artifacts/delta-000.arrow" delta-000.arrow --only-show-errors || exit 96
sha256sum -c >>hashes.log 2>&1 <<'HASHES' || exit 97
2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86  source.parquet
310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54  queries.parquet
fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11  truth.parquet
45fa4e708ab660151a7b1ea79e35eada6090faced1bfb147f7e20cac7055e754  generation.json
c2e86f6199777c21dde048ff4a46aae65ce96a4b20f03c11905851c80312b862  base-000.arrow
a0498ff17acbe8cc81a8b2501e7ffcd0d6998c5379efaae5895c64d3bcc7e869  delta-000.arrow
HASHES

phase=science
export PYTHONPATH="$root/repo"
/usr/bin/time -v -o screen.time timeout 1200 .venv/bin/python \
  repo/scripts/v85_pq16_page_nomination.py \
  --source source.parquet --source-uri "$source_uri" --source-sha256 2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86 \
  --queries queries.parquet --queries-uri "$query_uri" --queries-sha256 310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54 \
  --truth truth.parquet --truth-uri "$truth_uri" --truth-sha256 fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11 \
  --generation generation.json --generation-uri "$generation_uri" --generation-sha256 45fa4e708ab660151a7b1ea79e35eada6090faced1bfb147f7e20cac7055e754 \
  --base base-000.arrow --delta delta-000.arrow --output result.json \
  --dimensions 768 --neighbors 100 --queries-count 128 --query-start 456 \
  --query-field embedding --truth-layout long \
  --shortlist-rows 2048 --gap-pages 0 --seed 7216 \
  --sparse-residual-fraction-ppm 250000 --compare-sparse-residual-exact \
  >/dev/null 2>screen.log || exit 98

phase=validate
.venv/bin/python - <<'PY' >summary.json 2>validate.log || exit 99
import hashlib
import json
from pathlib import Path
from scripts.v85_pq16_rescore_summary import validate_sparse_residual_development_ceiling

body = Path("result.json").read_bytes()
summary = validate_sparse_residual_development_ceiling(
    json.loads(body), result_sha256=hashlib.sha256(body).hexdigest()
)
print(json.dumps(summary, separators=(",", ":"), sort_keys=True))
PY

phase=complete
exit 0
