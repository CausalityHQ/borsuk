#!/bin/bash
set -uo pipefail

case "${1:-}" in
  --describe)
    printf '%s\n' '{"baseline_rerun":false,"blas_threads":16,"instance_type":"c7i.8xlarge","max_wall_seconds":1800,"page_budget":32,"queries":1000,"query_parallelism":1,"residual_fraction_ppm":250000,"residual_row_bytes":12,"reuse_frozen_artifacts":true,"shortlist_rows":2048,"split":"validation-burned-no-retuning","spot_only":true}'
    exit 0
    ;;
  "") ;;
  *) echo "usage: $0 [--describe]" >&2; exit 2 ;;
esac

: "${V85_SOURCE_ARCHIVE_URI:?}"
: "${V85_SOURCE_ARCHIVE_SHA256:?}"
: "${V85_SOURCE_COMMIT:?}"
: "${V85_OUTPUT_URI:?}"

root=/mnt/v85-sparse-residual-1m
phase=bootstrap

publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  cd "$root" 2>/dev/null || true
  for name in install.log hashes.log residual.time residual.json summary.json validate.log; do
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
source_uri=s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/source.parquet
frozen_prefix=s3://borsuk-bench-453182569524-euc1/research/v85-pq16-1m-validation/da42b3da7a78a7a44ab259bb06f12c42527e8800/runs/v85-1m-validation-20260920T100638Z-da42b3d/a0001
query_uri=$frozen_prefix/inputs/queries.parquet
truth_uri=$frozen_prefix/inputs/truth.parquet
generation_uri=$frozen_prefix/artifacts/generation.json
aws s3 cp "$source_uri" source.parquet --only-show-errors || exit 96
aws s3 cp "$query_uri" queries.parquet --only-show-errors || exit 96
aws s3 cp "$truth_uri" truth.parquet --only-show-errors || exit 96
aws s3 cp "$generation_uri" generation.json --only-show-errors || exit 96
aws s3 cp "$frozen_prefix/artifacts/base-000.arrow" base-000.arrow --only-show-errors || exit 96
aws s3 cp "$frozen_prefix/artifacts/delta-000.arrow" delta-000.arrow --only-show-errors || exit 96
aws s3 cp "$frozen_prefix/evidence/result.json" baseline.json --only-show-errors || exit 96
sha256sum -c >>hashes.log 2>&1 <<'HASHES' || exit 97
2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86  source.parquet
420194c5d554eddea321ffb58827a972ac0039c64c4cc6860b70047cf9a9493d  queries.parquet
9210281d05f0fa4190d297a025dce9d24c68239fb0948aff454d5b8c4fcd7ba7  truth.parquet
45fa4e708ab660151a7b1ea79e35eada6090faced1bfb147f7e20cac7055e754  generation.json
c2e86f6199777c21dde048ff4a46aae65ce96a4b20f03c11905851c80312b862  base-000.arrow
a0498ff17acbe8cc81a8b2501e7ffcd0d6998c5379efaae5895c64d3bcc7e869  delta-000.arrow
62f629f7706adc0972ce209dffc5ba4e02a8f2567aa3cad154fefda3a68aa0f2  baseline.json
HASHES

phase=science
export PYTHONPATH="$root/repo"
/usr/bin/time -v -o residual.time timeout 1200 .venv/bin/python \
  repo/scripts/v85_pq16_page_nomination.py \
  --source source.parquet --source-uri "$source_uri" --source-sha256 2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86 \
  --queries queries.parquet --queries-uri "$query_uri" --queries-sha256 420194c5d554eddea321ffb58827a972ac0039c64c4cc6860b70047cf9a9493d \
  --truth truth.parquet --truth-uri "$truth_uri" --truth-sha256 9210281d05f0fa4190d297a025dce9d24c68239fb0948aff454d5b8c4fcd7ba7 \
  --generation generation.json --generation-uri "$generation_uri" --generation-sha256 45fa4e708ab660151a7b1ea79e35eada6090faced1bfb147f7e20cac7055e754 \
  --base base-000.arrow --delta delta-000.arrow --output residual.json \
  --dimensions 768 --neighbors 100 --queries-count 1000 --shortlist-rows 2048 \
  --gap-pages 0 --seed 7216 --sparse-residual-fraction-ppm 250000 >/dev/null || exit 98

phase=validate
.venv/bin/python - <<'PY' >summary.json 2>validate.log || exit 99
import hashlib
import json
from pathlib import Path
from scripts.v85_pq16_rescore_summary import summarize_paired_sparse_residual

baseline_body = Path("baseline.json").read_bytes()
challenger_body = Path("residual.json").read_bytes()
summary = summarize_paired_sparse_residual(
    json.loads(baseline_body),
    json.loads(challenger_body),
    baseline_sha256=hashlib.sha256(baseline_body).hexdigest(),
    challenger_sha256=hashlib.sha256(challenger_body).hexdigest(),
    expected_fraction_ppm=250_000,
)
print(json.dumps(summary, separators=(",", ":"), sort_keys=True))
PY

phase=complete
exit 0
