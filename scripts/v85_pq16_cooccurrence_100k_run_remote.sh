#!/bin/bash
set -uo pipefail

case "${1:-}" in
  --describe)
    printf '%s\n' '{"blas_threads":16,"instance_type":"c7i.8xlarge","max_wall_seconds":1200,"page_budget":32,"pseudoqueries":256,"pseudoquery_shortlist_rows":512,"pseudoquery_unique_pages":32,"queries":1000,"query_parallelism":1,"shortlist_rows":2048,"split":"burned-development","spot_only":true}'
    exit 0
    ;;
  "") ;;
  *) echo "usage: $0 [--describe]" >&2; exit 2 ;;
esac

: "${V85_SOURCE_ARCHIVE_URI:?}"
: "${V85_SOURCE_ARCHIVE_SHA256:?}"
: "${V85_SOURCE_COMMIT:?}"
: "${V85_OUTPUT_URI:?}"

root=/mnt/v85-pq16-cooccurrence-100k
phase=bootstrap

publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  cd "$root" 2>/dev/null || true
  for name in install.log hashes.log cooccurrence.time result.json summary.json; do
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
ulimit -v $((16 * 1024 * 1024))
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
artifact_prefix=s3://borsuk-bench-453182569524-euc1/research/v85-delta-qualification/856a5988e185f9f26be96354353510468c583ae7/100k-current-a0021/attempt/artifacts
source_uri=s3://borsuk-bench-453182569524-euc1/research/v85-pq16-page-nomination/24383d853474a19702d18d2de700bee3618167f5/100k-a0023/attempt/inputs/source-100k.parquet
rescore_prefix=s3://borsuk-bench-453182569524-euc1/research/v85-competitive-rescore/fb976932ecd4076e2f76a7cb7e7aa7efe01e9a2d/runs/v85-100k-dev1000-20260920T094401Z-fb976932/a0001
query_uri=$rescore_prefix/inputs/queries.parquet
truth_uri=$rescore_prefix/inputs/truth-100k.parquet
aws s3 cp "$source_uri" source.parquet --only-show-errors || exit 96
aws s3 cp "$query_uri" queries.parquet --only-show-errors || exit 96
aws s3 cp "$truth_uri" truth.parquet --only-show-errors || exit 96
for name in generation.json base-000.arrow delta-000.arrow; do
  aws s3 cp "$artifact_prefix/$name" "$name" --only-show-errors || exit 96
done
sha256sum -c >>hashes.log 2>&1 <<'HASHES' || exit 97
a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d  source.parquet
4834cf63a50971b7d605c00f91b5142f67b049e91ea2c62c220271b50bffa6ac  queries.parquet
ab8bfae34f753512f352581218596fc0f043354f8168192c856278b3ab5a0ce7  truth.parquet
04ad43e8e085231d355f9e65f4f6ad32b65b660f885a6c0d7724359cc51ff002  generation.json
8b8130994b2b0bb3217e3bfd993f3967adc5b6d0020da725755bcd18d8135081  base-000.arrow
0167013decfdd2b486e66229768bd89ef01d6c989f53dab739711e01f123023f  delta-000.arrow
HASHES

phase=science
export PYTHONPATH="$root/repo"
generation_sha=$(sha256sum generation.json | cut -d' ' -f1)
/usr/bin/time -v -o cooccurrence.time timeout 900 .venv/bin/python \
  repo/scripts/v85_pq16_page_nomination.py \
  --source source.parquet --source-uri "$source_uri" --source-sha256 a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d \
  --queries queries.parquet --queries-uri "$query_uri" --queries-sha256 4834cf63a50971b7d605c00f91b5142f67b049e91ea2c62c220271b50bffa6ac \
  --truth truth.parquet --truth-uri "$truth_uri" --truth-sha256 ab8bfae34f753512f352581218596fc0f043354f8168192c856278b3ab5a0ce7 \
  --generation generation.json --generation-uri "$artifact_prefix/generation.json" --generation-sha256 "$generation_sha" \
  --base base-000.arrow --delta delta-000.arrow --output result.json \
  --dimensions 768 --neighbors 100 --queries-count 1000 --shortlist-rows 2048 \
  --gap-pages 0 --seed 7216 --cooccurrence-pseudoqueries 256 \
  --cooccurrence-shortlist-rows 512 --cooccurrence-unique-pages 32 >summary.json || exit 98

phase=validate
.venv/bin/python - <<'PY' || exit 99
import hashlib, json
from pathlib import Path

body = Path("result.json").read_bytes()
result = json.loads(body)
if (
    result["schema"] != "borsuk-v85-pq16-cooccurrence-layout-result-v1"
    or result["queries"] != 1_000
    or len(result["control"]["samples"]) != 1_000
    or len(result["cooccurrence"]["samples"]) != 1_000
    or result["layout"]["pseudoquery_count"] != 256
    or not result["layout"]["constructed_without_evaluation_queries_or_truth"]
):
    raise ValueError("PQ cooccurrence evidence differs")
print(hashlib.sha256(body).hexdigest())
PY

phase=complete
exit 0
