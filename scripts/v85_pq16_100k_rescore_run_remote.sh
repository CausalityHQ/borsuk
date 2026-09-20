#!/bin/bash
set -uo pipefail

case "${1:-}" in
  --describe)
    printf '%s\n' '{"blas_threads":16,"instance_type":"c7i.8xlarge","max_wall_seconds":1800,"page_budget":32,"queries":1000,"query_parallelism":1,"shortlist_rows":2048,"spot_only":true}'
    exit 0
    ;;
  "") ;;
  *) echo "usage: $0 [--describe]" >&2; exit 2 ;;
esac

: "${V85_SOURCE_ARCHIVE_URI:?}"
: "${V85_SOURCE_ARCHIVE_SHA256:?}"
: "${V85_SOURCE_COMMIT:?}"
: "${V85_OUTPUT_URI:?}"

root=/mnt/v85-pq16-100k-rescore
phase=bootstrap

publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  cd "$root" 2>/dev/null || true
  for name in install.log hashes.log prepare.log truth.time centroid.time pq16.time summary.json centroid.json pq16.json; do
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
dnf install -y -q gcc zstd python3-devel >install.log 2>&1 || exit 91
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |
    sh -s -- -y --profile minimal --default-toolchain stable >>install.log 2>&1 || exit 92
fi
export PATH="$HOME/.cargo/bin:$PATH"
python3 -m venv .venv >>install.log 2>&1 || exit 93
.venv/bin/python -m pip install --disable-pip-version-check --quiet \
  'numpy==1.26.4' 'pyarrow==17.0.0' >>install.log 2>&1 || exit 93

phase=source
aws s3 cp "$V85_SOURCE_ARCHIVE_URI" source.tar.zst --only-show-errors || exit 94
printf '%s  source.tar.zst\n' "$V85_SOURCE_ARCHIVE_SHA256" | sha256sum -c - >hashes.log 2>&1 || exit 95
mkdir repo
tar --zstd -xf source.tar.zst -C repo || exit 96
test "$(git -C repo rev-parse HEAD 2>/dev/null || printf '%s' "$V85_SOURCE_COMMIT")" = "$V85_SOURCE_COMMIT" || exit 96
(cd repo && cargo build --release --locked -p borsuk-v71 --bin v85_delta_reader) >>install.log 2>&1 || exit 97

phase=inputs
artifact_prefix=s3://borsuk-bench-453182569524-euc1/research/v85-delta-qualification/856a5988e185f9f26be96354353510468c583ae7/100k-current-a0021/attempt/artifacts
source_uri=s3://borsuk-bench-453182569524-euc1/research/v85-pq16-page-nomination/24383d853474a19702d18d2de700bee3618167f5/100k-a0023/attempt/inputs/source-100k.parquet
evaluation_prefix=s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000
aws s3 cp "$source_uri" source.parquet --only-show-errors || exit 98
aws s3 cp "$evaluation_prefix/development-query.parquet" query-source.parquet --only-show-errors || exit 98
aws s3 cp "$evaluation_prefix/development-gt100.parquet" truth-source.parquet --only-show-errors || exit 98
for name in generation.json router.arrow mutations.arrow base-000.arrow delta-000.arrow; do
  aws s3 cp "$artifact_prefix/$name" "$name" --only-show-errors || exit 98
done
sha256sum -c >>hashes.log 2>&1 <<'HASHES' || exit 99
a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d  source.parquet
310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54  query-source.parquet
fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11  truth-source.parquet
04ad43e8e085231d355f9e65f4f6ad32b65b660f885a6c0d7724359cc51ff002  generation.json
8b8130994b2b0bb3217e3bfd993f3967adc5b6d0020da725755bcd18d8135081  base-000.arrow
0167013decfdd2b486e66229768bd89ef01d6c989f53dab739711e01f123023f  delta-000.arrow
HASHES

phase=truth
export PYTHONPATH="$root/repo"
/usr/bin/time -v -o truth.time timeout 600 .venv/bin/python - <<'PY' >prepare.log 2>&1 || exit 100
from pathlib import Path
from scripts.v85_build_delta import canonicalize_evaluation, compute_exact_truth

canonicalize_evaluation(
    Path("query-source.parquet"), Path("truth-source.parquet"),
    Path("queries.parquet"), Path("truth-original.parquet"),
    dimensions=768, neighbors=100, query_limit=1_000,
)
compute_exact_truth(
    Path("source.parquet"), Path("query-source.parquet"), Path("truth.parquet"),
    dimensions=768, corpus_rows=100_000, neighbors=100, query_limit=1_000,
)
PY
queries_uri=$V85_OUTPUT_URI/inputs/queries.parquet
truth_uri=$V85_OUTPUT_URI/inputs/truth-100k.parquet
aws s3 cp queries.parquet "$queries_uri" --only-show-errors || exit 101
aws s3 cp truth.parquet "$truth_uri" --only-show-errors || exit 101

generation_sha=$(sha256sum generation.json | cut -d' ' -f1)
router_sha=$(sha256sum router.arrow | cut -d' ' -f1)
mutations_sha=$(sha256sum mutations.arrow | cut -d' ' -f1)
query_sha=$(sha256sum queries.parquet | cut -d' ' -f1)
truth_sha=$(sha256sum truth.parquet | cut -d' ' -f1)
phase=centroid
/usr/bin/time -v -o centroid.time timeout 600 repo/target/release/v85_delta_reader \
  --generation "$root/generation.json" "$artifact_prefix/generation.json" "$generation_sha" "$(stat -c %s generation.json)" \
  --router "$root/router.arrow" "$artifact_prefix/router.arrow" "$router_sha" "$(stat -c %s router.arrow)" \
  --mutations "$root/mutations.arrow" "$artifact_prefix/mutations.arrow" "$mutations_sha" "$(stat -c %s mutations.arrow)" \
  --run-s3 "$artifact_prefix/base-000.arrow" 8b8130994b2b0bb3217e3bfd993f3967adc5b6d0020da725755bcd18d8135081 70969704 \
  --run-s3 "$artifact_prefix/delta-000.arrow" 0167013decfdd2b486e66229768bd89ef01d6c989f53dab739711e01f123023f 8170000 \
  --queries "$root/queries.parquet" "$queries_uri" "$query_sha" "$(stat -c %s queries.parquet)" \
  --truth "$root/truth.parquet" "$truth_uri" "$truth_sha" "$(stat -c %s truth.parquet)" \
  --page-budget 32 --range-concurrency 16 --region eu-central-1 >centroid.json || exit 102

phase=pq16
/usr/bin/time -v -o pq16.time timeout 1200 .venv/bin/python repo/scripts/v85_pq16_page_nomination.py \
  --source source.parquet --source-uri "$source_uri" --source-sha256 a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d \
  --queries queries.parquet --queries-uri "$queries_uri" --queries-sha256 "$query_sha" \
  --truth truth.parquet --truth-uri "$truth_uri" --truth-sha256 "$truth_sha" \
  --generation generation.json --generation-uri "$artifact_prefix/generation.json" --generation-sha256 "$generation_sha" \
  --base base-000.arrow --delta delta-000.arrow --output pq16.json \
  --dimensions 768 --neighbors 100 --queries-count 1000 --shortlist-rows 2048 --gap-pages 0 --seed 7216 || exit 103

phase=summary
.venv/bin/python - <<'PY' >summary.json || exit 104
import hashlib
import json
from pathlib import Path
import numpy as np

centroid = json.loads(Path("centroid.json").read_bytes())
pq16 = json.loads(Path("pq16.json").read_bytes())
if len(centroid["samples"]) != 1_000 or len(pq16["samples"]) != 1_000:
    raise ValueError("paired rescore query count differs")
centroid100 = []
centroid10 = []
pq100 = []
pq10 = []
for left, right in zip(centroid["samples"], pq16["samples"], strict=True):
    if left["query"] != right["query"] or left["truth_ids"] != right["truth_ids"]:
        raise ValueError("paired rescore authority differs")
    truth = left["truth_ids"]
    centroid100.append(len(set(left["result_ids"]).intersection(truth)))
    centroid10.append(len(set(left["result_ids"][:10]).intersection(truth[:10])))
    pq100.append(right["hits"])
    pq10.append(right["hits10"])

def paired_ci(delta, denominator):
    values = np.asarray(delta, dtype=np.int16)
    rng = np.random.default_rng(85_100_000)
    draws = rng.choice(values, size=(10_000, len(values)), replace=True).mean(axis=1)
    lo, hi = np.quantile(draws, [0.025, 0.975], method="nearest")
    return [int(lo * 1_000_000 // denominator), int(hi * 1_000_000 // denominator)]

summary = {
    "centroid_average_recall10_ppm": sum(centroid10) * 1_000_000 // 10_000,
    "centroid_average_recall100_ppm": sum(centroid100) * 1_000_000 // 100_000,
    "centroid_result_sha256": hashlib.sha256(Path("centroid.json").read_bytes()).hexdigest(),
    "paired_recall10_delta_ci95_ppm": paired_ci(np.subtract(pq10, centroid10), 10),
    "paired_recall100_delta_ci95_ppm": paired_ci(np.subtract(pq100, centroid100), 100),
    "pq16_average_recall10_ppm": pq16["average_recall10_ppm"],
    "pq16_average_recall100_ppm": pq16["average_recall100_ppm"],
    "pq16_gate_passed": pq16["gate_passed"],
    "pq16_p05_recall100_ppm": pq16["p05_recall100_ppm"],
    "pq16_result_sha256": hashlib.sha256(Path("pq16.json").read_bytes()).hexdigest(),
    "queries": 1_000,
    "schema": "borsuk-v85-pq16-100k-rescore-summary-v1",
}
print(json.dumps(summary, separators=(",", ":"), sort_keys=True))
PY

phase=complete
exit 0
