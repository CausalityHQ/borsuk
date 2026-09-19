#!/bin/bash
set -u

root=/mnt/v84-query-level-cpu
prefix=s3://borsuk-bench-453182569524-euc1/research/v84-algorithm-first/query-level-cpu
output_uri="$prefix/a0003"
sq8=s3://borsuk-bench-453182569524-euc1/research/v70-algorithm-first/single-stage-a4a695d66f508edf/index/sq8.bin
phase=bootstrap
pidstat_pid=

publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  [ -n "$pidstat_pid" ] && kill "$pidstat_pid" 2>/dev/null
  for name in build.log export.log run.log hashes.log perf-stat.log perf-report.txt pidstat.log perf.data.zst; do
    [ -f "$name" ] && aws s3 cp "$name" "$output_uri/$name" --only-show-errors
  done
  for name in result-*.json; do
    [ -f "$name" ] && aws s3 cp "$name" "$output_uri/$name" --only-show-errors
  done
  printf '{"exit_code":%d,"phase":"%s"}\n' "$code" "$phase" >/tmp/v84-terminal.json
  aws s3 cp /tmp/v84-terminal.json "$output_uri/terminal.json" --only-show-errors
  unlink /tmp/v84-terminal.json
  exit "$code"
}
trap publish_terminal EXIT

mkdir -p "$root/reader/src" && cd "$root" || exit 90
phase=toolchain
export HOME=${HOME:-/root}
dnf install -y -q gcc perf sysstat zstd >build.log 2>&1 || exit 88
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |
    sh -s -- -y --profile minimal --default-toolchain stable >>build.log 2>&1 || exit 89
fi
export PATH="$HOME/.cargo/bin:$PATH"

phase=source-download
aws s3 cp "$prefix/main.rs" reader/src/main.rs --only-show-errors || exit 91
aws s3 cp "$prefix/Cargo.toml" reader/Cargo.toml --only-show-errors || exit 91
aws s3 cp "$prefix/v77_export_manifest.py" export.py --only-show-errors || exit 91
sha256sum -c >hashes.log 2>&1 <<'HASHES'
a69ed93614e5915ddac1b7eda9e36596fb896c0432f68247f1c8f3fdf8fee4c8  reader/src/main.rs
2ed97eb5933f848ec35b89bb8afeee69ac41d2b718209366e02a3f93f266ca5f  reader/Cargo.toml
18e84a91c0fe1d4d41037e7d7e2d6aac23db3e379433076a39e9295016733775  export.py
HASHES
[ "$?" -ne 0 ] && exit 92

phase=build
export RUSTFLAGS='-C force-frame-pointers=yes -C debuginfo=1'
(cd reader && cargo build --release --quiet) >>build.log 2>&1 || exit 93
./reader/target/release/v71_native_reader --self-test >>build.log 2>&1 || exit 94

phase=dependency-install
python3 -m venv .venv >>build.log 2>&1 || exit 95
.venv/bin/python -m pip install --disable-pip-version-check --quiet \
  'numpy==1.26.4' 'pyarrow==17.0.0' >>build.log 2>&1 || exit 95

phase=input-download
base=s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000
aws s3 cp "$base/source.parquet" source.parquet --only-show-errors || exit 96
aws s3 cp "$base/development-query.parquet" query.parquet --only-show-errors || exit 96
aws s3 cp "$base/development-gt100.parquet" gt.parquet --only-show-errors || exit 96
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v63-algorithm-first/layout-oracle-e2f6c2bad99c720b/a0001/artifacts/kmeans_8192-order.npy layout-order.npy --only-show-errors || exit 96

phase=export-manifest
export OMP_NUM_THREADS=32 OPENBLAS_NUM_THREADS=32
.venv/bin/python export.py \
  --source source.parquet \
  --development-query query.parquet \
  --ground-truth gt.parquet \
  --layout-order layout-order.npy \
  --output manifest.bin >export.log 2>&1 || exit 97
.venv/bin/python - <<'PY' || exit 97
from pathlib import Path

if Path("manifest.bin").read_bytes()[:8] != b"BRSKV77\0":
    raise SystemExit("exported manifest magic differs from BRSKV77")
PY

phase=measure
export AWS_DEFAULT_REGION=eu-central-1 BORSUK_V71_REGION=eu-central-1
export BORSUK_V71_URI="$sq8" BORSUK_V71_MANIFEST="$root/manifest.bin"
export BORSUK_V71_QUERIES=16 BORSUK_V71_THROUGHPUT=1
export BORSUK_V71_REGIONS=256 BORSUK_V71_SHORTLIST=512
export BORSUK_V71_GAP=2 BORSUK_V71_CONCURRENCY=128
: >run.log
for cell in rayon-a sequential-a sequential-b rayon-b; do
  mode=${cell%-*}
  BORSUK_V71_IN_QUERY_CPU=$mode \
    BORSUK_V71_OUTPUT="$root/result-$cell.json" \
    ./reader/target/release/v71_native_reader >>run.log 2>&1 || exit 98
done

phase=profile-sequential
pidstat -t -p ALL 1 >pidstat.log 2>&1 &
pidstat_pid=$!
BORSUK_V71_IN_QUERY_CPU=sequential \
  BORSUK_V71_OUTPUT="$root/result-sequential-profile.json" \
  perf stat \
    -e task-clock,context-switches,cpu-migrations,page-faults,cycles,instructions,cache-references,cache-misses \
    -o perf-stat.log -- \
  perf record -F 199 -g --call-graph fp -o perf.data -- \
  ./reader/target/release/v71_native_reader >>run.log 2>&1 || exit 99
kill "$pidstat_pid" 2>/dev/null || true
wait "$pidstat_pid" 2>/dev/null || true
pidstat_pid=
perf report -i perf.data --stdio --no-children --sort comm,dso,symbol \
  --percent-limit 0.1 >perf-report.txt 2>&1 || exit 100
zstd -q -3 perf.data -o perf.data.zst || exit 101

phase=complete
exit 0
