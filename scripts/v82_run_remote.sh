#!/bin/bash
set -u
root=/mnt/v82-scale
prefix=s3://borsuk-bench-453182569524-euc1/research/v82-algorithm-first/scale-10m
output_uri="$prefix/a0001"
phase=bootstrap
publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  for name in build.log hashes.log buildreport.json run.log; do
    [ -f "$name" ] && aws s3 cp "$name" "$output_uri/$name" --only-show-errors
  done
  printf '{"exit_code":%d,"phase":"%s"}\n' "$code" "$phase" > /tmp/v82-terminal.json
  aws s3 cp /tmp/v82-terminal.json "$output_uri/terminal.json" --only-show-errors
  exit "$code"
}
trap publish_terminal EXIT
mkdir -p "$root/reader/src" && cd "$root" || exit 90
phase=toolchain
export HOME=${HOME:-/root}
dnf install -y -q gcc time python3-pip > build.log 2>&1 || exit 88
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable >> build.log 2>&1 || exit 89
fi
export PATH="$HOME/.cargo/bin:$PATH"
phase=source-download
aws s3 cp "$prefix/main.rs" reader/src/main.rs --only-show-errors || exit 91
aws s3 cp "$prefix/Cargo.toml" reader/Cargo.toml --only-show-errors || exit 91
aws s3 cp "$prefix/v82_scale_build.py" build.py --only-show-errors || exit 91
sha256sum -c > hashes.log 2>&1 <<HASHES
3e9ea285af3df3846772ffe96168dbf1ec47bdf12e0b62913fff6333b5aa29d6  reader/src/main.rs
0e647c73507c432d18a8c069fafe509ef9a280b9fa4f698833d8debb55de3077  reader/Cargo.toml
64eb70bfa2f49bb406b174fe1ab8ba39bb65c682c255db07862415b4ac4c3dc9  build.py
HASHES
[ "$?" -ne 0 ] && exit 92
phase=build-reader
(cd reader && cargo build --release --quiet) >> build.log 2>&1 || exit 93
./reader/target/release/v71_native_reader --self-test >> build.log 2>&1 || exit 94
phase=dependency-install
python3 -m venv .venv >> build.log 2>&1 || exit 95
.venv/bin/python -m pip install --disable-pip-version-check --quiet 'numpy==1.26.4' 'h5py' >> build.log 2>&1 || exit 95
.venv/bin/python build.py --self-test >> build.log 2>&1 || exit 95
phase=build-index
export OMP_NUM_THREADS=32 OPENBLAS_NUM_THREADS=32
.venv/bin/python build.py --source deep-image-96-angular.hdf5 --clusters 16384 --queries 1000   --sq8-out sq8.bin --manifest-out manifest.bin --report buildreport.json >> build.log 2>&1 || exit 96
phase=publish-index
aws s3 cp sq8.bin "$prefix/index/sq8.bin" --only-show-errors || exit 97
phase=measure
export AWS_DEFAULT_REGION=eu-central-1 BORSUK_V71_REGION=eu-central-1
export BORSUK_V71_URI="$prefix/index/sq8.bin" BORSUK_V71_MANIFEST="$root/manifest.bin"
export BORSUK_V71_QUERIES=200 BORSUK_V71_THROUGHPUT=1
: > run.log
for REGION in 4096 16384; do
  BORSUK_V71_REGIONS=$REGION BORSUK_V71_SHORTLIST=512 BORSUK_V71_GAP=2 BORSUK_V71_CONCURRENCY=128     BORSUK_V71_OUTPUT="$root/result-r$REGION.json" ./reader/target/release/v71_native_reader >> run.log 2>&1 || exit 98
done
aws s3 cp run.log "$output_uri/run.log" --only-show-errors
phase=complete
exit 0
