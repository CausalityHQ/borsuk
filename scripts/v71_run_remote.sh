#!/bin/bash
set -u
root=/mnt/v71-native-reader
prefix=s3://borsuk-bench-453182569524-euc1/research/v71-algorithm-first/native-reader
output_uri="$prefix/a0001"
sq8=s3://borsuk-bench-453182569524-euc1/research/v70-algorithm-first/single-stage-a4a695d66f508edf/index/sq8.bin
phase=bootstrap
publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  for name in build.log export.log run.log hashes.log; do
    [ -f "$name" ] && aws s3 cp "$name" "$output_uri/$name" --only-show-errors
  done
  for name in result-*.json; do
    [ -f "$name" ] && aws s3 cp "$name" "$output_uri/$name" --only-show-errors
  done
  printf '{"exit_code":%d,"phase":"%s","main_sha256":"%s","exporter_sha256":"%s"}\n'     "$code" "$phase" "835dc00620442b34fe7de23382a2bbf36718f947a7b3e00448a16164221e2691" "dcc256e0e8305efdffa2c8ea108dfee13d007bc518c8740f8a6a75cba46acd26" > /tmp/v71-terminal.json
  aws s3 cp /tmp/v71-terminal.json "$output_uri/terminal.json" --only-show-errors
  exit "$code"
}
trap publish_terminal EXIT
mkdir -p "$root/reader/src" && cd "$root" || exit 90
phase=toolchain
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable > build.log 2>&1 || exit 89
fi
export PATH="$HOME/.cargo/bin:$PATH"
phase=source-download
aws s3 cp "$prefix/main.rs" reader/src/main.rs --only-show-errors || exit 91
aws s3 cp "$prefix/Cargo.toml" reader/Cargo.toml --only-show-errors || exit 91
aws s3 cp "$prefix/v71_export_manifest.py" export.py --only-show-errors || exit 91
sha256sum -c > hashes.log 2>&1 <<HASHES
835dc00620442b34fe7de23382a2bbf36718f947a7b3e00448a16164221e2691  reader/src/main.rs
ebe64158a21bc6cd108a8d31c425a5be3487428a5087c7f9470d9fcef0c4eaee  reader/Cargo.toml
dcc256e0e8305efdffa2c8ea108dfee13d007bc518c8740f8a6a75cba46acd26  export.py
HASHES
[ "$?" -ne 0 ] && exit 92
phase=build
(cd reader && cargo build --release --quiet) >> build.log 2>&1 || exit 93
phase=reader-self-test
./reader/target/release/v71_native_reader --self-test >> build.log 2>&1 || exit 94
phase=dependency-install
python3 -m venv .venv >> build.log 2>&1 || exit 95
.venv/bin/python -m pip install --disable-pip-version-check --quiet 'numpy==1.26.4' 'pyarrow==17.0.0' >> build.log 2>&1 || exit 95
phase=input-download
base=s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000
aws s3 cp "$base/source.parquet" source.parquet --only-show-errors || exit 96
aws s3 cp "$base/development-query.parquet" query.parquet --only-show-errors || exit 96
aws s3 cp "$base/development-gt100.parquet" gt.parquet --only-show-errors || exit 96
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v63-algorithm-first/layout-oracle-e2f6c2bad99c720b/a0001/artifacts/kmeans_8192-order.npy layout-order.npy --only-show-errors || exit 96
phase=export-manifest
export OMP_NUM_THREADS=32 OPENBLAS_NUM_THREADS=32
.venv/bin/python export.py --source source.parquet --development-query query.parquet   --ground-truth gt.parquet --layout-order layout-order.npy --output manifest.bin > export.log 2>&1 || exit 97
phase=measure
export AWS_DEFAULT_REGION=eu-central-1 BORSUK_V71_REGION=eu-central-1
export BORSUK_V71_URI="$sq8" BORSUK_V71_MANIFEST="$root/manifest.bin" BORSUK_V71_QUERIES=200
: > run.log
for point in "128 8 64" "128 8 128" "256 8 128" "128 16 128" "256 16 128" "64 8 64" "512 8 128"; do
  set -- $point
  BORSUK_V71_PAGES=$1 BORSUK_V71_GAP=$2 BORSUK_V71_CONCURRENCY=$3     BORSUK_V71_OUTPUT="$root/result-m$1-g$2-c$3.json"     ./reader/target/release/v71_native_reader >> run.log 2>&1 || exit 98
done
phase=complete
exit 0
