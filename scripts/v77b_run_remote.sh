#!/bin/bash
set -u
root=/mnt/v77-hierarchical
prefix=s3://borsuk-bench-453182569524-euc1/research/v77-algorithm-first/hierarchical
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
  printf '{"exit_code":%d,"phase":"%s","main_sha256":"%s","exporter_sha256":"%s"}\n'     "$code" "$phase" "e45b38ec8e21911969c0f5f3a2888eaca46309447f6b8eb71548f3bbecbd2901" "45e4a226a68fe754ee7966097d4cf27969d298b1168a6e233b467cea485f4b17" > /tmp/v71-terminal.json
  aws s3 cp /tmp/v71-terminal.json "$output_uri/terminal.json" --only-show-errors
  exit "$code"
}
trap publish_terminal EXIT
mkdir -p "$root/reader/src" && cd "$root" || exit 90
phase=toolchain
# SSM runs commands without HOME, and AL2023 ships no C linker for rustc.
export HOME=${HOME:-/root}
dnf install -y -q gcc > build.log 2>&1 || exit 88
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable >> build.log 2>&1 || exit 89
fi
export PATH="$HOME/.cargo/bin:$PATH"
phase=source-download
aws s3 cp "$prefix/main.rs" reader/src/main.rs --only-show-errors || exit 91
aws s3 cp "$prefix/Cargo.toml" reader/Cargo.toml --only-show-errors || exit 91
aws s3 cp "$prefix/v77_export_manifest.py" export.py --only-show-errors || exit 91
sha256sum -c > hashes.log 2>&1 <<HASHES
d62538bd68ac595a4169b26a9c5f60c1b17374d583bea1354cf45bb0a3bce951  reader/src/main.rs
0e647c73507c432d18a8c069fafe509ef9a280b9fa4f698833d8debb55de3077  reader/Cargo.toml
18e84a91c0fe1d4d41037e7d7e2d6aac23db3e379433076a39e9295016733775  export.py
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
export BORSUK_V71_URI="$sq8" BORSUK_V71_MANIFEST="$root/manifest.bin" BORSUK_V71_QUERIES=50
: > run.log
export BORSUK_V71_THROUGHPUT=1
for REGION in 1024 256; do
  set -- 512 2 128
  BORSUK_V71_REGIONS=$REGION BORSUK_V71_SHORTLIST=$1 BORSUK_V71_GAP=$2 BORSUK_V71_CONCURRENCY=$3     BORSUK_V71_OUTPUT="$root/result-qps-r$REGION.json"     ./reader/target/release/v71_native_reader >> run.log 2>&1 || exit 98
done
phase=complete
exit 0
