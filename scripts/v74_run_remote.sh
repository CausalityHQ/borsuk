#!/bin/bash
set -u
root=/mnt/v74-ingest
prefix=s3://borsuk-bench-453182569524-euc1/research/v74-algorithm-first/ingest
output_uri="$prefix/a0001"
phase=bootstrap
publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  for name in build.log run.log hashes.log; do
    [ -f "$name" ] && aws s3 cp "$name" "$output_uri/$name" --only-show-errors
  done
  printf '{"exit_code":%d,"phase":"%s"}\n' "$code" "$phase" > /tmp/v74-terminal.json
  aws s3 cp /tmp/v74-terminal.json "$output_uri/terminal.json" --only-show-errors
  exit "$code"
}
trap publish_terminal EXIT
mkdir -p "$root/reader/src/bin" && cd "$root" || exit 90
phase=toolchain
export HOME=${HOME:-/root}
dnf install -y -q gcc > build.log 2>&1 || exit 88
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable >> build.log 2>&1 || exit 89
fi
export PATH="$HOME/.cargo/bin:$PATH"
phase=source-download
aws s3 cp "$prefix/main.rs" reader/src/main.rs --only-show-errors || exit 91
aws s3 cp "$prefix/v74_ingest.rs" reader/src/bin/v74_ingest.rs --only-show-errors || exit 91
aws s3 cp "$prefix/Cargo.toml" reader/Cargo.toml --only-show-errors || exit 91
sha256sum -c > hashes.log 2>&1 <<HASHES
07263123bfc82ca9172899153cbfe5518e55ea6fc8e9befe28db39b8518187bb  reader/src/main.rs
9daf065a3d56a48db3eaf8805bb86b0555fcb1f80ffeae8c1f7fe0dc6fac594f  reader/src/bin/v74_ingest.rs
0e647c73507c432d18a8c069fafe509ef9a280b9fa4f698833d8debb55de3077  reader/Cargo.toml
HASHES
[ "$?" -ne 0 ] && exit 92
phase=build
(cd reader && cargo build --release --quiet) >> build.log 2>&1 || exit 93
phase=self-test
./reader/target/release/v74_ingest --self-test >> build.log 2>&1 || exit 94
phase=measure
export AWS_DEFAULT_REGION=eu-central-1 BORSUK_V74_REGION=eu-central-1
: > run.log
for point in "1000 200 32" "10000 40 16" "10000 40 64" "50000 20 16" "100000 10 8"; do
  set -- $point
  BORSUK_V74_URI="$prefix/deltas-b$1-c$3" BORSUK_V74_BATCH=$1 BORSUK_V74_BATCHES=$2 BORSUK_V74_CONCURRENCY=$3     ./reader/target/release/v74_ingest >> run.log 2>&1 || exit 95
done
aws s3 cp run.log "$output_uri/run.log" --only-show-errors
phase=complete
exit 0
