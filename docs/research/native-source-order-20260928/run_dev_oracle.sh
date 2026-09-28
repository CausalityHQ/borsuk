#!/usr/bin/env bash
set -eu
campaign_dir=$(cd "$1" && pwd)
cd "$campaign_dir"
finish() {
    result=$?
    printf '{"exit_code":%s,"stage":"native-fit-and-dev-page-oracle"}\n' "$result" > terminal.json
}
trap finish EXIT
source_file="$campaign_dir/../native-sq8-cohere/normalized.f32"
truth_file="$campaign_dir/../native-sq8-cohere-input/cohere-truth.u32"
binary="$campaign_dir/../rust-repo/target/release/examples/build_sq8_source"
source_sha=57b9bb6297ee0c4c5eb276dec0ddf64c16307a73ca3bbb2e16ce986d24a743b3
truth_sha=06cd59b31962d4190367b54d7abf24dd4e018d3c4ac8da0b2b528d21a5a7cbb8
sha256sum "$binary" "$source_file" "$truth_file" v283_page_oracle.py oracle_check.py > inputs.sha256
# The virtual-address cap conservatively enforces the predeclared 4GiB RSS cap.
ulimit -v 4194304
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
/usr/bin/time -v -o fit.time timeout --signal=TERM --kill-after=10s 900 \
    "$binary" fit "$source_file" "$source_sha" 100000 768 268435456 "$campaign_dir/order.u64" > fit.json
/snap/bin/uv run --offline --no-project --with numpy==2.3.3 \
    python oracle_check.py "$campaign_dir" "$truth_file" "$truth_sha" > oracle.stdout
