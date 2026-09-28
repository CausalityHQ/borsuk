#!/usr/bin/env bash
set -eu
campaign_dir=$(cd "$1" && pwd)
cd "$campaign_dir"
finish() {
    result=$?
    printf '{"exit_code":%s,"stage":"native-paired-quality"}\n' "$result" > terminal.json
}
trap finish EXIT
ulimit -v 4194304
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
export PYTHONPATH="$campaign_dir/../quality-repo:$campaign_dir/../repo"
sha256sum config.json run.py run.sh ../rust-repo/target/release/examples/build_sq8_source ../rust-repo/target/release/build_two_bit_generation ../rust-repo/target/release/two_bit_plan_demo > inputs.sha256
/usr/bin/time -v -o campaign.time timeout --kill-after=10s 1800 \
  /snap/bin/uv run --offline --no-project --with numpy==2.3.3 --with pyarrow==24.0.0 python run.py "$campaign_dir" "${@:2}"
