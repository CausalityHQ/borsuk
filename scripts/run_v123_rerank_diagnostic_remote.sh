#!/bin/bash
# One sealed V121 postmortem: Rust SQ8 top-K, then source F32/FP16 rerank.
set -euo pipefail
root=/mnt/v123-rerank
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
deadline_epoch=$((started_epoch + V123_WALL_SECONDS))
monitor_pid=
imds_token=
imds() {
  if [ -z "$imds_token" ]; then
    imds_token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600' \
      http://169.254.169.254/latest/api/token) || return
  fi
  curl -fsS -H "X-aws-ec2-metadata-token: $imds_token" \
    "http://169.254.169.254/latest/meta-data/$1"
}
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  [ -n "$monitor_pid" ] && kill "$monitor_pid" 2>/dev/null
  [ -n "$monitor_pid" ] && wait "$monitor_pid" 2>/dev/null
  instance_id=$(imds instance-id || true)
  upload_failed=0
  for path in install.log index-download.log index-download-resources.txt \
      python-test.log python-test-resources.txt \
      rust-test.log rust-test-resources.txt compile.log compile-resources.txt \
      input-hashes.log \
      wide-replay.log wide-replay-resources.txt wide-replay.jsonl \
      capture.log capture-resources.txt capture-evidence.jsonl capture-summary.json \
      source-download.log source-download-resources.txt \
      reduce.log reduce-resources.txt evidence.jsonl summary.json \
      worker.log interrupt-stop.txt; do
    if [ -f "$path" ] && ! timeout 900 aws s3 cp "$path" \
        "$V123_OUTPUT_PREFIX/artifacts/$path" --only-show-errors; then
      upload_failed=1
    fi
  done
  if [ "$upload_failed" -ne 0 ]; then code=96; phase=evidence-upload; fi
  ended_epoch=$(date +%s)
  python3 - "$code" "$phase" "$instance_id" "$started_epoch" "$ended_epoch" >terminal.json <<'PY'
import hashlib,json,os,sys
from pathlib import Path
names=('install.log','index-download.log','index-download-resources.txt',
       'python-test.log','python-test-resources.txt',
       'rust-test.log','rust-test-resources.txt','compile.log',
       'compile-resources.txt','input-hashes.log',
       'wide-replay.log','wide-replay-resources.txt','wide-replay.jsonl',
       'capture.log','capture-resources.txt','capture-evidence.jsonl',
       'capture-summary.json',
       'source-download.log','source-download-resources.txt',
       'reduce.log','reduce-resources.txt','evidence.jsonl','summary.json',
       'worker.log','interrupt-stop.txt')
artifacts={}
for name in names:
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for block in iter(lambda: source.read(1024*1024),b''):
                digest.update(block)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(sys.argv[1]);phase=sys.argv[2]
print(json.dumps({'schema':'borsuk-v123-rerank-postmortem-spot-v1',
    'source_commit':os.environ['V123_SOURCE_COMMIT'],
    'v121_terminal_sha256':os.environ['V123_V121_TERMINAL_SHA256'],
    'instance_id':sys.argv[3], 'exit_code':code, 'phase':phase,
    'status':'complete' if code==0 and phase=='complete' else 'failed',
    'elapsed_seconds':int(sys.argv[5])-int(sys.argv[4]),
    'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V123_OUTPUT_PREFIX/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
imds_token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600' \
  http://169.254.169.254/latest/api/token)
monitor() {
  while true; do
    if imds spot/instance-action >/dev/null 2>&1; then
      : >interrupt-stop.txt
      if [ -f science-pgid ]; then
        kill -TERM -- "-$(cat science-pgid)" 2>/dev/null || true
      fi
      kill -TERM "$main_pid" 2>/dev/null || true
      return
    fi
    sleep 5
  done
}
run_science() {
  label=$1; shift
  remaining=$((deadline_epoch - $(date +%s)))
  [ "$remaining" -gt 0 ] || exit 124
  setsid timeout --signal=TERM --kill-after=30 "$remaining" \
    /usr/bin/time -v "$@" >"$label.log" 2>"$label-resources.txt" &
  science_pid=$!
  printf '%s\n' "$science_pid" >science-pgid
  status=0
  wait "$science_pid" || status=$?
  rm -f science-pgid
  [ ! -f interrupt-stop.txt ] || exit 97
  [ "$status" -eq 0 ] || return "$status"
}
main_pid=$BASHPID
monitor & monitor_pid=$!
phase=install
dnf install -y -q python3.12 python3.12-pip tar gzip time gcc >install.log 2>&1
python3.12 -m venv .venv >>install.log 2>&1
.venv/bin/pip install -q numpy==1.26.4 pyarrow==17.0.0 boto3 >>install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=8
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal \
  --default-toolchain 1.98.0 >>install.log 2>&1
export PYTHONPATH="$root/repo"
export OMP_NUM_THREADS=8 OPENBLAS_NUM_THREADS=8 MKL_NUM_THREADS=8
phase=python-test
run_science python-test .venv/bin/python -m unittest -q \
  scripts.test_v123_rerank_diagnostic scripts.test_v123_launcher
phase=index-download
run_science index-download .venv/bin/python repo/scripts/v121_download_index.py \
  --index-prefix "$V123_INDEX_PREFIX" \
  --terminal-sha256 "$V123_INDEX_TERMINAL_SHA256" --output-root .
phase=compile
run_science rust-test "$CARGO_HOME/bin/cargo" test --release --locked -j 8 \
  --manifest-path repo/scripts/v114_score_rust/Cargo.toml \
  replay_top_k_defaults_to_production_100_and_bounds_diagnostic_width
run_science compile "$CARGO_HOME/bin/cargo" build --release --locked -j 8 \
  --manifest-path repo/scripts/v114_score_rust/Cargo.toml
phase=input-download
aws s3 cp "$V123_REQUESTS_URI" requests.jsonl --only-show-errors
aws s3 cp "$V123_REPLAY_URI" sealed-replay.jsonl --only-show-errors
aws s3 cp "$V123_TRUTH_URI" neighbors.parquet --only-show-errors
printf '%s  requests.jsonl\n' "$V123_REQUESTS_SHA256" >input-hashes.log
printf '%s  sealed-replay.jsonl\n' "$V123_REPLAY_SHA256" >>input-hashes.log
printf '%s  neighbors.parquet\n' "$V123_TRUTH_SHA256" >>input-hashes.log
sha256sum -c input-hashes.log
phase=wide-replay
run_science wide-replay "$CARGO_TARGET_DIR/release/borsuk-v114-score-gate" \
  replay-returned mirror/manifest.json sq8.bin mirror/blocks.sha256 \
  requests.jsonl 1600
cp wide-replay.log wide-replay.jsonl
phase=capture
run_science capture .venv/bin/python -m scripts.v123_rerank_diagnostic \
  --phase capture --layout built/layout.npy \
  --built-manifest built/manifest.json --requests requests.jsonl \
  --sealed-replay sealed-replay.jsonl --wide-replay wide-replay.jsonl \
  --truth neighbors.parquet --evidence capture-evidence.jsonl \
  --summary capture-summary.json
for path in capture.log capture-resources.txt capture-evidence.jsonl \
    capture-summary.json; do
  aws s3 cp "$path" "$V123_OUTPUT_PREFIX/artifacts/$path" --only-show-errors
done
phase=source-download
run_science source-download aws s3 cp "$V123_SOURCE_URI" source.parquet --only-show-errors
[ "$(stat -c%s source.parquet)" = "$V123_SOURCE_BYTES" ] || exit 93
printf '%s  source.parquet\n' "$V123_SOURCE_SHA256" >>input-hashes.log
sha256sum -c input-hashes.log
phase=reduce
run_science reduce .venv/bin/python -m scripts.v123_rerank_diagnostic \
  --source source.parquet --layout built/layout.npy \
  --built-manifest built/manifest.json --requests requests.jsonl \
  --sealed-replay sealed-replay.jsonl --wide-replay wide-replay.jsonl \
  --truth neighbors.parquet --capture-evidence capture-evidence.jsonl \
  --evidence evidence.jsonl --summary summary.json
phase=complete
exit 0
