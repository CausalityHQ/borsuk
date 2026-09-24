#!/bin/bash
# One frozen V136 concurrent local route replay on a Causality Spot worker.
set -euo pipefail
root=/mnt/v136-concurrent-route
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
deadline_epoch=$((started_epoch + V136_WALL_SECONDS))
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
  for path in install.log compile.log compile-resources.txt \
      prepare.log prepare-resources.txt prepared/prepare-summary.json \
      evaluate.log evaluate-resources.txt replay.jsonl summary.json \
      cgroup-memory.txt worker.log interrupt-stop.txt; do
    if [ -f "$path" ] && ! timeout 900 aws s3 cp "$path" \
        "$V136_OUTPUT_PREFIX/artifacts/$path" --only-show-errors; then
      upload_failed=1
    fi
  done
  if [ "$upload_failed" -ne 0 ]; then code=96; phase=evidence-upload; fi
  ended_epoch=$(date +%s)
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" \
    STARTED_EPOCH="$started_epoch" ENDED_EPOCH="$ended_epoch" \
    python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
names=('install.log','compile.log','compile-resources.txt',
       'prepare.log','prepare-resources.txt','prepared/prepare-summary.json',
       'evaluate.log','evaluate-resources.txt','replay.jsonl','summary.json',
       'cgroup-memory.txt','worker.log','interrupt-stop.txt')
artifacts={}
for name in names:
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for block in iter(lambda:source.read(4*1024*1024),b''):
                digest.update(block)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE']);phase=os.environ['PHASE']
print(json.dumps({'schema':'borsuk-v136-concurrent-route-transport-spot-v1',
  'source_commit':os.environ['V136_SOURCE_COMMIT'],
  'source_archive_sha256':os.environ['V136_ARCHIVE_SHA256'],
  'generation_manifest_sha256':os.environ['V136_GENERATION_SHA256'],
  'generation_terminal_sha256':os.environ['V136_GENERATION_TERMINAL_SHA256'],
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'phase':phase,
  'status':'complete' if code==0 and phase=='complete' else 'failed',
  'elapsed_seconds':int(os.environ['ENDED_EPOCH'])-int(os.environ['STARTED_EPOCH']),
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V136_OUTPUT_PREFIX/terminal.json" --only-show-errors
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
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time >install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/cargo-target" CARGO_BUILD_JOBS=8
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal \
  --default-toolchain 1.98.0 >>install.log 2>&1
phase=compile
run_science compile "$CARGO_HOME/bin/cargo" build --release --locked -j 8 \
  --manifest-path repo/crates/borsuk/Cargo.toml --example v134_native_route
phase=prepare
run_science prepare python3 repo/scripts/prepare_v132_live_inputs.py prepared
phase=evaluate
mkdir /sys/fs/cgroup/v136-serving
sync
echo 3 >/proc/sys/vm/drop_caches
run_science evaluate bash -c \
  'echo "$BASHPID" >/sys/fs/cgroup/v136-serving/cgroup.procs; exec "$@"' \
  v136 "$CARGO_TARGET_DIR/release/examples/v134_native_route" \
  prepared replay.jsonl summary.json --concurrent
{
  cat /sys/fs/cgroup/v136-serving/memory.current
  cat /sys/fs/cgroup/v136-serving/memory.peak
  cat /sys/fs/cgroup/v136-serving/memory.stat
} >cgroup-memory.txt
phase=complete
exit 0
