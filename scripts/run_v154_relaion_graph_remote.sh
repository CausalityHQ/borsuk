#!/bin/bash
# One frozen V154 ReLAION-1M paired CPU screen on Causality Spot.
set -euo pipefail
root=/mnt/v154-relaion-graph
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
deadline_epoch=$((started_epoch + V154_WALL_SECONDS - 900))
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
  trap - EXIT
  trap '' TERM
  set +e
  [ -n "$monitor_pid" ] && kill "$monitor_pid" 2>/dev/null
  [ -n "$monitor_pid" ] && wait "$monitor_pid" 2>/dev/null
  instance_id=$(imds instance-id || true)
  if [ -d /sys/fs/cgroup/v154-science ]; then
    { cat /sys/fs/cgroup/v154-science/memory.current;
      cat /sys/fs/cgroup/v154-science/memory.peak;
      cat /sys/fs/cgroup/v154-science/memory.stat; } >science.cgroup-memory.txt
  fi
  upload_failed=0
  for path in install.log core-check.log core-check-resources.txt \
      core-test.log core-test-resources.txt \
      gate-build.log gate-build-resources.txt download.log \
      prepare.log prepare-resources.txt fresh/queries.jsonl \
      fresh/routing.jsonl fresh/truth.npy fresh/manifest.json \
      science.log science-resources.txt science.jsonl \
      science.summary.json science.graph.bin science.cgroup-memory.txt \
      replay.log replay-resources.txt replay.jsonl replay.seal.json \
      audit.log audit-resources.txt returned.audit.json \
      reduce.log reduce-resources.txt evidence.jsonl returned.summary.json \
      decision.json worker.log interrupt-stop.txt; do
    if [ -f "$path" ] && ! timeout 300 aws s3 cp "$path" \
        "$V154_OUTPUT_PREFIX/artifacts/$path" --only-show-errors; then
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
names=('install.log','core-check.log','core-check-resources.txt',
       'core-test.log','core-test-resources.txt',
       'gate-build.log','gate-build-resources.txt','download.log',
       'prepare.log','prepare-resources.txt','fresh/queries.jsonl',
       'fresh/routing.jsonl','fresh/truth.npy','fresh/manifest.json',
       'science.log','science-resources.txt','science.jsonl',
       'science.summary.json','science.graph.bin','science.cgroup-memory.txt',
       'replay.log','replay-resources.txt','replay.jsonl','replay.seal.json',
       'audit.log','audit-resources.txt','returned.audit.json',
       'reduce.log','reduce-resources.txt','evidence.jsonl','returned.summary.json',
       'decision.json','worker.log','interrupt-stop.txt')
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
print(json.dumps({'schema':'borsuk-v154-relaion-graph-spot-v1',
  'source_commit':os.environ['V154_SOURCE_COMMIT'],
  'source_archive_sha256':os.environ['V154_ARCHIVE_SHA256'],
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'phase':phase,
  'status':'complete' if code==0 and phase=='complete' else 'failed',
  'interrupted':os.path.isfile('interrupt-stop.txt'),
  'elapsed_seconds':int(os.environ['ENDED_EPOCH'])-int(os.environ['STARTED_EPOCH']),
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V154_OUTPUT_PREFIX/terminal.json" --only-show-errors
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
download_checked() {
  name=$1; key=$2; bytes=$3; sha=$4
  mkdir -p "$(dirname "$name")"
  aws s3 cp "s3://borsuk-bench-453182569524-euc1/$key" "$name" \
    --only-show-errors >>download.log 2>&1
  [ "$(stat -c%s "$name")" = "$bytes" ] || exit 93
  printf '%s  %s\n' "$sha" "$name" | sha256sum -c - >>download.log
}
main_pid=$BASHPID
monitor & monitor_pid=$!
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time >install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/cargo-target" CARGO_BUILD_JOBS=8
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal \
  --default-toolchain 1.98.0 >>install.log 2>&1
phase=core-check
run_science core-check "$CARGO_HOME/bin/cargo" check --lib --locked -j 8 \
  --manifest-path repo/crates/borsuk/Cargo.toml
phase=core-test
run_science core-test "$CARGO_HOME/bin/cargo" test --lib --locked -j 8 \
  --manifest-path repo/crates/borsuk/Cargo.toml unit_centroid
phase=gate-build
run_science gate-build "$CARGO_HOME/bin/cargo" build --release --locked -j 8 \
  --manifest-path repo/crates/borsuk/Cargo.toml --bin v154_relaion_page_graph
phase=download
: >download.log
v146=research/v146-centroid-score/ac9c60b3b2a8cb18bfa3c9a9417b0f8e7d831bc0/runs/v146-20260924T111500Z/a0001
download_checked v146-terminal.json "$v146/terminal.json" 2844 45d98502b274be939c9e9fd4ba4fca2033cdb3a0323cf28a6148ffd88a095086
download_checked relaion/centroids.bin "$v146/artifacts/relaion.centroids.bin" 48000032 07ad4736d8eb6867d46523de71ed61d82220dabc923f3c88ead1cd13eb776651
v116=research/v116-validation-paired/5e9b35ad40ea023eab4407aa611d759e1893bb34/runs/v116-validation-20260923T235426Z/a0001/artifacts
download_checked relaion/requests.jsonl "$v116/requests.jsonl" 18726909 c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9
python3 - <<'PYINNER'
import json
v146=json.load(open('v146-terminal.json'))
if (v146.get('status')!='complete' or v146.get('exit_code')!=0
    or v146.get('source_commit')!='ac9c60b3b2a8cb18bfa3c9a9417b0f8e7d831bc0'
    or v146['artifacts']['relaion.centroids.bin']['sha256']!='07ad4736d8eb6867d46523de71ed61d82220dabc923f3c88ead1cd13eb776651'):
    raise ValueError('V146 terminal or centroid identity differs')
PYINNER
mkdir /sys/fs/cgroup/v154-science
phase=prepare
mkdir fresh
run_science prepare python3 repo/scripts/v154_prepare_relaion_graph.py \
  --requests relaion/requests.jsonl \
  --primary repo/docs/research/inputs/v139-relaion-primary.jsonl \
  --queries-out fresh/queries.jsonl --routing-out fresh/routing.jsonl \
  --manifest-out fresh/manifest.json
python3 - <<'PYINNER'
import json
seal=json.load(open('fresh/manifest.json'))
if (seal['queries_sha256']!='4ef4734ff40aeda8b8e99ec01ca993be46e8ad9c337b5410259eeb171f57acfd'
    or seal['routing_sha256']!='2545c5762d04d28c91941607ae1801a8aa7d5a927f4a02aa92b28d43b1cdf1cf'):
    raise ValueError('V154 input seal differs')
PYINNER
phase=science
sync
echo 3 >/proc/sys/vm/drop_caches
run_science science bash -c \
  'echo "$BASHPID" >/sys/fs/cgroup/v154-science/cgroup.procs; exec "$@"' \
  v154 "$CARGO_TARGET_DIR/release/v154_relaion_page_graph" \
  fresh/queries.jsonl fresh/routing.jsonl relaion/centroids.bin \
  science.graph.bin science.summary.json
cp science.log science.jsonl
{ cat /sys/fs/cgroup/v154-science/memory.current;
  cat /sys/fs/cgroup/v154-science/memory.peak;
  cat /sys/fs/cgroup/v154-science/memory.stat; } >science.cgroup-memory.txt
python3 - <<'PYINNER'
import json
summary=json.load(open('science.summary.json'))
if summary.get('schema')!='borsuk-v154-relaion-page-graph-plan-v1':
    raise ValueError('V154 science summary differs')
if not summary['paired_primary_retention']:
    verdict='reject-primary-parity'
elif not summary['all_plan_caps']:
    verdict='reject-caps'
elif summary['max_abs_page_score_difference']>0.0001:
    verdict='reject-score-parity'
elif (summary['v152_p95_ms']>=summary['flat_p95_ms']
      or summary['v152_even_p95_ms']>=summary['flat_even_p95_ms']
      or summary['v152_odd_p95_ms']>=summary['flat_odd_p95_ms']):
    verdict='reject-cpu'
else:
    verdict='pass-cpu'
with open('decision.json','w') as output:
    json.dump({'schema':'borsuk-v154-decision-v1',
               'verdict':verdict},output,sort_keys=True)
    output.write('\n')
PYINNER
phase=complete
exit 0
