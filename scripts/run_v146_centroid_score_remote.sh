#!/bin/bash
# One frozen, fail-fast, source-only centroid scoring screen on Spot.
set -euo pipefail
root=/mnt/v146-centroid-score
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
deadline_epoch=$((started_epoch + V146_WALL_SECONDS - 1200))
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
  for cohort in deep relaion; do
    group=/sys/fs/cgroup/v146-$cohort
    if [ -d "$group" ]; then
      { cat "$group/memory.current"; cat "$group/memory.peak";
        cat "$group/memory.stat"; } >"$cohort.cgroup-memory.txt"
    fi
  done
  upload_failed=0
  for path in install.log core-check.log core-check-resources.txt \
      score-build.log score-build-resources.txt download.log \
      deep-score.log deep-score-resources.txt deep.score.jsonl \
      deep.score.summary.json deep.centroids.bin deep.rust-scores.bin \
      deep.cgroup-memory.txt \
      relaion-score.log relaion-score-resources.txt relaion.score.jsonl \
      relaion.score.summary.json relaion.centroids.bin relaion.rust-scores.bin \
      relaion.cgroup-memory.txt decision.json worker.log interrupt-stop.txt; do
    if [ -f "$path" ] && ! timeout 900 aws s3 cp "$path" \
        "$V146_OUTPUT_PREFIX/artifacts/$path" --only-show-errors; then
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
       'score-build.log','score-build-resources.txt','download.log',
       'deep-score.log','deep-score-resources.txt','deep.score.jsonl',
       'deep.score.summary.json','deep.centroids.bin','deep.rust-scores.bin',
       'deep.cgroup-memory.txt',
       'relaion-score.log','relaion-score-resources.txt','relaion.score.jsonl',
       'relaion.score.summary.json','relaion.centroids.bin','relaion.rust-scores.bin',
       'relaion.cgroup-memory.txt','decision.json','worker.log','interrupt-stop.txt')
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
print(json.dumps({'schema':'borsuk-v146-centroid-score-spot-v1',
  'source_commit':os.environ['V146_SOURCE_COMMIT'],
  'source_archive_sha256':os.environ['V146_ARCHIVE_SHA256'],
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'phase':phase,
  'status':'complete' if code==0 and phase=='complete' else 'failed',
  'interrupted':os.path.isfile('interrupt-stop.txt'),
  'elapsed_seconds':int(os.environ['ENDED_EPOCH'])-int(os.environ['STARTED_EPOCH']),
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V146_OUTPUT_PREFIX/terminal.json" --only-show-errors
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
phase=score-build
run_science score-build "$CARGO_HOME/bin/cargo" build --release --locked -j 8 \
  --manifest-path repo/scripts/v114_score_rust/Cargo.toml --bin v146_centroid_score
phase=download-deep
: >download.log
v140=research/v140-budgeted-page/3213ca34cf6a8c32824dec6967b39eb06dd5fba6/runs/v140-20260924T092255Z/a0001
download_checked v140-terminal.json "$v140/terminal.json" 1738 d9e15698b83c6be6456aeaff2864763b91192c063f0bda5dfca0cdc392a1f3c4
download_checked v140-deep.raw.jsonl "$v140/artifacts/deep.raw.jsonl" 3757457 e2a1b31161bb2190cb7dc36d0601c50449c6d6aba0cc1a4985c032d713bc54d5
download_checked v140-relaion.raw.jsonl "$v140/artifacts/relaion.raw.jsonl" 3430072 3dc54d101814b0f8e2b2d80f1b79e2762edcc885ad423ee14ae601669eb0804a
v145=research/v145-page-parity/e0e8e01f89be2c40438e09f325d9c1f72da1ca76/runs/v145-20260924T110000Z/a0001
download_checked v145-terminal.json "$v145/terminal.json" 3349 6de71d7f552c096067a0c42dbef7d95c0c3e1b86b555bdc474c4f1ef021a4354
download_checked deep.ref-scores.bin "$v145/artifacts/deep.scores.bin" 1564000 1e31f8981bcf9558399b52d75254169d1406b1a60607c3883d4b6844af798480
download_checked relaion.ref-scores.bin "$v145/artifacts/relaion.scores.bin" 15628000 5624e9cff1c1d032d7b502b21d7e52293e34a7ac21a040a10494cb60dcb95bbc
python3 - <<'PYINNER'
import json
v140=json.load(open('v140-terminal.json'))
v145=json.load(open('v145-terminal.json'))
if v140.get('status')!='complete' or v140.get('source_commit')!='3213ca34cf6a8c32824dec6967b39eb06dd5fba6':
    raise ValueError('V140 terminal identity differs')
if v145.get('status')!='complete' or v145.get('source_commit')!='e0e8e01f89be2c40438e09f325d9c1f72da1ca76':
    raise ValueError('V145 terminal identity differs')
PYINNER
v122=research/v122-deep-image-100k/afe07cb5a9ba8518263375595f589639fdf3f4f1/runs/v122-20260924T011355Z/a0001/artifacts
download_checked deep/queries.jsonl "$v122/queries.jsonl" 2041773 dd95571cc7c333f331b8c4c0b55070b366b389508b0e195e0e56d5982177da99
cp repo/docs/research/inputs/v138-deep-primary.jsonl deep/primary.jsonl
printf '%s  %s\n' 04cdbea9079837806059799d9a90c2f579526de100b7743f83a3794a15c0d6e3 deep/primary.jsonl | sha256sum -c - >>download.log
download_checked deep/sq8.bin "$v122/built/sq8.bin" 10800000 c20dcb8058d2409791c6c584d9f078491d4c239acbe7c19350be7757d533e8df
download_checked deep/low.bin "$v122/router/low.bin" 384 3fd035c242b99b6835727faa49f0dc52c77979b8c900980c01fcf23412222c9e
download_checked deep/step.bin "$v122/router/step.bin" 384 bd6a8adb231b89d18ad1edf72cff043512d663117955a826bd8ca11224d02038
phase=score-deep
mkdir /sys/fs/cgroup/v146-deep
sync
echo 3 >/proc/sys/vm/drop_caches
run_science deep-score bash -c \
  'echo "$BASHPID" >/sys/fs/cgroup/v146-deep/cgroup.procs; exec "$@"' \
  v146 "$CARGO_TARGET_DIR/release/v146_centroid_score" \
  deep-image-96-angular-random100k 100000 96 deep/queries.jsonl \
  deep/primary.jsonl deep/sq8.bin deep/low.bin deep/step.bin \
  deep.ref-scores.bin v140-deep.raw.jsonl \
  deep.centroids.bin deep.rust-scores.bin deep.score.summary.json
cp deep-score.log deep.score.jsonl
if python3 - <<'PYINNER'
import json
value=json.load(open('deep.score.summary.json'))
raise SystemExit(0 if value['exact_page_plan_matches']==1000 else 1)
PYINNER
then
  :
else
  printf '%s\n' '{"schema":"borsuk-v146-decision-v1","deep_completed":true,"relaion_completed":false,"verdict":"reject-deep-parity"}' >decision.json
  phase=complete
  exit 0
fi
phase=download-relaion
v116=research/v116-validation-paired/5e9b35ad40ea023eab4407aa611d759e1893bb34/runs/v116-validation-20260923T235426Z/a0001/artifacts
v115=research/v115-source-router-parity/8140fd86defff60ff35ef33be7596f2bda34f879/runs/v115-router-20260923T235000Z/a0001/artifacts/router
download_checked relaion/requests.jsonl "$v116/requests.jsonl" 18726909 c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9
cp repo/docs/research/inputs/v139-relaion-primary.jsonl relaion/primary.jsonl
printf '%s  %s\n' 71bfdf71f293ca1d23f58694866b3ba52ee8ee95ed9b02e676a2a8bf031f3162 relaion/primary.jsonl | sha256sum -c - >>download.log
download_checked relaion/sq8.bin research/v70-algorithm-first/single-stage-a4a695d66f508edf/index/sq8.bin 780000000 2284f24745f964ff2b125eedb883d5cd8ff6afab0738f49f9e16e593167a318b
download_checked relaion/low.bin "$v115/low.bin" 3072 ce036f48f918312f694adbd12994646987eb0e144df20f64ffe583bed0d1f891
download_checked relaion/step.bin "$v115/step.bin" 3072 64d49c7413f163fb3446183629d743f69199156f208fa959523d1bcde27da69c
phase=score-relaion
mkdir /sys/fs/cgroup/v146-relaion
sync
echo 3 >/proc/sys/vm/drop_caches
run_science relaion-score bash -c \
  'echo "$BASHPID" >/sys/fs/cgroup/v146-relaion/cgroup.procs; exec "$@"' \
  v146 "$CARGO_TARGET_DIR/release/v146_centroid_score" \
  ReLAION-1M 1000000 768 relaion/requests.jsonl \
  relaion/primary.jsonl relaion/sq8.bin relaion/low.bin relaion/step.bin \
  relaion.ref-scores.bin v140-relaion.raw.jsonl \
  relaion.centroids.bin relaion.rust-scores.bin relaion.score.summary.json
cp relaion-score.log relaion.score.jsonl
python3 - <<'PYINNER'
import json
deep=json.load(open('deep.score.summary.json'))
relaion=json.load(open('relaion.score.summary.json'))
parity=relaion['exact_page_plan_matches']==1000
cpu=deep['scoring_and_planner_p95_ms']<=10 and relaion['scoring_and_planner_p95_ms']<=10
verdict='pass' if parity and cpu else ('reject-relaion-parity' if not parity else 'reject-cpu')
with open('decision.json','w') as output:
    json.dump({'schema':'borsuk-v146-decision-v1','deep_completed':True,
               'relaion_completed':True,'verdict':verdict},output,sort_keys=True)
    output.write('\n')
PYINNER
phase=complete
exit 0
