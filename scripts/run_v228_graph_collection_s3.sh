#!/usr/bin/env bash
set -euo pipefail

root=/mnt/v228-graph-collection
mkdir -p "$root" && cd "$root"
bucket=$BORSUK_V228_BUCKET
prefix=$BORSUK_V228_PREFIX
phase=bootstrap
started=$(date +%s)
watcher_pid=

finish() {
  code=$?
  trap - EXIT TERM
  set +e
  [ -n "$watcher_pid" ] && kill "$watcher_pid" 2>/dev/null
  cd "$root" || code=96
  cp run.log run-closed.log || code=96
  for name in collection.json collection.time build.log install.log run-closed.log; do
    [ ! -f "$name" ] || aws s3 cp "$name" "s3://$bucket/$prefix/artifacts/$name" \
      --only-show-errors || code=96
  done
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
    http://169.254.169.254/latest/api/token)
  identity=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" \
    http://169.254.169.254/latest/dynamic/instance-identity/document)
  IDENTITY="$identity" EXIT_CODE="$code" PHASE="$phase" STARTED="$started" \
    python3 - <<'PY' >terminal.json
import hashlib,json,os,time
from pathlib import Path
names=['collection.json','collection.time','build.log','install.log','run-closed.log']
artifacts={}
for name in names:
    path=Path(name)
    if path.is_file():
        artifacts[name]={'bytes':path.stat().st_size,
                         'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
identity=json.loads(os.environ['IDENTITY'])
code=int(os.environ['EXIT_CODE']); phase=os.environ['PHASE']
print(json.dumps({'schema':'borsuk-v228-graph-collection-s3-terminal-v1',
    'source_commit':os.environ['BORSUK_V228_SOURCE_COMMIT'],
    'source_archive_sha256':os.environ['BORSUK_V228_ARCHIVE_SHA'],
    'instance_id':identity['instanceId'],'instance_type':identity['instanceType'],
    'region':identity['region'],'availability_zone':identity['availabilityZone'],
    'worker_started_epoch':int(os.environ['STARTED']),
    'worker_finished_epoch':time.time(),'exit_code':code,'phase':phase,
    'status':('interrupted' if Path('spot-interruption.json').is_file() else
              'complete' if code==0 and phase=='complete' else 'failed'),
    'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://$bucket/$prefix/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
touch run.log
exec >run.log 2>&1
main_pid=$$
spot_watch() {
  local token action
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600' \
    http://169.254.169.254/latest/api/token) || return
  while sleep 5; do
    if action=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" \
      http://169.254.169.254/latest/meta-data/spot/instance-action 2>/dev/null); then
      printf '%s\n' "$action" >spot-interruption.json
      aws s3 cp spot-interruption.json "s3://$bucket/$prefix/spot-interruption.json" \
        --only-show-errors || true
      kill -TERM "$main_pid"
      return
    fi
  done
}
spot_watch & watcher_pid=$!

phase=build
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time >install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo" \
  CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=6
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal \
  --default-toolchain 1.98.0 >>install.log 2>&1
cd repo
"$CARGO_HOME/bin/cargo" build --release --locked -p borsuk \
  --example v228_graph_collection_s3 --jobs 6 >"$root/build.log" 2>&1
cd "$root"
phase=switch
/usr/bin/time -v -o collection.time \
  "$CARGO_TARGET_DIR/release/examples/v228_graph_collection_s3" \
  "s3://$bucket/$prefix/published" work >collection.json
phase=complete
