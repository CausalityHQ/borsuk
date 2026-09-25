#!/usr/bin/env bash
set -euo pipefail

root=/mnt/v224-graph-store
mkdir -p "$root" && cd "$root"
bucket=$BORSUK_V224_BUCKET
prefix=$BORSUK_V224_PREFIX
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
  for name in roundtrip.json roundtrip.time build.log install.log run-closed.log; do
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
names=['roundtrip.json','roundtrip.time','build.log','install.log','run-closed.log']
artifacts={}
for name in names:
    path=Path(name)
    if path.is_file():
        artifacts[name]={'bytes':path.stat().st_size,
                         'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
identity=json.loads(os.environ['IDENTITY'])
code=int(os.environ['EXIT_CODE']); phase=os.environ['PHASE']
print(json.dumps({'schema':'borsuk-v224-graph-store-s3-terminal-v1',
    'source_commit':os.environ['BORSUK_V224_SOURCE_COMMIT'],
    'source_archive_sha256':os.environ['BORSUK_V224_ARCHIVE_SHA'],
    'generation_root_sha256':os.environ['BORSUK_V224_ROOT_SHA'],
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

download() {
  local name=$1 key=$2 bytes=$3 sha=$4
  aws s3 cp "s3://$bucket/$key" "$name" --only-show-errors
  [ "$(stat -c%s "$name")" = "$bytes" ]
  printf '%s  %s\n' "$sha" "$name" | sha256sum -c -
}

phase=inputs
cp repo/docs/research/v223-relaion-1m-generation.json generation.json
printf '%s  generation.json\n' "$BORSUK_V224_ROOT_SHA" | sha256sum -c -
download graph.bin \
  research/v219-reachable-graph-1m/008ab6fbc50e6293e0599a33993c619702109bd9/runs/a0002/artifacts/graph.bin \
  266910966 a2805a97c1955adf1cdc0b0da43b4ff205eb1d4646916c09d3c4d2b9f7c1ee0b
download map.u32 \
  research/v219-reachable-graph-1m/008ab6fbc50e6293e0599a33993c619702109bd9/runs/a0002/artifacts/map.u32 \
  4000000 61e355af30afeff6d48c2f619c39d88f8d25dfd1a042f79b5f8028a10de0a716
download plane.bin \
  research/v196-resident-fp16-preflight/c5fbb27ffba5fe5a78f15a4fb9e9c836a2afba04/runs/a0001/artifacts/plane.bin \
  1544000064 1bce4288b38d88384503d8cfeae21667f45dbfb62303ce510f676fc0d66d4c47
download books.bin \
  research/v115-source-router-parity/8140fd86defff60ff35ef33be7596f2bda34f879/runs/v115-router-20260923T235000Z/a0001/artifacts/router/books.bin \
  786432 1ca5aa29c32dd155f0309a4d9f5bd8294ccbe75f50d1a2ff213603fba08800ce
download codes.bin \
  research/v115-source-router-parity/8140fd86defff60ff35ef33be7596f2bda34f879/runs/v115-router-20260923T235000Z/a0001/artifacts/router/codes.bin \
  64000000 599e359b02ddb85876234f64bac3fcf7bfcb759e121f6a1fbcb4fbd5dfc95460

phase=build
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time >install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo" \
  CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=6
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal \
  --default-toolchain 1.98.0 >>install.log 2>&1
cd repo
"$CARGO_HOME/bin/cargo" build --release --locked -p borsuk \
  --example v224_graph_store_roundtrip --jobs 6 >"$root/build.log" 2>&1
cd "$root"
phase=roundtrip
/usr/bin/time -v -o roundtrip.time \
  "$CARGO_TARGET_DIR/release/examples/v224_graph_store_roundtrip" \
  "s3://$bucket/$prefix/published" generation.json . cache >roundtrip.json
phase=complete
