#!/usr/bin/env bash
set -euo pipefail

root=/mnt/v223-authenticated-graph-http
mkdir -p "$root" && cd "$root"
role=$BORSUK_V223_ROLE
bucket=$BORSUK_V223_BUCKET
prefix=$BORSUK_V223_PREFIX
phase=bootstrap
worker_started_epoch=$(date +%s)
server_pid=
watcher_pid=
if [ "$role" = server ]; then
  artifacts=(ready.json server-resources.json build.log install.log run-closed.log)
  if [ -n "${BORSUK_V223_MUTATION_STRIDE:-}" ]; then artifacts+=(health.json); fi
  if [ -n "${BORSUK_V223_COLLECTION_URI:-}" ]; then artifacts+=(hydrate.json); fi
  if [ -n "${BORSUK_V223_GENERATION_URI:-}" ]; then artifacts+=(hydrate.json publish.json); fi
elif [ "$role" = client ]; then
  artifacts=(first.raw.jsonl first.summary.json first.quality.json first.time
             repeat.raw.jsonl repeat.summary.json repeat.quality.json repeat.time
             install.log run-closed.log)
else
  exit 90
fi

finish() {
  code=$?
  trap - EXIT TERM
  set +e
  [ -n "$watcher_pid" ] && kill "$watcher_pid" 2>/dev/null
  [ -n "$server_pid" ] && kill "$server_pid" 2>/dev/null
  cd "$root"
  cp run.log run-closed.log || code=96
  for name in "${artifacts[@]}"; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://$bucket/$prefix/$role/artifacts/$name" \
        --only-show-errors || code=96
    fi
  done
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
    http://169.254.169.254/latest/api/token)
  identity=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" \
    http://169.254.169.254/latest/dynamic/instance-identity/document)
  IDENTITY="$identity" EXIT_CODE="$code" PHASE="$phase" \
    STARTED="$worker_started_epoch" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
role=os.environ['BORSUK_V223_ROLE']
names=({'server':['ready.json','server-resources.json','build.log','install.log','run-closed.log'],
        'client':['first.raw.jsonl','first.summary.json','first.quality.json','first.time',
                  'repeat.raw.jsonl','repeat.summary.json','repeat.quality.json',
                  'repeat.time','install.log','run-closed.log']}[role])
if role=='server' and os.environ.get('BORSUK_V223_MUTATION_STRIDE'):
    names.append('health.json')
if role=='server' and os.environ.get('BORSUK_V223_COLLECTION_URI'):
    names.append('hydrate.json')
if role=='server' and os.environ.get('BORSUK_V223_GENERATION_URI'):
    names.extend(['hydrate.json','publish.json'])
artifacts={}
for name in names:
    path=Path(name)
    if path.is_file():
        artifacts[name]={'bytes':path.stat().st_size,
                         'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
identity=json.loads(os.environ['IDENTITY'])
code=int(os.environ['EXIT_CODE']);phase=os.environ['PHASE']
print(json.dumps({'schema':'borsuk-v223-authenticated-graph-http-terminal-v1',
    'role':role,'source_commit':os.environ['BORSUK_V223_SOURCE_COMMIT'],
    'source_archive_sha256':os.environ['BORSUK_V223_ARCHIVE_SHA'],
    'generation_root_sha256':os.environ['BORSUK_V223_ROOT_SHA'],
    'mutation_stride':int(os.environ.get('BORSUK_V223_MUTATION_STRIDE') or 0),
    'delta_encoding':os.environ.get('BORSUK_V223_DELTA_ENCODING',''),
    'collection_uri':os.environ.get('BORSUK_V223_COLLECTION_URI',''),
    'generation_uri':os.environ.get('BORSUK_V223_GENERATION_URI',''),
    'instance_id':identity['instanceId'],'instance_type':identity['instanceType'],
    'region':identity['region'],'availability_zone':identity['availabilityZone'],
    'server_instance_id':os.environ.get('BORSUK_V223_SERVER_ID',''),
    'worker_started_epoch':int(os.environ['STARTED']),
    'worker_finished_epoch':__import__('time').time(),
    'exit_code':code,'phase':phase,
    'status':('interrupted' if Path('spot-interruption.json').is_file() else
              'complete' if code==0 and phase=='complete' else 'failed'),
    'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://$bucket/$prefix/$role/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
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
      aws s3 cp spot-interruption.json \
        "s3://$bucket/$prefix/$role/spot-interruption.json" --only-show-errors || true
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
if [ -n "${BORSUK_V223_GENERATION_URI:-}" ]; then
  cp repo/docs/research/v246-relaion-1m-generation.json generation.json
else
  cp repo/docs/research/v223-relaion-1m-generation.json generation.json
fi
printf '%s  generation.json\n' "$BORSUK_V223_ROOT_SHA" | sha256sum -c -
download prep.json \
  research/v219-reachable-graph-1m/008ab6fbc50e6293e0599a33993c619702109bd9/runs/a0002/artifacts/prep.json \
  844 a2afb5d183c1d2dbf43dc8e2f67b7ec61e25ed7ee3daea6f83e27716511fdc82
if [ "$role" = server ]; then
  if [ -z "${BORSUK_V223_COLLECTION_URI:-}" ]; then
  download build-summary.json \
    research/v219-reachable-graph-1m/008ab6fbc50e6293e0599a33993c619702109bd9/runs/a0002/artifacts/build-summary.json \
    626 c596e8b8cdbbbc7245ce4d62b3162233370c5b1ce2989a62a0e7fb6bd2dd39d6
  if [ -n "${BORSUK_V223_GENERATION_URI:-}" ]; then
    download graph.bin \
      research/v245-owner-graph-1m/a45c8f9635b7663334367960d2e423ddf29769f4/runs/a0001/artifacts/graph.bin \
      266910242 92df3782b2836e608d206401c4efdd3d71bd8e81fe18887b0e93af4d23d8ade3
  else
    download graph.bin \
      research/v219-reachable-graph-1m/008ab6fbc50e6293e0599a33993c619702109bd9/runs/a0002/artifacts/graph.bin \
      266910966 a2805a97c1955adf1cdc0b0da43b4ff205eb1d4646916c09d3c4d2b9f7c1ee0b
  fi
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
  fi
  phase=build
  dnf install -y -q python3.12 gcc gcc-c++ cmake perl tar gzip time >install.log 2>&1
  export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo" \
    CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=6
  curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal \
    --default-toolchain 1.98.0 >>install.log 2>&1
  cd repo
  "$CARGO_HOME/bin/cargo" build --release --locked -p borsuk \
    --example v220_graph_http --example v246_publish_graph_s3 --jobs 6 >"$root/build.log" 2>&1
  cd "$root"
  phase=serve
  if [ -n "${BORSUK_V223_GENERATION_URI:-}" ]; then
    "$CARGO_TARGET_DIR/release/examples/v246_publish_graph_s3" \
      "$BORSUK_V223_GENERATION_URI" generation.json . "$BORSUK_V223_ROOT_SHA" >publish.json
    server_args=(generation-s3 "$BORSUK_V223_GENERATION_URI" cache 3221225472 0.0.0.0:8080)
  elif [ -n "${BORSUK_V223_COLLECTION_URI:-}" ]; then
    server_args=(collection "$BORSUK_V223_COLLECTION_URI" cache 3221225472 0.0.0.0:8080)
  else
    server_args=(generation.json "$BORSUK_V223_ROOT_SHA" . 3221225472 0.0.0.0:8080)
    if [ -n "${BORSUK_V223_MUTATION_STRIDE:-}" ]; then server_args+=("$BORSUK_V223_MUTATION_STRIDE"); fi
    if [ -n "${BORSUK_V223_DELTA_ENCODING:-}" ]; then server_args+=("$BORSUK_V223_DELTA_ENCODING"); fi
  fi
  "$CARGO_TARGET_DIR/release/examples/v220_graph_http" "${server_args[@]}" &
  server_pid=$!
  for attempt in $(seq 1 120); do
    if curl -fsS http://127.0.0.1:8080/health >/dev/null 2>&1; then break; fi
    kill -0 "$server_pid"
    sleep 1
  done
  curl -fsS http://127.0.0.1:8080/health >health.json
  if [ -n "${BORSUK_V223_GENERATION_URI:-}" ]; then
    python3 - <<'PY'
import json,os
v=json.load(open('hydrate.json'))
assert v['base_root_sha256']==os.environ['BORSUK_V223_ROOT_SHA']
assert v['graph_blob_gets']==5 and v['graph_response_bytes']==1879696738
PY
  elif [ -n "${BORSUK_V223_COLLECTION_URI:-}" ]; then
    python3 - <<'PY'
import json,os
v=json.load(open('hydrate.json'))
assert v['revision']==2 and v['base_root_sha256']==os.environ['BORSUK_V223_ROOT_SHA']
assert v['mutation_sha256']==os.environ['BORSUK_V223_COLLECTION_SHA']
assert v['graph_blob_gets']==5 and v['graph_response_bytes']==1879697462
PY
  fi
  if [ -n "${BORSUK_V223_MUTATION_STRIDE:-}" ]; then
    python3 - <<'PY'
import json,os
v=json.load(open('health.json'))
assert v['status']=='ok' and v['delta_rows']==10000 and v['overlay_resident_bytes']<=(64 if os.environ.get('BORSUK_V223_DELTA_ENCODING') else 32)*1024*1024
PY
  fi
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
    http://169.254.169.254/latest/api/token)
  private_ip=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" \
    http://169.254.169.254/latest/meta-data/local-ipv4)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" \
    http://169.254.169.254/latest/meta-data/instance-id)
  INSTANCE_ID="$instance_id" PRIVATE_IP="$private_ip" python3 - <<'PY' >ready.json
import json,os
print(json.dumps({'schema':'borsuk-v223-authenticated-graph-http-ready-v1',
    'instance_id':os.environ['INSTANCE_ID'],'private_ip':os.environ['PRIVATE_IP'],
    'generation_root_sha256':os.environ['BORSUK_V223_ROOT_SHA'],
    'mutation_stride':int(os.environ.get('BORSUK_V223_MUTATION_STRIDE') or 0),
    'delta_encoding':os.environ.get('BORSUK_V223_DELTA_ENCODING',''),
    'collection_uri':os.environ.get('BORSUK_V223_COLLECTION_URI',''),
    'generation_uri':os.environ.get('BORSUK_V223_GENERATION_URI',''),
    'mutation_sha256':os.environ.get('BORSUK_V223_COLLECTION_SHA',''),
    'source_commit':os.environ['BORSUK_V223_SOURCE_COMMIT']},sort_keys=True))
PY
  aws s3api put-object --bucket "$bucket" --key "$prefix/server/ready.json" \
    --body ready.json --if-none-match '*' --no-cli-pager >/dev/null
  phase=await_client
  for attempt in $(seq 1 900); do
    if aws s3api head-object --bucket "$bucket" --key "$prefix/client/terminal.json" \
      --no-cli-pager >/dev/null 2>&1; then break; fi
    kill -0 "$server_pid"
    sleep 5
  done
  aws s3 cp "s3://$bucket/$prefix/client/terminal.json" client-terminal.json --only-show-errors
  python3 - <<'PY'
import json,os
v=json.load(open('client-terminal.json'))
assert v['source_commit']==os.environ['BORSUK_V223_SOURCE_COMMIT']
PY
  SERVER_PID="$server_pid" python3 - <<'PY' >server-resources.json
import json,os
from pathlib import Path
pid=os.environ['SERVER_PID']
lines=Path(f'/proc/{pid}/status').read_text().splitlines()
values={line.split(':',1)[0]:int(line.split(':',1)[1].split()[0])*1024
        for line in lines if line.startswith(('VmRSS:','VmHWM:'))}
print(json.dumps({'peak_rss_bytes':values['VmHWM'],'rss_bytes':values['VmRSS']},sort_keys=True))
PY
  kill "$server_pid"
  wait "$server_pid" || true
  server_pid=
else
  download requests.jsonl \
    research/v116-validation-paired/5e9b35ad40ea023eab4407aa611d759e1893bb34/runs/v116-validation-20260923T235426Z/a0001/artifacts/requests.jsonl \
    18726909 c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9
  phase=install
  dnf install -y -q python3.12 time >install.log 2>&1
  export PYTHONPATH="$root/repo"
  for attempt in $(seq 1 60); do
    if curl -fsS "http://$BORSUK_V223_SERVER_IP:8080/health" >/dev/null 2>&1; then break; fi
    sleep 2
  done
  curl -fsS "http://$BORSUK_V223_SERVER_IP:8080/health"
  phase=measure
  for pass in first repeat; do
    label=first_pass
    [ "$pass" = repeat ] && label=immediate_repeat
    /usr/bin/time -v -o "$pass.time" python3.12 -m scripts.v220_bench_graph_http \
      --host "$BORSUK_V223_SERVER_IP" --port 8080 --pass-label "$label" \
      --prep prep.json --requests requests.jsonl --raw "$pass.raw.jsonl" \
      --summary "$pass.summary.json"
  done
  for pass in first repeat; do
    aws s3api put-object --bucket "$bucket" --key "$prefix/client/sealed/$pass.raw.jsonl" \
      --body "$pass.raw.jsonl" --if-none-match '*' --no-cli-pager >/dev/null
    local_sha=$(sha256sum "$pass.raw.jsonl" | cut -d ' ' -f1)
    remote_sha=$(aws s3 cp "s3://$bucket/$prefix/client/sealed/$pass.raw.jsonl" - \
      --only-show-errors | sha256sum | cut -d ' ' -f1)
    [ "$local_sha" = "$remote_sha" ]
  done
  if [ "${BORSUK_V223_DELTA_ENCODING:-}" = decoded ]; then
    download v230-first.raw.jsonl \
      research/v230-mutation-graph-http-1m/014d1fb36f9f69004c2c25b0648765bc369c4cd0/runs/a0001/client/sealed/first.raw.jsonl \
      1167273 b4597049dd6959dbf5516342408a31e863fbe13505987f88811d58a4ccdd5d14
    download v230-repeat.raw.jsonl \
      research/v230-mutation-graph-http-1m/014d1fb36f9f69004c2c25b0648765bc369c4cd0/runs/a0001/client/sealed/repeat.raw.jsonl \
      1167275 e14e53a2601e0b6f0fe64bdf227b9c4a2431e7db21d93cd4db3acd1967dbf30c
  fi
  phase=truth
  if [ -n "${BORSUK_V223_GENERATION_URI:-}" ]; then
    download v219-raw.jsonl \
      research/v245-owner-graph-1m/a45c8f9635b7663334367960d2e423ddf29769f4/runs/a0001/artifacts/raw.jsonl \
      4239186 dffb4e97d7ab0d00745673d299df6d2c9c8b62f881d7d85eaf61f0fdd585216a
  else
    download v219-raw.jsonl \
      research/v219-reachable-graph-1m/008ab6fbc50e6293e0599a33993c619702109bd9/runs/a0002/artifacts/raw.jsonl \
      4239096 ccc29dd912248c6bc86c49bdcd86bfc3cd28534d37e05690102425df2c3bcab9
  fi
  download v198-raw.jsonl \
    research/v198-real-query-resident-fp16/fdc51a358be350678d9f6f279a2be95a57709c02/runs/a0001/artifacts/out/raw.jsonl \
    3230820 2b18321435642de3fad4df02b84abcc046fb6c9b17808e72d6a50eea54a9ec98
  for pass in first repeat; do
    scorer=scripts.v222_score_external_graph_http
    [ -n "${BORSUK_V223_MUTATION_STRIDE:-}" ] && scorer=scripts.v230_score_mutation_graph_http
    if [ -n "${BORSUK_V223_GENERATION_URI:-}" ]; then
      python3.12 -m scripts.v246_score_graph_http --raw "$pass.raw.jsonl" \
        --reference v219-raw.jsonl --summary "$pass.summary.json" \
        --truth v198-raw.jsonl --output "$pass.quality.json"
    elif [ "${BORSUK_V223_DELTA_ENCODING:-}" = decoded ]; then
      python3.12 -m scripts.v233_score_decoded_graph_http --raw "$pass.raw.jsonl" \
        --reference "v230-$pass.raw.jsonl" --summary "$pass.summary.json" \
        --truth v198-raw.jsonl --output "$pass.quality.json"
    else
      python3.12 -m "$scorer" --raw "$pass.raw.jsonl" \
        --summary "$pass.summary.json" --previous v219-raw.jsonl \
        --truth v198-raw.jsonl --output "$pass.quality.json"
    fi
  done
fi
phase=complete
