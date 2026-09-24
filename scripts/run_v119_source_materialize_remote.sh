#!/bin/bash
# Corpus-only deep-image staging on one Causality Spot instance.
set -euo pipefail
root=/mnt/v119-source
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
deadline_epoch=$((started_epoch + V119_WALL_SECONDS))
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
  for path in install.log hashes.log download.py download.log download-resources.txt \
      materialize.log materialize-resources.txt source.parquet source.json \
      worker.log interrupt-stop.txt; do
    if [ -f "$path" ] && ! aws s3 cp "$path" \
        "$V119_OUTPUT_PREFIX/artifacts/$path" --only-show-errors; then
      upload_failed=1
    fi
  done
  if [ "$upload_failed" -ne 0 ]; then code=96; phase=evidence-upload; fi
  ended_epoch=$(date +%s)
  python3 - "$code" "$phase" "$instance_id" "$started_epoch" "$ended_epoch" >terminal.json <<'PY'
import hashlib,json,os,sys
from pathlib import Path
names=('install.log','hashes.log','download.py','download.log',
       'download-resources.txt','materialize.log','materialize-resources.txt',
       'source.parquet','source.json','worker.log','interrupt-stop.txt')
files={}
for name in names:
    path=Path(name)
    if path.is_file():
        h=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''): h.update(chunk)
        files[name]={'bytes':path.stat().st_size,'sha256':h.hexdigest()}
code=int(sys.argv[1]);phase=sys.argv[2]
print(json.dumps({'schema':'borsuk-v119-source-materialize-spot-v1',
    'source_commit':os.environ['V119_SOURCE_COMMIT'],
    'instance_id':sys.argv[3], 'exit_code':code, 'phase':phase,
    'status':'complete' if code==0 and phase=='complete' else 'failed',
    'elapsed_seconds':int(sys.argv[5])-int(sys.argv[4]),
    'artifacts':files},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V119_OUTPUT_PREFIX/terminal.json" --only-show-errors
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
dnf install -y -q python3.12 python3.12-pip tar gzip time >install.log 2>&1
python3.12 -m venv .venv >>install.log 2>&1
.venv/bin/pip install -q numpy==1.26.4 pyarrow==17.0.0 boto3 >>install.log 2>&1
export PYTHONPATH="$root/repo"
phase=staging-download
aws s3 cp "$V119_STAGING_URI" staging.json --only-show-errors
[ "$(stat -c%s staging.json)" = "$V119_STAGING_BYTES" ] || exit 93
printf '%s  staging.json\n' "$V119_STAGING_SHA256" | sha256sum -c - >hashes.log
cat >download.py <<'PY'
import hashlib,json,os
from pathlib import Path
from urllib.parse import urlsplit
import boto3
receipt=json.loads(Path('staging.json').read_text())
objects=sorted((item for item in receipt['objects'] if item['role']=='train'),
               key=lambda item:item['uri'])
if len(objects)!=58 or sum(item['rows'] for item in objects)!=9990000:
    raise ValueError('training shard count or rows differ')
root=Path('shards');root.mkdir()
s3=boto3.client('s3',region_name='eu-central-1')
prefix=os.environ['V119_TRAIN_URI_PREFIX']
for ordinal,item in enumerate(objects):
    uri=item['uri']; parsed=urlsplit(uri)
    name=f'train-{ordinal:08d}.parquet'
    if uri!=prefix+name or parsed.scheme!='s3' or not parsed.netloc:
        raise ValueError('training shard URI or order differs')
    path=root/name;pending=root/(name+'.pending')
    s3.download_file(parsed.netloc,parsed.path.lstrip('/'),str(pending))
    h=hashlib.sha256()
    with pending.open('rb') as source:
        for block in iter(lambda:source.read(1024*1024),b''):h.update(block)
    if pending.stat().st_size!=item['bytes'] or h.hexdigest()!=item['sha256']:
        raise ValueError(f'training shard digest differs: {name}')
    pending.rename(path)
    print(json.dumps({'ordinal':ordinal,'bytes':item['bytes'],
                      'sha256':item['sha256']}),flush=True)
PY
phase=download
run_science download .venv/bin/python download.py
phase=materialize
run_science materialize .venv/bin/python -m scripts.materialize_source_shards \
  --staging staging.json --shard-root shards --output source.parquet \
  --provenance source.json --dimensions 96 --rows 9990000 --metric cosine
phase=complete
exit 0
