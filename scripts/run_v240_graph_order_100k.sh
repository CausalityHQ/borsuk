#!/bin/bash
set -euo pipefail
systemd-run --unit=v240-hard-stop --on-active=7200s /usr/sbin/shutdown -h now
root=/mnt/v240-graph-order-100k
mkdir -p "$root" && cd "$root"
phase=bootstrap
artifacts=(order.npy layout-seal.json counts.jsonl summary.json layout.time replay.time install.log run-closed.log)
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  cp run.log run-closed.log || code=96
  for name in "${artifacts[@]}"; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://$BORSUK_V240_BUCKET/$BORSUK_V240_PREFIX/artifacts/$name" --only-show-errors || code=96
    fi
  done
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os,time
from pathlib import Path
artifacts={}
for name in ('order.npy','layout-seal.json','counts.jsonl','summary.json',
             'layout.time','replay.time','install.log','run-closed.log'):
    path=Path(name)
    if path.is_file():
        h=hashlib.sha256()
        with path.open('rb') as source:
            for block in iter(lambda: source.read(1024*1024),b''):
                h.update(block)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':h.hexdigest()}
code=int(os.environ['EXIT_CODE']);phase=os.environ['PHASE']
print(json.dumps({'schema':'borsuk-v240-graph-order-100k-terminal-v1',
  'source_commit':os.environ['BORSUK_V240_SOURCE_COMMIT'],
  'source_archive_sha256':os.environ['BORSUK_V240_ARCHIVE_SHA'],
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'phase':phase,
  'status':'complete' if code==0 and phase=='complete' else 'failed',
  'worker_finished_epoch':time.time(),'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://$BORSUK_V240_BUCKET/$BORSUK_V240_PREFIX/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
phase=install
dnf install -y -q python3.12 python3.12-pip time >install.log 2>&1
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==1.26.4 scipy==1.14.1 >>install.log 2>&1
export PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
phase=graph
aws s3 cp "s3://$BORSUK_V240_BUCKET/$BORSUK_V240_GRAPH_KEY" graph.bin --only-show-errors
printf '%s  graph.bin\n' "$BORSUK_V240_GRAPH_SHA" | sha256sum -c -
phase=layout
/usr/bin/time -v -o layout.time .venv/bin/python -m scripts.v240_graph_order_100k \
  layout --graph graph.bin --order order.npy --seal layout-seal.json
aws s3api put-object --bucket "$BORSUK_V240_BUCKET" \
  --key "$BORSUK_V240_PREFIX/sealed/layout-seal.json" \
  --body layout-seal.json --if-none-match '*' --no-cli-pager >/dev/null
local_sha=$(sha256sum layout-seal.json | cut -d ' ' -f1)
remote_sha=$(aws s3 cp "s3://$BORSUK_V240_BUCKET/$BORSUK_V240_PREFIX/sealed/layout-seal.json" - \
  --only-show-errors | sha256sum | cut -d ' ' -f1)
[ "$local_sha" = "$remote_sha" ]
phase=candidates
aws s3 cp "s3://$BORSUK_V240_BUCKET/$BORSUK_V240_RAW_KEY" raw.jsonl --only-show-errors
printf '%s  raw.jsonl\n' "$BORSUK_V240_RAW_SHA" | sha256sum -c -
phase=replay
/usr/bin/time -v -o replay.time .venv/bin/python -m scripts.v240_graph_order_100k \
  replay --order order.npy --seal layout-seal.json --raw raw.jsonl \
  --counts counts.jsonl --summary summary.json
phase=complete
