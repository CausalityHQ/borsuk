#!/usr/bin/env bash
set -euo pipefail
root=/mnt/v271-rust-rc-fresh-frontier
mkdir -p "$root"
cd "$root"
phase=bootstrap
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  files=(install.log compile.time prepare.time prepare.json queries.f32 queries100.f32
    truth100.u32 truth1m.u32 preflight.build.json preflight.build.time
    preflight.raw.jsonl preflight.search.json preflight.search.time
    preflight.quality.json prior.raw.jsonl prior.search.json prior.search.time
    prior.quality.json new1m.build.json new1m.build.time new1m.raw.jsonl
    new1m.search.json new1m.search.time new1m.quality.json faiss.raw.jsonl
    faiss.control.json faiss.control.time faiss.quality.json decision.json)
  if [ -f new100/root.json ]; then
    for name in root.json plane.bin graph.bin map.u32 books.bin codes.bin; do
      files+=("new100/$name")
    done
  fi
  if [ -f new1m/root.json ]; then
    for name in root.json plane.bin graph.bin map.u32 books.bin codes.bin; do
      files+=("new1m/$name")
    done
  fi
  files+=(run.log)
  : > artifact-list.txt
  for name in "${files[@]}"; do
    if [ -f "$name" ]; then
      printf '%s\n' "$name" >>artifact-list.txt
      aws s3 cp "$name" "s3://$BORSUK_V271_BUCKET/$BORSUK_V271_PREFIX/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os,time
from pathlib import Path
artifacts={}
for name in Path('artifact-list.txt').read_text().splitlines():
    path=Path(name)
    h=hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda:source.read(1024*1024),b''):
            h.update(block)
    artifacts[name]={'bytes':path.stat().st_size,'sha256':h.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-v271-rust-rc-fresh-frontier-terminal-v1',
    'source_commit':os.environ['BORSUK_V271_SOURCE_COMMIT'],
    'source_archive_sha256':os.environ['BORSUK_V271_ARCHIVE_SHA'],
    'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
    'finished_epoch':int(time.time()),
    'phase':os.environ['PHASE'],
    'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
    'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://$BORSUK_V271_BUCKET/$BORSUK_V271_PREFIX/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
phase=install
dnf install -y -q python3.12 python3.12-pip gcc gcc-c++ cmake perl tar gzip time >install.log 2>&1
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3 pyarrow==24.0.0 faiss-cpu==1.15.1 >>install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo" CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=6
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0 >>install.log 2>&1
export PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
phase=inputs
aws s3 cp "s3://$BORSUK_V271_BUCKET/$BORSUK_V271_V261_PREFIX/artifacts/vectors.raw" vectors.raw --only-show-errors
printf '%s  vectors.raw\n' '6c82a340e3e1b4226640e593efa9c4000c6a5962d4b13063093a1dab689a9005' | sha256sum -c -
aws s3 cp "s3://$BORSUK_V271_BUCKET/$BORSUK_V271_SOURCE_PREFIX/STAGING_COMPLETE.json" receipt.json --only-show-errors
printf '%s  receipt.json\n' '0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87' | sha256sum -c -
aws s3 cp "s3://$BORSUK_V271_BUCKET/$BORSUK_V271_SOURCE_PREFIX/materialized/train-00000045.parquet" shard45.parquet --only-show-errors
printf '%s  shard45.parquet\n' '1fae833dd9cbdb775b177176a2d301f0ee887988575b1152c13d7d0517cd25f4' | sha256sum -c -
phase=compile
cd repo
/usr/bin/time -v -o "$root/compile.time" "$CARGO_HOME/bin/cargo" build --release --locked -p borsuk --example resident_graph_frontier
cd "$root"
runner="$CARGO_TARGET_DIR/release/examples/resident_graph_frontier"
phase=prepare
/usr/bin/time -v -o prepare.time .venv/bin/python -m scripts.v271_fresh_frontier prepare \
  --receipt receipt.json --shard shard45.parquet --source vectors.raw \
  --queries queries.f32 --truth100 truth100.u32 --truth1m truth1m.u32 >prepare.json
head -c 307200 queries.f32 >queries100.f32
phase=preflight
/usr/bin/time -v -o preflight.build.time "$runner" build vectors.raw 100000 new100 >preflight.build.json
sha=$(.venv/bin/python -c 'import json;print(json.load(open("preflight.build.json"))["root_sha256"])')
/usr/bin/time -v -o preflight.search.time "$runner" local new100 "$sha" queries100.f32 100 preflight.raw.jsonl >preflight.search.json
.venv/bin/python -m scripts.v271_fresh_frontier score --raw preflight.raw.jsonl \
  --truth truth100.u32 --label borsuk-new100 --output preflight.quality.json
hits=$(.venv/bin/python -c 'import json;print(json.load(open("preflight.quality.json"))["borsuk-new100"]["hits"])')
if [ "$hits" -lt 9800 ]; then
  .venv/bin/python -c 'import json,sys;print(json.dumps({"decision":"no_go_100k","hits":int(sys.argv[1])}))' "$hits" >decision.json
  phase=complete
  exit 0
fi
phase=build1m
/usr/bin/time -v -o new1m.build.time "$runner" build vectors.raw 1000000 new1m >new1m.build.json
sha=$(.venv/bin/python -c 'import json;print(json.load(open("new1m.build.json"))["root_sha256"])')
phase=search
/usr/bin/time -v -o new1m.search.time "$runner" local new1m "$sha" queries.f32 1000 new1m.raw.jsonl >new1m.search.json
.venv/bin/python -m scripts.v271_fresh_frontier score --raw new1m.raw.jsonl \
  --truth truth1m.u32 --label borsuk-new1m --output new1m.quality.json
/usr/bin/time -v -o prior.search.time "$runner" s3 "$BORSUK_V271_GENERATION_URI" \
  '1e483859b96f5270209678e0f76f7cc9e26a80162a24c7fa48a942cf010ec92e' \
  prior-cache queries.f32 1000 prior.raw.jsonl >prior.search.json
.venv/bin/python -m scripts.v271_fresh_frontier score --raw prior.raw.jsonl \
  --truth truth1m.u32 --label borsuk-v269-artifact --output prior.quality.json
phase=control
/usr/bin/time -v -o faiss.control.time .venv/bin/python -m scripts.v271_fresh_frontier control \
  --source vectors.raw --queries queries.f32 --raw faiss.raw.jsonl >faiss.control.json
.venv/bin/python -m scripts.v271_fresh_frontier score --raw faiss.raw.jsonl \
  --truth truth1m.u32 --output faiss.quality.json
.venv/bin/python - <<'PY' >decision.json
import json,re
from pathlib import Path
old=json.loads(Path('prior.quality.json').read_text())['borsuk-v269-artifact']
new=json.loads(Path('new1m.quality.json').read_text())['borsuk-new1m']
rss=int(re.search(r'Maximum resident set size \(kbytes\): (\d+)',Path('new1m.search.time').read_text()).group(1))*1024
go=(new['recall_at_100']>=old['recall_at_100']-0.005
    and new['p95_ms']<=old['p95_ms']*1.2 and rss<=3*1024**3)
print(json.dumps({'decision':'go_10m' if go else 'no_go_1m',
    'new_recall':new['recall_at_100'],'old_recall':old['recall_at_100'],
    'new_p95_ms':new['p95_ms'],'old_p95_ms':old['p95_ms'],
    'new_peak_rss_bytes':rss},sort_keys=True))
PY
phase=complete
