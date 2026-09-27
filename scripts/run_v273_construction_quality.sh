#!/usr/bin/env bash
set -euo pipefail
root=/mnt/v273-construction-quality
cd "$root"
phase=bootstrap
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  files=(install.log compile.time prepare.time prepare.json vectors.raw.sha256
    fresh_queries.raw.sha256 baseline-terminal.json truth.u32 baseline.search.time
    baseline.search.json baseline.raw.jsonl candidate.build.time candidate.build.json
    candidate.search.time candidate.search.json candidate.raw.jsonl decision.json run.log)
  for name in root.json plane.bin graph.bin map.u32 books.bin codes.bin; do
    [ ! -f "candidate/$name" ] || files+=("candidate/$name")
  done
  : >artifact-list.txt
  for name in "${files[@]}"; do
    if [ -f "$name" ]; then
      printf '%s\n' "$name" >>artifact-list.txt
      aws s3 cp "$name" "s3://$BORSUK_V273_BUCKET/$BORSUK_V273_PREFIX/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-v273-construction-quality-terminal-v1',
    'source_commit':os.environ['BORSUK_V273_SOURCE_COMMIT'],
    'source_archive_sha256':os.environ['BORSUK_V273_ARCHIVE_SHA'],
    'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
    'finished_epoch':int(time.time()),'phase':os.environ['PHASE'],
    'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
    'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://$BORSUK_V273_BUCKET/$BORSUK_V273_PREFIX/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
phase=install
dnf install -y -q python3.12 python3.12-pip gcc gcc-c++ cmake perl tar gzip time >install.log 2>&1
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3 faiss-cpu==1.15.1 >>install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo" CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=6
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0 >>install.log 2>&1
export PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
phase=inputs
source_key="$BORSUK_V273_V261_PREFIX/artifacts/vectors.raw"
aws s3api get-object --bucket "$BORSUK_V273_BUCKET" --key "$source_key" --range bytes=0-307199999 vectors.raw >/dev/null
printf '%s  vectors.raw\n' '0f3631d71c105e5ea3d701c96033b362c2f84bd43002a9c8a5c70040801be06e' | sha256sum -c - >vectors.raw.sha256
aws s3api get-object --bucket "$BORSUK_V273_BUCKET" --key "$source_key" --range bytes=307200000-310271999 fresh_queries.raw >/dev/null
printf '%s  fresh_queries.raw\n' '10322f59ee236849e60137c081432c3a8ef55d6c09dc1585356e984b2bfc30c0' | sha256sum -c - >fresh_queries.raw.sha256
aws s3 cp "s3://$BORSUK_V273_BUCKET/$BORSUK_V273_V271_PREFIX/terminal.json" baseline-terminal.json --only-show-errors
printf '%s  baseline-terminal.json\n' '13775dc6c5176c636fc8442b4bd2bddd2ee375818a98d1328d2c0352423bc80b' | sha256sum -c -
mkdir baseline
for name in root.json plane.bin graph.bin map.u32 books.bin codes.bin; do
  aws s3 cp "s3://$BORSUK_V273_BUCKET/$BORSUK_V273_V271_PREFIX/artifacts/new100/$name" "baseline/$name" --only-show-errors
  expected=$(python3 -c 'import json,sys; print(json.load(open("baseline-terminal.json"))["artifacts"]["new100/"+sys.argv[1]]["sha256"])' "$name")
  printf '%s  %s\n' "$expected" "baseline/$name" | sha256sum -c -
done
phase=compile
cd repo
/usr/bin/time -v -o "$root/compile.time" "$CARGO_HOME/bin/cargo" build --release --locked -p borsuk --example resident_graph_frontier
cd "$root"
runner="$CARGO_TARGET_DIR/release/examples/resident_graph_frontier"
phase=truth
/usr/bin/time -v -o prepare.time .venv/bin/python -m scripts.v273_construction_quality prepare vectors.raw fresh_queries.raw truth.u32 >prepare.json
phase=baseline
/usr/bin/time -v -o baseline.search.time "$runner" local_stress baseline \
  '440beefd321dfeeae25ba6277a2e2f1f938c389a0c4a9007177f4bacc4b1d6e3' \
  fresh_queries.raw 1000 baseline.raw.jsonl >baseline.search.json
phase=candidate
/usr/bin/time -v -o candidate.build.time "$runner" build vectors.raw 100000 candidate >candidate.build.json
sha=$(.venv/bin/python -c 'import json;print(json.load(open("candidate.build.json"))["root_sha256"])')
/usr/bin/time -v -o candidate.search.time "$runner" local_stress candidate "$sha" \
  fresh_queries.raw 1000 candidate.raw.jsonl >candidate.search.json
phase=score
.venv/bin/python -m scripts.v273_construction_quality compare \
  baseline.raw.jsonl candidate.raw.jsonl truth.u32 decision.json
phase=complete
