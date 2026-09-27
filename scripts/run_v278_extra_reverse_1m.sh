#!/usr/bin/env bash
set -euo pipefail
root=/mnt/v278-extra-reverse-1m
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
    receipt.json.sha256 shard45.parquet.sha256 baseline-terminal.json queries.f32 truth.u32 baseline.search.time
    baseline.search.json baseline.raw.jsonl candidate.build.time candidate.build.json
    candidate.search.time candidate.search.json candidate.raw.jsonl
    baseline.default.time baseline.default.json baseline.default.raw.jsonl
    candidate.default.time candidate.default.json candidate.default.raw.jsonl
    decision.json run.log)
  for name in root.json plane.bin graph.bin map.u32 books.bin codes.bin; do
    [ ! -f "candidate/$name" ] || files+=("candidate/$name")
  done
  : >artifact-list.txt
  for name in "${files[@]}"; do
    if [ -f "$name" ]; then
      printf '%s\n' "$name" >>artifact-list.txt
      aws s3 cp "$name" "s3://$BORSUK_V278_BUCKET/$BORSUK_V278_PREFIX/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-v278-extra-reverse-1m-terminal-v1',
    'source_commit':os.environ['BORSUK_V278_SOURCE_COMMIT'],
    'source_archive_sha256':os.environ['BORSUK_V278_ARCHIVE_SHA'],
    'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
    'finished_epoch':int(time.time()),'phase':os.environ['PHASE'],
    'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
    'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://$BORSUK_V278_BUCKET/$BORSUK_V278_PREFIX/terminal.json" --only-show-errors
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
aws s3 cp "s3://$BORSUK_V278_BUCKET/$BORSUK_V278_V261_PREFIX/artifacts/vectors.raw" vectors.raw --only-show-errors
printf '%s  vectors.raw\n' '6c82a340e3e1b4226640e593efa9c4000c6a5962d4b13063093a1dab689a9005' | sha256sum -c - >vectors.raw.sha256
aws s3 cp "s3://$BORSUK_V278_BUCKET/$BORSUK_V278_SOURCE_PREFIX/STAGING_COMPLETE.json" receipt.json --only-show-errors
printf '%s  receipt.json\n' '0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87' | sha256sum -c - >receipt.json.sha256
aws s3 cp "s3://$BORSUK_V278_BUCKET/$BORSUK_V278_SOURCE_PREFIX/materialized/train-00000045.parquet" shard45.parquet --only-show-errors
printf '%s  shard45.parquet\n' '1fae833dd9cbdb775b177176a2d301f0ee887988575b1152c13d7d0517cd25f4' | sha256sum -c - >shard45.parquet.sha256
aws s3 cp "s3://$BORSUK_V278_BUCKET/$BORSUK_V278_V271_PREFIX/terminal.json" baseline-terminal.json --only-show-errors
printf '%s  baseline-terminal.json\n' '13775dc6c5176c636fc8442b4bd2bddd2ee375818a98d1328d2c0352423bc80b' | sha256sum -c -
mkdir baseline
for name in root.json plane.bin graph.bin map.u32 books.bin codes.bin; do
  aws s3 cp "s3://$BORSUK_V278_BUCKET/$BORSUK_V278_V271_PREFIX/artifacts/new1m/$name" "baseline/$name" --only-show-errors
  expected=$(python3 -c 'import json,sys; print(json.load(open("baseline-terminal.json"))["artifacts"]["new1m/"+sys.argv[1]]["sha256"])' "$name")
  printf '%s  %s\n' "$expected" "baseline/$name" | sha256sum -c -
done
phase=compile
cd repo
/usr/bin/time -v -o "$root/compile.time" "$CARGO_HOME/bin/cargo" build --release --locked -p borsuk --example resident_graph_frontier
cd "$root"
runner="$CARGO_TARGET_DIR/release/examples/resident_graph_frontier"
phase=truth
/usr/bin/time -v -o prepare.time .venv/bin/python -m scripts.v278_extra_reverse_1m prepare \
  vectors.raw receipt.json shard45.parquet queries.f32 truth.u32 >prepare.json
phase=baseline
/usr/bin/time -v -o baseline.search.time "$runner" local_stress baseline \
  'c3a60f9969f8bc0fc6cf2f24831c45d3918bd7090a474c090b5812629851f86a' \
  queries.f32 1000 baseline.raw.jsonl >baseline.search.json
phase=candidate
/usr/bin/time -v -o candidate.build.time "$runner" build_reverse_extra vectors.raw 1000000 candidate >candidate.build.json
sha=$(.venv/bin/python -c 'import json;print(json.load(open("candidate.build.json"))["root_sha256"])')
/usr/bin/time -v -o candidate.search.time "$runner" local_stress candidate "$sha" \
  queries.f32 1000 candidate.raw.jsonl >candidate.search.json
phase=default
/usr/bin/time -v -o baseline.default.time "$runner" local baseline \
  'c3a60f9969f8bc0fc6cf2f24831c45d3918bd7090a474c090b5812629851f86a' \
  queries.f32 1000 baseline.default.raw.jsonl >baseline.default.json
/usr/bin/time -v -o candidate.default.time "$runner" local candidate "$sha" \
  queries.f32 1000 candidate.default.raw.jsonl >candidate.default.json
phase=score
.venv/bin/python -m scripts.v278_extra_reverse_1m compare \
  baseline.raw.jsonl candidate.raw.jsonl truth.u32 baseline/graph.bin \
  candidate/graph.bin baseline.default.raw.jsonl candidate.default.raw.jsonl decision.json \
  --baseline-rss-kib "$(awk -F': ' '/Maximum resident set size/{print $2}' baseline.default.time)" \
  --candidate-rss-kib "$(awk -F': ' '/Maximum resident set size/{print $2}' candidate.default.time)" \
  --diagnostic-baseline-rss-kib "$(awk -F': ' '/Maximum resident set size/{print $2}' baseline.search.time)" \
  --diagnostic-candidate-rss-kib "$(awk -F': ' '/Maximum resident set size/{print $2}' candidate.search.time)" \
  --build-seconds "$(.venv/bin/python -c 'import json;print(json.load(open("candidate.build.json"))["build_ms"]/1000)')" \
  --build-rss-kib "$(awk -F': ' '/Maximum resident set size/{print $2}' candidate.build.time)"
phase=complete
