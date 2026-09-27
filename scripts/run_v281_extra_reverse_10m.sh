#!/usr/bin/env bash
set -euo pipefail
root=/mnt/v281-extra-reverse-10m
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
  files=(install.log compile.time prepare.time source.json queries.f32 old-root.json
    old-raw.jsonl truth.u32 candidate.build.time candidate.build.json publish.time
    publish.json baseline.search.time baseline.search.json baseline.raw.jsonl
    baseline.quality.json candidate.search.time candidate.search.json candidate.raw.jsonl
    candidate.quality.json resources.json decision.json candidate/root.json run.log)
  : >artifact-list.txt
  for name in "${files[@]}"; do
    if [ -f "$name" ]; then
      printf '%s\n' "$name" >>artifact-list.txt
      aws s3 cp "$name" "s3://$BORSUK_V281_BUCKET/$BORSUK_V281_PREFIX/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-v281-extra-reverse-10m-terminal-v1',
    'source_commit':os.environ['BORSUK_V281_SOURCE_COMMIT'],
    'source_archive_sha256':os.environ['BORSUK_V281_ARCHIVE_SHA'],
    'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
    'finished_epoch':int(time.time()),'phase':os.environ['PHASE'],
    'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
    'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://$BORSUK_V281_BUCKET/$BORSUK_V281_PREFIX/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
phase=install
dnf install -y -q python3.12 python3.12-pip gcc gcc-c++ cmake perl tar gzip time >install.log 2>&1
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3 pyarrow==24.0.0 faiss-cpu==1.15.1 boto3==1.40.37 >>install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo" CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=12
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0 >>install.log 2>&1
export PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
phase=inputs
aws s3 cp "s3://$BORSUK_V281_BUCKET/$BORSUK_V281_SOURCE_PREFIX/STAGING_COMPLETE.json" receipt.json --only-show-errors
printf '%s  receipt.json\n' '0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87' | sha256sum -c -
aws s3 cp "s3://$BORSUK_V281_BUCKET/$BORSUK_V281_SOURCE_PREFIX/materialized/test.parquet" test.parquet --only-show-errors
printf '%s  test.parquet\n' '5e0123f163df0e53a7e329fd92fbfd49f079756acfb47387ee6664c267b6f94e' | sha256sum -c -
aws s3 cp "s3://$BORSUK_V281_BUCKET/$BORSUK_V281_V272_PREFIX/artifacts/generation/root.json" old-root.json --only-show-errors
printf '%s  old-root.json\n' '948e8a5555f44011b22681ca2cd20edde93f6fec5e6261e20e3a7b799d467022' | sha256sum -c -
aws s3 cp "s3://$BORSUK_V281_BUCKET/$BORSUK_V281_V272_PREFIX/artifacts/raw.jsonl" old-raw.jsonl --only-show-errors
printf '%s  old-raw.jsonl\n' '62e8065f8cd7b1d06b23daef50519adfb0a64e6b65b8e1ed171f314d9686f3e3' | sha256sum -c -
aws s3 cp "s3://$BORSUK_V281_BUCKET/$BORSUK_V281_V272_PREFIX/artifacts/truth.u32" truth.u32 --only-show-errors
printf '%s  truth.u32\n' '9d08b49fef274d5bee2572ed8ed186ff8f2063b759f556748fe83b6e1d21c4f1' | sha256sum -c -
phase=compile
cd repo
/usr/bin/time -v -o "$root/compile.time" "$CARGO_HOME/bin/cargo" build --release --locked -p borsuk \
  --example resident_graph_frontier --example v246_publish_graph_s3
cd "$root"
runner="$CARGO_TARGET_DIR/release/examples/resident_graph_frontier"
publisher="$CARGO_TARGET_DIR/release/examples/v246_publish_graph_s3"
phase=prepare
/usr/bin/time -v -o prepare.time .venv/bin/python -m scripts.v272_10m_scale prepare \
  --receipt receipt.json --test test.parquet --source vectors.raw \
  --queries queries.f32 --output source.json
python3 - <<'PY'
import json
source=json.load(open('source.json'))
assert source['raw_sha256']=='2e33abfc666e652455a815b90d88a13b617f6dbb431538d6324ad06eccf06f1f'
assert source['queries_sha256']=='4394cb0f28238fe713182094dbb90e2dbc52db634f48c1419997e86ad085ddd4'
PY
phase=baseline
old_uri="s3://$BORSUK_V281_BUCKET/$BORSUK_V281_V272_PREFIX/generation"
/usr/bin/time -v -o baseline.search.time "$runner" s3 "$old_uri" \
  '948e8a5555f44011b22681ca2cd20edde93f6fec5e6261e20e3a7b799d467022' \
  cache-baseline queries.f32 1000 baseline.raw.jsonl >baseline.search.json
.venv/bin/python - <<'PY'
import json
old=[json.loads(line)['ids'] for line in open('old-raw.jsonl')]
new=[json.loads(line)['ids'] for line in open('baseline.raw.jsonl')]
assert len(old)==len(new)==1000 and old==new, 'V272 baseline ID parity differs'
PY
.venv/bin/python -m scripts.v272_10m_scale score --raw baseline.raw.jsonl \
  --truth truth.u32 --output baseline.quality.json
phase=build
/usr/bin/time -v -o candidate.build.time timeout --signal=TERM --kill-after=30s 37194s \
  "$runner" build_reverse_extra vectors.raw 10000000 candidate >candidate.build.json
sha=$(.venv/bin/python -c 'import json;print(json.load(open("candidate.build.json"))["root_sha256"])')
phase=publish
uri="s3://$BORSUK_V281_BUCKET/$BORSUK_V281_PREFIX/generation"
/usr/bin/time -v -o publish.time "$publisher" "$uri" candidate/root.json candidate "$sha" >publish.json
phase=candidate
/usr/bin/time -v -o candidate.search.time "$runner" s3 "$uri" "$sha" \
  cache-candidate queries.f32 1000 candidate.raw.jsonl >candidate.search.json
phase=score
.venv/bin/python -m scripts.v272_10m_scale score --raw candidate.raw.jsonl \
  --truth truth.u32 --output candidate.quality.json
.venv/bin/python - <<'PY' >resources.json
import json,re
from pathlib import Path
def rss(name):
    return int(re.search(r'Maximum resident set size \(kbytes\): (\d+)',
             Path(name).read_text()).group(1))*1024
search=json.loads(Path('candidate.search.json').read_text())
build=json.loads(Path('candidate.build.json').read_text())
print(json.dumps({'search_rss_bytes':rss('candidate.search.time'),
    'build_rss_bytes':rss('candidate.build.time'),
    'build_seconds':build['build_ms']/1000,
    'hydration_gets':search['hydration']['object_gets'],
    'query_gets':0},sort_keys=True))
PY
.venv/bin/python -m scripts.v281_10m --old-raw old-raw.jsonl \
  --old-root old-root.json --baseline-raw baseline.raw.jsonl \
  --baseline-quality baseline.quality.json --candidate-quality candidate.quality.json \
  --candidate-root candidate/root.json --resources resources.json \
  --truth truth.u32 --output decision.json
phase=complete
