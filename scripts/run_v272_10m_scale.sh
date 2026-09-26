#!/usr/bin/env bash
set -euo pipefail
root=/mnt/v272-rust-rc-10m-scale
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
  files=(install.log compile.time prepare.time source.json queries.f32 build.time build.json
    publish.time publish.json search.time search.json raw.jsonl truth.time truth.json
    truth.u32 quality.json decision.json generation/root.json run.log)
  : >artifact-list.txt
  for name in "${files[@]}"; do
    if [ -f "$name" ]; then
      printf '%s\n' "$name" >>artifact-list.txt
      aws s3 cp "$name" "s3://$BORSUK_V272_BUCKET/$BORSUK_V272_PREFIX/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-v272-rust-rc-10m-scale-terminal-v1',
    'source_commit':os.environ['BORSUK_V272_SOURCE_COMMIT'],
    'source_archive_sha256':os.environ['BORSUK_V272_ARCHIVE_SHA'],
    'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
    'finished_epoch':int(time.time()),'phase':os.environ['PHASE'],
    'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
    'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://$BORSUK_V272_BUCKET/$BORSUK_V272_PREFIX/terminal.json" --only-show-errors
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
aws s3 cp "s3://$BORSUK_V272_BUCKET/$BORSUK_V272_SOURCE_PREFIX/STAGING_COMPLETE.json" receipt.json --only-show-errors
printf '%s  receipt.json\n' '0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87' | sha256sum -c -
aws s3 cp "s3://$BORSUK_V272_BUCKET/$BORSUK_V272_SOURCE_PREFIX/materialized/test.parquet" test.parquet --only-show-errors
printf '%s  test.parquet\n' '5e0123f163df0e53a7e329fd92fbfd49f079756acfb47387ee6664c267b6f94e' | sha256sum -c -
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
phase=build
/usr/bin/time -v -o build.time "$runner" build vectors.raw 10000000 generation >build.json
sha=$(.venv/bin/python -c 'import json;print(json.load(open("build.json"))["root_sha256"])')
phase=publish
uri="s3://$BORSUK_V272_BUCKET/$BORSUK_V272_PREFIX/generation"
/usr/bin/time -v -o publish.time "$publisher" "$uri" generation/root.json generation "$sha" >publish.json
phase=search
/usr/bin/time -v -o search.time "$runner" s3 "$uri" "$sha" cache queries.f32 1000 raw.jsonl >search.json
phase=truth
/usr/bin/time -v -o truth.time .venv/bin/python -m scripts.v272_10m_scale truth \
  --prepared source.json --source vectors.raw --queries queries.f32 \
  --truth truth.u32 --output truth.json
.venv/bin/python -m scripts.v272_10m_scale score --raw raw.jsonl \
  --truth truth.u32 --output quality.json
.venv/bin/python - <<'PY' >decision.json
import json,re
from pathlib import Path
quality=json.loads(Path('quality.json').read_text())
search=json.loads(Path('search.json').read_text())
rss=int(re.search(r'Maximum resident set size \(kbytes\): (\d+)',Path('search.time').read_text()).group(1))*1024
splits=(quality['development_first256_prior_used'],quality['validation_remaining744_prior_used'])
go=(all(part['recall_at_100']>=0.995 and part['p05_hits']>=98 for part in splits)
    and quality['p95_ms']<=150 and quality['p99_ms']<=180
    and rss<=32*1024**3 and search['hydration']['object_gets']==5)
print(json.dumps({'decision':'go_product_gate' if go else 'no_go_10m',
    'combined_hits':quality['combined']['hits'],'p95_ms':quality['p95_ms'],
    'p99_ms':quality['p99_ms'],'search_rss_bytes':rss,
    'hydration':search['hydration']},sort_keys=True))
PY
phase=complete
