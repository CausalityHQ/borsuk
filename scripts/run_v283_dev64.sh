#!/usr/bin/env bash
set -euo pipefail
root=/mnt/v283-dev64
cd "$root"
phase=bootstrap
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  exec >/dev/null 2>&1
  cd "$root"
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  files=(install.log compile.time source.json router.time routing.time replay.time
    summary.json raw.jsonl root.sha256 generation.tar.gz run.log)
  : >artifact-list.txt
  for name in "${files[@]}"; do
    if [ -f "$name" ]; then
      printf '%s\n' "$name" >>artifact-list.txt
      aws s3 cp "$name" "s3://$BORSUK_V283_BUCKET/$BORSUK_V283_PREFIX/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os,time
from pathlib import Path
artifacts={}
for name in Path('artifact-list.txt').read_text().splitlines():
    path=Path(name); body=path.read_bytes()
    artifacts[name]={'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest()}
code=int(os.environ['EXIT_CODE']); phase=os.environ['PHASE']
print(json.dumps({'schema':'borsuk-v283-dev64-terminal-v1',
  'source_commit':os.environ['BORSUK_V283_COMMIT'],
  'source_archive_sha256':os.environ['BORSUK_V283_ARCHIVE_SHA'],
  'layout_sha256':'303f31ab8a182a0aaa304c4ef551a046be41071ac24e67a793882eb74c5b532e',
  'sq8_sha256':'301696df05ca03122951b66ad8a9bedb5d5f1e675c6fc66f6019abbce3fcda58',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'finished_epoch':int(time.time()),'phase':phase,
  'status':'complete' if code==0 and phase=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://$BORSUK_V283_BUCKET/$BORSUK_V283_PREFIX/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
phase=install
dnf install -y -q python3.12 python3.12-pip gcc gcc-c++ cmake perl tar gzip time >install.log 2>&1
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3 pyarrow==24.0.0 boto3==1.40.37 >>install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo" CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=12
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0 >>install.log 2>&1
export PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=16 OMP_NUM_THREADS=16 MKL_NUM_THREADS=16
phase=compile
cd repo
/usr/bin/time -v -o "$root/compile.time" "$CARGO_HOME/bin/cargo" build --release --locked -p borsuk \
  --bin v282_build_routing --bin v282_local_falsifier
cd "$root"
phase=inputs
source_prefix=research/v248-cohere-transfer-100k/7b66f9f7d306a0f1febc2091783de68a6ece72c7/runs/a0001/artifacts
aws s3 cp "s3://$BORSUK_V283_BUCKET/$source_prefix/vectors.raw" vectors.raw --only-show-errors
printf '%s  vectors.raw\n' '0f3631d71c105e5ea3d701c96033b362c2f84bd43002a9c8a5c70040801be06e' | sha256sum -c -
aws s3 cp "s3://$BORSUK_V283_BUCKET/$source_prefix/requests.jsonl" requests.jsonl --only-show-errors
printf '%s  requests.jsonl\n' '86d9406486a2bb27aa2e603f019e078dd3ecaed47f79ec685558ba3536433812' | sha256sum -c -
aws s3 cp "s3://$BORSUK_V283_BUCKET/$source_prefix/truth.u32" truth.u32 --only-show-errors
printf '%s  truth.u32\n' '06cd59b31962d4190367b54d7abf24dd4e018d3c4ac8da0b2b528d21a5a7cbb8' | sha256sum -c -
aws s3 cp "s3://$BORSUK_V283_BUCKET/$BORSUK_V283_LAYOUT_KEY" layout.npy --only-show-errors
printf '%s  layout.npy\n' '303f31ab8a182a0aaa304c4ef551a046be41071ac24e67a793882eb74c5b532e' | sha256sum -c -
aws s3 cp "s3://$BORSUK_V283_BUCKET/$BORSUK_V283_SQ8_KEY" sq8.bin --only-show-errors
printf '%s  sq8.bin\n' '301696df05ca03122951b66ad8a9bedb5d5f1e675c6fc66f6019abbce3fcda58' | sha256sum -c -
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/v282-paired-100k/e74d75acb40647f606b498023b11dec26701e4aa/runs/a0002/artifacts/cohere/evidence/raw.jsonl' baseline.raw.jsonl --only-show-errors
printf '%s  baseline.raw.jsonl\n' '9afb4480ab9bf17625afb9a1212589915ebe719a00d626edad57049781fb346c' | sha256sum -c -
.venv/bin/python - <<'PY'
import json
from pathlib import Path
lines=Path('requests.jsonl').read_bytes().splitlines(keepends=True)
assert len(lines)==1000 and [json.loads(x)['query_ordinal'] for x in lines[:64]]==list(range(64))
Path('requests64.jsonl').write_bytes(b''.join(lines[:64]))
truth=Path('truth.u32').read_bytes(); assert len(truth)==400000
Path('truth64.u32').write_bytes(truth[:25600])
PY
phase=source
.venv/bin/python -m scripts.v282_prepare_pair source --input vectors.raw \
  --input-sha 0f3631d71c105e5ea3d701c96033b362c2f84bd43002a9c8a5c70040801be06e \
  --kind raw --output source >source.json
printf '%s  source/source.parquet\n' 'f779b8b64722277273d41f684acbb66d82d9613745162af534dac478c1cfed00' | sha256sum -c -
printf '%s  source/provenance.json\n' '826945ac71ee9496f512e70a48dfd14b1f8c15db4669a6d5bff193ccec0aa730' | sha256sum -c -
phase=router
/usr/bin/time -v -o router.time timeout --signal=TERM --kill-after=30s 1200s \
  .venv/bin/python -m scripts.v115_source_router --source source/source.parquet \
  --layout layout.npy --output router \
  --source-sha256 f779b8b64722277273d41f684acbb66d82d9613745162af534dac478c1cfed00 \
  --layout-sha256 303f31ab8a182a0aaa304c4ef551a046be41071ac24e67a793882eb74c5b532e \
  --sq8-sha256 301696df05ca03122951b66ad8a9bedb5d5f1e675c6fc66f6019abbce3fcda58 \
  --rows 100000 --dimensions 768 --page-rows 256 --blocks-per-page 2 --generation 1
phase=routing
.venv/bin/python - <<'PY'
from pathlib import Path
from scripts.v115_page_authority import build_page_authority
build_page_authority(Path('sq8.bin'),Path('pages'),
  expected_object_sha256='301696df05ca03122951b66ad8a9bedb5d5f1e675c6fc66f6019abbce3fcda58',
  rows=100000,dimensions=768,page_rows=256,generation=1)
PY
router_sha=$(sha256sum router/manifest.json | cut -d' ' -f1)
/usr/bin/time -v -o routing.time "$CARGO_TARGET_DIR/release/v282_build_routing" \
  sq8.bin router "$router_sha" routing
object_key="$BORSUK_V283_PREFIX/objects/301696df05ca03122951b66ad8a9bedb5d5f1e675c6fc66f6019abbce3fcda58"
aws s3 cp sq8.bin "s3://$BORSUK_V283_BUCKET/$object_key" --only-show-errors
etag=$(aws s3api head-object --bucket "$BORSUK_V283_BUCKET" --key "$object_key" --query ETag --output text)
.venv/bin/python -m scripts.v282_seal_generation --router router --pages pages \
  --routing routing --etag "$etag" --object-key "$object_key" \
  --output generation >root.sha256
tar -czf generation.tar.gz generation
phase=replay
/usr/bin/time -v -o replay.time "$CARGO_TARGET_DIR/release/v282_local_falsifier" \
  generation "$(cat root.sha256)" sq8.bin requests64.jsonl truth64.u32 raw.jsonl 64
.venv/bin/python -m scripts.v283_dev64_summary --candidate raw.jsonl \
  --baseline baseline.raw.jsonl \
  --baseline-sha 9afb4480ab9bf17625afb9a1212589915ebe719a00d626edad57049781fb346c \
  --output summary.json
phase=complete
