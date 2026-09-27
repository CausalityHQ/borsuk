#!/usr/bin/env bash
set -euo pipefail
root=/mnt/v282-100k
cd "$root"
phase=bootstrap
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  aws s3 sync relaion/evidence "s3://$BORSUK_V282_BUCKET/$BORSUK_V282_PREFIX/artifacts/relaion/evidence" --only-show-errors || code=96
  aws s3 sync cohere/evidence "s3://$BORSUK_V282_BUCKET/$BORSUK_V282_PREFIX/artifacts/cohere/evidence" --only-show-errors || code=96
  for name in install.log compile.time run.log summary.json; do
    if [ -f "$name" ]; then aws s3 cp "$name" "s3://$BORSUK_V282_BUCKET/$BORSUK_V282_PREFIX/artifacts/$name" --only-show-errors || code=96; fi
  done
  python3 - "$code" "$phase" "$instance_id" <<'PY' >terminal.json
import hashlib,json,os,sys,time
from pathlib import Path
artifacts={}
for path in sorted(Path('.').glob('*/evidence/*')):
    if not path.is_file(): continue
    h=hashlib.sha256(path.read_bytes()).hexdigest()
    artifacts[str(path)]={'bytes':path.stat().st_size,'sha256':h}
for name in ('install.log','compile.time','run.log','summary.json'):
    path=Path(name)
    if path.is_file():
        artifacts[name]={'bytes':path.stat().st_size,
                         'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
code=int(sys.argv[1]); phase=sys.argv[2]
print(json.dumps({'schema':'borsuk-v282-paired-100k-terminal-v1',
    'source_commit':os.environ['BORSUK_V282_COMMIT'],
    'source_archive_sha256':os.environ['BORSUK_V282_ARCHIVE_SHA'],
    'instance_id':sys.argv[3], 'exit_code':code, 'phase':phase,
    'finished_epoch':int(time.time()),
    'status':'complete' if code==0 and phase=='complete' else 'failed',
    'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://$BORSUK_V282_BUCKET/$BORSUK_V282_PREFIX/terminal.json" --only-show-errors
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
mkdir -p relaion/evidence cohere/evidence
phase=inputs
aws s3 cp "s3://$BORSUK_V282_BUCKET/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/source.parquet" relaion/input.parquet --only-show-errors
printf '%s  relaion/input.parquet\n' '2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86' | sha256sum -c -
aws s3 cp "s3://$BORSUK_V282_BUCKET/research/v116-validation-paired/5e9b35ad40ea023eab4407aa611d759e1893bb34/runs/v116-validation-20260923T235426Z/a0001/artifacts/requests.jsonl" relaion/requests.jsonl --only-show-errors
printf '%s  relaion/requests.jsonl\n' 'c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9' | sha256sum -c -
aws s3 cp "s3://$BORSUK_V282_BUCKET/research/v248-cohere-transfer-100k/7b66f9f7d306a0f1febc2091783de68a6ece72c7/runs/a0001/artifacts/vectors.raw" cohere/input.raw --only-show-errors
printf '%s  cohere/input.raw\n' '0f3631d71c105e5ea3d701c96033b362c2f84bd43002a9c8a5c70040801be06e' | sha256sum -c -
aws s3 cp "s3://$BORSUK_V282_BUCKET/research/v248-cohere-transfer-100k/7b66f9f7d306a0f1febc2091783de68a6ece72c7/runs/a0001/artifacts/requests.jsonl" cohere/requests.jsonl --only-show-errors
printf '%s  cohere/requests.jsonl\n' '86d9406486a2bb27aa2e603f019e078dd3ecaed47f79ec685558ba3536433812' | sha256sum -c -
for dataset in relaion cohere; do
  phase="${dataset}-source"
  if [ "$dataset" = relaion ]; then kind=parquet; input_sha=2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86; input="$dataset/input.parquet"; requests_sha=c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9; else kind=raw; input_sha=0f3631d71c105e5ea3d701c96033b362c2f84bd43002a9c8a5c70040801be06e; input="$dataset/input.raw"; requests_sha=86d9406486a2bb27aa2e603f019e078dd3ecaed47f79ec685558ba3536433812; fi
  /usr/bin/time -v -o "$dataset/evidence/source.time" .venv/bin/python -m scripts.v282_prepare_pair source --input "$input" --input-sha "$input_sha" --kind "$kind" --output "$dataset/source" >"$dataset/evidence/source.json"
  source_sha=$(.venv/bin/python -c "import json;print(json.load(open('$dataset/evidence/source.json'))['source_sha256'])")
  provenance_sha=$(.venv/bin/python -c "import json;print(json.load(open('$dataset/evidence/source.json'))['provenance_sha256'])")
  phase="${dataset}-layout"
  /usr/bin/time -v -o "$dataset/evidence/layout.time" .venv/bin/python -m scripts.v120_source_layout --source "$dataset/source/source.parquet" --provenance "$dataset/source/provenance.json" --output "$dataset/layout" --source-sha256 "$source_sha" --provenance-sha256 "$provenance_sha" >"$dataset/evidence/layout.json"
  layout_sha=$(.venv/bin/python -c "import json;print(json.load(open('$dataset/evidence/layout.json'))['layout_sha256'])")
  sq8_sha=$(.venv/bin/python -c "import json;print(json.load(open('$dataset/evidence/layout.json'))['sq8_sha256'])")
  phase="${dataset}-router"
  /usr/bin/time -v -o "$dataset/evidence/router.time" .venv/bin/python -m scripts.v115_source_router --source "$dataset/source/source.parquet" --layout "$dataset/layout/layout.npy" --output "$dataset/router" --source-sha256 "$source_sha" --layout-sha256 "$layout_sha" --sq8-sha256 "$sq8_sha" --rows 100000 --dimensions 768 --page-rows 256 --blocks-per-page 2 --generation 1
  .venv/bin/python - "$dataset" "$sq8_sha" <<'PY'
import hashlib,sys
from pathlib import Path
from scripts.v115_page_authority import build_page_authority
name,expected=sys.argv[1:]
build_page_authority(Path(name)/'layout/sq8.bin',Path(name)/'pages',
    expected_object_sha256=expected,rows=100000,dimensions=768,page_rows=256,generation=1)
PY
  router_sha=$(sha256sum "$dataset/router/manifest.json" | cut -d' ' -f1)
  phase="${dataset}-routing"
  /usr/bin/time -v -o "$dataset/evidence/routing.time" "$CARGO_TARGET_DIR/release/v282_build_routing" "$dataset/layout/sq8.bin" "$dataset/router" "$router_sha" "$dataset/routing" >"$dataset/evidence/routing.json"
  object_key="$BORSUK_V282_PREFIX/objects/$sq8_sha"
  aws s3 cp "$dataset/layout/sq8.bin" "s3://$BORSUK_V282_BUCKET/$object_key" --only-show-errors
  etag=$(aws s3api head-object --bucket "$BORSUK_V282_BUCKET" --key "$object_key" --query ETag --output text)
  phase="${dataset}-seal"
  .venv/bin/python -m scripts.v282_seal_generation --router "$dataset/router" --pages "$dataset/pages" --routing "$dataset/routing" --etag "$etag" --output "$dataset/generation" >"$dataset/evidence/root.sha256"
  cp "$dataset/generation/manifest.json" "$dataset/evidence/generation-manifest.json"
  cp "$dataset/source/provenance.json" "$dataset/evidence/provenance.json"
  cp "$dataset/layout/manifest.json" "$dataset/evidence/layout-manifest.json"
  cp "$dataset/router/manifest.json" "$dataset/evidence/router-manifest.json"
  cp "$dataset/pages/manifest.json" "$dataset/evidence/page-manifest.json"
  aws s3 sync "$dataset/generation" "s3://$BORSUK_V282_BUCKET/$BORSUK_V282_PREFIX/artifacts/$dataset/generation" --only-show-errors
  phase="${dataset}-truth"
  /usr/bin/time -v -o "$dataset/evidence/truth.time" .venv/bin/python -m scripts.v282_prepare_pair truth --source "$dataset/source/source.parquet" --source-sha "$source_sha" --requests "$dataset/requests.jsonl" --requests-sha "$requests_sha" --output "$dataset/truth.u32" >"$dataset/evidence/truth.json"
  phase="${dataset}-replay"
  /usr/bin/time -v -o "$dataset/evidence/replay.time" "$CARGO_TARGET_DIR/release/v282_local_falsifier" "$dataset/generation" "$(cat "$dataset/evidence/root.sha256")" "$dataset/layout/sq8.bin" "$dataset/requests.jsonl" "$dataset/truth.u32" "$dataset/evidence/raw.jsonl"
  sha256sum "$dataset/evidence/raw.jsonl" "$dataset/truth.u32" >"$dataset/evidence/sha256.txt"
  aws s3 sync "$dataset/evidence" "s3://$BORSUK_V282_BUCKET/$BORSUK_V282_PREFIX/artifacts/$dataset/evidence" --only-show-errors
done
phase=summary
.venv/bin/python -m scripts.v282_summarize_falsifier --relaion relaion/evidence/raw.jsonl --cohere cohere/evidence/raw.jsonl --output summary.json
phase=complete
