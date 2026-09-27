#!/usr/bin/env bash
set -euo pipefail
root=/mnt/v277-extra-reverse-fresh
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
    fresh_queries.raw.sha256 baseline-terminal.json candidate-terminal.json truth.u32 baseline.search.time
    baseline.search.json baseline.raw.jsonl
    candidate.search.time candidate.search.json candidate.raw.jsonl
    baseline.default.time baseline.default.json baseline.default.raw.jsonl
    candidate.default.time candidate.default.json candidate.default.raw.jsonl
    decision.json run.log)
  : >artifact-list.txt
  for name in "${files[@]}"; do
    if [ -f "$name" ]; then
      printf '%s\n' "$name" >>artifact-list.txt
      aws s3 cp "$name" "s3://$BORSUK_V277_BUCKET/$BORSUK_V277_PREFIX/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-v277-extra-reverse-fresh-terminal-v1',
    'source_commit':os.environ['BORSUK_V277_SOURCE_COMMIT'],
    'source_archive_sha256':os.environ['BORSUK_V277_ARCHIVE_SHA'],
    'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
    'finished_epoch':int(time.time()),'phase':os.environ['PHASE'],
    'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
    'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://$BORSUK_V277_BUCKET/$BORSUK_V277_PREFIX/terminal.json" --only-show-errors
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
source_key="$BORSUK_V277_V261_PREFIX/artifacts/vectors.raw"
aws s3api get-object --bucket "$BORSUK_V277_BUCKET" --key "$source_key" --range bytes=0-307199999 vectors.raw >/dev/null
printf '%s  vectors.raw\n' '0f3631d71c105e5ea3d701c96033b362c2f84bd43002a9c8a5c70040801be06e' | sha256sum -c - >vectors.raw.sha256
aws s3api get-object --bucket "$BORSUK_V277_BUCKET" --key "$source_key" --range bytes=319488000-322559999 fresh_queries.raw >/dev/null
printf '%s  fresh_queries.raw\n' '0099cdcd57a80d33437a63a3cd9e9fab4bd333ba27ad4d1cedbc2d32e2a932cb' | sha256sum -c - >fresh_queries.raw.sha256
aws s3 cp "s3://$BORSUK_V277_BUCKET/$BORSUK_V277_V271_PREFIX/terminal.json" baseline-terminal.json --only-show-errors
printf '%s  baseline-terminal.json\n' '13775dc6c5176c636fc8442b4bd2bddd2ee375818a98d1328d2c0352423bc80b' | sha256sum -c -
mkdir baseline
for name in root.json plane.bin graph.bin map.u32 books.bin codes.bin; do
  aws s3 cp "s3://$BORSUK_V277_BUCKET/$BORSUK_V277_V271_PREFIX/artifacts/new100/$name" "baseline/$name" --only-show-errors
  expected=$(python3 -c 'import json,sys; print(json.load(open("baseline-terminal.json"))["artifacts"]["new100/"+sys.argv[1]]["sha256"])' "$name")
  printf '%s  %s\n' "$expected" "baseline/$name" | sha256sum -c -
done
aws s3 cp "s3://$BORSUK_V277_BUCKET/$BORSUK_V277_V276_PREFIX/terminal.json" candidate-terminal.json --only-show-errors
printf '%s  candidate-terminal.json\n' '6f83e1877ed0e5eaf44ba5514c7588df6b924476f035cea5a77feffc0d8148a1' | sha256sum -c -
mkdir candidate
for name in root.json plane.bin graph.bin map.u32 books.bin codes.bin; do
  aws s3 cp "s3://$BORSUK_V277_BUCKET/$BORSUK_V277_V276_PREFIX/artifacts/candidate/$name" "candidate/$name" --only-show-errors
  expected=$(python3 -c 'import json,sys; print(json.load(open("candidate-terminal.json"))["artifacts"]["candidate/"+sys.argv[1]]["sha256"])' "$name")
  printf '%s  %s\n' "$expected" "candidate/$name" | sha256sum -c -
done
phase=compile
cd repo
/usr/bin/time -v -o "$root/compile.time" "$CARGO_HOME/bin/cargo" build --release --locked -p borsuk --example resident_graph_frontier
cd "$root"
runner="$CARGO_TARGET_DIR/release/examples/resident_graph_frontier"
phase=truth
/usr/bin/time -v -o prepare.time .venv/bin/python -m scripts.v277_extra_reverse_fresh prepare vectors.raw fresh_queries.raw truth.u32 >prepare.json
phase=baseline
/usr/bin/time -v -o baseline.search.time "$runner" local_stress baseline \
  '440beefd321dfeeae25ba6277a2e2f1f938c389a0c4a9007177f4bacc4b1d6e3' \
  fresh_queries.raw 1000 baseline.raw.jsonl >baseline.search.json
phase=candidate
sha='56de9f4768271611683193f4fa5d36795cb5926adabb89efb7bd4c851b1e340b'
/usr/bin/time -v -o candidate.search.time "$runner" local_stress candidate "$sha" \
  fresh_queries.raw 1000 candidate.raw.jsonl >candidate.search.json
phase=default
/usr/bin/time -v -o baseline.default.time "$runner" local baseline \
  '440beefd321dfeeae25ba6277a2e2f1f938c389a0c4a9007177f4bacc4b1d6e3' \
  fresh_queries.raw 1000 baseline.default.raw.jsonl >baseline.default.json
/usr/bin/time -v -o candidate.default.time "$runner" local candidate "$sha" \
  fresh_queries.raw 1000 candidate.default.raw.jsonl >candidate.default.json
phase=score
.venv/bin/python -m scripts.v277_extra_reverse_fresh compare \
  baseline.raw.jsonl candidate.raw.jsonl truth.u32 baseline/graph.bin \
  candidate/graph.bin baseline.default.raw.jsonl candidate.default.raw.jsonl decision.json \
  --baseline-rss-kib "$(awk -F': ' '/Maximum resident set size/{print $2}' baseline.search.time)" \
  --candidate-rss-kib "$(awk -F': ' '/Maximum resident set size/{print $2}' candidate.search.time)"
phase=complete
