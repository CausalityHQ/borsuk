#!/bin/bash
# One source-only deep-image layout, SQ8 and balanced PQ64 construction cell.
set -euo pipefail
root=/mnt/v120-index
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
deadline_epoch=$((started_epoch + V120_WALL_SECONDS))
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
  for path in hashes.log install.log download.log download-resources.txt \
      layout.log layout-resources.txt router.log router-resources.txt \
      seal.log seal-resources.txt compile.log compile-resources.txt \
      built/manifest.json built/layout.npy built/sq8.bin \
      router/manifest.json router/summaries.bin router/books.bin \
      router/codes.bin router/low.bin router/step.bin \
      mirror/manifest.json mirror/source.json mirror/blocks.sha256 \
      authority/manifest.json authority/pages.sha256 \
      worker.log interrupt-stop.txt; do
    if [ -f "$path" ] && ! timeout 1200 aws s3 cp "$path" \
        "$V120_OUTPUT_PREFIX/artifacts/$path" --only-show-errors; then
      upload_failed=1
    fi
  done
  if [ "$upload_failed" -ne 0 ]; then code=96; phase=evidence-upload; fi
  ended_epoch=$(date +%s)
  python3 - "$code" "$phase" "$instance_id" "$started_epoch" "$ended_epoch" >terminal.json <<'PY'
import hashlib,json,os,sys
from pathlib import Path
names=('hashes.log','install.log','download.log','download-resources.txt',
       'layout.log','layout-resources.txt','router.log','router-resources.txt',
       'seal.log','seal-resources.txt','compile.log','compile-resources.txt',
       'built/manifest.json','built/layout.npy','built/sq8.bin',
       'router/manifest.json','router/summaries.bin','router/books.bin',
       'router/codes.bin','router/low.bin','router/step.bin',
       'mirror/manifest.json','mirror/source.json','mirror/blocks.sha256',
       'authority/manifest.json','authority/pages.sha256','worker.log',
       'interrupt-stop.txt')
artifacts={}
for name in names:
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for block in iter(lambda: source.read(1024*1024),b''):
                digest.update(block)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(sys.argv[1]);phase=sys.argv[2]
print(json.dumps({'schema':'borsuk-v120-source-index-spot-v1',
    'source_commit':os.environ['V120_SOURCE_COMMIT'],
    'instance_id':sys.argv[3], 'exit_code':code, 'phase':phase,
    'status':'complete' if code==0 and phase=='complete' else 'failed',
    'elapsed_seconds':int(sys.argv[5])-int(sys.argv[4]),
    'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V120_OUTPUT_PREFIX/terminal.json" --only-show-errors
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
dnf install -y -q python3.12 python3.12-pip gcc gcc-c++ cmake perl tar gzip time >install.log 2>&1
python3.12 -m venv .venv >>install.log 2>&1
.venv/bin/pip install -q numpy==1.26.4 pyarrow==17.0.0 >>install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal \
  --default-toolchain 1.98.0 >>install.log 2>&1
export PYTHONPATH="$root/repo"
phase=source-download
run_science download aws s3 cp "$V120_SOURCE_URI" source.parquet --only-show-errors
aws s3 cp "$V120_PROVENANCE_URI" source.json --only-show-errors
[ "$(stat -c%s source.parquet)" = "$V120_SOURCE_BYTES" ] || exit 93
printf '%s  source.parquet\n' "$V120_SOURCE_SHA256" >hashes.log
printf '%s  source.json\n' "$V120_PROVENANCE_SHA256" >>hashes.log
sha256sum -c hashes.log
phase=layout
run_science layout .venv/bin/python -m scripts.v120_source_layout \
  --source source.parquet --provenance source.json --output built \
  --source-sha256 "$V120_SOURCE_SHA256" \
  --provenance-sha256 "$V120_PROVENANCE_SHA256"
read -r layout_sha sq8_sha < <(.venv/bin/python - <<'PY'
import json
value=json.load(open('built/manifest.json'))
print(value['layout_sha256'],value['sq8_sha256'])
PY
)
phase=router
run_science router .venv/bin/python -m scripts.v115_source_router \
  --source source.parquet --layout built/layout.npy --output router \
  --source-sha256 "$V120_SOURCE_SHA256" --layout-sha256 "$layout_sha" \
  --sq8-sha256 "$sq8_sha" --rows 9990000 --dimensions 96 \
  --page-rows 256 --blocks-per-page 2 --generation 1
phase=seal
run_science seal bash -c '
  .venv/bin/python -m scripts.v114_1m_mirror \
    --source source.parquet --sq8 built/sq8.bin --mirror mirror \
    --source-sha256 "$V120_SOURCE_SHA256" --sq8-sha256 "$1" \
    --rows 9990000 --dimensions 96 --max-nominees 512
  .venv/bin/python - "$1" <<"PY"
import sys
from pathlib import Path
from scripts.v115_page_authority import build_page_authority
build_page_authority(
    Path("built/sq8.bin"), Path("authority"),
    expected_object_sha256=sys.argv[1], rows=9990000, dimensions=96,
    page_rows=256, generation=1,
)
PY
' _ "$sq8_sha"
phase=compile
run_science compile "$CARGO_HOME/bin/cargo" test --manifest-path repo/Cargo.toml \
  -p borsuk --lib pq64_ --jobs 4 -- --nocapture
phase=complete
exit 0
