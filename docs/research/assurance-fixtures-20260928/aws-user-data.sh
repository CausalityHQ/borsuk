#!/bin/bash
set -euo pipefail
systemd-run --unit=native-callable-gc-check-stop --on-active=1800s /usr/sbin/shutdown -h now
root=/mnt/native-callable-gc-check
mkdir -p "$root" && cd "$root"
phase=bootstrap
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  cp run.log run-closed.log || code=96
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  for name in test.log test-resources.txt run-closed.log red-test.log red-exit.txt; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-library-check/assurance-fixtures/c57a294917cd3d4bbf4d4a89e5b6f219f2b57f0a2bb473785c44a9572e22a2a7/a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'red-test.log', 'red-exit.txt'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-library-spot-check-v1','source_base_commit':'29e578badbb9d62314b71c7b92aa241e5babcc77',
  'source_archive_sha256':'c57a294917cd3d4bbf4d4a89e5b6f219f2b57f0a2bb473785c44a9572e22a2a7',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-library-check/assurance-fixtures/c57a294917cd3d4bbf4d4a89e5b6f219f2b57f0a2bb473785c44a9572e22a2a7/a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/c57a294917cd3d4bbf4d4a89e5b6f219f2b57f0a2bb473785c44a9572e22a2a7.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'c57a294917cd3d4bbf4d4a89e5b6f219f2b57f0a2bb473785c44a9572e22a2a7' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time python3-devel pkgconf-pkg-config
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal   --default-toolchain 1.98.0
phase=test
export BORSUK_NATIVE_TEST_BUCKET='borsuk-bench-453182569524-euc1' BORSUK_NATIVE_TEST_REGION='eu-central-1' BORSUK_NATIVE_TEST_PREFIX='research/native-library-check/gc/c57a294917cd3d4bbf4d4a89e5b6f219f2b57f0a2bb473785c44a9572e22a2a7/review-a0003/smoke'
/usr/bin/time -v -o test-resources.txt timeout --signal=TERM --kill-after=30 1200 \
  bash -c 'set -e; for case in format::tests::manifest_from_parquet_rejects_invalid native_ann_build::tests::native_bounded_build_is_query_blind_deterministic_and_reopens_sq8 v36_funnel_geometry::tests::v36_resident_posting_diagnostic_trains_authenticated_runs_but_assigns_globally v37_relation_router::tests::v37_relation_local_build_rejects_worker_and_backend_authority_drift; do "$1" test --locked --manifest-path repo/Cargo.toml -p borsuk --lib "$case" --jobs 4; done' _ "$CARGO_HOME/bin/cargo" >test.log 2>&1
phase=complete
