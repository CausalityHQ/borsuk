#!/bin/bash
set -euo pipefail
systemd-run --unit=native-startup-profile-stop --on-active=3600s /usr/sbin/shutdown -h now
root=/mnt/native-startup-profile
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
  for name in test.log test-resources.txt run-closed.log cpu.txt object-native.log generation.log http.log release.log boundary-check.json boundary-cgroup.json compiled-source.json source-qualification.json binaries/two_bit_http profile.log profile-resources.txt profile-cgroup.json screen/summary.json compiled-source/crates/borsuk/examples/two_bit_http.rs compiled-source/crates/borsuk/src/object_native_generation.rs compiled-source/crates/borsuk/src/two_bit_generation.rs compiled-source/crates/borsuk/tests/two_bit_generation.rs screen/cell0-server.log screen/cell0-server.time screen/cell0-close.json screen/cell0-profile.json screen/cell1-server.log screen/cell1-server.time screen/cell1-close.json screen/cell1-profile.json screen/cell2-server.log screen/cell2-server.time screen/cell2-close.json screen/cell2-profile.json screen/cell3-server.log screen/cell3-server.time screen/cell3-close.json screen/cell3-profile.json screen/cell4-server.log screen/cell4-server.time screen/cell4-close.json screen/cell4-profile.json screen/cell5-server.log screen/cell5-server.time screen/cell5-close.json screen/cell5-profile.json; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260929/startup-profile-a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt', 'object-native.log', 'generation.log', 'http.log', 'release.log', 'boundary-check.json', 'boundary-cgroup.json', 'compiled-source.json', 'source-qualification.json', 'binaries/two_bit_http', 'profile.log', 'profile-resources.txt', 'profile-cgroup.json', 'screen/summary.json', 'compiled-source/crates/borsuk/examples/two_bit_http.rs', 'compiled-source/crates/borsuk/src/object_native_generation.rs', 'compiled-source/crates/borsuk/src/two_bit_generation.rs', 'compiled-source/crates/borsuk/tests/two_bit_generation.rs', 'screen/cell0-server.log', 'screen/cell0-server.time', 'screen/cell0-close.json', 'screen/cell0-profile.json', 'screen/cell1-server.log', 'screen/cell1-server.time', 'screen/cell1-close.json', 'screen/cell1-profile.json', 'screen/cell2-server.log', 'screen/cell2-server.time', 'screen/cell2-close.json', 'screen/cell2-profile.json', 'screen/cell3-server.log', 'screen/cell3-server.time', 'screen/cell3-close.json', 'screen/cell3-profile.json', 'screen/cell4-server.log', 'screen/cell4-server.time', 'screen/cell4-close.json', 'screen/cell4-profile.json', 'screen/cell5-server.log', 'screen/cell5-server.time', 'screen/cell5-close.json', 'screen/cell5-profile.json'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-startup-profile-spot-v1','source_commit':'217a33dfc6b63d74169a793c3c49ccd9912e4419',
  'source_archive_sha256':'4a8437ff664adbbd4cf8fb6e09382ac7dc923f886615a8a0be6e7990ab6cf6c0',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260929/startup-profile-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/4a8437ff664adbbd4cf8fb6e09382ac7dc923f886615a8a0be6e7990ab6cf6c0.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '4a8437ff664adbbd4cf8fb6e09382ac7dc923f886615a8a0be6e7990ab6cf6c0' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time python3-devel python3.12 pkgconf-pkg-config
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal   --default-toolchain 1.98.0
cat >source-qualification.json <<'QUALIFICATION'
{"changed_runtime_files": ["crates/borsuk/examples/two_bit_http.rs", "crates/borsuk/src/object_native_generation.rs", "crates/borsuk/src/two_bit_generation.rs"], "changed_test_files": ["crates/borsuk/tests/two_bit_generation.rs"], "code_sha256": {"scripts/check_native_startup_build.py": "0a045164e74d0099f394d0ce1bc5e3bbeb248c70311ac3617f9861c94c2e3af8", "scripts/check_native_startup_stats.py": "56b4989f5be09f5fdfee424862c32372e42762a7a0b7b8290e6f4b35c3354a29", "scripts/launch_native_startup_profile_spot.py": "ea8f55d3af70810be3a6d09104cc3a8d32b2fe4b3079279ad5bb22087f256d9e", "scripts/rest_coexistence_load.py": "04c0eed4709d569509ad89012754fbfe996170b2ee6f49388790357af6a70fdd", "scripts/run_native_peer_1m_worker.py": "8c84ed799fc0a2e00e991bfcf8ac81102dede8c9a505ac55e5c220d09cad93d3", "scripts/run_native_peer_offered_http.py": "7c86bab8c78a75ef42b20f54c0e91ab5e247e0b521f47de06f89b7b6fdc4610b", "scripts/run_native_startup_profile.py": "4fbfdf65558abe92a918bd854b25283e628c7270e2405b7067f0b199bccfd14e", "scripts/run_native_union_http.py": "886f84ea639b822668a848e8f085027aec12818e316d0839fbf607844d18f880", "scripts/run_native_union_offered_http.py": "8a2c5993d282c3f67d841544828793989550157d3ac70091a18a26f2074517d6"}, "compiled_native_sha256": {"crates/borsuk/examples/two_bit_http.rs": "c8f2327561304a088b0bfd3a4509047a94c05210b8d5d44e008b90faea3cdc29", "crates/borsuk/src/object_native_generation.rs": "c235938f22b8cfaee18edd631ff26e64a16d4f73e179057ba1d3f1e0a9060920", "crates/borsuk/src/two_bit_generation.rs": "7a134fe3e03e735bad63c611ce2b78d865345c8617a6a4b1c5dd70ce1384cc45", "crates/borsuk/tests/two_bit_generation.rs": "da29358a0c797f2ad232a3efd033d7c8a7eb370dd43d44fbb37264885cbdf0f6"}, "config_sha256": "8f002b7888dd45c36eabdadb0e923fe98cb258cf700756c778099ad3cf5658c3", "current_full_suite_pass_claim": false, "items": [{"authority": {"control_epoch": 1, "generation": 1, "root_sha256": "d8e7ccf090f322bbb39600f096e8d64b7dc15ed8f5d7dfd6344b1c076dc99cce"}, "closed_dev_verification_path": "docs/research/native-union-20260928/fresh-rank16-dev64/a0002/verification.json", "closed_dev_verification_sha256": "56e2421ce84f9ca3cf045a77f440aaaf02f766c06412631fbb200666c1bbbc3d", "dataset": "ReLAION", "index": "research/native-union/20260928/fresh-rank16-dev64-a0002/indexes/relaion/k10", "metadata_files": {"centroids.bin": 48000032, "diverse_graph.bin": 4265768, "graph.bin": 4265768, "manifest.json": 33910, "page_digests.bin": 125024, "page_manifest.json": 280, "plane/manifest.json": 549, "plane/mean.bin": 3072, "plane/records.bin": 200000000}}, {"authority": {"control_epoch": 1, "generation": 1, "root_sha256": "a4eb4851c545e828f3d08181a0c3e9cca09ed9341032ee3e69c511b9b7caf67e"}, "closed_dev_verification_path": "docs/research/native-union-20260928/fresh-cohere-dev64/a0002/verification.json", "closed_dev_verification_sha256": "fb2cf8d616618f0fb33a42340358c2704ed30751557fbb16c23fda0964dd7912", "dataset": "CoHere", "index": "research/native-union/20260929/fresh-cohere-dev64-a0002/indexes/cohere/k10", "metadata_files": {"centroids.bin": 48000032, "diverse_graph.bin": 4265768, "graph.bin": 4265768, "manifest.json": 33963, "page_digests.bin": 125024, "page_manifest.json": 280, "plane/manifest.json": 549, "plane/mean.bin": 3072, "plane/records.bin": 200000000}}], "prior_assurance_sha256": "5322570dd1d26a8c1f3cb43cafa74bc965e11cc163260105a694426b26584f6f", "prior_unaffected_assurance_tests": 2696}
QUALIFICATION
phase=example-qualification
lscpu >cpu.txt
systemd-run --unit=startup-profile --wait --pipe -p MemoryMax=10G -p MemorySwapMax=0 -p RuntimeMaxSec=2430 \
 --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$CARGO_HOME/bin:$PATH" \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 2400 \
 taskset -c 0-3 python3 "$root/repo/scripts/check_native_startup_build.py" "$CARGO_HOME/bin/cargo" "$root/repo" "$root" >test.log 2>&1
test -s boundary-check.json && test -s binaries/two_bit_http
phase=profile
systemd-run --unit=native-startup-profile --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=630 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 600 \
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 0-3 python3.12 scripts/run_native_startup_profile.py docs/research/native-union-20260928/startup-profile-config.json 8f002b7888dd45c36eabdadb0e923fe98cb258cf700756c778099ad3cf5658c3 "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 scripts/check_native_startup_build.py --cgroup "$1/profile-cgroup.json" || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
test -s "$root/screen/summary.json"
phase=complete
