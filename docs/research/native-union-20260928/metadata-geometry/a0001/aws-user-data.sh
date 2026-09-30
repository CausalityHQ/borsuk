#!/bin/bash
set -euo pipefail
systemd-run --unit=native-metadata-geometry-stop --on-active=4200s /usr/sbin/shutdown -h now
root=/mnt/native-metadata-geometry
mkdir -p "$root" && cd "$root"
cat >artifact-roster.json <<'ROSTER'
["test.log","test-resources.txt","run-closed.log","cpu.txt","source-qualification.json","rustc-version.txt","cargo-version.txt","cpuinfo.txt","arm-feature-tree.txt","x86-feature-tree.txt","boundary-cgroup.json","profile.log","profile-resources.txt","profile-cgroup.json","binaries/two_bit_http","boundary-check.json","compiled-source.json","compiled-source/crates/borsuk/examples/two_bit_http.rs","compiled-source/crates/borsuk/src/object_native_generation.rs","compiled-source/crates/borsuk/src/two_bit_generation.rs","compiled-source/crates/borsuk/tests/two_bit_generation.rs","compiled-source/Cargo.toml","compiled-source/Cargo.lock","compiled-source/crates/borsuk/Cargo.toml","compiled-source/crates/borsuk/tests/two_bit_source.rs","compiled-source/crates/borsuk/src/unit_centroid_graph.rs","object-native.log","generation.log","http.log","source.log","graph.log","release.log","control/binaries/two_bit_http","control/boundary-check.json","control/compiled-source.json","control/compiled-source/crates/borsuk/examples/two_bit_http.rs","control/compiled-source/crates/borsuk/src/object_native_generation.rs","control/compiled-source/crates/borsuk/src/two_bit_generation.rs","control/compiled-source/crates/borsuk/tests/two_bit_generation.rs","control/compiled-source/Cargo.toml","control/compiled-source/Cargo.lock","control/compiled-source/crates/borsuk/Cargo.toml","control/compiled-source/crates/borsuk/tests/two_bit_source.rs","control/compiled-source/crates/borsuk/src/unit_centroid_graph.rs","control/object-native.log","control/generation.log","control/http.log","control/source.log","control/graph.log","control/release.log","control/rustc-version.txt","control/cargo-version.txt","control/cpuinfo.txt","control/arm-feature-tree.txt","control/x86-feature-tree.txt","screen/summary.json","screen/block0-records.jsonl","screen/block1-records.jsonl","screen/block2-records.jsonl","screen/block3-records.jsonl"]
ROSTER
phase=bootstrap
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  cp run.log run-closed.log || code=96
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  for name in $(python3 -c 'import json; print(" ".join(json.load(open("artifact-roster.json"))))'); do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260930/metadata-geometry-a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in json.loads(Path("artifact-roster.json").read_text()):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-metadata-geometry-spot-v1','source_commit':'a5c0cceef4f876c6cc987a311ae7e5c59ce4686c',
  'source_archive_sha256':'8fed38f6d99c117255d5a52d661d338d705cf160e174d1c386866987628e58b0',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260930/metadata-geometry-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/8fed38f6d99c117255d5a52d661d338d705cf160e174d1c386866987628e58b0.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '8fed38f6d99c117255d5a52d661d338d705cf160e174d1c386866987628e58b0' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time python3-devel python3.12 pkgconf-pkg-config
python3.12 -m ensurepip
python3.12 -m pip install -q boto3
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal   --default-toolchain 1.98.0
cat >source-qualification.json <<'QUALIFICATION'
{"artifact_roster_sha256":"4a35b3846e131d2c76e4cb291609a4a3793732937e007c828cb6c589df6aedde","campaign_schema":"borsuk-native-metadata-geometry-spot-v1","code_sha256":{"scripts/check_native_metadata_geometry_build.py":"446d1c4b2508fa6f330a04ec2d91ec6189aaebd5cd69bfb0e750c8a852a1689f","scripts/check_native_metadata_ranges_stats.py":"3c17d3202d7d67247d6502428f17a8e981414f58261007ad6fcf794367aea8f3","scripts/check_native_startup_stats.py":"56b4989f5be09f5fdfee424862c32372e42762a7a0b7b8290e6f4b35c3354a29","scripts/launch_native_metadata_geometry_spot.py":"b9aa299b7eedb3fd460b1f55ce4aa345c06b1a714c832dbdb952b46fd44c8e7c","scripts/rest_coexistence_load.py":"04c0eed4709d569509ad89012754fbfe996170b2ee6f49388790357af6a70fdd","scripts/run_native_cold_first_query.py":"9ced034924594c7443b5e5a435da22e8f2204151ba6ecc7869ff0ef5b87b0c42","scripts/run_native_metadata_ranges_cold.py":"1322fe19b705caeab275bce69ea6b4a9f2ed84cb6505dcbfe01b25301fdb71b8","scripts/run_native_peer_1m_worker.py":"8c84ed799fc0a2e00e991bfcf8ac81102dede8c9a505ac55e5c220d09cad93d3","scripts/run_native_peer_offered_http.py":"7c86bab8c78a75ef42b20f54c0e91ab5e247e0b521f47de06f89b7b6fdc4610b","scripts/run_native_union_http.py":"886f84ea639b822668a848e8f085027aec12818e316d0839fbf607844d18f880","scripts/run_native_union_offered_http.py":"8a2c5993d282c3f67d841544828793989550157d3ac70091a18a26f2074517d6"},"compiled_native_sha256":{"Cargo.lock":"92c0da6805ad13fa78a7d6cbffaccf7738e346d3605ed8c8ac763cf5079f2590","Cargo.toml":"1ec9ac514515662a0f3a371b1a65d72bf825d67f218dc3f6cc1e5fb0925547d6","crates/borsuk/Cargo.toml":"474efdef5bd398bef758009882c8bdd859553de8f0271230430469dd96bc4d71","crates/borsuk/examples/two_bit_http.rs":"c8f2327561304a088b0bfd3a4509047a94c05210b8d5d44e008b90faea3cdc29","crates/borsuk/src/object_native_generation.rs":"36dc8f040f862b329164be2cb58151270518f5c659bfbb317f77f1e8855f579c","crates/borsuk/src/two_bit_generation.rs":"7a134fe3e03e735bad63c611ce2b78d865345c8617a6a4b1c5dd70ce1384cc45","crates/borsuk/src/unit_centroid_graph.rs":"7a47654900e1ec2d5f96384ee2b67ea4c647c2eb05c00a0e4d5902636bcebc42","crates/borsuk/tests/two_bit_generation.rs":"da29358a0c797f2ad232a3efd033d7c8a7eb370dd43d44fbb37264885cbdf0f6","crates/borsuk/tests/two_bit_source.rs":"d341033f45c614da69b70c3f3c4b1ce86ebaf1f26ba59da03a54e926d00a98d3"},"config_path":"docs/research/native-union-20260928/metadata-geometry-config.json","config_sha256":"410ce351d764bccccafcfe6e34697dcc961856288c67f8519c451fa4266143c3","control_compiled_native_sha256":{"Cargo.lock":"92c0da6805ad13fa78a7d6cbffaccf7738e346d3605ed8c8ac763cf5079f2590","Cargo.toml":"1ec9ac514515662a0f3a371b1a65d72bf825d67f218dc3f6cc1e5fb0925547d6","crates/borsuk/Cargo.toml":"474efdef5bd398bef758009882c8bdd859553de8f0271230430469dd96bc4d71","crates/borsuk/examples/two_bit_http.rs":"c8f2327561304a088b0bfd3a4509047a94c05210b8d5d44e008b90faea3cdc29","crates/borsuk/src/object_native_generation.rs":"14b2b51cf97449b4e23e343049c0896ec94f731408a45e5798b79f720631cef6","crates/borsuk/src/two_bit_generation.rs":"7a134fe3e03e735bad63c611ce2b78d865345c8617a6a4b1c5dd70ce1384cc45","crates/borsuk/src/unit_centroid_graph.rs":"7a47654900e1ec2d5f96384ee2b67ea4c647c2eb05c00a0e4d5902636bcebc42","crates/borsuk/tests/two_bit_generation.rs":"da29358a0c797f2ad232a3efd033d7c8a7eb370dd43d44fbb37264885cbdf0f6","crates/borsuk/tests/two_bit_source.rs":"d341033f45c614da69b70c3f3c4b1ce86ebaf1f26ba59da03a54e926d00a98d3"},"control_native_rebuilt":true,"control_source_commit":"1224634bfb121eef533789a422f1c06fd7adf142","control_source_identity_sha256":"6566a30c7ccfbf5ec8a8d4481b2568fc020b67e831e5d186a94f5025ab156ec2","control_stage":{"path":"docs/research/native-union-20260928/metadata-geometry-control-stage.txt","sha256":"14b2b51cf97449b4e23e343049c0896ec94f731408a45e5798b79f720631cef6"},"current_full_suite_pass_claim":false,"native_rebuilt":true,"source_file_count":395,"source_identity_sha256":"4bbc6c332e78e5a068001913597cdc82d6c13bd147acd96e019149c190a39a1c"}
QUALIFICATION
phase=binary-qualification
lscpu >cpu.txt
systemd-run --unit=metadata-geometry-build --wait --pipe -p MemoryMax=10G -p MemorySwapMax=0 -p RuntimeMaxSec=2430 \
 --setenv=PYTHONPATH="$root/repo" --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$CARGO_HOME/bin:$PATH" \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 2400 \
 taskset -c 0-3 python3.12 "$root/repo/scripts/check_native_metadata_geometry_build.py" "$CARGO_HOME/bin/cargo" "$root/repo" "$root" >test.log 2>&1
test -s boundary-check.json && test -s binaries/two_bit_http
phase=profile
systemd-run --unit=native-metadata-geometry --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=1530 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 1500 \
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 scripts/run_native_metadata_ranges_cold.py docs/research/native-union-20260928/metadata-geometry-config.json 410ce351d764bccccafcfe6e34697dcc961856288c67f8519c451fa4266143c3 "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/control/binaries/two_bit_http" "$1/control/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 scripts/check_native_startup_build.py --cgroup "$1/profile-cgroup.json" || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
test -s "$root/screen/summary.json"
test -s "$root/screen/block0-records.jsonl"
test -s "$root/screen/block1-records.jsonl"
test -s "$root/screen/block2-records.jsonl"
test -s "$root/screen/block3-records.jsonl"
phase=complete
