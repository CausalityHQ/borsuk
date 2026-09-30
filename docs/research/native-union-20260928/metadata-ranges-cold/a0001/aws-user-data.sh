#!/bin/bash
set -euo pipefail
systemd-run --unit=native-metadata-ranges-cold-stop --on-active=4200s /usr/sbin/shutdown -h now
root=/mnt/native-metadata-ranges-cold
mkdir -p "$root" && cd "$root"
cat >artifact-roster.json <<'ROSTER'
["test.log","test-resources.txt","run-closed.log","cpu.txt","source-qualification.json","boundary-check.json","boundary-cgroup.json","compiled-source.json","binaries/two_bit_http","object-native.log","generation.log","http.log","source.log","release.log","rustc-version.txt","cargo-version.txt","cpuinfo.txt","arm-feature-tree.txt","x86-feature-tree.txt","profile.log","profile-resources.txt","profile-cgroup.json","screen/summary.json","screen/block0-records.jsonl","screen/block1-records.jsonl","screen/block2-records.jsonl","screen/block3-records.jsonl","compiled-source/crates/borsuk/examples/two_bit_http.rs","compiled-source/crates/borsuk/src/object_native_generation.rs","compiled-source/crates/borsuk/src/two_bit_generation.rs","compiled-source/crates/borsuk/tests/two_bit_generation.rs","compiled-source/Cargo.toml","compiled-source/Cargo.lock","compiled-source/crates/borsuk/Cargo.toml","compiled-source/crates/borsuk/tests/two_bit_source.rs","control/binaries/two_bit_http","control/boundary-check.json","control/compiled-source.json","control/source-qualification.json","control/rustc-version.txt","control/cargo-version.txt","control/cpuinfo.txt","control/arm-feature-tree.txt","control/x86-feature-tree.txt","control/run-closed.log","control/compiled-source/crates/borsuk/examples/two_bit_http.rs","control/compiled-source/crates/borsuk/src/object_native_generation.rs","control/compiled-source/crates/borsuk/src/two_bit_generation.rs","control/compiled-source/crates/borsuk/tests/two_bit_generation.rs","control/compiled-source/Cargo.toml","control/compiled-source/Cargo.lock","control/compiled-source/crates/borsuk/Cargo.toml","control/compiled-source/crates/borsuk/tests/two_bit_source.rs"]
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
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260930/metadata-ranges-cold-a0001/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-metadata-ranges-cold-spot-v1','source_commit':'0525dcb826e740411c115e8ece4ffb82a9ee3394',
  'source_archive_sha256':'924e396904fc1d1deae9dba45c77f21eec103c98b4c1d3f38951543944da5485',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260930/metadata-ranges-cold-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/924e396904fc1d1deae9dba45c77f21eec103c98b4c1d3f38951543944da5485.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '924e396904fc1d1deae9dba45c77f21eec103c98b4c1d3f38951543944da5485' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time python3-devel python3.12 pkgconf-pkg-config
python3.12 -m ensurepip
python3.12 -m pip install -q boto3
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal   --default-toolchain 1.98.0
cat >source-qualification.json <<'QUALIFICATION'
{"artifact_roster_sha256":"4a98c384f73403d29695030eb5daa4bb1c456f4c699ba06835fbe7d394bd398a","campaign_schema":"borsuk-native-metadata-ranges-cold-spot-v1","code_sha256":{"scripts/check_native_metadata_ranges_build.py":"da48c53562220026c8024ba2400dca2ba897aa12d49c3e27c3c2fb40b9f0770e","scripts/check_native_metadata_ranges_stats.py":"4dfe0722756ceb46e54ee9f849e47724b5597976c7bbafa793c9add2852399e9","scripts/check_native_startup_stats.py":"56b4989f5be09f5fdfee424862c32372e42762a7a0b7b8290e6f4b35c3354a29","scripts/launch_native_metadata_ranges_cold_spot.py":"adca2868b44b9f471b1acfce7d0ebc674b4dc7bc90c3099c4bda279354fed0dd","scripts/rest_coexistence_load.py":"04c0eed4709d569509ad89012754fbfe996170b2ee6f49388790357af6a70fdd","scripts/run_native_cold_first_query.py":"563a1a1923ba4294984ce278e4c79b7ae614c8d6d00e5012b9d4c158ef306881","scripts/run_native_metadata_ranges_cold.py":"8a61093569f2e70c7bcd00988b3a350e19408cdcecbbecbd360f4b384bd805cb","scripts/run_native_peer_1m_worker.py":"8c84ed799fc0a2e00e991bfcf8ac81102dede8c9a505ac55e5c220d09cad93d3","scripts/run_native_peer_offered_http.py":"7c86bab8c78a75ef42b20f54c0e91ab5e247e0b521f47de06f89b7b6fdc4610b","scripts/run_native_union_http.py":"886f84ea639b822668a848e8f085027aec12818e316d0839fbf607844d18f880","scripts/run_native_union_offered_http.py":"8a2c5993d282c3f67d841544828793989550157d3ac70091a18a26f2074517d6"},"compiled_native_sha256":{"Cargo.lock":"92c0da6805ad13fa78a7d6cbffaccf7738e346d3605ed8c8ac763cf5079f2590","Cargo.toml":"1ec9ac514515662a0f3a371b1a65d72bf825d67f218dc3f6cc1e5fb0925547d6","crates/borsuk/Cargo.toml":"474efdef5bd398bef758009882c8bdd859553de8f0271230430469dd96bc4d71","crates/borsuk/examples/two_bit_http.rs":"c8f2327561304a088b0bfd3a4509047a94c05210b8d5d44e008b90faea3cdc29","crates/borsuk/src/object_native_generation.rs":"14b2b51cf97449b4e23e343049c0896ec94f731408a45e5798b79f720631cef6","crates/borsuk/src/two_bit_generation.rs":"7a134fe3e03e735bad63c611ce2b78d865345c8617a6a4b1c5dd70ce1384cc45","crates/borsuk/tests/two_bit_generation.rs":"da29358a0c797f2ad232a3efd033d7c8a7eb370dd43d44fbb37264885cbdf0f6","crates/borsuk/tests/two_bit_source.rs":"d341033f45c614da69b70c3f3c4b1ce86ebaf1f26ba59da03a54e926d00a98d3"},"config_path":"docs/research/native-union-20260928/metadata-ranges-config.json","config_sha256":"e1fed6a3648bba6dfb8307a24bee083476b8d69166f10b9b41ca679e91fc01da","control_compiled_native_sha256":{"Cargo.lock":"92c0da6805ad13fa78a7d6cbffaccf7738e346d3605ed8c8ac763cf5079f2590","Cargo.toml":"1ec9ac514515662a0f3a371b1a65d72bf825d67f218dc3f6cc1e5fb0925547d6","crates/borsuk/Cargo.toml":"474efdef5bd398bef758009882c8bdd859553de8f0271230430469dd96bc4d71","crates/borsuk/examples/two_bit_http.rs":"c8f2327561304a088b0bfd3a4509047a94c05210b8d5d44e008b90faea3cdc29","crates/borsuk/src/object_native_generation.rs":"c235938f22b8cfaee18edd631ff26e64a16d4f73e179057ba1d3f1e0a9060920","crates/borsuk/src/two_bit_generation.rs":"7a134fe3e03e735bad63c611ce2b78d865345c8617a6a4b1c5dd70ce1384cc45","crates/borsuk/tests/two_bit_generation.rs":"da29358a0c797f2ad232a3efd033d7c8a7eb370dd43d44fbb37264885cbdf0f6","crates/borsuk/tests/two_bit_source.rs":"d341033f45c614da69b70c3f3c4b1ce86ebaf1f26ba59da03a54e926d00a98d3"},"control_source_commit":"d32d472293d7300be77eb5f1e87cf69892cc504b","control_source_identity_sha256":"c2151132c6df9000f74181a0e52e0131d148f71653c450d2c4f752ac91f8580b","current_full_suite_pass_claim":false,"frozen_binary_artifacts":{"arm-feature-tree.txt":{"bytes":47667,"sha256":"39e9f2e067c7a74759b42c1a59e728b7149a892b9c0ea6994b3007319ca005a5"},"binaries/two_bit_http":{"bytes":12561552,"sha256":"3565d27ffbe7a88e2a4c6d0c982ef795072d944e1dab8f11720a9bf9b31556af"},"boundary-check.json":{"bytes":2524,"sha256":"88bdfc6e81aef3bd1920ea9c867c57f1fbab7d6a9549caffdf24c8730a13658c"},"cargo-version.txt":{"bytes":36,"sha256":"fb3f25aefd6d5441842d285f5a9b125b3f0f0b0bf2013d9a32677f9e99c6c854"},"compiled-source.json":{"bytes":839,"sha256":"f96c82a24dd0efc26a46dfa632b7861db288bec82224f4ab6d00a8ad282c19b1"},"compiled-source/Cargo.lock":{"bytes":90530,"sha256":"92c0da6805ad13fa78a7d6cbffaccf7738e346d3605ed8c8ac763cf5079f2590"},"compiled-source/Cargo.toml":{"bytes":621,"sha256":"1ec9ac514515662a0f3a371b1a65d72bf825d67f218dc3f6cc1e5fb0925547d6"},"compiled-source/crates/borsuk/Cargo.toml":{"bytes":2640,"sha256":"474efdef5bd398bef758009882c8bdd859553de8f0271230430469dd96bc4d71"},"compiled-source/crates/borsuk/examples/two_bit_http.rs":{"bytes":9731,"sha256":"c8f2327561304a088b0bfd3a4509047a94c05210b8d5d44e008b90faea3cdc29"},"compiled-source/crates/borsuk/src/object_native_generation.rs":{"bytes":36148,"sha256":"c235938f22b8cfaee18edd631ff26e64a16d4f73e179057ba1d3f1e0a9060920"},"compiled-source/crates/borsuk/src/two_bit_generation.rs":{"bytes":38553,"sha256":"7a134fe3e03e735bad63c611ce2b78d865345c8617a6a4b1c5dd70ce1384cc45"},"compiled-source/crates/borsuk/tests/two_bit_generation.rs":{"bytes":32488,"sha256":"da29358a0c797f2ad232a3efd033d7c8a7eb370dd43d44fbb37264885cbdf0f6"},"compiled-source/crates/borsuk/tests/two_bit_source.rs":{"bytes":7871,"sha256":"d341033f45c614da69b70c3f3c4b1ce86ebaf1f26ba59da03a54e926d00a98d3"},"cpuinfo.txt":{"bytes":2856,"sha256":"91561b86cc166f6c939619e5bcb5b755b52e874a5284639f17179ec51278646d"},"run-closed.log":{"bytes":18254,"sha256":"e84c8d29411242bace954fa0ddccfc5ba54f4d7bd38ee648e9432bee1244c8b8"},"rustc-version.txt":{"bytes":197,"sha256":"ec378196e9fa222ab807fd49cb9dae156a9bd18069b9440fd4e0a6c57c6971cb"},"source-qualification.json":{"bytes":6185,"sha256":"3a2bc2f07e50e4b8dfb3994bc63fa6fe5254d5ae92c6036aae14fdd2006cd87b"},"x86-feature-tree.txt":{"bytes":47591,"sha256":"2e310f57ef44e75e9a79d4ac61d74354c3300130126627103db8895d03bfe8b2"}},"frozen_native_qualification":{"path":"docs/research/native-union-20260928/arm-sha-startup/a0001/verification.json","sha256":"b98e17328df1d83b583f06b4d1f0f36a77a159893bb41ff25c8accc6724a7e58","source_archive_sha256":"a37b33402a82bf9653ed6542b7a523fd46d64307d449ff3d575d4c572e751ace","source_commit":"d32d472293d7300be77eb5f1e87cf69892cc504b","terminal_sha256":"4e898e7a59a5a044112c805ae8c0ef8ca51d3183e8a6ad32330e3da580fcd6be"},"historical_assurance_scope":"2696 prior tests only; changed dependency not fully requalified","native_rebuilt":true,"reviewed_native_delta_sha256":{"crates/borsuk/src/object_native_generation.rs":"14b2b51cf97449b4e23e343049c0896ec94f731408a45e5798b79f720631cef6"},"source_file_count":395,"source_identity_sha256":"1720c277b493c1097f68aa89ea44c8555bf5222bf0de69de28144f900fdee969"}
QUALIFICATION
phase=binary-qualification
lscpu >cpu.txt
systemd-run --unit=metadata-ranges-build --wait --pipe -p MemoryMax=10G -p MemorySwapMax=0 -p RuntimeMaxSec=2430 \
 --setenv=PYTHONPATH="$root/repo" --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$CARGO_HOME/bin:$PATH" \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 2400 \
 taskset -c 0-3 python3.12 "$root/repo/scripts/check_native_metadata_ranges_build.py" "$CARGO_HOME/bin/cargo" "$root/repo" "$root" >test.log 2>&1
test -s boundary-check.json && test -s binaries/two_bit_http
phase=profile
systemd-run --unit=native-metadata-ranges-cold --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=1530 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 1500 \
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 scripts/run_native_metadata_ranges_cold.py docs/research/native-union-20260928/metadata-ranges-config.json e1fed6a3648bba6dfb8307a24bee083476b8d69166f10b9b41ca679e91fc01da "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/control/binaries/two_bit_http" "$1/control/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 scripts/check_native_startup_build.py --cgroup "$1/profile-cgroup.json" || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
test -s "$root/screen/summary.json"
test -s "$root/screen/block0-records.jsonl"
test -s "$root/screen/block1-records.jsonl"
test -s "$root/screen/block2-records.jsonl"
test -s "$root/screen/block3-records.jsonl"
phase=complete
