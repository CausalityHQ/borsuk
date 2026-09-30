#!/bin/bash
set -euo pipefail
systemd-run --unit=native-cold-first-query-stop --on-active=1800s /usr/sbin/shutdown -h now
root=/mnt/native-cold-first-query
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
  for name in test.log test-resources.txt run-closed.log cpu.txt source-qualification.json boundary-check.json compiled-source.json binaries/two_bit_http profile.log profile-resources.txt profile-cgroup.json screen/summary.json screen/relaion-records.jsonl screen/cohere-records.jsonl; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260929/cold-first-query-a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt', 'source-qualification.json', 'boundary-check.json', 'compiled-source.json', 'binaries/two_bit_http', 'profile.log', 'profile-resources.txt', 'profile-cgroup.json', 'screen/summary.json', 'screen/relaion-records.jsonl', 'screen/cohere-records.jsonl'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-cold-first-query-spot-v1','source_commit':'2ea57d8f95d1808f3750a68b60561dc333773081',
  'source_archive_sha256':'38267b760cd6bf165cda31d4df2881594df245bbec3096213f248a3ce7e1a4c2',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260929/cold-first-query-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/38267b760cd6bf165cda31d4df2881594df245bbec3096213f248a3ce7e1a4c2.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '38267b760cd6bf165cda31d4df2881594df245bbec3096213f248a3ce7e1a4c2' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q tar gzip time python3.12
python3.12 -m ensurepip
python3.12 -m pip install -q boto3
cat >source-qualification.json <<'QUALIFICATION'
{"code_sha256":{"scripts/check_native_startup_build.py":"e8d85e6a59f3f6d0d551d3772febbf9a930476a866078da4c846690fbfd83824","scripts/check_native_startup_stats.py":"56b4989f5be09f5fdfee424862c32372e42762a7a0b7b8290e6f4b35c3354a29","scripts/launch_native_cold_first_query_spot.py":"be3d4c85c88bef6396db5c0e47914c22741b4fcdbf465df03e9f30e71c131849","scripts/rest_coexistence_load.py":"04c0eed4709d569509ad89012754fbfe996170b2ee6f49388790357af6a70fdd","scripts/run_native_cold_first_query.py":"563a1a1923ba4294984ce278e4c79b7ae614c8d6d00e5012b9d4c158ef306881","scripts/run_native_peer_1m_worker.py":"8c84ed799fc0a2e00e991bfcf8ac81102dede8c9a505ac55e5c220d09cad93d3","scripts/run_native_peer_offered_http.py":"7c86bab8c78a75ef42b20f54c0e91ab5e247e0b521f47de06f89b7b6fdc4610b","scripts/run_native_union_http.py":"886f84ea639b822668a848e8f085027aec12818e316d0839fbf607844d18f880","scripts/run_native_union_offered_http.py":"8a2c5993d282c3f67d841544828793989550157d3ac70091a18a26f2074517d6"},"compiled_native_sha256":{"Cargo.lock":"92c0da6805ad13fa78a7d6cbffaccf7738e346d3605ed8c8ac763cf5079f2590","Cargo.toml":"1ec9ac514515662a0f3a371b1a65d72bf825d67f218dc3f6cc1e5fb0925547d6","crates/borsuk/Cargo.toml":"474efdef5bd398bef758009882c8bdd859553de8f0271230430469dd96bc4d71","crates/borsuk/examples/two_bit_http.rs":"c8f2327561304a088b0bfd3a4509047a94c05210b8d5d44e008b90faea3cdc29","crates/borsuk/src/object_native_generation.rs":"c235938f22b8cfaee18edd631ff26e64a16d4f73e179057ba1d3f1e0a9060920","crates/borsuk/src/two_bit_generation.rs":"7a134fe3e03e735bad63c611ce2b78d865345c8617a6a4b1c5dd70ce1384cc45","crates/borsuk/tests/two_bit_generation.rs":"da29358a0c797f2ad232a3efd033d7c8a7eb370dd43d44fbb37264885cbdf0f6","crates/borsuk/tests/two_bit_source.rs":"d341033f45c614da69b70c3f3c4b1ce86ebaf1f26ba59da03a54e926d00a98d3"},"config_sha256":"78ea8ea182432ad8dc38a5b879b97229796232cb6f21d30a40b204eec38c26a1","current_full_suite_pass_claim":false,"frozen_binary_artifacts":{"arm-feature-tree.txt":{"bytes":47667,"sha256":"39e9f2e067c7a74759b42c1a59e728b7149a892b9c0ea6994b3007319ca005a5"},"binaries/two_bit_http":{"bytes":12561552,"sha256":"3565d27ffbe7a88e2a4c6d0c982ef795072d944e1dab8f11720a9bf9b31556af"},"boundary-check.json":{"bytes":2524,"sha256":"88bdfc6e81aef3bd1920ea9c867c57f1fbab7d6a9549caffdf24c8730a13658c"},"cargo-version.txt":{"bytes":36,"sha256":"fb3f25aefd6d5441842d285f5a9b125b3f0f0b0bf2013d9a32677f9e99c6c854"},"compiled-source.json":{"bytes":839,"sha256":"f96c82a24dd0efc26a46dfa632b7861db288bec82224f4ab6d00a8ad282c19b1"},"compiled-source/Cargo.lock":{"bytes":90530,"sha256":"92c0da6805ad13fa78a7d6cbffaccf7738e346d3605ed8c8ac763cf5079f2590"},"compiled-source/Cargo.toml":{"bytes":621,"sha256":"1ec9ac514515662a0f3a371b1a65d72bf825d67f218dc3f6cc1e5fb0925547d6"},"compiled-source/crates/borsuk/Cargo.toml":{"bytes":2640,"sha256":"474efdef5bd398bef758009882c8bdd859553de8f0271230430469dd96bc4d71"},"compiled-source/crates/borsuk/examples/two_bit_http.rs":{"bytes":9731,"sha256":"c8f2327561304a088b0bfd3a4509047a94c05210b8d5d44e008b90faea3cdc29"},"compiled-source/crates/borsuk/src/object_native_generation.rs":{"bytes":36148,"sha256":"c235938f22b8cfaee18edd631ff26e64a16d4f73e179057ba1d3f1e0a9060920"},"compiled-source/crates/borsuk/src/two_bit_generation.rs":{"bytes":38553,"sha256":"7a134fe3e03e735bad63c611ce2b78d865345c8617a6a4b1c5dd70ce1384cc45"},"compiled-source/crates/borsuk/tests/two_bit_generation.rs":{"bytes":32488,"sha256":"da29358a0c797f2ad232a3efd033d7c8a7eb370dd43d44fbb37264885cbdf0f6"},"compiled-source/crates/borsuk/tests/two_bit_source.rs":{"bytes":7871,"sha256":"d341033f45c614da69b70c3f3c4b1ce86ebaf1f26ba59da03a54e926d00a98d3"},"cpuinfo.txt":{"bytes":2856,"sha256":"91561b86cc166f6c939619e5bcb5b755b52e874a5284639f17179ec51278646d"},"run-closed.log":{"bytes":18254,"sha256":"e84c8d29411242bace954fa0ddccfc5ba54f4d7bd38ee648e9432bee1244c8b8"},"rustc-version.txt":{"bytes":197,"sha256":"ec378196e9fa222ab807fd49cb9dae156a9bd18069b9440fd4e0a6c57c6971cb"},"source-qualification.json":{"bytes":6185,"sha256":"3a2bc2f07e50e4b8dfb3994bc63fa6fe5254d5ae92c6036aae14fdd2006cd87b"},"x86-feature-tree.txt":{"bytes":47591,"sha256":"2e310f57ef44e75e9a79d4ac61d74354c3300130126627103db8895d03bfe8b2"}},"frozen_native_qualification":{"path":"docs/research/native-union-20260928/arm-sha-startup/a0001/verification.json","sha256":"b98e17328df1d83b583f06b4d1f0f36a77a159893bb41ff25c8accc6724a7e58","source_archive_sha256":"a37b33402a82bf9653ed6542b7a523fd46d64307d449ff3d575d4c572e751ace","source_commit":"d32d472293d7300be77eb5f1e87cf69892cc504b","terminal_sha256":"4e898e7a59a5a044112c805ae8c0ef8ca51d3183e8a6ad32330e3da580fcd6be"},"historical_assurance_scope":"2696 prior tests only; changed dependency not fully requalified","native_rebuilt":false,"native_source_commit":"d32d472293d7300be77eb5f1e87cf69892cc504b","source_file_count":395,"source_identity_sha256":"c2151132c6df9000f74181a0e52e0131d148f71653c450d2c4f752ac91f8580b"}
QUALIFICATION
phase=binary-qualification
lscpu >cpu.txt
/usr/bin/time -v -o test-resources.txt bash -c 'cd repo; PYTHONPATH=. python3.12 -m scripts.launch_native_cold_first_query_spot --extract-frozen "$1"' _ "$root" >test.log 2>&1
test -s boundary-check.json && test -s binaries/two_bit_http
phase=profile
systemd-run --unit=native-cold-first-query --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=1230 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 1200 \
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 scripts/run_native_cold_first_query.py docs/research/native-union-20260928/cold-first-query-config.json 78ea8ea182432ad8dc38a5b879b97229796232cb6f21d30a40b204eec38c26a1 "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 scripts/check_native_startup_build.py --cgroup "$1/profile-cgroup.json" || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
test -s "$root/screen/summary.json"
test -s "$root/screen/relaion-records.jsonl"
test -s "$root/screen/cohere-records.jsonl"
phase=complete
