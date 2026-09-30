#!/bin/bash
set -euo pipefail
systemd-run --unit=native-cold-offered-stop --on-active=2700s /usr/sbin/shutdown -h now
root=/mnt/native-cold-offered
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
  for name in source-qualification.json boundary-check.json compiled-source.json binaries/two_bit_http cpu.txt test.log run-closed.log profile.log profile-resources.txt profile-cgroup.json screen/summary.json screen/rate0-relaion-records.jsonl screen/rate0-cohere-records.jsonl screen/rate1-relaion-records.jsonl screen/rate1-cohere-records.jsonl screen/rate2-relaion-records.jsonl screen/rate2-cohere-records.jsonl screen/rate3-relaion-records.jsonl screen/rate3-cohere-records.jsonl screen/rate4-relaion-records.jsonl screen/rate4-cohere-records.jsonl screen/rate5-relaion-records.jsonl screen/rate5-cohere-records.jsonl frozen/binaries/two_bit_http frozen/boundary-check.json frozen/compiled-source.json frozen/source-qualification.json frozen/rustc-version.txt frozen/cargo-version.txt frozen/cpuinfo.txt frozen/arm-feature-tree.txt frozen/x86-feature-tree.txt frozen/run-closed.log frozen/compiled-source/crates/borsuk/examples/two_bit_http.rs frozen/compiled-source/crates/borsuk/src/object_native_generation.rs frozen/compiled-source/crates/borsuk/src/two_bit_generation.rs frozen/compiled-source/crates/borsuk/tests/two_bit_generation.rs frozen/compiled-source/Cargo.toml frozen/compiled-source/Cargo.lock frozen/compiled-source/crates/borsuk/Cargo.toml frozen/compiled-source/crates/borsuk/tests/two_bit_source.rs; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260930/cold-offered-a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('source-qualification.json', 'boundary-check.json', 'compiled-source.json', 'binaries/two_bit_http', 'cpu.txt', 'test.log', 'run-closed.log', 'profile.log', 'profile-resources.txt', 'profile-cgroup.json', 'screen/summary.json', 'screen/rate0-relaion-records.jsonl', 'screen/rate0-cohere-records.jsonl', 'screen/rate1-relaion-records.jsonl', 'screen/rate1-cohere-records.jsonl', 'screen/rate2-relaion-records.jsonl', 'screen/rate2-cohere-records.jsonl', 'screen/rate3-relaion-records.jsonl', 'screen/rate3-cohere-records.jsonl', 'screen/rate4-relaion-records.jsonl', 'screen/rate4-cohere-records.jsonl', 'screen/rate5-relaion-records.jsonl', 'screen/rate5-cohere-records.jsonl', 'frozen/binaries/two_bit_http', 'frozen/boundary-check.json', 'frozen/compiled-source.json', 'frozen/source-qualification.json', 'frozen/rustc-version.txt', 'frozen/cargo-version.txt', 'frozen/cpuinfo.txt', 'frozen/arm-feature-tree.txt', 'frozen/x86-feature-tree.txt', 'frozen/run-closed.log', 'frozen/compiled-source/crates/borsuk/examples/two_bit_http.rs', 'frozen/compiled-source/crates/borsuk/src/object_native_generation.rs', 'frozen/compiled-source/crates/borsuk/src/two_bit_generation.rs', 'frozen/compiled-source/crates/borsuk/tests/two_bit_generation.rs', 'frozen/compiled-source/Cargo.toml', 'frozen/compiled-source/Cargo.lock', 'frozen/compiled-source/crates/borsuk/Cargo.toml', 'frozen/compiled-source/crates/borsuk/tests/two_bit_source.rs'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-cold-offered-spot-v1','source_commit':'9f1abe4ebd1b80dd8e216a3b37d824c813fb8d14',
  'source_archive_sha256':'73b6542e061af216978b6c6669db23edbf770514b29a2bb99dd223c6a1e91510',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260930/cold-offered-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/73b6542e061af216978b6c6669db23edbf770514b29a2bb99dd223c6a1e91510.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '73b6542e061af216978b6c6669db23edbf770514b29a2bb99dd223c6a1e91510' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q tar gzip time python3.12
python3.12 -m ensurepip
python3.12 -m pip install -q boto3
cat >source-qualification.json <<'QUALIFICATION'
{"code_sha256":{"scripts/check_native_metadata_ranges_stats.py":"4dfe0722756ceb46e54ee9f849e47724b5597976c7bbafa793c9add2852399e9","scripts/check_native_startup_build.py":"e8d85e6a59f3f6d0d551d3772febbf9a930476a866078da4c846690fbfd83824","scripts/check_native_startup_stats.py":"56b4989f5be09f5fdfee424862c32372e42762a7a0b7b8290e6f4b35c3354a29","scripts/launch_native_cold_first_query_spot.py":"be3d4c85c88bef6396db5c0e47914c22741b4fcdbf465df03e9f30e71c131849","scripts/launch_native_cold_offered_spot.py":"f5f7d786e769bde760076a65acf8064a937df98799d17fb587a8c5fbcde2d5e5","scripts/launch_native_metadata_ranges_cold_spot.py":"862e3b7c1a39c28f461fdba037d3174e654ba3a476c942c36b94bd64c9a3d305","scripts/rest_coexistence_load.py":"04c0eed4709d569509ad89012754fbfe996170b2ee6f49388790357af6a70fdd","scripts/run_native_cold_first_query.py":"9ced034924594c7443b5e5a435da22e8f2204151ba6ecc7869ff0ef5b87b0c42","scripts/run_native_cold_offered.py":"56732459d192a532e38c69b788c77136fa6e34864e1d6b1618fdfde33dd54b8d","scripts/run_native_metadata_ranges_cold.py":"8a61093569f2e70c7bcd00988b3a350e19408cdcecbbecbd360f4b384bd805cb","scripts/run_native_peer_1m_worker.py":"8c84ed799fc0a2e00e991bfcf8ac81102dede8c9a505ac55e5c220d09cad93d3","scripts/run_native_peer_offered_http.py":"7c86bab8c78a75ef42b20f54c0e91ab5e247e0b521f47de06f89b7b6fdc4610b","scripts/run_native_union_http.py":"886f84ea639b822668a848e8f085027aec12818e316d0839fbf607844d18f880","scripts/run_native_union_offered_http.py":"8a2c5993d282c3f67d841544828793989550157d3ac70091a18a26f2074517d6"},"compiled_native_sha256":{"Cargo.lock":"92c0da6805ad13fa78a7d6cbffaccf7738e346d3605ed8c8ac763cf5079f2590","Cargo.toml":"1ec9ac514515662a0f3a371b1a65d72bf825d67f218dc3f6cc1e5fb0925547d6","crates/borsuk/Cargo.toml":"474efdef5bd398bef758009882c8bdd859553de8f0271230430469dd96bc4d71","crates/borsuk/examples/two_bit_http.rs":"c8f2327561304a088b0bfd3a4509047a94c05210b8d5d44e008b90faea3cdc29","crates/borsuk/src/object_native_generation.rs":"14b2b51cf97449b4e23e343049c0896ec94f731408a45e5798b79f720631cef6","crates/borsuk/src/two_bit_generation.rs":"7a134fe3e03e735bad63c611ce2b78d865345c8617a6a4b1c5dd70ce1384cc45","crates/borsuk/tests/two_bit_generation.rs":"da29358a0c797f2ad232a3efd033d7c8a7eb370dd43d44fbb37264885cbdf0f6","crates/borsuk/tests/two_bit_source.rs":"d341033f45c614da69b70c3f3c4b1ce86ebaf1f26ba59da03a54e926d00a98d3"},"config_sha256":"f0a1e8338980d869c738096f34733c5a48431b9705adbffbc44d49a794985ec4","current_full_suite_pass_claim":false,"frozen_binary_artifacts":{"arm-feature-tree.txt":{"bytes":47682,"sha256":"c0339e2b53f482fd27db37ba95bb37c7bd1830af833a299a524a098a4c79191f"},"binaries/two_bit_http":{"bytes":12471000,"sha256":"3d96aa35461bea6985d7e990f623aab9643ec55316be64af214c3c23dccc192d"},"boundary-check.json":{"bytes":2939,"sha256":"4b25d650681371da79cc0bf1c91c1bc4f6029321cb7f992119d7fe9150a9e4d1"},"cargo-version.txt":{"bytes":36,"sha256":"fb3f25aefd6d5441842d285f5a9b125b3f0f0b0bf2013d9a32677f9e99c6c854"},"compiled-source.json":{"bytes":839,"sha256":"7ce7a80cb5f1488459a3bde31cf16296575ba86efc43afc463432e4ec4e0d018"},"compiled-source/Cargo.lock":{"bytes":90530,"sha256":"92c0da6805ad13fa78a7d6cbffaccf7738e346d3605ed8c8ac763cf5079f2590"},"compiled-source/Cargo.toml":{"bytes":621,"sha256":"1ec9ac514515662a0f3a371b1a65d72bf825d67f218dc3f6cc1e5fb0925547d6"},"compiled-source/crates/borsuk/Cargo.toml":{"bytes":2640,"sha256":"474efdef5bd398bef758009882c8bdd859553de8f0271230430469dd96bc4d71"},"compiled-source/crates/borsuk/examples/two_bit_http.rs":{"bytes":9731,"sha256":"c8f2327561304a088b0bfd3a4509047a94c05210b8d5d44e008b90faea3cdc29"},"compiled-source/crates/borsuk/src/object_native_generation.rs":{"bytes":54597,"sha256":"14b2b51cf97449b4e23e343049c0896ec94f731408a45e5798b79f720631cef6"},"compiled-source/crates/borsuk/src/two_bit_generation.rs":{"bytes":38553,"sha256":"7a134fe3e03e735bad63c611ce2b78d865345c8617a6a4b1c5dd70ce1384cc45"},"compiled-source/crates/borsuk/tests/two_bit_generation.rs":{"bytes":32488,"sha256":"da29358a0c797f2ad232a3efd033d7c8a7eb370dd43d44fbb37264885cbdf0f6"},"compiled-source/crates/borsuk/tests/two_bit_source.rs":{"bytes":7871,"sha256":"d341033f45c614da69b70c3f3c4b1ce86ebaf1f26ba59da03a54e926d00a98d3"},"cpuinfo.txt":{"bytes":2856,"sha256":"91561b86cc166f6c939619e5bcb5b755b52e874a5284639f17179ec51278646d"},"run-closed.log":{"bytes":19005,"sha256":"36d6cbf32f2aa43884adf59e4966d0e7901213ca98d1d0eeccaf7e295b447fdf"},"rustc-version.txt":{"bytes":197,"sha256":"ec378196e9fa222ab807fd49cb9dae156a9bd18069b9440fd4e0a6c57c6971cb"},"source-qualification.json":{"bytes":6508,"sha256":"a72b9a6fa2244ec7da9e3df9ca5d3ea4b73ef1276977f37900dec0a7b7bb008e"},"x86-feature-tree.txt":{"bytes":47606,"sha256":"9d48c43a723bd0bc25c6416482c00c7a9793e334ce1785c86d02293e0571a7d7"}},"frozen_native_qualification":{"path":"docs/research/native-union-20260928/metadata-ranges-cold/a0001/verification.json","sha256":"e24ddfbb646f5bca185429b2d027799dd9e7f8c9c4de3db8c7fbf7b5f4ee5826","source_archive_sha256":"924e396904fc1d1deae9dba45c77f21eec103c98b4c1d3f38951543944da5485","source_commit":"0525dcb826e740411c115e8ece4ffb82a9ee3394","terminal_sha256":"91ae24f6bc0f1a6680777d45e35e66f21906e81b639b1a86701de71fbf503235"},"native_rebuilt":false,"native_source_commit":"0525dcb826e740411c115e8ece4ffb82a9ee3394","source_file_count":395,"source_identity_sha256":"1720c277b493c1097f68aa89ea44c8555bf5222bf0de69de28144f900fdee969"}
QUALIFICATION
phase=binary-qualification
lscpu >cpu.txt
(cd repo; PYTHONPATH=. python3.12 -m scripts.launch_native_cold_offered_spot --extract-frozen "$root") >test.log 2>&1
phase=profile
systemd-run --unit=native-cold-offered --wait --pipe -p MemoryMax=7G -p MemorySwapMax=0 -p RuntimeMaxSec=2430 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 2400 \
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_cold_offered docs/research/native-union-20260928/cold-offered-config.json f0a1e8338980d869c738096f34733c5a48431b9705adbffbc44d49a794985ec4 "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 scripts/check_native_startup_build.py --cgroup "$1/profile-cgroup.json" || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
test -s "$root/screen/summary.json"
test -s "$root/screen/rate0-relaion-records.jsonl"
test -s "$root/screen/rate0-cohere-records.jsonl"
test -s "$root/screen/rate1-relaion-records.jsonl"
test -s "$root/screen/rate1-cohere-records.jsonl"
test -s "$root/screen/rate2-relaion-records.jsonl"
test -s "$root/screen/rate2-cohere-records.jsonl"
test -s "$root/screen/rate3-relaion-records.jsonl"
test -s "$root/screen/rate3-cohere-records.jsonl"
test -s "$root/screen/rate4-relaion-records.jsonl"
test -s "$root/screen/rate4-cohere-records.jsonl"
test -s "$root/screen/rate5-relaion-records.jsonl"
test -s "$root/screen/rate5-cohere-records.jsonl"
phase=complete
