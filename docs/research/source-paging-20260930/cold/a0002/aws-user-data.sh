#!/bin/bash
set -euo pipefail
systemd-run --unit=native-paged-source-cold-stop --on-active=4200s /usr/sbin/shutdown -h now
root=/mnt/native-paged-source-cold
mkdir -p "$root" && cd "$root"
cat >artifact-roster.json <<'ROSTER'
["test.log","test-resources.txt","run-closed.log","cpu.txt","source-qualification.json","boundary-check.json","boundary-cgroup.json","compiled-source.json","binaries/two_bit_http","resolved-config.json","source.log","source-walk.log","sq8-transport.log","object-native.log","graph.log","generation.log","application-ids.log","gc.log","http.log","release.log","rustc-version.txt","cargo-version.txt","cpuinfo.txt","arm-feature-tree.txt","x86-feature-tree.txt","profile.log","profile-resources.txt","profile-cgroup.json","screen/summary.json","screen/relaion-records.jsonl","screen/cohere-records.jsonl","compiled-source/crates/borsuk/examples/two_bit_http.rs","compiled-source/crates/borsuk/src/object_native_generation.rs","compiled-source/crates/borsuk/src/two_bit_generation.rs","compiled-source/crates/borsuk/tests/two_bit_generation.rs","compiled-source/Cargo.toml","compiled-source/Cargo.lock","compiled-source/crates/borsuk/Cargo.toml","compiled-source/crates/borsuk/tests/two_bit_source.rs","compiled-source/crates/borsuk/src/sq8_s3_range.rs","compiled-source/crates/borsuk/src/sq8_page_authority.rs","compiled-source/crates/borsuk/src/two_bit_source.rs","compiled-source/crates/borsuk/src/two_bit_build.rs","compiled-source/crates/borsuk/src/two_bit_index.rs","compiled-source/crates/borsuk/src/unit_centroid_graph.rs","compiled-source/crates/borsuk/src/bin/build_two_bit_graph_variant.rs","compiled-source/crates/borsuk/src/bin/two_bit_plan_demo.rs","compiled-source/crates/borsuk/src/bin/two_bit_union_nomination.rs","compiled-source/crates/borsuk/src/bin/two_bit_walk_nomination.rs","compiled-source/crates/borsuk/tests/two_bit_application_ids.rs","compiled-source/crates/borsuk/tests/two_bit_gc_delayed_delete.rs"]
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
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/source-paging/20260930/cold-a0002/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-paged-source-cold-spot-v1','source_commit':'3345f0f6283fad463439947e7bafab1c816b3ced',
  'source_archive_sha256':'e0d65e41b39f6f412f2842a6edfe62e56e1545e07148fd70cbe71353191c0334',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'config_sha256':'92a5e94ab57cced22405d42527bd4f1b54f0f0a839b8444722b7a111145100b9',
  'original_config_sha256':'92a5e94ab57cced22405d42527bd4f1b54f0f0a839b8444722b7a111145100b9',
  'manifest_sha256':'e23787927669acf07cbf0d5e687e99ef5a09b24f27e42e76de4c2070991a744c',
  'resolved_config_sha256':artifacts.get('resolved-config.json',{}).get('sha256'),
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/source-paging/20260930/cold-a0002/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/e0d65e41b39f6f412f2842a6edfe62e56e1545e07148fd70cbe71353191c0334.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'e0d65e41b39f6f412f2842a6edfe62e56e1545e07148fd70cbe71353191c0334' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time python3-devel python3.12 pkgconf-pkg-config
python3.12 -m ensurepip
python3.12 -m pip install -q boto3
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal   --default-toolchain 1.98.0
cat >source-qualification.json <<'QUALIFICATION'
{"artifact_roster_sha256":"29ee67a9c41d5de129c1f76879badbc34a82397ac1495452ee4f0c37e6ec7d7f","campaign_schema":"borsuk-native-paged-source-cold-spot-v1","code_sha256":{"scripts/check_native_metadata_ranges_stats.py":"3c17d3202d7d67247d6502428f17a8e981414f58261007ad6fcf794367aea8f3","scripts/check_native_paged_source_build.py":"c2c323aa614c58b306292103282236f60bdd9059e124ba3d6b57b639e489372b","scripts/check_native_paged_source_stats.py":"51219979f622c74e5521a68bff59982b1db03e18b77930034b3ea01fabaf6116","scripts/check_native_startup_build.py":"5abfe3678bf59454871277238baa1de0901610a04b1e3712cbb09b563c48ef52","scripts/check_native_startup_stats.py":"56b4989f5be09f5fdfee424862c32372e42762a7a0b7b8290e6f4b35c3354a29","scripts/launch_native_metadata_ranges_cold_spot.py":"dcafc370d72fb2df6643a234ab4714ddebcaa484cf56bfa89c824382a2ba36b3","scripts/launch_native_paged_source_cold_spot.py":"a048dd9aeedf328442d4456f649807db5274ecbdbb6d9dbc73902fcb8e743bae","scripts/rest_coexistence_load.py":"04c0eed4709d569509ad89012754fbfe996170b2ee6f49388790357af6a70fdd","scripts/run_native_cold_first_query.py":"9ced034924594c7443b5e5a435da22e8f2204151ba6ecc7869ff0ef5b87b0c42","scripts/run_native_paged_cold_first_query.py":"e3a4c1c1674dda0c29c1ea28f01801452bb5d9fe6ba87ea8096fa4820263e795","scripts/run_native_peer_1m_worker.py":"8c84ed799fc0a2e00e991bfcf8ac81102dede8c9a505ac55e5c220d09cad93d3","scripts/run_native_peer_offered_http.py":"7c86bab8c78a75ef42b20f54c0e91ab5e247e0b521f47de06f89b7b6fdc4610b","scripts/run_native_union_http.py":"886f84ea639b822668a848e8f085027aec12818e316d0839fbf607844d18f880","scripts/run_native_union_offered_http.py":"8a2c5993d282c3f67d841544828793989550157d3ac70091a18a26f2074517d6"},"compiled_native_sha256":{"Cargo.lock":"178c5f35a744ec2d11e21f040e72e74016e4502e532dbc2edfd8e2a5433b3b76","Cargo.toml":"1ec9ac514515662a0f3a371b1a65d72bf825d67f218dc3f6cc1e5fb0925547d6","crates/borsuk/Cargo.toml":"76f88b2f0d8805f7727d1aef19469974fe8735aa24c0bd7f12175a3f88826c45","crates/borsuk/examples/two_bit_http.rs":"7d2d97b2a0579bfbaffd4ff8e5b35f13f7ecbd323d9b4f0b24598569129adfa7","crates/borsuk/src/bin/build_two_bit_graph_variant.rs":"2278f5d0811c21c3252e287c87717dcb34e59570f9b4dccdebedcc963604a9a7","crates/borsuk/src/bin/two_bit_plan_demo.rs":"2a89f80da6ba650cb9b3c3de5650c43d9facdbfb0a6c5ce00c41522d37b0ef54","crates/borsuk/src/bin/two_bit_union_nomination.rs":"79ed545cdee7183e921295e2cb40fa9371bd1078b8a930c2db7a6cdbb68b03e9","crates/borsuk/src/bin/two_bit_walk_nomination.rs":"ba2383c638f1254a17dd08b45d4035070da897d991246029f5fa2cd8b442e437","crates/borsuk/src/object_native_generation.rs":"36dc8f040f862b329164be2cb58151270518f5c659bfbb317f77f1e8855f579c","crates/borsuk/src/sq8_page_authority.rs":"e4b34461509c78ca558f49a0ff11151dc1454589f472a5911913b5dbcea9af23","crates/borsuk/src/sq8_s3_range.rs":"80022b4b8669015b24dfc055dda162877f096fd20530fcb2811232d483e40c12","crates/borsuk/src/two_bit_build.rs":"fe5e18007b2db7b240ab792024b81be12e5dc3c35cbf82190f1159555876171f","crates/borsuk/src/two_bit_generation.rs":"3ced573da33bc0c82a9562eb7973c6ae1821fb613dab8c3b79b04b5ff34b8e34","crates/borsuk/src/two_bit_index.rs":"0d5588593477518d7620a976c11759ffaf8a0c286555ce9b8110049cc53eb640","crates/borsuk/src/two_bit_source.rs":"11016833a444183e8f8a3cb1303d5244f7becb2684ff156d2dcbb8b9b5f4f93d","crates/borsuk/src/unit_centroid_graph.rs":"7a47654900e1ec2d5f96384ee2b67ea4c647c2eb05c00a0e4d5902636bcebc42","crates/borsuk/tests/two_bit_application_ids.rs":"18bcf05e43f88ce95794279f48ae311325b023a06688577d656aaae532307531","crates/borsuk/tests/two_bit_gc_delayed_delete.rs":"197c64ec26bf6ece0a0a812b8e6d1de6c1197e26b8234424d3023d5d8b923341","crates/borsuk/tests/two_bit_generation.rs":"81ac7d2281269445b6ee2c8d7fd7267fa45c413d76f024ae4f4baddb903d403a","crates/borsuk/tests/two_bit_source.rs":"bb034601f3b2813d7dd6f22f2493c6005bb872c0ff39c03c6569ae408ab47f11"},"config_path":"docs/research/source-paging-20260930/cold-config.json","config_sha256":"92a5e94ab57cced22405d42527bd4f1b54f0f0a839b8444722b7a111145100b9","current_full_suite_pass_claim":false,"manifest_path":"docs/research/source-paging-20260930/native-source-manifest.json","manifest_sha256":"e23787927669acf07cbf0d5e687e99ef5a09b24f27e42e76de4c2070991a744c","matched_control_latency_measured":false,"matched_vendor_measured":false,"native_rebuilt":true,"native_source_commit":"2b0827369965ede5414a23e7be3abfefb92166b9","source_file_count":395,"source_identity_sha256":"3f05bdfd2c399fb4f93ab5c73827c7b55d8fd31f73ed967e6ac235976a7d7f64"}
QUALIFICATION
phase=binary-qualification
lscpu >cpu.txt
systemd-run --unit=paged-source-build --wait --pipe -p MemoryMax=10G -p MemorySwapMax=0 -p RuntimeMaxSec=2430 \
 --setenv=PYTHONPATH="$root/repo" --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$CARGO_HOME/bin:$PATH" \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 2400 \
 taskset -c 0-3 python3.12 "$root/repo/scripts/check_native_paged_source_build.py" "$CARGO_HOME/bin/cargo" "$root/repo" "$root" >test.log 2>&1
test -s boundary-check.json && test -s binaries/two_bit_http && test -s resolved-config.json
resolved_sha=$(python3.12 -c 'import json; print(json.load(open("boundary-check.json"))["resolved_config_sha256"])')
phase=profile
systemd-run --unit=native-paged-source-cold --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=1530 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 1500 \
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_paged_cold_first_query "$1/resolved-config.json" "$2" "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 scripts/check_native_startup_build.py --cgroup "$1/profile-cgroup.json" || exit 96; exit "$code"' _ "$root" "$resolved_sha" >profile.log 2>&1
test -s "$root/screen/summary.json"
test -s "$root/screen/relaion-records.jsonl"
test -s "$root/screen/cohere-records.jsonl"
phase=complete
