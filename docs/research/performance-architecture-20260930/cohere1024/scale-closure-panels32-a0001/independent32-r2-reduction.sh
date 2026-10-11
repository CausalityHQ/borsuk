#!/bin/bash
set -euo pipefail
cat > /mnt/borsuk-scale1m/closed/s3/independent32-r2-completed-template.json <<'TEMPLATE'
{
  "expected_bound_inputs": {
    "backend": {
      "bucket": "borsuk-bench-453182569524-euc1",
      "kind": "s3",
      "physical_prefix": "research/semantic-router/20261010/full1000-local-admission-a0004/retained-source",
      "region": "eu-central-1",
      "sq8_etag": "\"4875c438eb819e00fc58ac993227fb2b\"",
      "sq8_object_key": "semantic/objects/cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1"
    },
    "cohort_receipt_sha256": "e0503d41493157a6a0a48627515e62ec452386d0496827028bb9571d406e3a17",
    "corpus_intervals": [
      {
        "end": 100000,
        "start": 0
      },
      {
        "end": 1001000,
        "start": 101000
      }
    ],
    "corpus_source_first": 0,
    "count": 1000,
    "credential_source": "imds_instance_role_only",
    "dataset": "CohereLabs/wikipedia-2023-11-embed-multilingual-v3",
    "derivation_receipt_sha256": "00e59ee26745c092f1097ba8892b4f205f7c96b48a757311aedc9072fcc3c96e",
    "dimensions": 1024,
    "execution": {
      "mode": "diagnostic_panel",
      "ordinals": [
        69,
        83,
        114,
        149,
        151,
        159,
        188,
        189,
        211,
        237,
        257,
        296,
        321,
        351,
        388,
        408,
        413,
        513,
        537,
        587,
        626,
        667,
        669,
        731,
        742,
        821,
        823,
        849,
        851,
        890,
        956,
        991
      ],
      "trace": false
    },
    "fetch_parallelism": 16,
    "generation_prefix": "semantic/rebound-full1000-a0001/generations/cb9f3a64f6f709005b897aa1dad7352a93fba2f080aca43b31106613de622307",
    "generation_root_sha256": "cb9f3a64f6f709005b897aa1dad7352a93fba2f080aca43b31106613de622307",
    "k": 10,
    "max_memory_bytes": 536870912,
    "metric": "cosine",
    "native_order_sha256": "1060c919e34e93cc9a3d88b93eda226a8635bba41bfc60e5ba47caafb711d413",
    "native_source_sha256": "31f28fa211f9237f0f7e41f569773cd207cefcdce22d4932ec96aee01ff2c40d",
    "native_sq8_sha256": "cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1",
    "phase": "bound_inputs",
    "producer_authority": {
      "executable_sha256": "4f502be7d15a064dba525816c63ad471c3ad2fe616ede14712b67cdb85e3cf66",
      "producer_source_sha256": "a7ba542c3ada061002b09e638a68423236d5499d11aae0e0083cd6e52d38adfd",
      "source_commit": "bc3082a8210c4370ebadae4ba093a7c211072b62",
      "source_order_source_sha256": "1393f99e71be1e68916b598256d1200d56879b439c05d8c66c098345856c9eb4",
      "sq8_source_sha256": "16f03aa7187b4f18676aa511339b07b6e7b8776323710a60071e6ea0d62eb795"
    },
    "profile": "scale1m",
    "query_source_first": 100000,
    "requests_bytes": 4096000,
    "requests_sha256": "8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e",
    "reserved_queries_sha256": "8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e",
    "reserved_query_interval": {
      "end": 101000,
      "start": 100000
    },
    "revision": "ade45fb52bd549f5e8c065636fe4160a43c2af36",
    "rows": 1000000,
    "selected_count": 32,
    "serving": {
      "mode": "baseline"
    },
    "source_cache": "off",
    "tie_rule": "corpus_ordinal_ascending",
    "truth_bytes": 80000,
    "truth_opened": false,
    "truth_sha256": "0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f"
  },
  "expected_identity": {
    "billed_bytes": null,
    "billed_requests": null,
    "binary_sha256": "72ac92e78f0e12ada77f6f6dc8c48d66bc547067a8e12440b469424993fe21f0",
    "codec_source_sha256": "eddf88c6c8ee23a732751f49291293f1cc081545ebb9a21c149eb757aa5c38ec",
    "config_sha256": "e597b90c6566d13488aede8d304982b6b22f9ccaba8c2412075a62e88e91fdff",
    "execution": {
      "mode": "diagnostic_panel",
      "ordinals": [
        69,
        83,
        114,
        149,
        151,
        159,
        188,
        189,
        211,
        237,
        257,
        296,
        321,
        351,
        388,
        408,
        413,
        513,
        537,
        587,
        626,
        667,
        669,
        731,
        742,
        821,
        823,
        849,
        851,
        890,
        956,
        991
      ],
      "trace": false
    },
    "external_gate_required": true,
    "fetch_parallelism": 16,
    "generation_source_sha256": "3cb8d291eeda39bb29f27884f898e34a4a7f444059d52599a36439aa8fe4a0a9",
    "io_measurement": "logical_GET_charges_separate_from_cumulative_process_native_transport",
    "native_transport_includes": "S3_and_IMDS_credential_requests_including_PUT",
    "phase": "identity",
    "physical_s3_measured": false,
    "returned_source_sha256": "a4dcc7f9bc06cdea835f72860f3d2e796fcb6f29276858c349d624884cc9f74b",
    "router_source_sha256": "4a580f2bb3afe2c0c30c5a11b6491b28d4165788d69bb39432b748e67fc68718",
    "runner_source_sha256": "d5e593e5f8e2aa02697b366cc3bcd8e6fa4160071ea0e2ac337063f9ff0023c4",
    "s3_credential_source": "imds_instance_role_only",
    "schema": "borsuk-cohere-native-baseline-result-v7",
    "scope": "AUTHENTICATED_NATIVE_QUALITY_CORRECTNESS",
    "serving": {
      "mode": "baseline"
    },
    "source_plane_source_sha256": "dbcc4cdbc4bb5c244354b375afd657f8b5cb1892df3f0ff0b8f41ef580de42e0",
    "sq8_range_source_sha256": "3f6407664d5f2c1be817e4b32e6a9dce8d4bf8e66ca455e237d0e7b0716ad711",
    "truth_opened": false,
    "unread_bytes": null,
    "wire_bytes": null
  },
  "input": {
    "path": "/mnt/borsuk-scale1m/closed/s3/independent32-r2-result.jsonl",
    "bytes": 0,
    "sha256": "UNBOUND_WHOLE_FILE_PIN"
  },
  "schema": "borsuk-completed-native-reduction-config-v4"
}
TEMPLATE
printf '%s  %s
' af6155cb4c1fd7300b610f984a3fcd7c05e6bf7b5c9234987e49d8610548ec05 /mnt/borsuk-scale1m/closed/s3/independent32-r2-completed-template.json | sha256sum -c -
result=/mnt/borsuk-scale1m/closed/s3/independent32-r2-result.jsonl
bytes=$(stat -c %s "$result")
(( bytes > 0 && bytes <= 67108864 ))
sha=$(sha256sum "$result" | cut -d' ' -f1)
[[ "$sha" =~ ^[0-9a-f]{64}$ ]]
jq --argjson bytes "$bytes" --arg sha "$sha" '.input.bytes=$bytes | .input.sha256=$sha' /mnt/borsuk-scale1m/closed/s3/independent32-r2-completed-template.json > /mnt/borsuk-scale1m/closed/s3/completed-config.independent32-r2.json
test ! -e /mnt/borsuk-scale1m/closed/s3/completed-config.original-before-independent32-r2.json
mv /mnt/borsuk-scale1m/closed/s3/completed-config.json /mnt/borsuk-scale1m/closed/s3/completed-config.original-before-independent32-r2.json
mv /mnt/borsuk-scale1m/closed/s3/completed-config.independent32-r2.json /mnt/borsuk-scale1m/closed/s3/completed-config.json
set -euo pipefail
root=/mnt/borsuk-scale1m
ev="$root/evidence/independent32-r2-reduction"
mkdir "$ev"
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
configsha=$(sha256sum "$root/closed/s3/completed-config.json" | cut -d" " -f1)
left=$(( $(cat "$root/gate-deadline.epoch") - $(date +%s) ))
(( left > 30 )) || exit 124
(( left <= 120 )) || left=120
set +e
systemd-run --expand-environment=no --unit=borsuk-rust-replay-independent32-r2-reduction --wait --pipe -p WorkingDirectory="$root" -p CPUQuota=100% -p CPUQuotaPeriodSec=100ms -p AllowedCPUs=0 -p MemoryMax=1G -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p TimeoutStopSec=30 -p RuntimeMaxSec="$(( left + 120 ))" --setenv=RUSTUP_HOME="$root/.rustup" --setenv=CARGO_HOME="$root/.cargo" --setenv=PATH="$root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" /bin/bash -c 'set -euo pipefail
trap '\''exit 97'\'' TERM HUP INT
root=/mnt/borsuk-scale1m
ev="$root/evidence/independent32-r2-reduction"
cg=$(awk -F: '\''$1==0 {print $3}'\'' /proc/self/cgroup)
printf "%s\n" "$cg" > "$ev/cgroup-path.txt"
for f in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.before"; done
test "$(cat "$ev/cpu.max.before")" = "100000 100000"
test "$(cat "$ev/cpuset.cpus.effective.before")" = 0
test "$(cat "$ev/memory.max.before")" = 1073741824
test "$(cat "$ev/memory.swap.max.before")" = 0
test "$(cat "$ev/pids.max.before")" = 128
configsha=$(sha256sum "$root/closed/s3/completed-config.json" | cut -d" " -f1)
left=$(( $(cat "$root/gate-deadline.epoch") - $(date +%s) ))
(( left > 30 )) || exit 124
(( left <= 120 )) || left=120
printf "%s\n" '\''/mnt/borsuk-scale1m/bin/compare_native_replay --completed-scale /mnt/borsuk-scale1m/closed/s3/completed-config.json "$configsha" /mnt/borsuk-scale1m/closed/s3/independent32-r2-reduction.json'\'' > "$ev/native-command.txt"
printf "%s  %s\n" 4cd7cd24d33e66f3504d42c2b2191a312f471a2a2c8a6b5f6b6d0c8e0e0938a9 "$root/bin/compare_native_replay" | sha256sum -c - > "$ev/release-before.log"
date -u +%FT%TZ > "$ev/native-started.txt"
set +e
/usr/bin/time -v -o "$ev/native-time.txt" timeout --kill-after=30 "$left" /mnt/borsuk-scale1m/bin/compare_native_replay --completed-scale /mnt/borsuk-scale1m/closed/s3/completed-config.json "$configsha" /mnt/borsuk-scale1m/closed/s3/independent32-r2-reduction.json > "$ev/native.log" 2>&1
status=$?
set -e
printf "%s\n" "$status" > "$ev/native-exit.txt"
date -u +%FT%TZ > "$ev/native-finished.txt"
for f in memory.peak memory.events memory.swap.peak pids.peak cpu.stat; do if test -f "/sys/fs/cgroup$cg/$f"; then cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.after"; fi; done
exit "$status"' > "$ev/systemd-run.log" 2>&1
manager=$?
set -e
printf "%s\n" "$manager" > "$ev/manager-exit.txt"
set +e
timeout --kill-after=2 10 systemctl show borsuk-rust-replay-independent32-r2-reduction.service -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup > "$ev/systemd-after.txt" 2> "$ev/systemd-after.stderr"
show_status=$?
set -e
printf "%s\n" "$show_status" > "$ev/systemd-show-exit.txt"
drained=false
if test -s "$ev/cgroup-path.txt"; then cg=$(cat "$ev/cgroup-path.txt"); if [[ "$cg" == "/system.slice/borsuk-rust-replay-independent32-r2-reduction.service" ]]; then if test ! -d "/sys/fs/cgroup$cg"; then drained=true; elif grep -qx "populated 0" "/sys/fs/cgroup$cg/cgroup.events"; then drained=true; fi; fi; fi
native=null; if test -s "$ev/native-exit.txt"; then native=$(cat "$ev/native-exit.txt"); [[ "$native" =~ ^[0-9]+$ ]] || native=null; fi
if [[ "$native" == 0 ]]; then cp --no-clobber /mnt/borsuk-scale1m/closed/s3/independent32-r2-reduction.json "$ev/independent32-r2-reduction.json"; cp --no-clobber /mnt/borsuk-scale1m/closed/s3/completed-config.json "$ev/completed-config.json"; fi
jq -n --arg schema borsuk-full1000-rust-replay-stage-v1 --arg stage 'independent32-r2-reduction' --arg source 2a7b7dd19eeeb88439f79a73e256eb5c0ad6adaf --argjson manager "$manager" --argjson native "$native" --argjson drained "$drained" '{schema:$schema,stage:$stage,source_commit:$source,manager_exit:$manager,native_exit:$native,owned_cgroup_drained:$drained,performance_claim:false}' > "$ev/terminal.json"
(cd "$ev" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$ev/SHA256SUMS"
publication_deadline=$(( $(date +%s) + 120 ))
for file in "$ev"/*; do remaining=$(( publication_deadline - $(date +%s) - 2 )); (( remaining > 0 )) || exit 124; (( remaining <= 90 )) || remaining=90; timeout --kill-after=2 "$remaining" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261011/scale-closure-panels32-a0001/stages/independent32-r2-reduction/"$(basename "$file")" --body "$file" --if-none-match "*" > /dev/null; done
cat "$ev/terminal.json"
[[ "$native" == 0 && "$manager" == 0 && "$drained" == true ]]
