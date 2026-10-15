#!/bin/bash
# Source-only freezer; root owns execution, proof/publication and scientific gates.
# Usage: runtime PREFIX ROOT_SHA | completed PREFIX ROOT_SHA HISTORICAL_REPORT
set -euo pipefail
set -C
umask 077
export LC_ALL=C

die() { printf 'freeze-v9: %s\n' "$*" >&2; exit 125; }
[[ $# == 3 || $# == 4 ]] || die 'expected runtime PREFIX ROOT_SHA or completed PREFIX ROOT_SHA HISTORICAL_REPORT'
mode=$1; prefix=$2; root_sha=$3
case $mode:$# in runtime:3|completed:4) ;; *) die 'invalid subcommand/arguments';; esac
[[ $prefix =~ ^semantic/v9-[a-zA-Z0-9][a-zA-Z0-9._-]*(/[a-zA-Z0-9][a-zA-Z0-9._-]*)*$ && $prefix != *PENDING* ]] || die 'invalid generation prefix'
[[ $root_sha =~ ^[0-9a-f]{64}$ ]] || die 'invalid root SHA256'

root=/mnt/borsuk-v9-panel
templates=$root/config/templates
runtime_template=$templates/native-panel-config.pending.json
header_template=$templates/completed-panel-headers.pending.json
oracle_template=$templates/oracle-input-descriptors.pending.json
fixture_template=$templates/fixture-config.json
runtime_pin=fa73ffcd25262d8e15a001384dec6c7e74eaad8d2eb8744e2f7f28011966231a
header_pin=a02c08205d5fb4b33cd8bb1734fe01f3a4409e81f63272cfd766b05322866962
oracle_pin=53b18001020b00f20f57d056e070c6c41b7de2de3bbac0feb7380d52ab0eefea
fixture_pin=16cc2cf2391a6d00d7ff5fc9564c3a8dbe0e4d5e7dd58112d87cb73417692a74
helper_pin=0845bddd426a6e7b9a56317f73ff92fdfa15b14b2f1d109cf62d7a77bbb54fbd
baseline_pin=912f23d34dd175f365aab2797896da2c45306bf9dd4faf7d44723c8bdce50995
historical_pin=31dfa30cbf176254e7bec7cc8eb5288071f85cfbb9bb98455b61b3c87f5ec786
runtime=$root/config/panel.json
manifest=$root/new-generation/manifest.json
files=(); snapshots=()

# Canonical paths reject symlinks in every component, including output parents.
canonical() { [[ $1 == /* && $(realpath -e -- "$1") == "$1" ]] || die "noncanonical path: $1"; }
seal() {
    local before after
    canonical "$1"
    [[ -f $1 && ! -L $1 ]] || die "not a regular file: $1"
    before=$(stat -c '%d:%i:%s:%y:%z' -- "$1")
    file_bytes=$(stat -c %s -- "$1")
    [[ $file_bytes -gt 0 && $file_bytes -le $2 ]] || die "file size outside cap: $1"
    file_sha=$(sha256sum < "$1"); file_sha=${file_sha%% *}
    after=$(stat -c '%d:%i:%s:%y:%z' -- "$1")
    [[ $before == "$after" ]] || die "file changed while hashing: $1"
    files+=("$1"); snapshots+=("$after")
}
pin_template() {
    seal "$1" 32768
    [[ $file_sha == "$2" ]] || die "template pin mismatch: $1"
}
unchanged() {
    local i
    for i in "${!files[@]}"; do
        canonical "${files[i]}"
        [[ $(stat -c '%d:%i:%s:%y:%z' -- "${files[i]}") == "${snapshots[i]}" ]] || die "input changed: ${files[i]}"
    done
}
vacant() {
    canonical "${1%/*}"
    [[ -d ${1%/*} && ! -e $1 && ! -L $1 ]] || die "output collision: $1"
}
descriptor() {
    jq -n --arg path "$1" --argjson bytes "$file_bytes" --arg sha "$file_sha" '{path:$path,bytes:$bytes,sha256:$sha}'
}
write_config() {
    local path=$1 body=$2
    [[ $body != *PENDING* && ${#body} -lt 32768 ]] || die 'pending/oversize output config'
    unchanged
    vacant "$path"
    # Noclobber is the create-only boundary; failed writes remain for root disposition.
    printf '%s\n' "$body" > "$path"
    sync -f "$path"
    sync -f "${path%/*}"
    sha256sum -- "$path"
}
stage() {
    local phase=$1 config_sha=$2 binary_sha=$3 output=$4 cap=$5 ev
    ev=$root/evidence/$phase
    seal "$ev/supervision.json" 65536
    jq -es --arg phase "$phase" --arg config "$config_sha" --arg binary "$binary_sha" '
        length == 1 and (.[0] |
        .schema == "borsuk-v9-correctness-stage-v1" and .phase == $phase and
        .config_sha256 == $config and .binary_sha256 == $binary and
        .manager_exit == 0 and .native_exit == 0 and .timeout_wrapper_exit == 0 and
        .owned_cgroup_drained == true and .forced_cleanup == false and
        .execution_closed == true and .performance_claim == false)
    ' "$ev/supervision.json" >/dev/null || die "unsuccessful/unbound $phase supervision"
    seal "$ev/result.bytes" 32
    seal "$ev/result.sha256" 4096
    seal "$output" "$cap"
    [[ $(cat "$ev/result.bytes") == "$file_bytes" &&
       $(cat "$ev/result.sha256") == "$file_sha  $output" ]] || die "unbound $phase output"
}

if [[ $mode == runtime ]]; then
    vacant "$runtime"
else
    for name in reader parity oracle; do vacant "$root/config/$name.json"; done
fi
pin_template "$runtime_template" "$runtime_pin"
pin_template "$fixture_template" "$fixture_pin"
stage fixture "$fixture_pin" "$helper_pin" "$root/results/fixture.json" 65536
seal "$manifest" 65536
[[ $file_sha == "$root_sha" ]] || die 'new manifest hash does not match root SHA'
manifest_descriptor=$(descriptor "$manifest")
jq -es --arg config "$fixture_pin" --arg sha "$root_sha" --arg output "$root/new-generation" \
    --slurpfile fixture "$fixture_template" '
    length == 1 and (.[0] |
    .schema == "borsuk-source-preserving-v9-fixture-v1" and
    .status == "IDENTITY_REBOUND_PROOFS_ONLY" and .complete == true and
    .config_sha256 == $config and .original_root == $fixture[0].root and
    .sq8 == $fixture[0].sq8 and .output == $output and .generation_root_sha256 == $sha and
    .rows == 1000000 and .dimensions == 1024 and .sq8_proof_rows == 32 and
    .max_payload_bytes == $fixture[0].max_payload_bytes and
    .root_fields_changed == ["schema", "page_manifest_sha256"] and
    .source_and_router_refitted == false and .canonical_body_authenticated == false and
    .serving_qualified == false and .query_or_truth_opened == false and
    .performance_claim == false and .vendor_or_scientific_win_claim == false)
' "$root/results/fixture.json" >/dev/null || die 'constructor receipt mismatch'

expected_runtime=$(jq --arg prefix "$prefix" --arg sha "$root_sha" '
    .generation_prefix = $prefix | .generation_root_sha256 = $sha
' "$runtime_template")
[[ $expected_runtime != *PENDING* ]] || die 'runtime still pending'
if [[ $mode == runtime ]]; then
    write_config "$runtime" "$expected_runtime"
    exit 0
fi

seal "$runtime" 32768
runtime_sha=$file_sha
expected_sha=$(printf '%s\n' "$expected_runtime" | sha256sum); expected_sha=${expected_sha%% *}
# Byte equality also rejects duplicate keys, extra JSON values and unapproved edits.
[[ $runtime_sha == "$expected_sha" ]] || die 'runtime differs from pinned template substitutions'
stage panel "$runtime_sha" "$baseline_pin" "$root/results/panel.jsonl" 67108864
result_descriptor=$(descriptor "$root/results/panel.jsonl")
pin_template "$header_template" "$header_pin"
pin_template "$oracle_template" "$oracle_pin"
[[ $4 != *PENDING* ]] || die 'pending historical report path'
seal "$4" 16777216
[[ $file_sha == "$historical_pin" ]] || die 'historical report pin mismatch'
historical_descriptor=$(descriptor "$4")

# Expected authority comes only from the independent pinned header template.
completed=$(jq --arg config "$runtime_sha" --arg prefix "$prefix" --arg sha "$root_sha" \
    --argjson input "$result_descriptor" '
    .expected_identity.config_sha256 = $config |
    .expected_bound_inputs.generation_prefix = $prefix |
    .expected_bound_inputs.generation_root_sha256 = $sha |
    .input = $input
' "$header_template")
reader=$(jq -n --argjson completed "$completed" '
    {schema:"borsuk-current-v9-panel-reader-config-v1",completed:$completed,max_payload_bytes:8388608}
')
parity=$(jq -n --argjson panel "$reader" --argjson historical "$historical_descriptor" '
    {schema:"borsuk-v9-historical-count-parity-config-v1",
     panel:($panel | .max_payload_bytes = 167772160),historical_count_report:$historical}
')
oracle=$(jq --argjson panel "$reader" --argjson manifest "$manifest_descriptor" '
    {schema:"borsuk-v9-source-cover-oracle-config-v1",
     panel:($panel | .max_payload_bytes = 402653184),generation_manifest:$manifest,
     plane_manifest,records,mean,source_digests,requests}
' "$oracle_template")
# Validate every body before writing any member of the completed bundle.
for body in "$reader" "$parity" "$oracle"; do
    [[ $body != *PENDING* && ${#body} -lt 32768 ]] || die 'pending/oversize completed config'
done
write_config "$root/config/reader.json" "$reader"
write_config "$root/config/parity.json" "$parity"
write_config "$root/config/oracle.json" "$oracle"
