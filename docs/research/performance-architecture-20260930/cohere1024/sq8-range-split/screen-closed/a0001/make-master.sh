#!/bin/bash
# Metadata assembly only, after setup observations are independently checked.
set -euo pipefail
set -C
[[ $# == 2 ]] || exit 125
observation=$1 output=$2
out=/data/target/borsuk-sq8-panel-attribution.pending/launch-split-screen-a0001
bundle=$out/support-bundle.pending
root=/mnt/borsuk-sq8-split-panel
[[ -f $observation && ! -L $observation && ! -e $output && ! -L $output ]]
jq -se 'length==1 and (.[0]|.schema=="borsuk-split-prerequisites-setup-observation-v1" and .sys_vendor=="Amazon EC2" and (.remote_machine_id|test("^[0-9a-f]{32}$")) and .bash_package_verify_exit==0 and .native_executed==false)' "$observation" >/dev/null
start=$(cat "$out/launch-started.epoch")
[[ $start =~ ^[1-9][0-9]{9}$ ]]
(( $(date +%s) > start && $(date +%s) < start+3300 ))
(cd "$bundle"; sha256sum --strict --check "$out/frozen-support-files.sha256") > "$out/frozen-support-files.verified.log"
descriptor() {
 local file=$1 remote=$2 sha bytes
 [[ -f $file && ! -L $file ]]
 sha=$(sha256sum "$file"); sha=${sha%% *}
 bytes=$(stat -c %s "$file")
 jq -nc --arg path "$remote" --arg sha "$sha" --argjson bytes "$bytes" '{path:$path,bytes:$bytes,sha256:$sha}'
}
inputs=$out/master-descriptors.jsonl
[[ ! -e $inputs ]]
for name in ADMITTED_SOURCE native-qualification.json real-input-admission.json smoke.json disabled-historical-parity.json builder-removal-mutation.json scientific-freeze.json; do descriptor "$bundle/receipts/$name" "$root/receipts/$name" >> "$inputs"; done
for name in run-sq8-split-panel.sh panel-stage.sh; do descriptor "$bundle/support/$name" "$root/support/$name" >> "$inputs"; done
for name in native-source.json protocol.md; do descriptor "$bundle/authority/$name" "$root/authority/$name" >> "$inputs"; done
for label in B0 C0 B1 C1 C2 B2; do
 for suffix in runtime expected; do descriptor "$bundle/runtime/$label.$suffix.json" "$root/runtime/$label.$suffix.json" >> "$inputs"; done
done
assets=/data/target/borsuk-sq8-panel-attribution.pending/split-runtime.pending/remote-assets.draft.json
jq -n --slurpfile d "$inputs" --slurpfile assets "$assets" --slurpfile config "$bundle/runtime/B0.runtime.json" --arg root "$root" --argjson start "$start" '
 def asset($role): $assets[0].assets[]|select(.role==$role)|{path:($root+"/"+.target),bytes,sha256};
 {schema:"borsuk-sq8-split-execution-input-v1",campaign_dir:($root+"/measurement"),source_commit:"8e7076951936b0434b7947afd5b23e192c98fe70",machine_started_epoch:$start,shared_deadline_epoch:($start+3720),admission_marker:$d[0],driver:$d[7],worker:$d[8],producer:asset("check_cohere_native_baseline"),reducer:asset("compare_native_replay"),authority:{source:$d[9],protocol:$d[10],cohort:$config[0].cohort_receipt},receipts:{native_qualification:$d[1],real_input_admission:$d[2],smoke:$d[3],disabled_historical_parity:$d[4],builder_removal_mutation:$d[5],scientific_freeze:$d[6]},cells:(["B0","C0","B1","C1","C2","B2"]|to_entries|map({label:.value,runtime_config:$d[11+2*.key],expected_headers:$d[12+2*.key]}))}' > "$output"
sha256sum "$output"
