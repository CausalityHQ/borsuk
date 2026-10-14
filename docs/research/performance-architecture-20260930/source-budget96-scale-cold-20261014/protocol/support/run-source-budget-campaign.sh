#!/usr/bin/env bash
# EC2-only glue: Rust owns all query/result validation and statistics.
set -euo pipefail
[[ $# == 1 && $1 == /mnt/borsuk-source-width64 ]] || exit 125
root=$1
mkdir -m 700 "$root/dynamic"
: > "$root/evidence/stages.tsv"
run() {
 local phase=$1 kind=$2 config=$3 cap=$4 sha
 sha=$(sha256sum "$config"); sha=${sha%% *}
 bash "$root/supervise-native-stage.sh" "$root" "$phase" "$kind" "$config" "$sha" "$cap"
 printf '%s\t%s\t%s\n' "$phase" "$kind" "$sha" >> "$root/evidence/stages.tsv"
}
validate_closed() {
 local phase=$1 template=$2 output bytes sha
 output=$root/results/$phase.jsonl
 jq -e '.execution_closed==true and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true and .forced_cleanup==false' "$root/evidence/$phase/supervision.json" > /dev/null
 [[ -f $output && ! -L $output ]] || exit 125
 bytes=$(stat -c %s "$output"); ((bytes>0 && bytes<=67108864)) || exit 125
 sha=$(sha256sum "$output"); sha=${sha%% *}
 jq --arg path "$output" --argjson bytes "$bytes" --arg sha "$sha" '.input={path:$path,bytes:$bytes,sha256:$sha}' "$root/inputs/$template.completed.EXPECTED.json" > "$root/dynamic/$phase.completed.json"
 run "completed-$phase" completed "$root/dynamic/$phase.completed.json" 180
 jq -e '.schema=="borsuk-completed-native-reduction-v4" and .status=="MEASURED"' "$root/results/completed-$phase.jsonl" > "$root/evidence/completed-$phase/valid-check.txt"
}
for stage in admission canary; do
 for width in original candidate; do
  phase=$stage-$width
  run "$phase" baseline "$root/inputs/$phase.config.json" 60
  validate_closed "$phase" "$phase"
 done
done
printf '%s\n' ACTUAL_INPUT_ADMISSION_AND_CANARY_CLOSED > "$root/evidence/prerequisites.status"
sha256sum -c "$root/input-files.sha256" > "$root/evidence/measurement-freeze-input-check.log"
cp "$root/input-files.sha256" "$root/evidence/measurement-freeze-inputs.sha256"
date -u +%FT%TZ > "$root/evidence/measurement-freeze.timestamp"
sync -f "$root/evidence/measurement-freeze-inputs.sha256"
sync -f "$root/evidence/measurement-freeze.timestamp"
for cell in original-a1 candidate-b1 candidate-b2 original-a2; do
 width=${cell%%-*}
 phase=formal-$cell
 run "$phase" baseline "$root/inputs/formal-$width.config.json" 1800
 validate_closed "$phase" "formal-$width"
 jq -e '.population_percentiles_valid==true and .diagnostic_only==false and .cold_s3_claim==false' "$root/results/completed-$phase.jsonl" > "$root/evidence/completed-$phase/population-check.txt"
done
# Rust binds all four sealed runs and the exact two native runtime configs.
a=$root/inputs/formal-original.config.json
b=$root/inputs/formal-candidate.config.json
ah=$(sha256sum "$a"); ah=${ah%% *}
bh=$(sha256sum "$b"); bh=${bh%% *}
jq -n --slurpfile a1 "$root/dynamic/formal-original-a1.completed.json" --slurpfile b1 "$root/dynamic/formal-candidate-b1.completed.json" --slurpfile b2 "$root/dynamic/formal-candidate-b2.completed.json" --slurpfile a2 "$root/dynamic/formal-original-a2.completed.json" --arg a "$a" --arg b "$b" --argjson an "$(stat -c %s "$a")" --argjson bn "$(stat -c %s "$b")" --arg ah "$ah" --arg bh "$bh" '{schema:"borsuk-source-budget-abba-native-reduction-config-v1",runs:[$a1[0],$b1[0],$b2[0],$a2[0]],runtime_configs:[{path:$a,bytes:$an,sha256:$ah},{path:$b,bytes:$bn,sha256:$bh}]}' > "$root/dynamic/source-budget-abba.json"
run source-budget-abba abba "$root/dynamic/source-budget-abba.json" 180
jq -e '.schema=="borsuk-source-budget-abba-native-reduction-v1" and .status=="MEASURED" and .complete==true and .quality_trace_sq8_parity==true and .recall_anchor_verified==true and .vendor_win_claim==false' "$root/results/source-budget-abba.jsonl" > "$root/evidence/source-budget-abba/parity-check.txt"
[[ $(wc -l < "$root/evidence/stages.tsv") == 17 ]] || exit 125
printf '%s\n' FOUR_FULL1000_CELLS_AND_NATIVE_SOURCE_BUDGET_ABBA_CLOSED > "$root/evidence/source-budget.status"
