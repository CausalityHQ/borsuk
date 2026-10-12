#!/usr/bin/env bash
# EC2-only glue: Rust owns all query/result validation and statistics.
set -euo pipefail
[[ $# == 1 && $1 == /mnt/borsuk-fetch-width-scale ]] || exit 125
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
 for width in 16 32; do
  phase=$stage-width$width
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
for cell in width16-a1 width32-b1 width32-b2 width16-a2; do
 width=${cell%%-*}
 phase=formal-$cell
 run "$phase" baseline "$root/inputs/formal-$width.config.json" 1800
 validate_closed "$phase" "formal-$width"
 jq -e '.population_percentiles_valid==true and .diagnostic_only==false and .cold_s3_claim==false' "$root/results/completed-$phase.jsonl" > "$root/evidence/completed-$phase/population-check.txt"
done
for repetition in 1 2; do
 a=formal-width16-a$repetition
 b=formal-width32-b$repetition
 jq -n --slurpfile a "$root/dynamic/$a.completed.json" --slurpfile b "$root/dynamic/$b.completed.json" '{schema:"borsuk-scale-paired-native-reduction-config-v1",arms:[$a[0],$b[0]]}' > "$root/dynamic/paired-$repetition.json"
 run "paired-$repetition" paired "$root/dynamic/paired-$repetition.json" 180
 jq -e '.schema=="borsuk-scale-paired-native-reduction-v1" and .status=="MEASURED" and .complete==true and .semantic_parity==true and .trace_plan_parity==true and .logical_charge_parity==true and .vendor_or_scientific_win_claim==false and .performance_pass_claim==false' "$root/results/paired-$repetition.jsonl" > "$root/evidence/paired-$repetition/parity-check.txt"
done
[[ $(wc -l < "$root/evidence/stages.tsv") == 18 ]] || exit 125
printf '%s\n' FOUR_FULL1000_CELLS_AND_BOTH_NATIVE_PARITY_PAIRS_CLOSED > "$root/evidence/fetch-width.status"
