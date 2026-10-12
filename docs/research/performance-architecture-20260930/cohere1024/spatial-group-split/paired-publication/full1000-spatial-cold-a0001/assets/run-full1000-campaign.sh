#!/usr/bin/env bash
set -euo pipefail
[[ $# == 1 && $1 == /mnt/borsuk-spatial-full1000 ]] || exit 125
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
 local phase=$1 output bytes sha cap
 output=$root/results/$phase.jsonl
 jq -e '.execution_closed==true and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true and .forced_cleanup==false' "$root/evidence/$phase/supervision.json" > /dev/null
 [[ -f $output && ! -L $output ]] || exit 125
 cap=4194304; [[ $phase != formal-* ]] || cap=67108864
 bytes=$(stat -c %s "$output"); ((bytes>0 && bytes<=cap)) || exit 125
 sha=$(sha256sum "$output"); sha=${sha%% *}
 jq --arg path "$output" --argjson bytes "$bytes" --arg sha "$sha" '.input={path:$path,bytes:$bytes,sha256:$sha}' "$root/inputs/$phase.completed.EXPECTED.json" > "$root/dynamic/$phase.completed.json"
 run "completed-$phase" completed "$root/dynamic/$phase.completed.json" 180
 jq -e '.schema=="borsuk-completed-native-reduction-v4" and .status=="MEASURED"' "$root/results/completed-$phase.jsonl" > "$root/evidence/completed-$phase/valid-check.txt"
}
for stage in admission canary; do
 for arm in radial spatial; do
  phase=$stage-$arm
  run "$phase" baseline "$root/inputs/$phase.config.json" 60
  validate_closed "$phase"
 done
done
for arm in radial spatial; do
 phase=formal-$arm
 run "$phase" baseline "$root/inputs/$phase.config.json" 1800
 validate_closed "$phase"
 jq -e '.population_percentiles_valid==true and .diagnostic_only==false and .cold_s3_claim==false' "$root/results/completed-$phase.jsonl" > "$root/evidence/completed-$phase/population-check.txt"
done
printf '%s\n' BOTH_FULL1000_NATIVE_OUTPUTS_VALIDATED_COLD_AND_VENDOR_INTERPRETATION_PENDING > "$root/evidence/full1000.status"
