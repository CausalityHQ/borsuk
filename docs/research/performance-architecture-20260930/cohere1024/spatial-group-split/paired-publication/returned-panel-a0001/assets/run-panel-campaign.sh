#!/usr/bin/env bash
set -euo pipefail
[[ $# == 1 && $1 == /mnt/borsuk-paired-returned-panel ]] || exit 125
root=$1
[[ -f $root/input-files.sha256 && -f $root/machine-stop.epoch ]] || exit 125
mkdir -m 700 "$root/dynamic"
: > "$root/evidence/stages.tsv"
run() {
 local phase=$1 kind=$2 config=$3 cap=$4 sha
 sha=$(sha256sum "$config"); sha=${sha%% *}
 bash "$root/supervise-native-stage.sh" "$root" "$phase" "$kind" "$config" "$sha" "$cap"
 printf '%s\t%s\t%s\n' "$phase" "$kind" "$sha" >> "$root/evidence/stages.tsv"
}
validate_closed() {
 local phase=$1 output bytes sha
 output=$root/results/$phase.jsonl
 jq -e '.execution_closed==true and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true and .forced_cleanup==false' "$root/evidence/$phase/supervision.json" > /dev/null
 [[ -f $output && ! -L $output ]] || exit 125
 bytes=$(stat -c %s "$output"); ((bytes>0 && bytes<=4194304)) || exit 125
 sha=$(sha256sum "$output"); sha=${sha%% *}
 jq --arg path "$output" --argjson bytes "$bytes" --arg sha "$sha" '.input={path:$path,bytes:$bytes,sha256:$sha}' "$root/inputs/$phase.completed.EXPECTED.json" > "$root/dynamic/$phase.completed.json"
 run "completed-$phase" completed "$root/dynamic/$phase.completed.json" 180
 # Actual rows were validated and reduced by Rust; this checks its disposition.
 jq -e '.schema=="borsuk-completed-native-reduction-v4" and .status=="MEASURED"' "$root/results/completed-$phase.jsonl" > "$root/evidence/completed-$phase/valid-check.txt"
}
for stage in admission canary; do
 for arm in radial spatial; do
  phase=$stage-$arm
  run "$phase" baseline "$root/inputs/$phase.config.json" 60
  validate_closed "$phase"
 done
done
# All actual-input and distinct canary stages must have passed first.
for arm in radial spatial; do
 phase=formal-$arm
 run "$phase" baseline "$root/inputs/$arm.config.BOUND.json" 300
 validate_closed "$phase"
done
# Fill only the closed whole-file pins; expected headers/descriptors stay approved.
jq --slurpfile r "$root/dynamic/formal-radial.completed.json" --slurpfile s "$root/dynamic/formal-spatial.completed.json" '.radial.completed=$r[0] | .spatial.completed=$s[0]' "$root/inputs/paired.config.APPROVED-PENDING-RESULT-PINS.json" > "$root/dynamic/paired.config.json"
run paired paired "$root/dynamic/paired.config.json" 180
jq -e '.complete==true and (.status=="ADVANCE" or .status=="FAIL") and .radial_control_ordered_id_score_bits_parity==true and .full_valid_top10==true and .cold_latency_or_qps_claim==false and .vendor_or_product_win_claim==false' "$root/results/paired.jsonl" > "$root/evidence/paired/report-disposition-check.txt"
status=$(jq -er .status "$root/results/paired.jsonl")
native=$(cat "$root/evidence/paired/native-exit.txt")
if [[ $status == ADVANCE ]]; then [[ $native == 0 ]]; else [[ $native == 2 ]]; fi
printf '%s\n' "$status" > "$root/evidence/paired-decision.txt"
