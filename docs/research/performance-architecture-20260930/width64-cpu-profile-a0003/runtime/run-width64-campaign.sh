#!/usr/bin/env bash
# Diagnostic EC2-only run; the qualified Rust reader owns all query rows.
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
 jq -e '.schema=="borsuk-completed-native-reduction-v4" and .status=="MEASURED" and .diagnostic_only==true and .population_percentiles_valid==false' "$root/results/completed-$phase.jsonl" > "$root/evidence/completed-$phase/valid-check.txt"
}
for phase in admission-candidate canary-candidate; do
 run "$phase" baseline "$root/inputs/$phase.config.json" 60
 validate_closed "$phase" "$phase"
done
printf '%s\n' ACTUAL_INPUT_ADMISSION_AND_CANARY_CLOSED > "$root/evidence/prerequisites.status"
sha256sum -c "$root/input-files.sha256" > "$root/evidence/profile-freeze-input-check.log"
run formal-candidate-b1 baseline "$root/inputs/formal-candidate.config.json" 180
validate_closed formal-candidate-b1 formal-candidate
perf_bin=$(cat "$root/perf.path")
ev=$root/evidence/formal-candidate-b1
timeout -k 2 60 "$perf_bin" report --stdio --no-children --sort dso,symbol -i "$ev/perf.data" > "$ev/perf-self-report.txt" 2> "$ev/perf-self-report.stderr"
timeout -k 2 60 "$perf_bin" report --stdio --children --sort dso,symbol -i "$ev/perf.data" > "$ev/perf-inclusive-report.txt" 2> "$ev/perf-inclusive-report.stderr"
[[ -s $ev/perf-self-report.txt && -s $ev/perf-inclusive-report.txt ]]
[[ $(wc -l < "$root/evidence/stages.tsv") == 6 ]]
printf '%s\n' FIXED128_CPU_PROFILE_AND_NATIVE_REDUCTION_CLOSED > "$root/evidence/width64.status"
