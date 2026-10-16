#!/bin/bash
set -euo pipefail
set -C
out=/data/target/borsuk-sq8-panel-attribution.pending/launch-split-screen-a0001
[[ $(cat "$out/collection.closed") == OPAQUE_ARCHIVE_COLLECTED_NOT_VERIFIED ]]
[[ ! -e $out/collected ]]
mkdir "$out/collected"
extract() {
 local member=$1 cap=$2 relative=${1#measurement/} detail mode owner bytes day clock name
 detail=$(timeout -k2 20 tar --numeric-owner --full-time -tvzf "$out/evidence.tar.gz" -- "$member")
 read -r mode owner bytes day clock name <<< "$detail"
 [[ $mode == -* && $name == "$member" && $bytes =~ ^[0-9]+$ && $bytes -gt 0 && $bytes -le $cap ]]
 mkdir -p "$out/collected/$(dirname "$relative")"
 timeout -k2 20 tar -xOzf "$out/evidence.tar.gz" -- "$member" > "$out/collected/$relative"
 [[ $(stat -c %s "$out/collected/$relative") == "$bytes" ]]
}
for f in terminal.json input-manifest.json report-complete.txt; do extract "measurement/$f" 1048576; done
# Native-generated closed report kept opaque; select only already-computed aggregate fields.
extract measurement/report.json 12582912
jq '{schema,status,complete,screen,screen_scope,fetch_only_screen,primary_fetch_improvements_ns,primary_net_fetch_to_rank_improvements_ns,primary_candidate_critical_logical_range_was_split,net_performance_screen,reduction_config_sha256,reduction_config_bytes,diagnostic_only,selected_count,population_percentiles_valid,population_p95_claim,qps_claim,causal_claim,vendor_or_scientific_win_claim,recorded_ordered_returned_physical_ordinal_parity,recorded_logical_coverage_parity,recorded_ordered_id_scorebit_parity,valid_tradeoffs_retained_independent_of_screen,proof_gaps,semantics}' "$out/collected/report.json" > "$out/report-summary.native.json"
for stage in B0 C0 B1 C1 C2 B2 reducer; do
 for f in execution.json owner.txt invocation-id.txt unit-start-status.txt unit-stop-status.txt service-result.txt native-exit.txt timeout-exit.txt time-exit.txt systemd-run-exit.txt manager-envelope-exit.txt worker-exit.txt binary-after-exit.txt cpu.max.before cpu.max.after cpuset.cpus.effective.before cpuset.cpus.effective.after memory.max.before memory.max.after memory.swap.max.before memory.swap.max.after pids.max.before pids.max.after memory.swap.peak.after memory.events.after cgroup.events.drained; do extract "measurement/$stage/$f" 1048576; done
done
printf '%s\n' CLOSED_METADATA_EXTRACTED_NO_RAW_JSONL > "$out/extraction.closed"
cat "$out/report-summary.native.json"
