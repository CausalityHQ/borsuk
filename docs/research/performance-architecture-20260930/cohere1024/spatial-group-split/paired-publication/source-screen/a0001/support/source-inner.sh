#!/bin/bash
set -euo pipefail
phase=${1:?canary or screen}
case $phase in canary|screen) ;; *) exit 125;; esac
root=/mnt/borsuk-paired-source-screen
support=/opt/borsuk-paired-admission
ev=$root/evidence/source-$phase
cd "$support"
sha256sum -c source-support.sha256 > "$ev/support-check.log"
sha256sum -c source-inputs.sha256 > "$ev/input-check.log"
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
[[ $cg == /system.slice/borsuk-paired-source-$phase.service ]]
for f in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.before"; done
[[ $(cat "$ev/cpu.max.before") == '100000 100000' && $(cat "$ev/cpuset.cpus.effective.before") == 0 && $(cat "$ev/memory.max.before") == 4294967296 && $(cat "$ev/memory.swap.max.before") == 0 && $(cat "$ev/pids.max.before") == 256 && $(ulimit -c) == 0 ]]
export BORSUK_CPU_THREADS=1 RAYON_NUM_THREADS=1 RUST_TEST_THREADS=1
unset BORSUK_SOURCE_NEIGHBORHOOD_CRASH_DIR
sha=78c29dc3388f991c5db17fef7883ca9ee503ed2b4da9a1632b6cc19200dfb1a8
if [[ $phase == canary ]]; then sha=$(printf '%064d' 0); fi
command=("$root/bin/compare_native_replay" --paired-partition-source-coverage "$root/paired.config.json" "$sha" "$root/paired.authority.json" eae672ca128acd9e67d6c92c93422b264579754ebd00934bf3d911858e6f5737 "$ev/selection.json" "$ev/report.json")
printf '%q ' "${command[@]}" > "$ev/native-command.txt"
printf '\n' >> "$ev/native-command.txt"
set +e
/usr/bin/time -v -o "$ev/native-time.txt" timeout -k 30 1200 "${command[@]}" > "$ev/native.stdout" 2> "$ev/native.stderr"
native=$?
set -e
printf '%s\n' "$native" > "$ev/native-exit.txt"
for f in memory.peak memory.events memory.swap.peak pids.peak cpu.stat; do if test -f "/sys/fs/cgroup$cg/$f"; then cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.after"; fi; done
[[ $native == 0 || $native == 2 ]]
if [[ $phase == canary ]]; then
 [[ $native == 2 && ! -e $ev/report.json && ! -e $ev/selection.json ]]
 grep -Fx 'INVALID: config SHA/length/identity; external gates required' "$ev/native.stderr" > "$ev/canary-refusal.txt"
else
 test -s "$ev/report.json"
 # Projection of fields already computed by Rust; no independent reduction or decision.
 jq '{schema,status,complete,error,resource_gate_failed,admission_refused,original_returned_hits,radial_source_coverage,spatial_source_coverage,truth_slots,gained_slots,displaced_lost_slots,radial_bridge_only_slots,spatial_bridge_only_slots,truth_opened_after_durable_paired_selection,consumed_panel,planner_only,source_payload_read,candidate_returned_ids_computed,cold_s3_claim,default_promotion,qualified,selection_seal_sha256}' "$ev/report.json" > "$ev/native-summary.json"
fi
[[ $(cat "$ev/memory.swap.peak.after") == 0 ]]
awk '$1 ~ /^oom/ && $2 != 0 {bad=1} END {exit bad}' "$ev/memory.events.after"
