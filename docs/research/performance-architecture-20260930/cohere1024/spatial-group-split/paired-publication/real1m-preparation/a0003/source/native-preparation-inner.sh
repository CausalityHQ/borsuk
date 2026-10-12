#!/usr/bin/env bash
set -euo pipefail
phase=${1:?canary or preparation}
case $phase in canary|preparation) ;; *) exit 125;; esac
root=/mnt/borsuk-paired-source-screen
ev=$root/evidence/$phase
export BORSUK_CPU_THREADS=1 RAYON_NUM_THREADS=1 RUST_TEST_THREADS=1
unset BORSUK_SOURCE_NEIGHBORHOOD_CRASH_DIR
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
[[ $cg == /system.slice/borsuk-paired-$phase.service ]]
printf '%s\n' "$cg" > "$ev/cgroup-path.txt"
for f in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.before"; done
test "$(cat "$ev/cpu.max.before")" = '100000 100000'
test "$(cat "$ev/cpuset.cpus.effective.before")" = 0
test "$(cat "$ev/memory.max.before")" = 4294967296
test "$(cat "$ev/memory.swap.max.before")" = 0
test "$(cat "$ev/pids.max.before")" = 256
printf '%s  %s\n' fd6d36f6bfbb4e370a94707941cec396af59492f832f6d1090c24eacbc1ec748 "$root/bin/compare_native_replay" | sha256sum -c -
config_sha=3779326cba9d54a2da8bf98dd4e284011f87d5968448b87918a23434894c5908
authority_sha=8c672fbd059a71b0ba692cbbdbd4a2a4e9a9f01f4a43634ff5d1509d25fd5bcd
if [[ $phase == canary ]]; then config_sha=$(printf '%064d' 0); fi
printf '%s\n' "compare_native_replay --prepare-spatial-publication $root/preparation.config.json $config_sha $root/authority.json $authority_sha" > "$ev/native-command.txt"
date -u +%FT%TZ > "$ev/native-started.txt"
set +e
/usr/bin/time -v -o "$ev/native-time.txt" timeout -k 30 1200 "$root/bin/compare_native_replay" --prepare-spatial-publication "$root/preparation.config.json" "$config_sha" "$root/authority.json" "$authority_sha" > "$ev/native.stdout" 2> "$ev/native.stderr"
status=$?
set -e
printf '%s\n' "$status" > "$ev/native-exit.txt"
date -u +%FT%TZ > "$ev/native-finished.txt"
for f in memory.peak memory.events memory.swap.peak pids.peak cpu.stat; do if test -f "/sys/fs/cgroup$cg/$f"; then cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.after"; fi; done
if [[ $phase == canary ]]; then
 test "$status" = 2
 grep -Fx 'INVALID: config SHA/length/identity; external gates required' "$ev/native.stderr"
 test ! -e "$root/prepared-spatial"
 printf '%s\n' EXPECTED_BAD_CONFIG_SHA_REFUSAL > "$ev/canary-verdict.txt"
else
 test "$status" = 0
 cp "$root/prepared-spatial/receipt.json" "$ev/native-receipt.json"
 jq -e '.schema=="borsuk-spatial-publication-receipt-v1" and .status=="SPATIAL_PREPARED" and .complete==true and .requests_opened==false and .truth_opened==false and .performance_claim==false' "$ev/native-receipt.json"
 jq -r --arg dir "$root/prepared-spatial" '.publication as $p | [[$p.root.sha256,"spatial-root.bin"],[$p.membership.sha256,"spatial-membership.bin"],[$p.leaves.sha256,"spatial-leaves.bin"]][] | "\(.[0])  \($dir)/\(.[1])"' "$ev/native-receipt.json" > "$ev/derived.sha256"
 sha256sum --check "$ev/derived.sha256" > "$ev/derived-authentication.log"
fi
test ! -e "$root/queries.f32"
test ! -e "$root/truth.u64"
test "$(cat "$ev/memory.swap.peak.after")" = 0
awk '$1 ~ /^oom/ && $2 != 0 {bad=1} END {exit bad}' "$ev/memory.events.after"
