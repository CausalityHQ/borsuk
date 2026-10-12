#!/usr/bin/env bash
# Exact native experiment glue; no launch, retry, or scientific interpretation.
set -euo pipefail
[[ $# == 4 ]] || exit 125
root=$1; spec=$2; spec_sha=$3; ev=$4
[[ $root == /mnt/borsuk-retained-returned-publication && $spec == "$root/"* && $ev == "$root/evidence/"* && $spec_sha =~ ^[0-9a-f]{64}$ ]] || exit 125
[[ -f $spec && ! -L $spec && -d $ev && ! -L $ev && $(stat -c %s "$spec") -le 65536 ]] || exit 125
printf '%s  %s\n' "$spec_sha" "$spec" | sha256sum -c - > "$ev/spec-check.log"
jq -e 'keys==["argv","cap_seconds","expected_error","expected_exit","phase","schema"] and .schema=="borsuk-retained-publication-stage-v1" and (.phase|type=="string") and (.argv|type=="array" and length>=5 and length<=7 and all(.[];type=="string" and (contains("\n")|not) and (contains("\u0000")|not))) and (.cap_seconds|type=="number" and floor==. and .>=1 and .<=720) and (.expected_exit==0 or .expected_exit==2) and (.expected_error|type=="string")' "$spec" > "$ev/spec-shape-check.txt"
jq -er '.argv[]' "$spec" > "$ev/native-argv.lines"
mapfile -t command < "$ev/native-argv.lines"
[[ ${command[0]} == "$root/bin/publish_two_bit_generation" && ( ${command[1]} == --retained || ${command[1]} == --retained-router ) ]] || exit 125
[[ ${command[-1]} == "$root/output/"* && ! -e ${command[-1]} && ! -L ${command[-1]} ]] || exit 125
expected=$(jq -r .expected_exit "$spec"); cap=$(jq -r .cap_seconds "$spec")
export BORSUK_CPU_THREADS=1 RAYON_NUM_THREADS=1 AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
unset BORSUK_TEST_BUILD_COMMAND RUSTFLAGS CARGO_ENCODED_RUSTFLAGS
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
[[ $cg == /system.slice/borsuk-publication-*.service ]] || exit 125
printf '%s\n' "$cg" > "$ev/cgroup-path.txt"
for file in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do cat "/sys/fs/cgroup$cg/$file" > "$ev/$file.before"; done
test "$(cat "$ev/cpu.max.before")" = '400000 100000'
test "$(cat "$ev/cpuset.cpus.effective.before")" = 0-3
test "$(cat "$ev/memory.max.before")" = 4294967296
test "$(cat "$ev/memory.swap.max.before")" = 0
test "$(cat "$ev/pids.max.before")" = 256
sha256sum -c "$root/input-files.sha256" > "$ev/input-before.log"
now=$(date +%s); deadline=$(cat "$root/machine-stop.epoch")
[[ $deadline =~ ^[0-9]+$ ]] || exit 125
(( now + cap + 180 < deadline )) || exit 124
printf '%s\n' "$expected" > "$ev/expected-native-exit.txt"
date -u +%FT%TZ > "$ev/native-started.txt"
set +e
/usr/bin/time -v -o "$ev/native-time.txt" /usr/bin/timeout -k 20 "$cap" "${command[@]}" > "$ev/native.stdout" 2> "$ev/native.stderr"
status=$?
set -e
printf '%s\n' "$status" > "$ev/native-exit.txt"
date -u +%FT%TZ > "$ev/native-finished.txt"
sha256sum -c "$root/input-files.sha256" > "$ev/input-after.log"
cmp "$ev/input-before.log" "$ev/input-after.log"
for file in memory.peak memory.events memory.swap.peak pids.peak cpu.stat; do cat "/sys/fs/cgroup$cg/$file" > "$ev/$file.after"; done
[[ $status == "$expected" ]] || exit 1
if [[ $expected == 2 ]]; then
 jq -er .expected_error "$spec" > "$ev/expected-error.txt"
 test -s "$ev/expected-error.txt"
 grep -Fx -f "$ev/expected-error.txt" "$ev/native.stderr" > "$ev/error-match.txt"
 [[ ! -e ${command[-1]} && ! -L ${command[-1]} ]] || exit 1
else
 [[ -f ${command[-1]} && ! -L ${command[-1]} && $(stat -c %s "${command[-1]}") -le 65536 ]] || exit 1
 cp -- "${command[-1]}" "$ev/native-receipt.json"
 jq -e 'type=="object"' "$ev/native-receipt.json" > "$ev/receipt-json-check.txt"
fi
# Supervisor must independently join manager/native exits, drain, receipt/head,
# full owned rosters, uploads and closure. This inner exit is not acceptance.
