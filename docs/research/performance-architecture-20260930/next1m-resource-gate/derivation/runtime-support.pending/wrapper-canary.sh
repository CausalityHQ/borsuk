#!/usr/bin/env bash
# Disposable REMOTE checks only. Phase commands below are Bash fixtures, never ANN measurements.
set -Eeuo pipefail
umask 077
[[ $# == 3 ]] || exit 125
wrapper=$(realpath -e -- "$1"); bins=$(realpath -e -- "$2"); out=$3
[[ $out == /* && ! -e $out && ! -L $out && -d ${out%/*} ]] || exit 125
[[ $(sha256sum "$wrapper" | cut -d' ' -f1) == c5f4c4b8be203d73347924447fcad461c676a09d1c925ed31187c11a64f5b422 ]] || exit 125
(( BASH_VERSINFO[0] > 5 || (BASH_VERSINFO[0] == 5 && BASH_VERSINFO[1] >= 1) )) || exit 125
mkdir -- "$out"
out=$(realpath -e -- "$out")
printf '{}\n' > "$out/invalid.json"
config_sha=$(sha256sum "$out/invalid.json" | cut -d' ' -f1)
# Exact source excerpts, including the original traps and finish routine; no copied implementation.
awk '/^# ---- jq programs/{exit} {print}' "$wrapper" > "$out/prefix.sh"
awk '/^sample_disk\(\)/{p=1} p{print} p && /^}/{exit}' "$wrapper" > "$out/sample.sh"
awk '/^# ---- one native process/{p=1} /^# ---- start:/{exit} p{print}' "$wrapper" > "$out/phases.sh"
awk '/^cfg\(\)/{print}' "$wrapper" > "$out/cfg.sh"
awk '/^IFS= read -r -d .*JQ_F32 /{p=1;next} p && /^EOF$/{exit} p{print}' "$wrapper" > "$out/f32.jq"
for part in prefix.sh sample.sh phases.sh cfg.sh f32.jq; do [[ -s $out/$part ]]; done
sha256sum "$wrapper" "$out"/{prefix.sh,sample.sh,phases.sh,cfg.sh,f32.jq} > "$out/source.sha256"
run() {
 local name=$1 expected=$2 rc
 shift 2
 set +e
 timeout -k 2 40 "$@" > "$out/$name.stdout" 2> "$out/$name.stderr"
 rc=$?
 set -e
 printf '%s\n' "$rc" > "$out/$name.exit"
 [[ $rc == "$expected" ]]
}
run schema-invalid 98 bash "$wrapper" "$out/invalid.json" "$config_sha" "$out/schema-evidence"
jq -e '.status=="INVALID" and .intended_exit==98 and .original_exit==1 and .baseline_invoked==false' "$out/schema-evidence/terminal.json" > "$out/schema.assert"
mkfifo "$out/fifo"
run fifo 98 bash -c 'source "$1/prefix.sh" "$1/invalid.json" "$2" "$1/fifo-evidence"; authenticate "$1/fifo" 1145 "$2"' _ "$out" "$config_sha"
grep -F 'regular pinned file:' "$out/fifo.stderr" > "$out/fifo.assert"
head -c 1146 /dev/zero > "$out/oversized"
run oversized 98 bash -c 'source "$1/prefix.sh" "$1/invalid.json" "$2" "$1/oversized-evidence"; authenticate "$1/oversized" 1145 "$2"' _ "$out" "$config_sha"
grep -F 'length:' "$out/oversized.stderr" > "$out/oversized.assert"
run disk-write 98 bash -c 'source "$1/prefix.sh" "$1/invalid.json" "$2" "$1/disk-evidence"; source "$1/sample.sh"; scratch_root="$1"; scratch_ns=(); mkdir "$evidence/disk.test.txt"; sample_disk test' _ "$out" "$config_sha"
jq -e '.status=="INVALID" and .intended_exit==98 and .baseline_invoked==false' "$out/disk-evidence/terminal.json" > "$out/disk.assert"
run raw-exit2 98 bash -c 'source "$1/prefix.sh" "$1/invalid.json" "$2" "$1/raw-evidence"; exit 2' _ "$out" "$config_sha"
jq -e '.status=="INVALID" and .original_exit==2 and .intended_exit==98' "$out/raw-evidence/terminal.json" > "$out/raw.assert"
run signal-hup 129 bash -c 'source "$1/prefix.sh" "$1/invalid.json" "$2" "$1/signal-evidence"; kill -HUP $$' _ "$out" "$config_sha"
jq -e '.status=="INVALID" and .signal=="HUP" and .original_exit==129 and .intended_exit==129' "$out/signal-evidence/terminal.json" > "$out/signal.assert"
run carried2 2 bash -c 'source "$1/prefix.sh" "$1/invalid.json" "$2" "$1/carried-evidence"; verified=1; baseline_exit=2; exit 2' _ "$out" "$config_sha"
jq -e '.status=="BASELINE_NONZERO_EXIT" and .intended_exit==2 and .baseline_native_exit==2' "$out/carried-evidence/terminal.json" > "$out/carried.assert"
# Independent literal values; signed-zero JSON roundtrip remains a native generation check.
cat >> "$out/f32.jq" <<'EOF'
[[1065353216,1],[3221225472,-2],[1,1.401298464324817e-45],[2139095039,3.4028234663852886e38]] |
all(.[]; . as $pair | ($pair[0]|f32dec)==$pair[1] and ($pair[0]|f32ok))
EOF
jq -ne -f "$out/f32.jq" > "$out/f32.assert"
# Actual phase supervisors and pipe writers; the command being supervised is a disclosed Bash fixture.
phase_case() {
 local name=$1 cap=$2 command=$3
 run "$name" 98 bash -c 'source "$1/prefix.sh" "$1/invalid.json" "$2" "$1/$3-evidence"; source "$1/cfg.sh"; source "$1/phases.sh"; deadline_seconds=9600; printf "{\"phases\":{\"fixture\":{\"timeout_seconds\":11}}}\n" > "$evidence/config.json"; run_phase fixture "$4" /bin/bash -c "$5"; require_success fixture' _ "$out" "$config_sha" "$name" "$cap" "$command"
}
phase_case fixture-exit17 1048576 'exit 17'
[[ $(cat "$out/fixture-exit17-evidence/phases/fixture/native.exit") == 17 ]]
[[ $(cat "$out/fixture-exit17-evidence/phases/fixture/time.exit") == 17 ]]
[[ $(cat "$out/fixture-exit17-evidence/phases/fixture/timeout.exit") == 17 ]]
[[ $(cat "$out/fixture-exit17-evidence/phases/fixture/tee.exit") == 0 ]]
for writer in time-log supervisor-stderr-log native-stderr-log; do
 [[ $(cat "$out/fixture-exit17-evidence/phases/fixture/$writer.exit") == 0 ]]
done
grep -F 'phase fixture: native exit nonzero' "$out/fixture-exit17.stderr" > "$out/fixture-exit17.assert"
phase_case fixture-timeout 1048576 'exec sleep 30'
[[ $(cat "$out/fixture-timeout-evidence/phases/fixture/timeout.exit") == 124 ]]
phase_case fixture-log-cap 64 'head -c 65536 /dev/zero'
[[ $(cat "$out/fixture-log-cap-evidence/phases/fixture/tee.exit") != 0 ]]
grep -F 'phase fixture: nonzero tee exit' "$out/fixture-log-cap.stderr" > "$out/fixture-log-cap.assert"
# Qualified real ELFs, usage only. No records, queries, truth or ANN calls.
declare -A pins=(
 [build_sq8_source]=4f502be7d15a064dba525816c63ad471c3ad2fe616ede14712b67cdb85e3cf66
 [build_two_bit_generation]=73c0d7da26dc5c52f7b548d04590221b13e0c883af5e0a10990bfae7268b01fe
 [publish_two_bit_generation]=076afd99e6dc6c814461161fd3bab0fd7bc1ab4db68c1337c8c98137fef15982
 [check_cohere_native_baseline]=4f25ed9db388e899f42459a4fd184f19dad594fc5f5a38c01a10d945f3b9beb1
)
for name in "${!pins[@]}"; do
 [[ -f $bins/$name && ! -L $bins/$name && -x $bins/$name ]]
 [[ $(sha256sum "$bins/$name" | cut -d' ' -f1) == "${pins[$name]}" ]]
 case $name in
  build_sq8_source) run "usage-$name" 1 "$bins/$name" --derive;;
  build_two_bit_generation) run "usage-$name" 1 "$bins/$name";;
  *) run "usage-$name" 2 "$bins/$name";;
 esac
done
jq -n '{schema:"borsuk-next1m-wrapper-canary-v1",status:"WRAPPER_CHECKS_VERIFIED",runtime_scope:"disposable remote only",phase_commands_are_bash_fixtures:true,native_cli_usage_only:true,ann_run:false,performance_claim:false,unexercised:["bootstrap transport and scratch-volume binding","full data authentication and native chain","inventory-write failure","target cgroup resource closure","original outer manager exit and instance cleanup","signed-zero native f32 serialization"]}' > "$out/result.json"
(cd "$out" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$out/SHA256SUMS"
sync -f "$out"
