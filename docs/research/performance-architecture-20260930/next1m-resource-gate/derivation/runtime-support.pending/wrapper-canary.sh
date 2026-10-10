#!/usr/bin/env bash
# Disposable REMOTE checks only. Phase commands below are Bash fixtures, never ANN measurements.
# Single-quoted Bash bodies deliberately expand in the child shell.
# shellcheck disable=SC2016
set -Eeuo pipefail
umask 077
[[ $# == 3 ]] || exit 125
wrapper=$(realpath -e -- "$1"); bins=$(realpath -e -- "$2"); out=$3
[[ $out == /* && ! -e $out && ! -L $out && -d ${out%/*} ]] || exit 125
[[ $(sha256sum "$wrapper" | cut -d' ' -f1) == b5e13fc8cab07303e8c9dbaa075e934447cc930dee4f12a215ec163e54be9452 ]] || exit 125
(( BASH_VERSINFO[0] > 5 || (BASH_VERSINFO[0] == 5 && BASH_VERSINFO[1] >= 1) )) || exit 125
(( EUID == 0 )) || exit 125
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
awk '/^observer_relative=/{p=1} /^resources before$/{exit} p{print}' "$wrapper" > "$out/observer-init.sh"
for part in prefix.sh sample.sh phases.sh cfg.sh f32.jq observer-init.sh; do [[ -s $out/$part ]]; done
sha256sum "$wrapper" "$out"/{prefix.sh,sample.sh,phases.sh,cfg.sh,f32.jq,observer-init.sh} > "$out/source.sha256"
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
cp "$out/f32.jq" "$out/f32-assertions.jq"
cat >> "$out/f32-assertions.jq" <<'EOF'
[[1065353216,1],[3221225472,-2],[1,1.401298464324817e-45],[2139095039,3.4028234663852886e38]] |
all(.[]; . as $pair | ($pair[0]|f32dec)==$pair[1] and ($pair[0]|f32ok))
EOF
jq -ne -f "$out/f32-assertions.jq" > "$out/f32.assert"
# Real kernel supervision of disclosed Bash fixtures; never ANN measurements.
observer_unit='' observer_id=''
stop_observer() {
 local show cg
 [[ -n $observer_unit && $observer_id =~ ^[0-9a-f]{32}$ ]] || return 0
 show=$(timeout -k 1 5 systemctl show "$observer_unit" -p InvocationID -p Description -p ControlGroup) || return 1
 grep -Fx "InvocationID=$observer_id" <<< "$show" >/dev/null || return 1
 grep -Fx "Description=$observer_unit" <<< "$show" >/dev/null || return 1
 cg=/sys/fs/cgroup/system.slice/$observer_unit
 if [[ -d $cg ]]; then
  [[ ! -L $cg && $(realpath -e "$cg") == "$cg" ]] || return 1
  printf '1\n' > "$cg/cgroup.kill" || return 1
 fi
 timeout -k 1 10 systemctl stop "$observer_unit" || return 1
 [[ ! -e $cg/cgroup.events ]] || grep -Fx 'populated 0' "$cg/cgroup.events" >/dev/null || return 1
 observer_unit='' observer_id=''
}
canary_cleanup() { local rc=$?; trap - EXIT; stop_observer || rc=98; exit "$rc"; }
trap canary_cleanup EXIT
cat > "$out/phase-fixture.sh" <<'FIXTURE'
set -Eeuo pipefail
out=$1 sha=$2 name=$3 phase=$4 command=$5 carried=$6
source "$out/prefix.sh" "$out/invalid.json" "$sha" "$out/$name-evidence"
source "$out/cfg.sh"
source "$out/phases.sh"
source "$out/sample.sh"
scratch_root=$out; scratch_ns=()
if [[ $name == fixture-escaped ]]; then
 ROOT=$out
 controlled_negative_identity=$out/staging-escaped/native/descendant.identity
fi
whole_deadline=$((SECONDS+120)); manager_deadline=$((SECONDS+40))
source "$out/observer-init.sh"
jq -n --arg phase "$phase" '{phases:{($phase):{timeout_seconds:11}}}' > "$evidence/config.json"
run_phase "$phase" 1048576 /bin/bash -c "$command"
if [[ $carried == 0 || $carried == 2 || $carried == 3 ]]; then
 [[ $(require_clean_exits "$phase") == "$carried" ]] || die 'fixture native exit mismatch'
 completed+=("$phase"); verified=1; baseline_invoked=1; baseline_exit=$carried
 stage=canary_fixture_closed
 exit "$carried"
fi
require_success "$phase"
FIXTURE
phase_case() {
 local name=$1 phase=$2 command=$3 expected=$4 carried=$5 end show rc cg
 observer_unit=borsuk-pid128-observer-$(cat /proc/sys/kernel/random/uuid).service
 timeout -k 1 10 systemd-run --expand-environment=no --quiet --unit="$observer_unit" --description="$observer_unit" --service-type=exec \
  -p RemainAfterExit=yes -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 \
  -p TasksMax=128 -p RuntimeMaxSec=120 -p TimeoutStopSec=10 -p KillMode=control-group -p LimitCORE=0 \
  /bin/bash "$out/phase-fixture.sh" "$out" "$config_sha" "$name" "$phase" "$command" "$carried" \
  > "$out/$name-launch.stdout" 2> "$out/$name-launch.stderr"
 observer_id=$(timeout -k 1 5 systemctl show "$observer_unit" -p InvocationID --value)
 [[ $observer_id =~ ^[0-9a-f]{32}$ ]]
 end=$((SECONDS+100))
 while :; do
  timeout -k 1 5 systemctl show "$observer_unit" -p InvocationID -p Description -p MainPID -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup \
    >| "$out/$name-manager.txt"
  show=$(< "$out/$name-manager.txt")
  grep -Fx "InvocationID=$observer_id" <<< "$show" >/dev/null
  grep -Fx "Description=$observer_unit" <<< "$show" >/dev/null
  if grep -Fx MainPID=0 <<< "$show" >/dev/null; then break; fi
  (( SECONDS < end )) || return 98
  sleep 0.1
 done
 grep -Fx ExecMainCode=1 <<< "$show" >/dev/null
 rc=$(sed -n 's/^ExecMainStatus=//p' "$out/$name-manager.txt")
 printf '%s\n' "$rc" > "$out/$name.exit"
 [[ $rc == "$expected" ]]
 cg=/sys/fs/cgroup/system.slice/$observer_unit
 if [[ -e $cg/cgroup.events ]]; then
  cat "$cg/cgroup.events" > "$out/$name-drain.events"
  grep -Fx 'populated 0' "$out/$name-drain.events" >/dev/null
 else
  printf 'removed\n' > "$out/$name-drain.events"
 fi
 stop_observer
}
phase_case fixture-build0 fixture 'exit 0' 0 0
phase_case fixture-query0 baseline 'exit 0' 0 0
phase_case fixture-query2 baseline 'exit 2' 2 2
phase_case fixture-query3 baseline 'exit 3' 3 3
for code in 0 2 3; do
 jq -e --argjson code "$code" '.intended_exit==$code and .baseline_native_exit==$code and .performance_claim==false' \
   "$out/fixture-query$code-evidence/terminal.json" > "$out/fixture-query$code.assert"
done
phase_case fixture-exit17 fixture 'exit 17' 98 no
for layer in native time timeout payload; do
 [[ $(< "$out/fixture-exit17-evidence/phases/fixture/$layer.exit") == 17 ]]
done
for writer in tee time-log supervisor-stderr-log native-stderr-log; do
 [[ $(< "$out/fixture-exit17-evidence/phases/fixture/$writer.exit") == 0 ]]
done
phase_case fixture-timeout fixture 'exec sleep 30' 98 no
[[ $(< "$out/fixture-timeout-evidence/phases/fixture/timeout.exit") == 124 ]]
phase_case fixture-log-cap fixture 'head -c 2097152 /dev/zero' 98 no
[[ $(< "$out/fixture-log-cap-evidence/phases/fixture/tee.exit") != 0 ]]
mkdir -p "$out/staging-escaped/native"
cat > "$out/escaped-child.sh" <<'ESCAPED'
set -eu
trap '' TERM
printf 'pid=%s\n' "$$" > "$1"
cat "/proc/$$/cgroup" >> "$1"
cat "/proc/$$/stat" > "$1.procstat"
# Exact logger-descriptor cleanup from the accepted r7 timeout descendant.
for ((fd=3;fd<64;fd++)); do eval "exec $fd>&-"; done
exec /bin/sleep 60
ESCAPED
printf -v escaped_command 'setsid /bin/bash %q %q </dev/null >/dev/null 2>&1 & exec sleep 30' "$out/escaped-child.sh" "$out/staging-escaped/native/descendant.identity"
phase_case fixture-escaped fixture "$escaped_command" 98 no
jq -e '.alive_after_timeout==true and .separate_process_group_and_session==true' \
 "$out/fixture-escaped-evidence/phases/fixture/child.witness.json" > "$out/fixture-escaped.assert"
jq -e '.state=="empty" or .state=="removed"' \
 "$out/fixture-escaped-evidence/phases/fixture/cleanup.drain.json" > "$out/fixture-escaped-cleanup.assert"
[[ $(< "$out/fixture-escaped-evidence/cleanup.exit") == 0 ]]

# Qualified real ELFs, usage only. No records, queries, truth or ANN calls.
declare -A pins=(
 [build_sq8_source]=4f502be7d15a064dba525816c63ad471c3ad2fe616ede14712b67cdb85e3cf66
 [build_two_bit_generation]=73c0d7da26dc5c52f7b548d04590221b13e0c883af5e0a10990bfae7268b01fe
 [publish_two_bit_generation]=076afd99e6dc6c814461161fd3bab0fd7bc1ab4db68c1337c8c98137fef15982
 [check_cohere_native_baseline]=59fe47aa1001b3ca24d1f9ff31444f97fcda72e3e297c8d7d846f5c3d811bfc3
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
jq -n '{schema:"borsuk-next1m-wrapper-canary-v2",status:"WRAPPER_CHECKS_VERIFIED",runtime_scope:"disposable remote only",phase_commands_are_bash_fixtures:true,native_cli_usage_only:true,ann_run:false,performance_claim:false,unexercised:["bootstrap transport and scratch-volume binding","full data authentication and native chain","inventory-write failure","full-chain root collector and instance cleanup","signed-zero native f32 serialization"]}' > "$out/result.json"
(cd "$out" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$out/SHA256SUMS"
sync -f "$out"
