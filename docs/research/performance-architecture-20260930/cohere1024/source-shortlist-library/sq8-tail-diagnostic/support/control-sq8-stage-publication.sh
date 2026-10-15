#!/bin/bash
# shellcheck disable=SC2016 # Source anchors and generated programs contain literal shell expressions.
# Remote, pre-input falsifiers: exact stage supervision and chain closeout, synthetic data only.
set -euo pipefail
if [[ $# == 0 ]]; then
 exec /usr/bin/timeout -k 25 150 /bin/bash "$0" --bounded
fi
root=/mnt/borsuk-v9-panel
[[ $# == 1 && $1 == --bounded && $EUID == 0 ]] || exit 125
umask 077
[[ ! -e $root/bin/original && ! -e $root/bin/candidate && ! -e $root/inputs/queries.f32 && ! -e $root/inputs/truth.u64 ]] || exit 125
[[ ! -e $root/evidence/cleanup-unverified ]] || exit 125
cg=/sys/fs/cgroup$(sed -n 's/^0:://p' /proc/self/cgroup)
[[ $(cat "$cg/cpu.max") == '100000 100000' && $(cat "$cg/cpuset.cpus.effective") == 0 && $(cat "$cg/memory.max") == 2147483648 && $(cat "$cg/memory.swap.max") == 0 && $(cat "$cg/pids.max") == 128 ]] || exit 125
[[ ! -e $root/evidence/stage-publication-fault-canary.json && ! -L $root/evidence/stage-publication-fault-canary.json ]] || exit 125
ev=$root/evidence/stage-publication-faults
mkdir -m 700 "$ev"
units=(); sands=(); checks_complete=false

# Never stop an unclaimed unit. Failed/inactive units retain their manager receipts.
drain_units() {
 local i unit sand cgroup state token invocation current failed=0
 for i in "${!units[@]}"; do
  unit=${units[$i]}; sand=${sands[$i]}; cgroup=/sys/fs/cgroup/system.slice/$unit
  state=$(/usr/bin/timeout -k 1 2 systemctl show "$unit" -p ActiveState --value) || { failed=1; continue; }
  if { [[ $state == inactive || $state == failed ]] && { [[ ! -d $cgroup ]] || grep -qx 'populated 0' "$cgroup/cgroup.events"; }; }; then continue; fi
  [[ -f $sand/evidence/canary-original/unit-token.txt ]] || { failed=1; continue; }
  token=$(cat "$sand/evidence/canary-original/unit-token.txt")
  [[ $token =~ ^[a-f0-9-]{36}$ ]] || { failed=1; continue; }
  invocation=$(/usr/bin/timeout -k 1 2 systemctl show "$unit" -p InvocationID --value) || { failed=1; continue; }
  [[ $invocation =~ ^[a-f0-9]{32}$ && $(/usr/bin/timeout -k 1 2 systemctl show "$unit" -p Environment --value) == "BORSUK_V9_STAGE_TOKEN=$token" ]] || { failed=1; continue; }
  current=$(/usr/bin/timeout -k 1 2 systemctl show "$unit" -p InvocationID --value) || { failed=1; continue; }
  [[ $current == "$invocation" ]] || { failed=1; continue; }
  if [[ -d $cgroup ]]; then printf '1\n' > "$cgroup/cgroup.kill" || failed=1; fi
  current=$(/usr/bin/timeout -k 1 2 systemctl show "$unit" -p InvocationID --value) || { failed=1; continue; }
  [[ $current == "$invocation" ]] || { failed=1; continue; }
  /usr/bin/timeout -k 1 3 systemctl stop "$unit" > "$ev/$i.cleanup.log" 2>&1 || failed=1
  state=$(/usr/bin/timeout -k 1 2 systemctl show "$unit" -p ActiveState --value) || { failed=1; continue; }
  [[ $state == inactive || $state == failed ]] || failed=1
  if [[ -d $cgroup ]] && ! grep -qx 'populated 0' "$cgroup/cgroup.events"; then failed=1; fi
 done
 return "$failed"
}
finish_canary() {
 local original=$? cleanup
 trap - EXIT TERM INT
 set +e
 drain_units; cleanup=$?
 if ((cleanup!=0)); then touch "$root/evidence/cleanup-unverified"; original=125; fi
 sha256sum --strict -c "$ev/source.sha256" > "$ev/source-after.log" || original=125
 if [[ $original == 0 && $checks_complete != true ]]; then original=1; fi
 if [[ $original == 0 && $checks_complete == true ]]; then
  jq -n '{schema:"borsuk-sq8-stage-publication-fault-canary-v1",status:"STAGE_PUBLICATION_CANARY_PASS",native_exit17_verified:true,native_timeout124_verified:true,client_timeout124_verified:true,publication_failures_preserve_exits:true,partial_receipts_unchanged_on_duplicate:true,native_actors_executed:false,scientific_bodies_opened:false,aws_mocked:true,real_systemd:true,production_actor_authentication_claimed:false,performance_claim:false}' > "$ev/summary.json" && sync -f "$ev/summary.json" && ln -- "$ev/summary.json" "$root/evidence/stage-publication-fault-canary.json" || original=126
 fi
 exit "$original"
}
trap finish_canary EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
sha256sum "$root/run-v9-stage.sh" "$root/run-sq8-panel-chain.sh" "$root/close-v9-runtime.sh" > "$ev/source.sha256"

# Refuse drift or duplicate anchors; substitutions affect sandbox copies only.
replace_once() {
 local file=$1 needle=$2 replacement=$3
 NEEDLE=$needle REPLACEMENT=$replacement perl -0777 -e '
  local $/; my $s=<>; my $n=()=$s=~/\Q$ENV{NEEDLE}\E/g;
  die "missing/duplicate source anchor\n" unless $n==1;
  $s=~s/\Q$ENV{NEEDLE}\E/$ENV{REPLACEMENT}/; print $s;
 ' "$file" > "$file.next"
 mv -- "$file.next" "$file"
}
extract_function() {
 local name=$1
 [[ $(grep -Fxc "$name() {" "$root/run-sq8-panel-chain.sh") == 1 ]] || exit 125
 awk -v anchor="$name() {" '$0==anchor{on=1} on{print} on && $0=="}"{exit}' "$root/run-sq8-panel-chain.sh" > "$ev/$name.sh"
 [[ $(tail -n 1 "$ev/$name.sh") == '}' ]] || exit 125
}
extract_function verify_code
extract_function finish
namespace=$(cat /proc/sys/kernel/random/uuid)
[[ $namespace =~ ^[a-f0-9-]{36}$ ]] || exit 125
for fault in exit17 native-timeout client-timeout; do
 sand=$ev/stage-$fault
 mkdir -m 700 "$sand" "$sand/bin" "$sand/config" "$sand/results" "$sand/evidence"
 cp /bin/bash "$sand/bin/original"
 binary_pin=$(sha256sum "$sand/bin/original" | cut -d ' ' -f 1)
 unit=borsuk-v9-stage-canary-$namespace-$fault-canary-original.service
 [[ ! -d /sys/fs/cgroup/system.slice/$unit && $(/usr/bin/timeout -k 1 2 systemctl show "$unit" -p LoadState --value) == not-found ]] || exit 125
 units+=("$unit"); sands+=("$sand")
 cap=5
 if [[ $fault == native-timeout ]]; then cap=2; fi
 if [[ $fault == client-timeout ]]; then cap=15; fi
 {
  printf '# Synthetic Bash config/program, never ANN input.\nset -euo pipefail\n'
  printf 'unit=%q\nev=%q\n' "$unit" "$sand/evidence/canary-original"
  cat <<'PROGRAM'
observed=$(sed -n 's/^0:://p' /proc/self/cgroup)
[[ $observed == /system.slice/"$unit" && $BORSUK_V9_STAGE_TOKEN == "$(cat "$ev/unit-token.txt")" ]] || exit 98
printf '%s\n' "$observed" > "$ev/synthetic.cgroup"
printf '%s\n' "$BORSUK_V9_STAGE_TOKEN" > "$ev/synthetic.token"
printf 'synthetic partial output\n' > "$2"
PROGRAM
  if [[ $fault == exit17 ]]; then printf 'exit 17\n'; else printf '/bin/sleep 30\n'; fi
 } > "$sand/config/synthetic.sh"
 config_pin=$(sha256sum "$sand/config/synthetic.sh" | cut -d ' ' -f 1)
 printf '%s\n' "$(( $(date +%s)+1000 ))" > "$sand/machine-stop.epoch"
 cp "$root/run-v9-stage.sh" "$sand/run-v9-stage.sh"
 replace_once "$sand/run-v9-stage.sh" 'root=/mnt/borsuk-v9-panel' "root=$sand"
 replace_once "$sand/run-v9-stage.sh" 'unit=borsuk-v9-cold-$phase.service' "unit=$unit"
 # The original-arm selection is unique as a two-line block; no reader pin is altered.
 replace_once "$sand/run-v9-stage.sh" $'  cap=60; [[ $phase != *panel-* ]] || cap=120\n  binary=$root/bin/original; pin=59a22eed2e7d3cb8142e35108171cdcd8c27ec83c8968ba307cd9dc092c78083' "  cap=$cap; binary=\$root/bin/original; pin=$binary_pin"
 if [[ $fault == client-timeout ]]; then
  replace_once "$sand/run-v9-stage.sh" '"$((cap+90))" systemd-run' '"2" systemd-run'
 fi
 sha256sum "$sand/run-v9-stage.sh" > "$sand/adapted-source.sha256"
 set +e
 /usr/bin/timeout -k 2 35 /bin/bash "$sand/run-v9-stage.sh" canary-original "$sand/config/synthetic.sh" "$config_pin" > "$sand/stage.log" 2>&1
 observed=$?
 set -e
 printf '%s\n' "$observed" > "$sand/stage.exit"
 [[ $observed == 1 && ! -e $sand/evidence/cleanup-unverified ]] || exit 1
 receipts=$sand/evidence/canary-original
 [[ $(cat "$receipts/synthetic.cgroup") == /system.slice/"$unit" && $(cat "$receipts/synthetic.token") == "$(cat "$receipts/unit-token.txt")" ]] || exit 1
 invocation=$(/usr/bin/timeout -k 1 2 systemctl show "$unit" -p InvocationID --value)
 [[ $invocation =~ ^[a-f0-9]{32}$ && $(/usr/bin/timeout -k 1 2 systemctl show "$unit" -p Environment --value) == "BORSUK_V9_STAGE_TOKEN=$(cat "$receipts/unit-token.txt")" ]] || exit 1
 printf '%s\n' "$invocation" > "$sand/observed-invocation.txt"
 [[ ! -e $receipts/result.sha256 && ! -e $receipts/result.bytes ]] || exit 1
 grep -Fxq 'synthetic partial output' "$sand/results/canary-original.jsonl"
 case $fault in
  exit17) native=17; wrapper=17; manager=1; forced=false;;
  native-timeout) native=null; wrapper=124; manager=1; forced=false;;
  client-timeout) native=null; wrapper=null; manager=124; forced=true;;
 esac
 jq -e --argjson native "$native" --argjson wrapper "$wrapper" --argjson manager "$manager" --argjson forced "$forced" '.native_exit==$native and .timeout_wrapper_exit==$wrapper and .manager_exit==$manager and .forced_cleanup==$forced and .owned_cgroup_drained==true and .execution_closed==false' "$receipts/supervision.json" >/dev/null
 [[ $(cat "$receipts/manager-exit.txt") == "$manager" ]] || exit 1
 if [[ $native == null ]]; then [[ ! -e $receipts/native-exit.txt ]]; else [[ $(cat "$receipts/native-exit.txt") == "$native" ]]; fi
 if [[ $wrapper == null ]]; then [[ ! -e $receipts/timeout-wrapper-exit.txt ]]; else [[ $(cat "$receipts/timeout-wrapper-exit.txt") == "$wrapper" ]]; fi
 if [[ $forced == true ]]; then [[ $(cat "$receipts/unit-invocation-id.txt") == "$invocation" ]]; fi
 sha256sum --strict -c "$sand/adapted-source.sha256" > "$sand/adapted-source-after.log"
 (cd "$sand" && find . -type f -print0 | sort -z | xargs -0 sha256sum) > "$ev/stage-$fault.partial-before.sha256"
 set +e
 /usr/bin/timeout -k 2 5 /bin/bash "$sand/run-v9-stage.sh" canary-original "$sand/config/synthetic.sh" "$config_pin" > "$ev/stage-$fault.duplicate.log" 2>&1
 observed=$?
 set -e
 printf '%s\n' "$observed" > "$ev/stage-$fault.duplicate.exit"
 [[ $observed == 125 ]] || exit 1
 (cd "$sand" && sha256sum --strict -c "$ev/stage-$fault.partial-before.sha256") > "$ev/stage-$fault.partial-after.log"
done

mkdir -m 700 "$ev/aws-bin"
cat > "$ev/aws-bin/aws" <<'SPY'
#!/bin/bash
set -euo pipefail
[[ $# == 10 && $1 == s3api && $2 == put-object && $3 == --bucket && $4 == borsuk-bench-453182569524-euc1 && $5 == --key && $7 == --body && $9 == --if-none-match && ${10} == '*' ]] || exit 97
prefix=research/semantic-router/20261015/sq8-panel-diagnostic-a0001/terminal
case $6 in
 "$prefix/evidence.tar.gz") name=archive; expected=$FAKE_ROOT/evidence.tar.gz;;
 "$prefix/terminal.json") name=terminal; expected=$FAKE_ROOT/terminal.json;;
 *) exit 97;;
esac
[[ $8 == "$expected" && -f $8 && ! -e $FAKE_STATE/$name.attempt ]] || exit 97
printf '%s\n' "$6" >> "$FAKE_STATE/calls"
(set -o noclobber; printf 'attempt\n' > "$FAKE_STATE/$name.attempt")
if [[ $name == archive && $FAKE_MODE == archive-failure ]]; then printf '{"synthetic_partial_failure":41}\n'; exit 41; fi
if [[ $name == terminal ]]; then printf '{"synthetic_partial_failure":43}\n'; exit 43; fi
cp -- "$8" "$FAKE_STATE/archive.body"
printf '{"synthetic_archive_success":true}\n'
SPY
chmod 0500 "$ev/aws-bin/aws"
for fault in terminal-zero terminal-nonzero archive-failure before-archive; do
 case_dir=$ev/publication-$fault
 sand=$case_dir/root
 mkdir -m 700 "$case_dir" "$sand" "$sand/evidence" "$sand/results" "$sand/config" "$case_dir/aws"
 original=17; publication=43
 if [[ $fault == terminal-zero ]]; then original=0; fi
 if [[ $fault == archive-failure ]]; then publication=41; fi
 if [[ $fault == before-archive ]]; then publication=125; touch "$sand/evidence/cleanup-unverified"; fi
 support=$(sha256sum "$root/close-v9-runtime.sh" | cut -d ' ' -f 1)
 jq -n --arg support "$support" '{support_sha256:$support,synthetic_control_fixture:true}' > "$sand/evidence/setup.json"
 cp "$root/close-v9-runtime.sh" "$sand/close-v9-runtime.sh"
 replace_once "$sand/close-v9-runtime.sh" 'original=$1; phase=$2; root=/mnt/borsuk-v9-panel' "original=\$1; phase=\$2; root=$sand"
 (cd "$sand" && sha256sum close-v9-runtime.sh > runtime-code.sha256)
 code_pin=$(sha256sum "$sand/runtime-code.sha256" | cut -d ' ' -f 1)
 {
  printf 'set -euo pipefail\nroot=%q\ncode_pin=%q\ncode_checks=0\nphase=synthetic-publication\n' "$sand" "$code_pin"
  cat "$ev/verify_code.sh" "$ev/finish.sh"
  printf 'trap finish EXIT\nexit %s\n' "$original"
 } > "$case_dir/finish-harness.sh"
 export FAKE_ROOT=$sand FAKE_STATE=$case_dir/aws FAKE_MODE=$fault
 set +e
 PATH="$ev/aws-bin:$PATH" /usr/bin/timeout -k 2 15 /bin/bash "$case_dir/finish-harness.sh" > "$case_dir/finish.log" 2>&1
 observed=$?
 set -e
 printf '%s\n' "$observed" > "$case_dir/finish.exit"
 [[ $observed == 126 ]] || exit 1
 grep -Fxq "ORIGINAL_CHAIN_EXIT=$original PUBLICATION_EXIT=$publication LAST_PHASE=synthetic-publication" "$case_dir/finish.log"
 if [[ $fault == before-archive ]]; then
  [[ ! -e $case_dir/aws/calls && ! -e $sand/evidence/chain-terminal.json && ! -e $sand/evidence.tar.gz && ! -e $sand/archive.put.json && ! -e $sand/terminal.json && ! -e $sand/terminal.put.json ]] || exit 1
  continue
 fi
 jq -e --argjson original "$original" '.original_exit==$original and .last_phase=="synthetic-publication" and .execution_closed==($original==0)' "$sand/evidence/chain-terminal.json" >/dev/null
 [[ -f $sand/archive.put.json && ! -e $case_dir/aws/terminal.success ]] || exit 1
 if [[ $fault == archive-failure ]]; then
  [[ $(wc -l < "$case_dir/aws/calls") == 1 && ! -e $case_dir/aws/archive.body && ! -e $sand/terminal.json && ! -e $sand/terminal.put.json ]] || exit 1
  jq -e '.synthetic_partial_failure==41' "$sand/archive.put.json" >/dev/null
 else
  [[ $(wc -l < "$case_dir/aws/calls") == 2 && -f $case_dir/aws/archive.body && -f $sand/terminal.json ]] || exit 1
  jq -e '.synthetic_archive_success==true' "$sand/archive.put.json" >/dev/null
  jq -e '.synthetic_partial_failure==43' "$sand/terminal.put.json" >/dev/null
  jq -e --argjson original "$original" '.original_exit==$original and .execution_closed==($original==0)' "$sand/terminal.json" >/dev/null
 fi
 # A duplicate direct close must refuse before overwriting partial receipts or retrying AWS.
 (cd "$case_dir" && find root aws -type f -print0 | sort -z | xargs -0 sha256sum) > "$case_dir/partial-before.sha256"
 set +e
 PATH="$ev/aws-bin:$PATH" /usr/bin/timeout -k 2 5 /bin/bash "$sand/close-v9-runtime.sh" "$original" synthetic-publication > "$case_dir/duplicate.log" 2>&1
 observed=$?
 set -e
 printf '%s\n' "$observed" > "$case_dir/duplicate.exit"
 [[ $observed == 125 ]] || exit 1
 (cd "$case_dir" && sha256sum --strict -c partial-before.sha256) > "$case_dir/partial-after.log"
done
checks_complete=true
