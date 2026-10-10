#!/usr/bin/env bash
# REMOTE ONLY: real systemd/kernel, synthetic chain metadata, no ANN/data/native binaries.
# COLLECTOR OBSERVER_COMMAND NEW_OUT
set -Eeuo pipefail
umask 077
[[ $# == 3 && $EUID == 0 ]] || exit 125
collector=$(realpath -e "$1"); observer=$(realpath -e "$2"); out=$3
[[ $out == /mnt/borsuk-pid-evidence/collector-smoke && ! -e $out && ! -L $out &&
   ! -e /mnt/borsuk-scale1m && ! -L /mnt/borsuk-scale1m &&
   $(< /proc/self/cgroup) == 0::/system.slice/borsuk-next1m-canary.service ]] || exit 125
mkdir "$out" /mnt/borsuk-scale1m /mnt/borsuk-scale1m/evidence-root
root=/mnt/borsuk-scale1m
batch_end=$((SECONDS+240))
owned='' owned_id=''
manager() { timeout -k 1 5 "$@"; }
# Cleanup fences the exact invocation; never stop a replacement under an old ID.
cleanup_unit() {
 local show reported
 [[ -n $owned ]] || return 0
 [[ -n $owned_id ]] || return 1
 show=$(manager systemctl show "$owned" -p InvocationID -p MainPID -p ControlGroup) || return 1
 reported=$(sed -n 's/^InvocationID=//p' <<< "$show")
 if [[ -z $reported && ! -e /sys/fs/cgroup/system.slice/$owned && ! -L /sys/fs/cgroup/system.slice/$owned ]]; then
  # A stopped unit may be GC'd. Accept only this collector's original closure/cleanup proof.
  if [[ -f $root/evidence-root/chain-outer/outer-closure.json ]]; then
   jq -e --arg id "$owned_id" --arg unit "$owned" '.status=="CLOSED" and .invocation_id==$id and .unit==$unit and .drained==true' \
    "$root/evidence-root/chain-outer/outer-closure.json" >/dev/null || return 1
  else
   [[ -f $root/evidence-root/chain-outer/failure-cleanup.exit &&
      $(< "$root/evidence-root/chain-outer/failure-cleanup.exit") == 0 ]] || return 1
  fi
  owned='' owned_id=''
  return 0
 fi
 [[ $reported == "$owned_id" ]] || return 1
 manager systemctl stop "$owned" || return 1
 if [[ -e /sys/fs/cgroup/system.slice/$owned/cgroup.events ]]; then
  grep -Fx 'populated 0' "/sys/fs/cgroup/system.slice/$owned/cgroup.events" >/dev/null || return 1
 fi
 owned='' owned_id=''
}
finish() {
 local rc=$?
 trap - EXIT
 cleanup_unit || rc=94
 exit "$rc"
}
trap finish EXIT
cat > "$out/recipe.sh" <<'RECIPE'
#!/usr/bin/env bash
set -Eeuo pipefail
config=$1 sha=$2 evidence=$3
mkdir "$evidence"
code=$(jq -er .exit "$config"); mode=$(jq -er .mode "$config")
cg=$(sed -n 's/^0:://p' /proc/self/cgroup)
[[ $INVOCATION_ID =~ ^[0-9a-f]{32}$ && $cg == /system.slice/borsuk-pid128-observer-* ]]
jq -n --arg cg "$cg" --arg id "$INVOCATION_ID" '{synthetic_metadata:true,invocation_id:$id,control_group:$cg}' > "$evidence/observer.identity.json"
jq -n --arg path "/sys/fs/cgroup$cg" '{path:$path,synthetic_metadata:true}' > "$evidence/observer.initial"
jq -n --arg sha "$sha" --argjson code "$code" '{schema:"borsuk-native-scale-build-gate-local-v2",synthetic_metadata:true,config_sha256:$sha,stage:"native_chain_closed",original_exit:$code,intended_exit:$code,baseline_native_exit:$code,baseline_invoked:true,signal:null,phases_completed:["derive","stage","generation","publish","baseline"],status:(if $code==0 then "NATIVE_CHAIN_CLOSED" else "BASELINE_NONZERO_EXIT" end),ann_executed:false,performance_claim:false}' > "$evidence/terminal.json"
printf '0\n' > "$evidence/cleanup.exit"
printf '%s\n' "$code" > "$evidence/wrapper.exit"
printf 'synthetic fixture; no phases or native program executed\n' > "$evidence/synthetic.txt"
if [[ $mode == descendant ]]; then
 /bin/sleep 60 </dev/null >/dev/null 2>&1 &
 child=$!
 printf '%s\n' "$child" > "$evidence/descendant.pid"
 cat "/proc/$child/cgroup" > "$evidence/descendant.cgroup"
 cat "/proc/$child/stat" > "$evidence/descendant.stat"
fi
(cd "$evidence"; find . -type f ! -name closure.sha256 ! -name wrapper.exit -print0 | sort -z | xargs -0 sha256sum) > "$evidence/closure.sha256"
if [[ $mode == deadline ]]; then exec /bin/sleep 60; fi
exit "$code"
RECIPE
recipe_sha=$(sha256sum "$out/recipe.sh"); recipe_sha=${recipe_sha%% *}
sha256sum "$collector" "$observer" "$out/recipe.sh" > "$out/source.sha256"
start_unit() {
 local code=$1 mode=$2 launch_rc=0
 owned=borsuk-pid128-observer-$(cat /proc/sys/kernel/random/uuid).service
 printf '%s\n' "$owned" > "$case_dir/unit"
 jq -n --argjson code "$code" --arg mode "$mode" '{synthetic_metadata:true,exit:$code,mode:$mode}' > "$case_dir/config.json"
 config_sha=$(sha256sum "$case_dir/config.json"); config_sha=${config_sha%% *}
 manager systemd-run --unit="$owned" --description="$owned" -p Type=exec -p RemainAfterExit=yes \
  -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 \
  -p RuntimeMaxSec=45 -p TimeoutStopSec=3 -p KillMode=control-group -p LimitCORE=0 \
  -p BindsTo=borsuk-next1m-canary.service -p After=borsuk-next1m-canary.service \
  /bin/bash "$observer" "$out/recipe.sh" "$case_dir/config.json" "$config_sha" \
  "$root/evidence-chain" "$root/evidence-root/chain-actual.exit" > "$case_dir/launch.stdout" 2> "$case_dir/launch.stderr" || launch_rc=$?
 printf '%s\n' "$launch_rc" > "$case_dir/launch.exit"
 manager systemctl show "$owned" -p Id -p InvocationID -p ControlGroup > "$case_dir/launch.identity"
 owned_id=$(sed -n 's/^InvocationID=//p' "$case_dir/launch.identity")
 [[ $owned_id =~ ^[0-9a-f]{32}$ ]]
 ((launch_rc==0)) || return 1
 wait_end=$((SECONDS+8))
 until [[ -f $root/evidence-root/chain-actual.exit || $mode == deadline ]]; do
  ((SECONDS<wait_end)) || return 1
  sleep 0.05
 done
}
for case_name in positive0 positive2 positive3 exit-disagreement wrong-config truncated-seal replaced populated deadline; do
 ((SECONDS<batch_end-20)) || exit 94
 case_dir=$out/$case_name; mkdir "$case_dir"
 code=0 mode=plain
 case $case_name in positive2) code=2;; positive3) code=3;; populated) mode=descendant;; deadline) mode=deadline;; esac
 start_unit "$code" "$mode"
 supplied_id=$owned_id
 case $case_name in
  exit-disagreement) printf '2\n' > "$root/evidence-root/chain-actual.exit";;
  wrong-config)
   jq '.config_sha256=("f"*64)' "$root/evidence-chain/terminal.json" > "$case_dir/changed.json"
   mv "$case_dir/changed.json" "$root/evidence-chain/terminal.json"
   (cd "$root/evidence-chain"; find . -type f ! -name closure.sha256 ! -name wrapper.exit -print0 | sort -z | xargs -0 sha256sum) > "$root/evidence-chain/closure.sha256";;
  truncated-seal)
   grep -v '  ./synthetic.txt$' "$root/evidence-chain/closure.sha256" > "$case_dir/short.sha256"
   mv "$case_dir/short.sha256" "$root/evidence-chain/closure.sha256";;
  replaced)
   old_unit=$owned; old_id=$owned_id
   cleanup_unit
   manager systemd-run --unit="$old_unit" --description="$old_unit" -p Type=exec -p RemainAfterExit=yes \
    -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 \
    -p RuntimeMaxSec=30 -p TimeoutStopSec=3 -p KillMode=control-group -p LimitCORE=0 \
    -p BindsTo=borsuk-next1m-canary.service /bin/sleep 20 > "$case_dir/replacement.launch" 2>&1
   owned=$old_unit
   owned_id=$(manager systemctl show "$owned" -p InvocationID --value)
   [[ $owned_id =~ ^[0-9a-f]{32}$ && $owned_id != "$old_id" ]]
   supplied_id=$old_id;;
 esac
 if [[ $case_name == populated || $case_name == deadline ]]; then
  witness_end=$((SECONDS+3))
  while :; do
   manager systemctl show "$owned" -p InvocationID -p MainPID -p ActiveState -p SubState > "$case_dir/live.before"
   grep -Fx "InvocationID=$owned_id" "$case_dir/live.before" >/dev/null
   main=$(sed -n 's/^MainPID=//p' "$case_dir/live.before")
   if [[ $case_name == populated && $main == 0 ]] || [[ $case_name == deadline && $main =~ ^[1-9][0-9]*$ ]]; then break; fi
   ((SECONDS<witness_end)) || exit 94
   sleep 0.05
  done
  cat "/sys/fs/cgroup/system.slice/$owned/cgroup.events" > "$case_dir/live.before.events"
  grep -Fx 'populated 1' "$case_dir/live.before.events" >/dev/null
  if [[ $case_name == populated ]]; then
   child=$(< "$root/evidence-chain/descendant.pid")
   [[ $child =~ ^[1-9][0-9]*$ ]]
   kill -0 "$child"
   grep -Fx "0::/system.slice/$owned" "$root/evidence-chain/descendant.cgroup" >/dev/null
  fi
 fi
 deadline=$(( $(date +%s)+12 )); [[ $case_name != deadline ]] || deadline=$(( $(date +%s)+4 ))
 rc=0
 timeout -k 2 28 bash "$collector" "$owned" "$supplied_id" "$root/evidence-chain" \
  "$root/evidence-root/chain-outer" "$root/evidence-root/chain-actual.exit" \
  "$recipe_sha" "$config_sha" "$deadline" > "$case_dir/collector.stdout" 2> "$case_dir/collector.stderr" || rc=$?
 printf '%s\n' "$rc" > "$case_dir/collector.exit"
 case $case_name in
  positive*)
   [[ $rc == 0 ]]
   jq -e --argjson code "$code" --arg id "$supplied_id" '.status=="CLOSED" and .actual_outer_exit==$code and .invocation_id==$id and .drained==true' "$root/evidence-root/chain-outer/outer-closure.json" > "$case_dir/assert";;
  *) [[ $rc != 0 && $rc != 124 && $rc != 137 && ! -e $root/evidence-root/chain-outer/outer-closure.json ]];;
 esac
 if [[ $case_name == populated || $case_name == deadline ]]; then
  # This must be checked BEFORE the smoke's independent safety cleanup.
  [[ $(< "$root/evidence-root/chain-outer/failure-cleanup.exit") == 0 ]]
  manager systemctl show "$owned" -p InvocationID -p MainPID -p ActiveState > "$case_dir/collector-cleanup.after"
  grep -Fx MainPID=0 "$case_dir/collector-cleanup.after" >/dev/null
  if [[ -e /sys/fs/cgroup/system.slice/$owned/cgroup.events ]]; then
   cat "/sys/fs/cgroup/system.slice/$owned/cgroup.events" > "$case_dir/collector-cleanup.after.events"
   grep -Fx 'populated 0' "$case_dir/collector-cleanup.after.events" >/dev/null
  else
   printf '%s\n' '/sys/fs/cgroup/system.slice/'"$owned" > "$case_dir/collector-cleanup.removed"
  fi
 fi
 if [[ $case_name == replaced ]]; then
  manager systemctl show "$owned" -p InvocationID -p MainPID -p ActiveState > "$case_dir/replacement.after"
  grep -Fx "InvocationID=$owned_id" "$case_dir/replacement.after" >/dev/null
  grep -Fx ActiveState=active "$case_dir/replacement.after" >/dev/null
  pid=$(sed -n 's/^MainPID=//p' "$case_dir/replacement.after"); [[ $pid =~ ^[1-9][0-9]*$ ]]
 fi
 cleanup_unit
 [[ ! -e $root/evidence-chain ]] || mv "$root/evidence-chain" "$case_dir/evidence-chain"
 mv "$root/evidence-root" "$case_dir/evidence-root"
 mkdir "$root/evidence-root"
 printf '%s\n' "$case_name" >> "$out/completed-cases.txt"
done
[[ $(wc -l < "$out/completed-cases.txt") == 9 ]]
sha256sum --check --strict "$out/source.sha256" > "$out/source-closure.assert"
jq -n '{schema:"borsuk-native-collector-smoke-v1",status:"COLLECTOR_MECHANICS_VERIFIED",real_systemd:true,synthetic_chain_metadata:true,positive_cases:3,refusal_cases:6,ann_executed:false,performance_claim:false,full_native_chain_qualified:false}' > "$out/result.json"
(cd "$out"; find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$out/SHA256SUMS"
sync -f "$out"
