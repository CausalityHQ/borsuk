#!/usr/bin/env bash
# SOURCE ONLY. Root collector outside the observer; no native computation here.
# UNIT INVOCATION_ID STAGE_EVIDENCE NEW_OUTER_DIR ACTUAL_EXIT_FILE RECIPE_SHA CONFIG_SHA DEADLINE_EPOCH MODE
set -Eeuo pipefail
set -o noclobber
umask 077
export LC_ALL=C
[[ $# == 9 && $EUID == 0 ]] || exit 98
unit=$1 id=$2 admission=$3 out=$4 actual_exit=$5 recipe_sha=$6 config_sha=$7 deadline=$8 mode=$9
case $mode in platform) expected_code=0; expected_result=success; expected_status=PLATFORM_CANARY_ROOT_GATE_REQUIRED;; staging) expected_code=0; expected_result=success; expected_status=NATIVE_STAGING_SMOKE_ROOT_GATE_REQUIRED;; widths) expected_code=0; expected_result=success; expected_status=PID128_MECHANICS_CLOSED_ROOT_REPLAY_REQUIRED;; negative) expected_code=98; expected_result=exit-code; expected_status=INVALID;; *) exit 98;; esac
[[ $unit =~ ^borsuk-pid128-observer-[a-z0-9-]+\.service$ && $id =~ ^[0-9a-f]{32}$ &&
   $recipe_sha =~ ^[0-9a-f]{64}$ && $config_sha =~ ^[0-9a-f]{64}$ && $deadline =~ ^[0-9]{10}$ ]] || exit 98
[[ $admission == /* && $out == /* && $actual_exit == /* &&
   $admission =~ ^/mnt/borsuk-pid-evidence/[a-z0-9-]+$ && ! -e $out && ! -L $out ]] || exit 98
campaign=${admission%/*}
[[ ${out%/*} == "$campaign" && $out != "$admission/"* && $actual_exit == "$campaign/"* &&
   $actual_exit != "$admission/"* && $(realpath -e "${out%/*}") == "${out%/*}" ]] || exit 98
mkdir "$out"
manager() {
    local now left cap
    now=$(date +%s); left=$((deadline-now-2)); ((left>0)) || return 98
    cap=5; ((cap<=left)) || cap=$left
    timeout --kill-after=1 "$cap" "$@"
}
# Exact check_limits/check_events bodies copied from the r6 recipe; outputs use external copies.
check_limits() {
    jq -es --argjson memory "$2" --argjson cores "$3" --arg cpus "$4" '
      def cap($k): [.[]|.[$k]|select(.!="absent" and .!="max")|tonumber]|min;
      (cap("memory_max")==$memory and cap("memory_swap_max")==0 and cap("pids_max")==128) and
      ([.[]|.cpu_max|select(.!="absent")|split(" ")|select(.[0]!="max")|(.[0]|tonumber)/(.[1]|tonumber)]|min)==$cores and
      .[0].cpuset_cpus_effective==$cpus and (.[0].memory_peak|tonumber)<=$memory and
      (.[0].memory_swap_current|tonumber)==0 and (.[0].memory_swap_peak|tonumber)==0 and
      (.[0].pids_peak|tonumber)<=128' "$1" > "$1.validated"
}
check_events() {
    jq -nes --slurpfile b "$1" --slurpfile a "$2" '
      def events: split("\n")|map(select(length>0)|split(" ")|{key:.[0],value:(.[1]|tonumber)})|from_entries;
      def limits: {path,cpu_max,cpuset_cpus_effective,memory_max,memory_swap_max,pids_max};
      ($a|length)==($b|length) and all(range(0;$a|length); . as $i |
        ($a[$i]|limits)==($b[$i]|limits) and $a[$i].pids_events==$b[$i].pids_events and
        (if $a[$i].memory_events=="absent" then $b[$i].memory_events=="absent" else
          ($a[$i].memory_events|events|{oom,oom_kill,oom_group_kill})==($b[$i].memory_events|events|{oom,oom_kill,oom_group_kill}) end) and
        (if $a[$i].memory_swap_current=="absent" then true else ($a[$i].memory_swap_current|tonumber)==0 and
          ($a[$i].memory_swap_peak|tonumber)==0 end))' > "$2.events-valid"
}
capture_drain() {
    local state events=null h bytes
    [[ $(realpath -e /sys/fs/cgroup/system.slice) == /sys/fs/cgroup/system.slice &&
       $(stat -f -c %T /sys/fs/cgroup/system.slice) == cgroup2fs ]] || return 98
    if [[ -e $cg || -L $cg ]]; then
        [[ -d $cg && ! -L $cg && $(realpath -e "$cg") == "$cg" && -r $cg/cgroup.events ]] || return 98
        cat "$cg/cgroup.events" > "$out/drain.events"
        awk '$1=="populated" {if (NF!=2 || $2!="0" || n++) bad=1} END {if (bad || n!=1) exit 1}' "$out/drain.events" || return 98
        state=empty
        h=$(sha256sum < "$out/drain.events"); h=${h%% *}; bytes=$(stat -c %s "$out/drain.events")
        events=$(jq -cn --arg path "$out/drain.events" --arg sha "$h" --argjson bytes "$bytes" '{path:$path,bytes:$bytes,sha256:$sha}')
    else
        # This is a real absence observation of the previously bound original path.
        state=removed
    fi
    jq -n --arg state "$state" --arg path "$cg" --arg id "$id" --argjson events "$events" \
      '{schema:"borsuk-native-pid128-drain-v1",state:$state,path:$path,invocation_id:$id,events:$events}' > "$out/drain.proof.json"
    reported=$(sed -n 's/^ControlGroup=//p' "$out/manager.show")
    [[ $reported == "/system.slice/$unit" || ( $reported == '' && $state == removed ) ]] || return 98
}
# Capture exact original invocation and exit fields before stop or GC. No --wait with retention.
while :; do
    manager systemctl show "$unit" -p Id -p Description -p InvocationID -p ControlGroup -p MainPID -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus >| "$out/poll.tmp"
    [[ $(sed -n 's/^InvocationID=//p' "$out/poll.tmp") == "$id" &&
       $(sed -n 's/^Id=//p' "$out/poll.tmp") == "$unit" &&
       $(sed -n 's/^Description=//p' "$out/poll.tmp") == "$unit" ]] || exit 98
    if grep -Fx MainPID=0 "$out/poll.tmp" >/dev/null; then
        [[ $(sed -n 's/^ActiveState=//p' "$out/poll.tmp") =~ ^(active|failed)$ &&
           $(sed -n 's/^Result=//p' "$out/poll.tmp") == "$expected_result" &&
           $(sed -n 's/^ExecMainCode=//p' "$out/poll.tmp") == 1 &&
           $(sed -n 's/^ExecMainStatus=//p' "$out/poll.tmp") == "$expected_code" ]] || exit 98
        break
    fi
    (( $(date +%s) < deadline-2 )) || exit 98
    sleep 1
done
[[ -d $admission && ! -L $admission && $(realpath -e "$admission") == "$admission" ]] || exit 98
cp "$out/poll.tmp" "$out/manager.show"
rm "$out/poll.tmp"
cg=/sys/fs/cgroup/system.slice/$unit
[[ ! -L $actual_exit && -f $actual_exit && $(realpath -e "$actual_exit") == "$actual_exit" ]] || exit 98
[[ $(stat -c %s "$actual_exit") == $((${#expected_code}+1)) && $(< "$actual_exit") == "$expected_code" ]] || exit 98
# Require the recipe's original closure and the original observer identity to agree.
for f in terminal.json observer.identity.json; do
    [[ -f $admission/$f && ! -L $admission/$f && $(stat -c %s "$admission/$f") -le 4194304 ]] || exit 98
    jq -es 'length==1 and (.[0]|type=="object")' "$admission/$f" >/dev/null
done
jq -e --arg sha "$config_sha" --arg status "$expected_status" --argjson code "$expected_code" '.schema=="borsuk-native-pid128-terminal-v1" and .status==$status and .config_sha256==$sha and .original_exit==$code and .intended_exit==$code and .cleanup_exit==0 and .performance_claim==false and .cold_claim==false and .competitor_claim==false and .one_million_claim==false' "$admission/terminal.json" >/dev/null
jq -e --arg id "$id" --arg cg "/system.slice/$unit" '.invocation_id==$id and .control_group==$cg' "$admission/observer.identity.json" >/dev/null
[[ -f $admission/wrapper.exit && ! -L $admission/wrapper.exit && $(stat -c %s "$admission/wrapper.exit") == $((${#expected_code}+1)) && $(< "$admission/wrapper.exit") == "$expected_code" ]]
# Require the entire sealed file roster, not a valid truncated subset.
[[ -f $admission/closure.sha256 && ! -L $admission/closure.sha256 && $(stat -c %s "$admission/closure.sha256") -le 4194304 ]] || exit 98
awk '{h=substr($0,1,64); p=substr($0,69);
 if(length(h)!=64 || h~/[^0-9a-f]/ || substr($0,65,4)!="  ./" || p!~/^[A-Za-z0-9._\/-]+$/ ||
    p~/(^|\/)\.\.?($|\/)/ || seen[p]++) exit 1;
 if(p=="terminal.json") t++; if(p=="observer.identity.json") o++; print "./" p}
 END {if(t!=1 || o!=1) exit 1}' "$admission/closure.sha256" > "$out/closure.paths"
sort "$out/closure.paths" > "$out/closure.sorted"
(cd "$admission" && find . -type l -print) > "$out/closure.symlinks"
[[ ! -s $out/closure.symlinks ]] || exit 98
(cd "$admission" && find . -type f ! -path ./closure.sha256 ! -path ./wrapper.exit -print | sort) > "$out/closure.actual"
cmp "$out/closure.sorted" "$out/closure.actual"
(cd "$admission" && sha256sum --check --strict --status closure.sha256)
# The live snapshot and identity bind removal to this original observer invocation.
jq -es --arg path "$cg" 'length>0 and .[0].path==$path' "$admission/observer.initial" > "$out/live-cgroup.validated"
capture_drain
# A second same-invocation snapshot fences the final stop. No success receipt if stop/drain fails.
manager systemctl show "$unit" -p InvocationID -p MainPID -p Result -p ExecMainCode -p ExecMainStatus > "$out/before-stop.show"
for line in "InvocationID=$id" MainPID=0 "Result=$expected_result" ExecMainCode=1 "ExecMainStatus=$expected_code"; do grep -Fx "$line" "$out/before-stop.show" >/dev/null; done
manager systemctl stop "$unit" > "$out/stop.stdout" 2> "$out/stop.stderr"
manager systemctl show "$unit" -p ActiveState -p MainPID > "$out/after-stop.show"
grep -Fx MainPID=0 "$out/after-stop.show" >/dev/null
grep -E '^ActiveState=(inactive|failed)$' "$out/after-stop.show" >/dev/null
[[ ! -e $cg/cgroup.events ]] || grep -Fx 'populated 0' "$cg/cgroup.events" > "$out/after-stop.events"
art() {
    local file=$1 sha bytes
    [[ -f $file && ! -L $file ]] || return 98
    sha=$(sha256sum < "$file"); sha=${sha%% *}; bytes=$(stat -c %s "$file")
    [[ $sha =~ ^[0-9a-f]{64}$ && $bytes =~ ^[1-9][0-9]*$ ]] || return 98
    jq -cn --arg path "$file" --arg sha "$sha" --argjson bytes "$bytes" '{path:$path,bytes:$bytes,sha256:$sha}'
}
negative_proof=null
if [[ $mode == negative ]]; then
    N=$admission/phases/diagnostic
    for spec in payload.initial:resources.initial payload.final:resources.final; do
        name=${spec%%:*}; body=${spec#*:}
        [[ -f $N/$body && ! -L $N/$body && $(stat -c %s "$N/$body") -le 65536 ]] || exit 98
        cp "$N/$body" "$out/$name"
    done
    for body in initial closure; do
        [[ -f $admission/observer.$body && ! -L $admission/observer.$body && $(stat -c %s "$admission/observer.$body") -le 65536 ]] || exit 98
        cp "$admission/observer.$body" "$out/observer.$body"
    done
    check_limits "$out/payload.final" 268435456 1 0
    check_events "$out/payload.initial" "$out/payload.final"
    check_limits "$out/observer.closure" 268435456 1 0
    check_events "$out/observer.initial" "$out/observer.closure"
    [[ $(< "$N/timeout.exit") == 124 ]] || exit 98
    payload_unit=$(< "$N/unit"); payload_id=$(sed -n 's/^InvocationID=//p' "$N/manager.initial.txt")
    [[ $payload_unit =~ ^borsuk-pid128-[a-z0-9-]+-diagnostic\.service$ && $payload_id =~ ^[0-9a-f]{32}$ ]] || exit 98
    [[ -f $N/child.witness.json && $(stat -c %s "$N/child.witness.json") -le 65536 ]] || exit 98
    jq -es --arg id "$payload_id" --arg cg "/system.slice/$payload_unit" 'length==1 and (.[0]|
      keys==["alive_after_timeout","control_group","invocation_id","pid","schema","separate_process_group_and_session","start_time_ticks"] and
      .schema=="borsuk-native-pid128-timeout-witness-v1" and .invocation_id==$id and .control_group==$cg and
      (.pid|type=="number" and floor==. and .>1 and .<=4194304) and (.start_time_ticks|type=="string" and test("^[1-9][0-9]*$")) and
      .alive_after_timeout==true and .separate_process_group_and_session==true)' "$N/child.witness.json" >/dev/null
    child_pid=$(jq -er .pid "$N/child.witness.json"); child_start=$(jq -er .start_time_ticks "$N/child.witness.json")
    [[ $(sed -n 's/^pid=//p' "$N/child.identity") == "$child_pid" &&
       $(sed -n 's/^0:://p' "$N/child.identity") == "/system.slice/$payload_unit" &&
       $(< "$N/child.after-timeout.cgroup") == "0::/system.slice/$payload_unit" ]] || exit 98
    for body in child.initial.procstat child.after-timeout.procstat; do
        awk -v pid="$child_pid" -v start="$child_start" '{n++; if(n!=1) exit 1; s=$0;sub(/^[0-9]+ \(.*\) /,"",s);split(s,a," ");
          if($1!=pid || a[1]!~/^(R|S|D)$/ || a[3]!=pid || a[4]!=pid || a[20]!=start) exit 1} END {if(n!=1) exit 1}' "$N/$body"
    done
    release=$(< "$N/release.uptime_cs"); witness=$(< "$N/child.witness.uptime_cs"); drain=$(< "$N/drain.uptime_cs")
    [[ $release =~ ^[0-9]{1,12}$ && $witness =~ ^[0-9]{1,12}$ && $drain =~ ^[0-9]{1,12}$ ]] || exit 98
    ((release<=witness && witness<=drain && witness-release>=500 && drain-release<6000)) || exit 98
    jq -es --arg path "/sys/fs/cgroup/system.slice/$payload_unit" --arg id "$payload_id" 'length==1 and (.[0]|
      keys==["invocation_id","path","schema","state"] and .schema=="borsuk-native-pid128-payload-drain-v1" and
      .path==$path and .invocation_id==$id and (.state=="empty" or .state=="removed"))' "$N/drain.json" >/dev/null
    if [[ $(jq -er .state "$N/drain.json") == empty ]]; then
        awk '$1=="populated" {if(NF!=2 || $2!="0" || n++) bad=1} END {if(bad || n!=1) exit 1}' "$N/drain.events"
    fi
    [[ $(< "$N/manager.stop.exit") == 0 ]] || exit 98
    grep -Fx Result=exit-code "$N/manager.final.txt" >/dev/null
    grep -Fx ExecMainCode=1 "$N/manager.final.txt" >/dev/null
    grep -Fx ExecMainStatus=98 "$N/manager.final.txt" >/dev/null
    [[ ! -e /sys/fs/cgroup/system.slice/$payload_unit ]] || grep -Fx 'populated 0' /sys/fs/cgroup/system.slice/"$payload_unit"/cgroup.events > "$out/negative.final-drain.events"
    jq -n --arg unit "$payload_unit" --arg id "$payload_id" --argjson pid "$child_pid" --argjson elapsed "$((drain-release))" \
      '{schema:"borsuk-native-pid128-negative-proof-v1",status:"EXPECTED_TIMEOUT_AND_DRAIN",unit:$unit,invocation_id:$id,
        timeout_exit:124,minimum_timeout_centiseconds:500,child_pid:$pid,elapsed_centiseconds:$elapsed,resource_events_unchanged:true,descendant_alive_after_timeout:true,
        drain_before_natural_exit:true,performance_claim:false}' > "$out/negative.proof.json"
    negative_proof=$(art "$out/negative.proof.json")
fi

t=$(art "$admission/terminal.json"); m=$(art "$admission/closure.sha256"); w=$(art "$admission/wrapper.exit")
manager_art=$(art "$out/manager.show"); exit_art=$(art "$actual_exit"); drain_art=$(art "$out/drain.proof.json")
jq -n --arg recipe "$recipe_sha" --arg config "$config_sha" --arg unit "$unit" --arg id "$id" --arg mode "$mode" --argjson code "$expected_code" \
  --argjson t "$t" --argjson m "$m" --argjson w "$w" --argjson manager "$manager_art" --argjson outer "$exit_art" --argjson drain "$drain_art" --argjson negative "$negative_proof" \
  '{schema:"borsuk-native-pid128-stage-closure-v2",status:"CLOSED",mode:$mode,recipe_sha256:$recipe,config_sha256:$config,
    terminal:$t,manifest:$m,wrapper_exit:$w,unit:$unit,invocation_id:$id,control_group:("/system.slice/"+$unit),
    manager_show:$manager,outer_exit_file:$outer,drain_proof:$drain,actual_outer_exit:$code,drained:true,
    controlled_negative_proof:$negative,root_scientific_acceptance_required:true,performance_claim:false}' > "$out/outer-closure.json"
sync -f "$out/outer-closure.json"; sync -f "$out"
