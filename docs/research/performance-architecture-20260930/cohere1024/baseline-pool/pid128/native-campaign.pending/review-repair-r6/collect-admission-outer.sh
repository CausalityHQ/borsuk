#!/usr/bin/env bash
# SOURCE ONLY. Root collector outside the observer; no native computation here.
# UNIT INVOCATION_ID ADMISSION_DIR NEW_OUTER_DIR ACTUAL_EXIT_FILE RECIPE_SHA CONFIG_SHA DEADLINE_EPOCH
set -Eeuo pipefail
set -o noclobber
umask 077
export LC_ALL=C
[[ $# == 8 && $EUID == 0 ]] || exit 98
unit=$1 id=$2 admission=$3 out=$4 actual_exit=$5 recipe_sha=$6 config_sha=$7 deadline=$8
[[ $unit =~ ^borsuk-pid128-observer-[a-z0-9-]+\.service$ && $id =~ ^[0-9a-f]{32}$ &&
   $recipe_sha =~ ^[0-9a-f]{64}$ && $config_sha =~ ^[0-9a-f]{64}$ && $deadline =~ ^[0-9]{10}$ ]] || exit 98
[[ $admission == /*/admission && $out == /* && $actual_exit == /* &&
   $admission =~ ^/mnt/borsuk-pid-evidence/[a-z0-9-]+$ && ! -e $out && ! -L $out ]] || exit 98
campaign=${admission%/*}
[[ $out == "$campaign/"* && $out != "$admission/"* && $actual_exit == "$campaign/"* &&
   $actual_exit != "$admission/"* && $(realpath -e "${out%/*}") == "${out%/*}" ]] || exit 98
mkdir "$out"
manager() {
    local now left cap
    now=$(date +%s); left=$((deadline-now-2)); ((left>0)) || return 98
    cap=5; ((cap<=left)) || cap=$left
    timeout --kill-after=1 "$cap" "$@"
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
        [[ $(sed -n 's/^ActiveState=//p' "$out/poll.tmp") == active &&
           $(sed -n 's/^SubState=//p' "$out/poll.tmp") == exited &&
           $(sed -n 's/^Result=//p' "$out/poll.tmp") == success &&
           $(sed -n 's/^ExecMainCode=//p' "$out/poll.tmp") == 1 &&
           $(sed -n 's/^ExecMainStatus=//p' "$out/poll.tmp") == 0 ]] || exit 98
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
[[ $(stat -c %s "$actual_exit") == 2 && $(< "$actual_exit") == 0 ]] || exit 98
# Require the recipe's original closure and the original observer identity to agree.
for f in admission.json terminal.json observer.identity.json; do
    [[ -f $admission/$f && ! -L $admission/$f && $(stat -c %s "$admission/$f") -le 4194304 ]] || exit 98
    jq -es 'length==1 and (.[0]|type=="object")' "$admission/$f" >/dev/null
done
jq -e --arg sha "$config_sha" --arg recipe "$recipe_sha" '.status=="SOURCE_ADMITTED" and .config_sha256==$sha and .recipe_sha256==$recipe' "$admission/admission.json" >/dev/null
jq -e --arg sha "$config_sha" '.status=="SOURCE_ADMITTED_ROOT_GATE_REQUIRED" and .stage=="closure" and .config_sha256==$sha and .original_exit==0 and .intended_exit==0 and .cleanup_exit==0' "$admission/terminal.json" >/dev/null
jq -e --arg id "$id" --arg cg "/system.slice/$unit" '.invocation_id==$id and .control_group==$cg' "$admission/observer.identity.json" >/dev/null
[[ -f $admission/wrapper.exit && ! -L $admission/wrapper.exit && $(stat -c %s "$admission/wrapper.exit") == 2 && $(< "$admission/wrapper.exit") == 0 ]]
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
for line in "InvocationID=$id" MainPID=0 Result=success ExecMainCode=1 ExecMainStatus=0; do grep -Fx "$line" "$out/before-stop.show" >/dev/null; done
manager systemctl stop "$unit" > "$out/stop.stdout" 2> "$out/stop.stderr"
manager systemctl show "$unit" -p ActiveState -p MainPID > "$out/after-stop.show"
grep -Fx MainPID=0 "$out/after-stop.show" >/dev/null
grep -Fx ActiveState=inactive "$out/after-stop.show" >/dev/null
[[ ! -e $cg/cgroup.events ]] || grep -Fx 'populated 0' "$cg/cgroup.events" > "$out/after-stop.events"
art() {
    local file=$1 sha bytes
    [[ -f $file && ! -L $file ]] || return 98
    sha=$(sha256sum < "$file"); sha=${sha%% *}; bytes=$(stat -c %s "$file")
    [[ $sha =~ ^[0-9a-f]{64}$ && $bytes =~ ^[1-9][0-9]*$ ]] || return 98
    jq -cn --arg path "$file" --arg sha "$sha" --argjson bytes "$bytes" '{path:$path,bytes:$bytes,sha256:$sha}'
}
a=$(art "$admission/admission.json"); t=$(art "$admission/terminal.json"); m=$(art "$admission/closure.sha256"); w=$(art "$admission/wrapper.exit")
manager_art=$(art "$out/manager.show"); exit_art=$(art "$actual_exit"); drain_art=$(art "$out/drain.proof.json")
jq -n --arg recipe "$recipe_sha" --arg config "$config_sha" --arg unit "$unit" --arg id "$id" \
  --argjson a "$a" --argjson t "$t" --argjson m "$m" --argjson w "$w" --argjson manager "$manager_art" --argjson outer "$exit_art" --argjson drain "$drain_art" \
  '{schema:"borsuk-native-pid128-outer-closure-v2",status:"CLOSED",recipe_sha256:$recipe,config_sha256:$config,
    admission_sha256:$a.sha256,terminal_sha256:$t.sha256,manifest_sha256:$m.sha256,wrapper_exit_sha256:$w.sha256,
    unit:$unit,invocation_id:$id,control_group:("/system.slice/"+$unit),manager_show:$manager,outer_exit_file:$outer,drain_proof:$drain,
    actual_outer_exit:0,drained:true}' > "$out/outer-closure.json"
sync -f "$out/outer-closure.json"; sync -f "$out"
