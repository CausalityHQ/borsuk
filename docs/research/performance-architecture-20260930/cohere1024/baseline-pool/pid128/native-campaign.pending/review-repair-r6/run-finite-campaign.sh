#!/usr/bin/env bash
# SOURCE ONLY: finite local-store PID128 mechanics campaign on causality EC2.
# API: ROOT_CAMPAIGN_CONFIG SHA. No transport/launch/benchmark authority here.
# shellcheck disable=SC2329 # finish is invoked by the EXIT trap.
set -Eeuo pipefail
set -o noclobber
umask 077
export LC_ALL=C
[[ $# == 2 && $EUID == 0 && $2 =~ ^[0-9a-f]{64}$ ]] || exit 98
input=$1 input_sha=$2
[[ -f $input && ! -L $input && $(stat -c %s "$input") -le 65536 && $(realpath -e "$input") == "$input" ]] || exit 98
printf '%s  %s\n' "$input_sha" "$input" | sha256sum -c -
jq -es 'length==1 and (.[0]|type=="object" and keys==["admission_config","deadline_seconds","schema","support"] and
 .schema=="borsuk-native-pid128-campaign-config-v1" and .deadline_seconds==16800 and
 (.support|type=="object" and keys==["admission_collector","library","negative","observer_command","recipe","stage_collector","staging","timeout_fixture"]))' "$input" >/dev/null
sha() { local h; h=$(sha256sum < "$1"); printf '%s\n' "${h%% *}"; }
auth() {
    local path bytes hash
    path=$(jq -er .path <<< "$1"); bytes=$(jq -er .bytes <<< "$1"); hash=$(jq -er .sha256 <<< "$1")
    [[ $path == /* && $(realpath -e "$path") == "$path" && -f $path && ! -L $path &&
       $bytes =~ ^[1-9][0-9]*$ && $hash =~ ^[0-9a-f]{64}$ && $(stat -c %s "$path") == "$bytes" && $(sha "$path") == "$hash" ]] || return 98
}
art() { jq -cn --arg path "$1" --argjson bytes "$(stat -c %s "$1")" --arg sha256 "$(sha "$1")" '{path:$path,bytes:$bytes,sha256:$sha256}'; }
support() { jq -er ".support.$1.path" "$input"; }
for role in recipe staging negative library timeout_fixture observer_command admission_collector stage_collector; do
    auth "$(jq -c ".support.$role" "$input")"
    [[ $(stat -c '%u:%a:%h' "$(support "$role")") == 0:500:1 ]] || exit 98
done
negative_entrypoint=$(support negative)
[[ $(support timeout_fixture) == "${negative_entrypoint%/*}/timeout-descendant.sh" ]] || exit 98
[[ $(support library) == "${negative_entrypoint%/*}/staging-library.sh" &&
   $(support library) == "$(dirname "$(support staging)")/staging-library.sh" ]] || exit 98
auth "$(jq -c .admission_config "$input")"
admission_config=$(jq -er .admission_config.path "$input"); admission_sha=$(jq -er .admission_config.sha256 "$input")
recipe=$(support recipe); recipe_sha=$(sha "$recipe")
[[ $recipe_sha == 40edcc412a842320f1e2abbaa4b743051da46d66c8df67fbf86afc9f028a87fe &&
   $(jq -er .recipe_sha256 "$admission_config") == "$recipe_sha" && $(jq -er .stage "$admission_config") == admission ]] || exit 98
E=$(jq -er .scratch.evidence_root "$admission_config")
[[ $E == /mnt/borsuk-pid-evidence && -d $E && ! -L $E && ! -e $E/campaign.started ]] || exit 98
printf '%s\n' "$input_sha" > "$E/campaign.started"
deadline=$(( $(date +%s)+16800 )); owned_unit='' owned_id=''; phase=admission
manager() {
    local left cap=$1; shift
    left=$((deadline-$(date +%s)-2)); ((left>0)) || return 98
    ((cap<=left)) || cap=$left
    timeout --kill-after=1 "$cap" "$@"
}
finish() {
    local rc=$? cleanup=0
    trap - EXIT; set +e
    # Teardown uses a separate finite safety grace, never a measurement extension.
    deadline=$(( $(date +%s)+45 ))
    if [[ -n $owned_unit ]]; then
        manager 5 systemctl show "$owned_unit" -p InvocationID -p ControlGroup > "$E/failure-owned.show"
        if [[ -n $owned_id && $(sed -n 's/^InvocationID=//p' "$E/failure-owned.show") == "$owned_id" &&
              $(sed -n 's/^ControlGroup=//p' "$E/failure-owned.show") == "/system.slice/$owned_unit" ]]; then
            manager 10 systemctl stop "$owned_unit" > "$E/failure.stop" 2>&1 || cleanup=1
            cg=/sys/fs/cgroup/system.slice/$owned_unit
            if [[ -e $cg/cgroup.events ]] && ! grep -Fx 'populated 0' "$cg/cgroup.events" > "$E/failure.drained"; then
                printf '1\n' > "$cg/cgroup.kill" || cleanup=1
                manager 10 systemctl stop "$owned_unit" > "$E/failure.stop-again" 2>&1 || cleanup=1
            fi
            [[ ! -e $cg/cgroup.events ]] || grep -Fx 'populated 0' "$cg/cgroup.events" > "$E/failure.drained-final" || cleanup=1
        else cleanup=1; fi
    fi
    printf 'original=%s cleanup=%s phase=%s\n' "$rc" "$cleanup" "$phase" > "$E/campaign.exit" || rc=97
    sync -f "$E" || rc=97
    ((cleanup==0)) || exit 97
    ((rc==0)) || exit "$rc"
    exit 0
}
trap finish EXIT
launch_observer() {
    local name=$1 command=$2 config=$3 config_sha=$4 evidence=$5 runtime=$6 token rc
    (( $(date +%s)+runtime+60 < deadline )) || return 98
    token=$(< /proc/sys/kernel/random/uuid); owned_unit=borsuk-pid128-observer-$token-$name.service
    owned_id=''
    [[ $(manager 5 systemctl show "$owned_unit" -p LoadState --value) == not-found ]] || return 98
    printf '%s\n' "$owned_unit" > "$E/$name.unit"
    rc=0
    manager 10 systemd-run --expand-environment=no --quiet --unit="$owned_unit" --description="$owned_unit" --service-type=exec \
      -p RemainAfterExit=yes -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=268435456 -p MemorySwapMax=0 -p TasksMax=128 \
      -p KillMode=control-group -p TimeoutStopSec=10 -p RuntimeMaxSec="$runtime" -p LimitCORE=0 \
      /usr/bin/taskset -c 0 /bin/bash "$(support observer_command)" "$command" "$config" "$config_sha" "$evidence" "$E/$name.actual-exit" \
      > "$E/$name.start.stdout" 2> "$E/$name.start.stderr" || rc=$?
    printf '%s\n' "$rc" > "$E/$name.start.exit"
    owned_id=$(manager 5 systemctl show "$owned_unit" -p InvocationID --value)
    [[ $owned_id =~ ^[0-9a-f]{32}$ && $rc == 0 ]] || return 98
    printf '%s\n' "$owned_id" > "$E/$name.invocation"
}
launch_observer admission "$recipe" "$admission_config" "$admission_sha" "$E/admission" 12720
manager 12800 /bin/bash "$(support admission_collector)" "$owned_unit" "$owned_id" "$E/admission" "$E/admission-outer" "$E/admission.actual-exit" "$recipe_sha" "$admission_sha" "$deadline"
owned_unit='' owned_id=''
admission=$(art "$E/admission/admission.json")
closure=$(jq -n --argjson t "$(art "$E/admission/terminal.json")" --argjson m "$(art "$E/admission/closure.sha256")" --argjson w "$(art "$E/admission/wrapper.exit")" --argjson o "$(art "$E/admission-outer/outer-closure.json")" '{terminal:$t,manifest:$m,wrapper_exit:$w,outer:$o}')
for label in staging negative; do
    phase=$label
    root=/mnt/borsuk-pool-pid/staging-$label
    jq -n --argjson recipe "$(art "$recipe")" --argjson source "$(art "$admission_config")" --argjson admission "$admission" --argjson closure "$closure" --arg root "$root" \
      '{schema:"borsuk-native-pid128-staging-config-v1",recipe:$recipe,source_config:$source,admission:$admission,admission_closure:$closure,staging_root:$root}' > "$E/$label.config.json"
    secs=1100; [[ $label != negative ]] || secs=180
    launch_observer "$label" "$(support "$label")" "$E/$label.config.json" "$(sha "$E/$label.config.json")" "$E/$label" "$secs"
    manager "$((secs+30))" /bin/bash "$(support stage_collector)" "$owned_unit" "$owned_id" "$E/$label" "$E/$label-outer" "$E/$label.actual-exit" "$recipe_sha" "$(sha "$E/$label.config.json")" "$deadline" "$label"
    owned_unit='' owned_id=''
done
phase=negative-proof
# Original external collector binds its independent negative proof by length and SHA.
negative_closure=$E/negative-outer/outer-closure.json
proof=$(jq -er .controlled_negative_proof.path "$negative_closure")
[[ $proof == "$E/negative-outer/negative.proof.json" && -f $proof && ! -L $proof &&
   $(stat -c %s "$proof") == "$(jq -er .controlled_negative_proof.bytes "$negative_closure")" ]] || exit 98
proof_sha=$(sha256sum < "$proof"); proof_sha=${proof_sha%% *}
[[ $proof_sha == "$(jq -er .controlled_negative_proof.sha256 "$negative_closure")" ]] || exit 98
jq -es 'length==1 and (.[0]|.schema=="borsuk-native-pid128-negative-proof-v1" and .status=="EXPECTED_TIMEOUT_AND_DRAIN" and
 .resource_events_unchanged==true and .descendant_alive_after_timeout==true and .drain_before_natural_exit==true and
 .timeout_exit==124 and .minimum_timeout_centiseconds==500 and .elapsed_centiseconds>=500 and .elapsed_centiseconds<6000 and .performance_claim==false)' "$proof" >/dev/null
# Fresh deterministic receipt binds the actual two independent stage closures; no performance-dependent choice.
jq -n --argjson admission "$admission" --argjson staging "$(art "$E/staging-outer/outer-closure.json")" --argjson negative "$(art "$E/negative-outer/outer-closure.json")" \
  '{schema:"borsuk-native-pid128-staging-root-v1",status:"PASS",admission:$admission,staging:$staging,controlled_negative:$negative,performance_claim:false}' > "$E/staging-root.json"
phase=widths-freeze
jq -n --arg recipe "$recipe_sha" --arg staging "$(sha "$E/staging-root.json")" --argjson admission "$admission" --argjson closure "$closure" \
  '{schema:"borsuk-native-pid128-root-gate-v2",status:"PASS",stage:"widths",recipe_sha256:$recipe,staging_receipt_sha256:$staging,
    admission_sha256:$admission.sha256,admission_terminal_sha256:$closure.terminal.sha256,admission_closure_sha256:$closure.manifest.sha256,admission_outer_sha256:$closure.outer.sha256}' > "$E/widths-gate.json"
jq --argjson gate "$(art "$E/widths-gate.json")" --argjson admission "$admission" --argjson closure "$closure" \
  '.stage="widths" | .root_gate=$gate | .admission=$admission | .admission_closure=$closure' "$admission_config" > "$E/widths-config.json"
sync -f "$E"
phase=widths
launch_observer widths "$recipe" "$E/widths-config.json" "$(sha "$E/widths-config.json")" "$E/widths" 2460
manager 2500 /bin/bash "$(support stage_collector)" "$owned_unit" "$owned_id" "$E/widths" "$E/widths-outer" "$E/widths.actual-exit" "$recipe_sha" "$(sha "$E/widths-config.json")" "$deadline" widths
owned_unit='' owned_id=''
# Recheck all original immutable support/config inputs; final external EC2 closure still required.
for role in recipe staging negative library timeout_fixture observer_command admission_collector stage_collector; do auth "$(jq -c ".support.$role" "$input")"; done
auth "$(jq -c .admission_config "$input")"
[[ $(sha "$input") == "$input_sha" ]] || exit 98
phase=closed
jq -n --arg source "$input_sha" '{schema:"borsuk-native-pid128-campaign-terminal-v1",status:"PID128_MECHANICS_ROOT_REPLAY_REQUIRED",config_sha256:$source,performance_claim:false,cold_claim:false,competitor_claim:false,one_million_claim:false}' > "$E/campaign-terminal.json"
exit 0
