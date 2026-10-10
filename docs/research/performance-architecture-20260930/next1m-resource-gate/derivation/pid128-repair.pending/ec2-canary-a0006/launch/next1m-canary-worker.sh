#!/usr/bin/env bash
# SOURCE ONLY. Causality EC2 bootstrap; real platform programs, no ANN/corpus/GT.
# Transport schema retained solely for the existing root watcher.
# shellcheck disable=SC2329 # finish is invoked by EXIT.
# shellcheck disable=SC2016 # jq programs are intentional single-quoted literals passed through finish_run.
set -Eeuo pipefail
umask 077
export LC_ALL=C AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER='' AWS_MAX_ATTEMPTS=1
bucket=${1:?}; prefix=${2:?}; assets=${3:?}; launch_epoch=${4:?}
[[ $# == 4 && $launch_epoch =~ ^[0-9]{10}$ && $bucket == borsuk-bench-453182569524-euc1 && $prefix == research/* && $assets == research/* ]] || exit 125
root=/var/lib/borsuk-validator
mkdir "$root"
exec > "$root/bootstrap.log" 2>&1
state=INVALID phase=setup test_exit=125 instance_id=UNKNOWN attempted=false parent_id=''
cleanup_deadline=0
bootstrap_sha=$(sha256sum "$0"); bootstrap_sha=${bootstrap_sha%% *}
now=$(date +%s)
(( launch_epoch<=now && now-launch_epoch<500 )) || exit 125
machine_deadline=$((launch_epoch+2400))
setup_deadline=$((SECONDS+500))
setup() {
    local cap=$1; shift
    local left=$((setup_deadline-SECONDS-2)) wall_left
    wall_left=$((machine_deadline-$(date +%s)-200))
    ((left<=wall_left)) || left=$wall_left
    ((left>0)) || return 125
    ((cap<=left)) || cap=$left
    timeout -k 1 "$cap" "$@"
}
finish() {
    local original=$? cleanup=0 unit cg registration identity tag p terminal stop_exit
    trap - EXIT; set +e
    finish_deadline=$((SECONDS+180))
    wall_finish_left=$((effective_shutdown_epoch-$(date +%s)-10))
    ((wall_finish_left>0)) || exit 94
    ((wall_finish_left>=180)) || finish_deadline=$((SECONDS+wall_finish_left))
    cleanup_deadline=$((SECONDS+120))
    ((cleanup_deadline<finish_deadline)) || cleanup_deadline=$((finish_deadline-1))
    finish_run() {
        local cap=$1 left; shift
        left=$((finish_deadline-SECONDS-2)); ((left>0)) || return 94
        ((cap<=left)) || cap=$left
        timeout -k 1 "$cap" "$@"
    }
    # Exact durable identities only; payloads are siblings, not observer children.
    drain() {
        local unit=$1 identity=$2 tag=$3 expected current cg rc=0 left
        [[ $unit =~ ^borsuk-(next1m-canary|pid128-[a-z0-9-]+)\.service$ && -f $identity && ! -L $identity ]] || return 1
        expected=$(sed -n 's/^InvocationID=//p' "$identity")
        [[ $expected =~ ^[0-9a-f]{32}$ ]] || return 1
        cg=/sys/fs/cgroup/system.slice/$unit
        if [[ ! -e $cg && ! -L $cg ]]; then
            # Absence alone cannot exclude a queued start. Require original terminal+stop proof.
            local terminal=${4:-} stop_exit=${5:-}
            if [[ ! -f $stop_exit ]]; then
                # No prior stop: authenticate the still-loaded ORIGINAL terminal before stopping.
                left=$((cleanup_deadline-SECONDS-2)); ((left>6)) || return 1
                timeout -k 1 5 systemctl show "$unit" -p InvocationID -p Description -p MainPID -p ActiveState -p SubState -p ExecMainCode > "$root/$tag.cleanup.terminal.show" || return 1
                current=$(< "$root/$tag.cleanup.terminal.show")
                grep -Fx "InvocationID=$expected" <<< "$current" >/dev/null || return 1
                grep -Fx "Description=$unit" <<< "$current" >/dev/null || return 1
                grep -Fx MainPID=0 <<< "$current" >/dev/null || return 1
                grep -Fx ExecMainCode=1 <<< "$current" >/dev/null || return 1
                grep -Ex 'ActiveState=(active|failed)' <<< "$current" >/dev/null || return 1
                grep -Ex 'SubState=(exited|failed)' <<< "$current" >/dev/null || return 1
                [[ ! -e $cg && ! -L $cg ]] || return 1
                timeout -k 1 5 systemctl stop "$unit" > "$root/$tag.cleanup.stop" 2>&1 || return 1
                [[ ! -e $cg && ! -L $cg ]] || return 1
                printf '%s\n' 'REMOVED_ORIGINAL_TERMINAL_AUTHENTICATED_THEN_STOPPED' > "$root/$tag.cleanup.removed" || return 1
                return 0
            fi
            [[ -f $terminal && ! -L $terminal && -f $stop_exit && ! -L $stop_exit ]] || return 1
            [[ $(stat -c %s "$stop_exit") == 2 && $(< "$stop_exit") == 0 ]] || return 1
            grep -Fx "InvocationID=$expected" "$terminal" >/dev/null || return 1
            grep -Fx "Description=$unit" "$terminal" >/dev/null || return 1
            grep -Fx MainPID=0 "$terminal" >/dev/null || return 1
            grep -Fx ExecMainCode=1 "$terminal" >/dev/null || return 1
            printf '%s\n' 'REMOVED_AFTER_AUTHENTICATED_ORIGINAL_TERMINAL_AND_STOP' > "$root/$tag.cleanup.removed" || return 1
            return 0
        fi
        [[ ! -L $cg && $(realpath -e "$cg") == "$cg" ]] || return 1
        left=$((cleanup_deadline-SECONDS)); ((left>6)) || return 1
        timeout -k 1 5 systemctl show "$unit" -p InvocationID -p Description -p ControlGroup > "$root/$tag.cleanup.show" || return 1
        current=$(< "$root/$tag.cleanup.show")
        grep -Fx "InvocationID=$expected" <<< "$current" >/dev/null || return 1
        grep -Fx "Description=$unit" <<< "$current" >/dev/null || return 1
        grep -Fx "ControlGroup=/system.slice/$unit" <<< "$current" >/dev/null || return 1
        [[ ! -e $cg/cgroup.events ]] || printf '1\n' > "$cg/cgroup.kill" || rc=1
        left=$((cleanup_deadline-SECONDS-2)); ((left>0)) || return 1
        ((left<=10)) || left=10
        timeout -k 1 "$left" systemctl stop "$unit" > "$root/$tag.cleanup.stop" 2>&1 || rc=1
        [[ ! -e $cg/cgroup.events ]] || grep -Fx 'populated 0' "$cg/cgroup.events" > "$root/$tag.cleanup.drained" || rc=1
        return "$rc"
    }
    if [[ $attempted == true ]]; then
        drain borsuk-next1m-canary.service "$root/parent.identity" parent || cleanup=1
    fi
    shopt -s nullglob
    for registration in /mnt/borsuk-pid-evidence/canary/*-launch.unit; do
        [[ -f $registration && ! -L $registration ]] || { cleanup=1; continue; }
        unit=$(< "$registration")
        [[ $unit =~ ^borsuk-pid128-observer-[a-z0-9-]+\.service$ ]] || { cleanup=1; continue; }
        tag=${registration##*/}; tag=${tag%-launch.unit}
        identity=${registration%-launch.unit}-launch.identity
        # Registration without identity remains unproven even if the cgroup is absent.
        drain "$unit" "$identity" "$tag-observer" \
          "/mnt/borsuk-pid-evidence/canary/$tag-manager.txt" \
          "/mnt/borsuk-pid-evidence/canary/$tag-observer.stop.exit" || cleanup=1
    done
    for p in /mnt/borsuk-pid-evidence/canary/*-evidence/phases/*; do
        [[ -f $p/unit && ! -L $p/unit ]] || { cleanup=1; continue; }
        unit=$(< "$p/unit")
        tag=${p%/phases/*}; tag=${tag##*/}; tag=$tag-${p##*/}
        # No invented identity when launch failed before its original receipt.
        terminal=$p/manager.final.txt; stop_exit=$p/manager.stop.exit
        if [[ ! -f $terminal ]]; then terminal=$p/manager.cleanup.txt; stop_exit=$p/cleanup.stop.exit; fi
        drain "$unit" "$p/manager.initial.txt" "$tag-payload" "$terminal" "$stop_exit" || cleanup=1
    done
    ((cleanup==0)) || { state=INVALID; original=94; }
    printf '%s\n' "$original" > "$root/bootstrap.exit" || exit 94
    finish_run 10 jq -n --arg state "$state" --arg phase "$phase" --arg instance "$instance_id" --arg prefix "$prefix" --arg bootstrap "$bootstrap_sha" --argjson original "$original" --argjson test "$test_exit" --argjson cleanup "$cleanup" \
      '{schema:"borsuk-validator-ec2-v1",status:$state,phase:$phase,instance_id:$instance,prefix:$prefix,bootstrap_sha256:$bootstrap,
        bootstrap_exit:$original,test_exit:$test,cleanup_exit:$cleanup,native_execution_status:"SYNTHETIC_COLLECTOR_ONLY_PENDING_ROOT_REPLAY",ann_executed:false,
        real_platform_programs:true,native_cli_usage_only:false,synthetic_chain_metadata:true,performance_claim:false,production_qualification:false}' > "$root/terminal.json" || exit 94
    finish_run 10 sync -f "$root" || exit 94
    finish_run 30 tar -czf /var/lib/borsuk-validator-evidence.tar.gz -C /var/lib borsuk-validator -C /mnt borsuk-pid-evidence || exit 94
    sha=$(finish_run 10 sha256sum /var/lib/borsuk-validator-evidence.tar.gz) || exit 94; sha=${sha%% *}
    bytes=$(finish_run 5 stat -c %s /var/lib/borsuk-validator-evidence.tar.gz) || exit 94
    [[ $sha =~ ^[0-9a-f]{64}$ && $bytes -gt 0 && $bytes -le 268435456 ]] || exit 94
    finish_run 30 aws s3api put-object --bucket "$bucket" --key "$prefix/evidence.tar.gz" --body /var/lib/borsuk-validator-evidence.tar.gz --if-none-match '*' > "$root/upload.json" || exit 94
    finish_run 10 jq --arg sha "$sha" --argjson bytes "$bytes" '.+{evidence_sha256:$sha,evidence_bytes:$bytes}' "$root/terminal.json" > /var/lib/borsuk-validator-terminal.json || exit 94
    finish_run 30 aws s3api put-object --bucket "$bucket" --key "$prefix/terminal.json" --body /var/lib/borsuk-validator-terminal.json --if-none-match '*' || exit 94
    shutdown -h now
    exit "$original"
}
trap finish EXIT
remaining_minutes=$(((machine_deadline-$(date +%s))/60))
((remaining_minutes>0 && remaining_minutes<=40)) || exit 125
effective_shutdown_epoch=$(($(date +%s)+remaining_minutes*60))
shutdown -h +"$remaining_minutes"
setup 180 apt-get update
setup 240 env DEBIAN_FRONTEND=noninteractive apt-get install -y jq curl unzip
token=$(setup 5 curl --fail --silent --show-error -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
instance_id=$(setup 5 curl --fail --silent --show-error -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
[[ $instance_id =~ ^i-[0-9a-f]{17}$ ]]
unset token
setup 60 curl --proto '=https' --max-time 60 --connect-timeout 10 -fsS \
  https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip -o "$root/awscli.zip"
[[ $(stat -c %s "$root/awscli.zip") == 73022935 ]]
printf '%s  %s\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 "$root/awscli.zip" | sha256sum -c -
setup 30 unzip -q "$root/awscli.zip" -d "$root/installer"
setup 30 "$root/installer/aws/install"
aws --version > "$root/aws.version"
grep -E '^aws-cli/2\.36\.11 ' "$root/aws.version"
setup 10 aws s3api put-object --generate-cli-skeleton input > "$root/s3-model.json"
jq -e 'has("IfNoneMatch")' "$root/s3-model.json"
rm "$root/awscli.zip"
rm -r "$root/installer"
command -v jq aws taskset systemd-run > "$root/tools.txt"
systemd --version > "$root/systemd.txt"
phase=transport
support=/mnt/borsuk-platform-support
mkdir "$support" /mnt/borsuk-pool-pid /mnt/borsuk-pid-evidence
for spec in \
  'collector-smoke.sh:14093:de33ff52adee978a9e2b4aa7bb00289460e307f53d5842f4a0b6f92db2f88cf1' \
  'collect-native-chain-outer.sh:10100:d32fae330c753cecfdb9d54141a83580a52417df2974c8d49d3f4b7f1f9f7d75' \
  'observer-command.sh:643:894544d6348c9a69ac34f038e631eeabc7396ec4091626f695b33b2aa5c40685'; do
    name=${spec%%:*}; rest=${spec#*:}; bytes=${rest%%:*}; expected=${rest#*:}
    ((bytes<=131072))
    setup 30 aws s3api get-object --bucket "$bucket" --key "$assets/$expected" "$support/$name" > "$root/$name.download.json"
    [[ $(stat -c %s "$support/$name") == "$bytes" ]]
    printf '%s  %s\n' "$expected" "$support/$name" | sha256sum -c -
    chmod 0500 "$support/$name"
done
phase=canary
(( $(date +%s)+490 <= effective_shutdown_epoch-100 )) || exit 125
[[ $(timeout -k 1 5 systemctl show borsuk-next1m-canary.service -p LoadState --value) == not-found ]] || exit 125
printf '%s\n' borsuk-next1m-canary.service > "$root/parent.unit"
sync -f "$root/parent.unit"; sync -f "$root"
attempted=true
set +e
timeout -k 1 10 systemd-run --expand-environment=no --quiet --unit=borsuk-next1m-canary --description=borsuk-next1m-canary.service --service-type=exec \
  -p RemainAfterExit=yes -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 \
  -p TasksMax=128 -p RuntimeMaxSec=310 -p TimeoutStopSec=10 -p KillMode=control-group -p LimitCORE=0 \
  -p "StandardOutput=append:$root/platform.log" -p "StandardError=append:$root/platform.log" \
  /usr/bin/taskset -c 0 /usr/bin/timeout -k 10 300 /bin/bash "$support/collector-smoke.sh" "$support/collect-native-chain-outer.sh" "$support/observer-command.sh" /mnt/borsuk-pid-evidence/collector-smoke
parent_launch_rc=$?
set -e
printf '%s\n' "$parent_launch_rc" > "$root/parent.launch.exit"
timeout -k 1 5 systemctl show borsuk-next1m-canary.service -p Id -p Description -p InvocationID -p ControlGroup > "$root/parent.identity"
parent_id=$(sed -n 's/^InvocationID=//p' "$root/parent.identity")
[[ $parent_id =~ ^[0-9a-f]{32}$ ]]
grep -Fx Description=borsuk-next1m-canary.service "$root/parent.identity" >/dev/null
grep -Fx ControlGroup=/system.slice/borsuk-next1m-canary.service "$root/parent.identity" >/dev/null
sync -f "$root/parent.identity"; sync -f "$root"
[[ $parent_launch_rc == 0 ]] || exit 94
deadline=$((SECONDS+320))
while :; do
    timeout -k 1 5 systemctl show borsuk-next1m-canary.service -p InvocationID -p ActiveState -p SubState -p MainPID -p Result -p ExecMainCode -p ExecMainStatus > "$root/parent.poll"
    [[ $(sed -n 's/^InvocationID=//p' "$root/parent.poll") == "$parent_id" ]] || exit 125
    if grep -Fx MainPID=0 "$root/parent.poll" >/dev/null && {
        { grep -Fx ActiveState=active "$root/parent.poll" >/dev/null && grep -Fx SubState=exited "$root/parent.poll" >/dev/null; } ||
        { grep -Fx ActiveState=failed "$root/parent.poll" >/dev/null && grep -Fx SubState=failed "$root/parent.poll" >/dev/null; }
    }; then break; fi
    ((SECONDS<deadline)) || { test_exit=124; exit 124; }
    sleep 1
done
cp "$root/parent.poll" "$root/parent.original.show"
test_exit=$(sed -n 's/^ExecMainStatus=//p' "$root/parent.original.show")
[[ $test_exit =~ ^[0-9]+$ ]] || exit 125
[[ $test_exit == 0 ]] || exit "$test_exit"
grep -Fx Result=success "$root/parent.original.show"
grep -Fx ExecMainCode=1 "$root/parent.original.show"
jq -es 'length==1 and .[0].status=="COLLECTOR_MECHANICS_VERIFIED" and .[0].ann_run==false and .[0].performance_claim==false' /mnt/borsuk-pid-evidence/collector-smoke/result.json >/dev/null
phase=closed state=COLLECTOR_SMOKE_ROOT_REPLAY_REQUIRED
exit 0
