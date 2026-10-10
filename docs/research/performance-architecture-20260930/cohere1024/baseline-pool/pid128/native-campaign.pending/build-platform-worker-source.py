from pathlib import Path
import hashlib
D=Path('/tmp/borsuk-pid128-native-campaign-next')
old=Path('/tmp/borsuk-pid128-validator-ec2/worker.sh').read_text()
setup=old[old.index('setup 180 apt-get update'):old.index('phase=transport')]
files={n:D/n for n in ['run-platform-pair.sh','run-platform-canary.sh','collect-stage-outer.sh','observer-command.sh','timeout-descendant.sh']}
files['staging-library.sh']=D/'staging-r2/staging-library.sh'
roster=[(n,len(p.read_bytes()),hashlib.sha256(p.read_bytes()).hexdigest()) for n,p in files.items()]
head='''#!/usr/bin/env bash
# SOURCE ONLY. Causality EC2 bootstrap; real platform programs, no ANN/corpus/GT.
# Transport schema retained solely for the existing root watcher.
# shellcheck disable=SC2329 # finish is invoked by EXIT.
set -Eeuo pipefail
umask 077
export LC_ALL=C AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER='' AWS_MAX_ATTEMPTS=1
bucket=${1:?}; prefix=${2:?}; assets=${3:?}
[[ $# == 3 && $bucket == borsuk-bench-453182569524-euc1 && $prefix == research/* && $assets == research/* ]] || exit 125
root=/var/lib/borsuk-validator
mkdir "$root"
exec > "$root/bootstrap.log" 2>&1
state=INVALID phase=setup test_exit=125 instance_id=UNKNOWN attempted=false parent_id=''
bootstrap_sha=$(sha256sum "$0"); bootstrap_sha=${bootstrap_sha%% *}
setup_deadline=$((SECONDS+500))
setup() {
    local cap=$1; shift
    local left=$((setup_deadline-SECONDS-2)); ((left>0)) || return 125
    ((cap<=left)) || cap=$left
    timeout -k 1 "$cap" "$@"
}
finish() {
    local original=$? cleanup=0 unit id cg
    trap - EXIT; set +e
    # Parent and observers are separate exact owned units; drain all recorded ones.
    if [[ $attempted == true ]]; then
        timeout -k 1 5 systemctl show borsuk-platform-pair.service > "$root/parent.before-cleanup" 2>&1
        id=$(sed -n 's/^InvocationID=//p' "$root/parent.before-cleanup")
        if [[ -n $parent_id && $id == "$parent_id" ]]; then
            timeout -k 1 10 systemctl stop borsuk-platform-pair.service > "$root/parent.stop" 2>&1
            cg=/sys/fs/cgroup/system.slice/borsuk-platform-pair.service
            if [[ -e $cg/cgroup.events ]] && ! grep -Fx 'populated 0' "$cg/cgroup.events" >/dev/null; then
                printf '1\\n' > "$cg/cgroup.kill" || cleanup=1
            fi
            [[ ! -e $cg/cgroup.events ]] || grep -Fx 'populated 0' "$cg/cgroup.events" > "$root/parent.drained" || cleanup=1
        else cleanup=1; fi
    fi
    for label in positive negative; do
        [[ -f /mnt/borsuk-pid-evidence/$label.unit ]] || continue
        unit=$(< /mnt/borsuk-pid-evidence/"$label".unit)
        [[ $unit =~ ^borsuk-pid128-observer-[a-z0-9-]+\\.service$ ]] || { cleanup=1; continue; }
        timeout -k 1 5 systemctl show "$unit" -p InvocationID -p ControlGroup -p ActiveState -p MainPID > "$root/$label.cleanup.show"
        cg=/sys/fs/cgroup/system.slice/$unit
        if [[ ! -e $cg ]]; then continue; fi
        id=''; [[ ! -f /mnt/borsuk-pid-evidence/$label.invocation ]] || id=$(< /mnt/borsuk-pid-evidence/"$label".invocation)
        if [[ $id =~ ^[0-9a-f]{32}$ && $(sed -n 's/^InvocationID=//p' "$root/$label.cleanup.show") == "$id" &&
              $(sed -n 's/^ControlGroup=//p' "$root/$label.cleanup.show") == "/system.slice/$unit" ]]; then
            timeout -k 1 10 systemctl stop "$unit" > "$root/$label.cleanup.stop" 2>&1
            if [[ -e $cg/cgroup.events ]] && ! grep -Fx 'populated 0' "$cg/cgroup.events" >/dev/null; then
                printf '1\\n' > "$cg/cgroup.kill" || cleanup=1
            fi
            [[ ! -e $cg/cgroup.events ]] || grep -Fx 'populated 0' "$cg/cgroup.events" > "$root/$label.cleanup.drained" || cleanup=1
        else cleanup=1; fi
    done
    # Phase units are siblings of observers; killing observers alone is insufficient.
    for label in positive negative; do
        p=/mnt/borsuk-pid-evidence/$label/phases/diagnostic
        [[ -f $p/unit ]] || continue
        unit=$(< "$p/unit")
        [[ $unit =~ ^borsuk-pid128-[a-z0-9-]+-diagnostic\\.service$ ]] || { cleanup=1; continue; }
        cg=/sys/fs/cgroup/system.slice/$unit
        [[ -d $cg ]] || continue
        if [[ ! -f $p/manager.initial.txt ]]; then cleanup=1; continue; fi
        id=$(sed -n 's/^InvocationID=//p' "$p/manager.initial.txt")
        timeout -k 1 5 systemctl show "$unit" -p InvocationID -p Description -p ControlGroup > "$root/$label.payload.cleanup.show"
        if [[ $id =~ ^[0-9a-f]{32}$ && $(sed -n 's/^InvocationID=//p' "$root/$label.payload.cleanup.show") == "$id" &&
              $(sed -n 's/^Description=//p' "$root/$label.payload.cleanup.show") == "$unit" &&
              $(sed -n 's/^ControlGroup=//p' "$root/$label.payload.cleanup.show") == "/system.slice/$unit" ]]; then
            timeout -k 1 10 systemctl stop "$unit" > "$root/$label.payload.cleanup.stop" 2>&1
            if [[ -e $cg/cgroup.events ]] && ! grep -Fx 'populated 0' "$cg/cgroup.events" >/dev/null; then
                printf '1\\n' > "$cg/cgroup.kill" || cleanup=1
            fi
            [[ ! -e $cg/cgroup.events ]] || grep -Fx 'populated 0' "$cg/cgroup.events" > "$root/$label.payload.cleanup.drained" || cleanup=1
        else cleanup=1; fi
    done
    ((cleanup==0)) || { state=INVALID; original=94; }
    printf '%s\\n' "$original" > "$root/bootstrap.exit" || exit 94
    jq -n --arg state "$state" --arg phase "$phase" --arg instance "$instance_id" --arg prefix "$prefix" --arg bootstrap "$bootstrap_sha" --argjson original "$original" --argjson test "$test_exit" --argjson cleanup "$cleanup" \\
      '{schema:"borsuk-validator-ec2-v1",status:$state,phase:$phase,instance_id:$instance,prefix:$prefix,bootstrap_sha256:$bootstrap,
        bootstrap_exit:$original,test_exit:$test,cleanup_exit:$cleanup,native_executed:false,ann_executed:false,
        real_platform_programs:true,performance_claim:false,production_qualification:false}' > "$root/terminal.json" || exit 94
    sync -f "$root" || exit 94
    tar -czf /var/lib/borsuk-validator-evidence.tar.gz -C /var/lib borsuk-validator -C /mnt borsuk-pid-evidence || exit 94
    sha=$(sha256sum /var/lib/borsuk-validator-evidence.tar.gz); sha=${sha%% *}
    bytes=$(stat -c %s /var/lib/borsuk-validator-evidence.tar.gz) || exit 94
    [[ $sha =~ ^[0-9a-f]{64}$ && $bytes -gt 0 && $bytes -le 16777216 ]] || exit 94
    timeout -k 1 30 aws s3api put-object --bucket "$bucket" --key "$prefix/evidence.tar.gz" --body /var/lib/borsuk-validator-evidence.tar.gz --if-none-match '*' > "$root/upload.json" || exit 94
    jq --arg sha "$sha" --argjson bytes "$bytes" '.+{evidence_sha256:$sha,evidence_bytes:$bytes}' "$root/terminal.json" > /var/lib/borsuk-validator-terminal.json || exit 94
    timeout -k 1 30 aws s3api put-object --bucket "$bucket" --key "$prefix/terminal.json" --body /var/lib/borsuk-validator-terminal.json --if-none-match '*' || exit 94
    shutdown -h now
    exit "$original"
}
trap finish EXIT
shutdown -h +18
'''
transport='''phase=transport
support=/mnt/borsuk-platform-support
mkdir "$support" /mnt/borsuk-pool-pid /mnt/borsuk-pid-evidence
for spec in \\
'''+ ' \\\n'.join(f"  '{n}:{b}:{h}'" for n,b,h in roster)+'''; do
    name=${spec%%:*}; rest=${spec#*:}; bytes=${rest%%:*}; expected=${rest#*:}
    setup 30 aws s3api get-object --bucket "$bucket" --key "$assets/$expected" "$support/$name" > "$root/$name.download.json"
    [[ $(stat -c %s "$support/$name") == "$bytes" ]]
    printf '%s  %s\\n' "$expected" "$support/$name" | sha256sum -c -
    chmod 0500 "$support/$name"
done
phase=platform
((SECONDS+350<=840)) || exit 125
attempted=true
systemd-run --quiet --unit=borsuk-platform-pair --description=borsuk-platform-pair.service --service-type=exec \\
  -p RemainAfterExit=yes -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 \\
  -p TasksMax=128 -p RuntimeMaxSec=330 -p TimeoutStopSec=10 -p LimitCORE=0 \\
  -p "StandardOutput=append:$root/platform.log" -p "StandardError=append:$root/platform.log" \\
  /usr/bin/taskset -c 0 /bin/bash "$support/run-platform-pair.sh" "$support"
parent_id=$(setup 5 systemctl show borsuk-platform-pair.service -p InvocationID --value)
[[ $parent_id =~ ^[0-9a-f]{32}$ ]]
deadline=$((SECONDS+345))
while :; do
    timeout -k 1 5 systemctl show borsuk-platform-pair.service -p InvocationID -p ActiveState -p SubState -p MainPID -p Result -p ExecMainCode -p ExecMainStatus > "$root/parent.poll"
    [[ $(sed -n 's/^InvocationID=//p' "$root/parent.poll") == "$parent_id" ]] || exit 125
    if grep -Fx MainPID=0 "$root/parent.poll" >/dev/null && grep -E '^ActiveState=(active|failed)$' "$root/parent.poll" >/dev/null; then break; fi
    ((SECONDS<deadline)) || { test_exit=124; exit 124; }
    sleep 1
done
cp "$root/parent.poll" "$root/parent.original.show"
test_exit=$(sed -n 's/^ExecMainStatus=//p' "$root/parent.original.show")
[[ $test_exit =~ ^[0-9]+$ ]] || exit 125
[[ $test_exit == 0 ]] || exit "$test_exit"
grep -Fx Result=success "$root/parent.original.show"
grep -Fx ExecMainCode=1 "$root/parent.original.show"
jq -es 'length==1 and .[0].status=="PLATFORM_PAIR_ROOT_REPLAY_REQUIRED" and .[0].ann_executed==false and .[0].production_qualification==false' /mnt/borsuk-pid-evidence/platform-pair.json >/dev/null
phase=closed state=PLATFORM_PAIR_ROOT_REPLAY_REQUIRED
exit 0
'''
(D/'platform-worker.sh').write_text(head+setup+transport)
watch=Path('/tmp/borsuk-pid128-validator-ec2/watch.sh').read_text()
assert watch.count('SECONDS+780-age')==1
watch=watch.replace('SECONDS+780-age','SECONDS+1080-age').replace('COLLECTED_SYNTHETIC_EVIDENCE_ONLY','COLLECTED_PLATFORM_EVIDENCE_ONLY')
(D/'platform-watch.sh').write_text(watch)
import json
(D/'platform-support-roster.json').write_text(json.dumps(roster,indent=2)+'\n')
