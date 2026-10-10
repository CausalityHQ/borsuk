#!/usr/bin/env bash
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
            timeout -k 1 10 systemctl stop borsuk-platform-pair.service > "$root/parent.stop" 2>&1 || cleanup=1
            cg=/sys/fs/cgroup/system.slice/borsuk-platform-pair.service
            if [[ -e $cg/cgroup.events ]] && ! grep -Fx 'populated 0' "$cg/cgroup.events" >/dev/null; then
                printf '1\n' > "$cg/cgroup.kill" || cleanup=1
            fi
            [[ ! -e $cg/cgroup.events ]] || grep -Fx 'populated 0' "$cg/cgroup.events" > "$root/parent.drained" || cleanup=1
        else cleanup=1; fi
    fi
    for label in positive negative; do
        [[ -f /mnt/borsuk-pid-evidence/$label.unit ]] || continue
        unit=$(< /mnt/borsuk-pid-evidence/"$label".unit)
        [[ $unit =~ ^borsuk-pid128-observer-[a-z0-9-]+\.service$ ]] || { cleanup=1; continue; }
        timeout -k 1 5 systemctl show "$unit" -p InvocationID -p ControlGroup -p ActiveState -p MainPID > "$root/$label.cleanup.show"
        cg=/sys/fs/cgroup/system.slice/$unit
        if [[ ! -e $cg ]]; then continue; fi
        id=''; [[ ! -f /mnt/borsuk-pid-evidence/$label.invocation ]] || id=$(< /mnt/borsuk-pid-evidence/"$label".invocation)
        if [[ $id =~ ^[0-9a-f]{32}$ && $(sed -n 's/^InvocationID=//p' "$root/$label.cleanup.show") == "$id" &&
              $(sed -n 's/^ControlGroup=//p' "$root/$label.cleanup.show") == "/system.slice/$unit" ]]; then
            timeout -k 1 10 systemctl stop "$unit" > "$root/$label.cleanup.stop" 2>&1 || cleanup=1
            if [[ -e $cg/cgroup.events ]] && ! grep -Fx 'populated 0' "$cg/cgroup.events" >/dev/null; then
                printf '1\n' > "$cg/cgroup.kill" || cleanup=1
            fi
            [[ ! -e $cg/cgroup.events ]] || grep -Fx 'populated 0' "$cg/cgroup.events" > "$root/$label.cleanup.drained" || cleanup=1
        else cleanup=1; fi
    done
    # Phase units are siblings of observers; killing observers alone is insufficient.
    for label in positive negative; do
        p=/mnt/borsuk-pid-evidence/$label/phases/diagnostic
        [[ -f $p/unit ]] || continue
        unit=$(< "$p/unit")
        [[ $unit =~ ^borsuk-pid128-[a-z0-9-]+-diagnostic\.service$ ]] || { cleanup=1; continue; }
        cg=/sys/fs/cgroup/system.slice/$unit
        [[ -d $cg ]] || continue
        if [[ ! -f $p/manager.initial.txt ]]; then cleanup=1; continue; fi
        id=$(sed -n 's/^InvocationID=//p' "$p/manager.initial.txt")
        timeout -k 1 5 systemctl show "$unit" -p InvocationID -p Description -p ControlGroup > "$root/$label.payload.cleanup.show"
        if [[ $id =~ ^[0-9a-f]{32}$ && $(sed -n 's/^InvocationID=//p' "$root/$label.payload.cleanup.show") == "$id" &&
              $(sed -n 's/^Description=//p' "$root/$label.payload.cleanup.show") == "$unit" &&
              $(sed -n 's/^ControlGroup=//p' "$root/$label.payload.cleanup.show") == "/system.slice/$unit" ]]; then
            timeout -k 1 10 systemctl stop "$unit" > "$root/$label.payload.cleanup.stop" 2>&1 || cleanup=1
            if [[ -e $cg/cgroup.events ]] && ! grep -Fx 'populated 0' "$cg/cgroup.events" >/dev/null; then
                printf '1\n' > "$cg/cgroup.kill" || cleanup=1
            fi
            [[ ! -e $cg/cgroup.events ]] || grep -Fx 'populated 0' "$cg/cgroup.events" > "$root/$label.payload.cleanup.drained" || cleanup=1
        else cleanup=1; fi
    done
    ((cleanup==0)) || { state=INVALID; original=94; }
    printf '%s\n' "$original" > "$root/bootstrap.exit" || exit 94
    jq -n --arg state "$state" --arg phase "$phase" --arg instance "$instance_id" --arg prefix "$prefix" --arg bootstrap "$bootstrap_sha" --argjson original "$original" --argjson test "$test_exit" --argjson cleanup "$cleanup" \
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
  'run-platform-pair.sh:5152:c8d66709c10498ecd625dc37dbb86ee8f30c1508e2ba6f7cb21af038eac7d932' \
  'run-platform-canary.sh:3342:b8ed4707013e32a77f87435c4813cd2bee6e57087202f5a95b03f992807da8af' \
  'collect-stage-outer.sh:14630:ac51d8b718acff95158c83dafd14d1da5a3919f3ed80077c3e071596ffdf5023' \
  'observer-command.sh:643:894544d6348c9a69ac34f038e631eeabc7396ec4091626f695b33b2aa5c40685' \
  'timeout-descendant.sh:992:1bda564e59d140aa5f28fa82b046d6678504ead9133b262c4513e6e8ce7b4436' \
  'staging-library.sh:42398:b2dc0740e5c643576fb6c18ef6630cefef9fc052c1a9e3e59310d78428f40c95'; do
    name=${spec%%:*}; rest=${spec#*:}; bytes=${rest%%:*}; expected=${rest#*:}
    setup 30 aws s3api get-object --bucket "$bucket" --key "$assets/$expected" "$support/$name" > "$root/$name.download.json"
    [[ $(stat -c %s "$support/$name") == "$bytes" ]]
    printf '%s  %s\n' "$expected" "$support/$name" | sha256sum -c -
    chmod 0500 "$support/$name"
done
phase=platform
((SECONDS+350<=840)) || exit 125
attempted=true
systemd-run --expand-environment=no --quiet --unit=borsuk-platform-pair --description=borsuk-platform-pair.service --service-type=exec \
  -p RemainAfterExit=yes -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 \
  -p TasksMax=128 -p RuntimeMaxSec=330 -p TimeoutStopSec=10 -p LimitCORE=0 \
  -p "StandardOutput=append:$root/platform.log" -p "StandardError=append:$root/platform.log" \
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
