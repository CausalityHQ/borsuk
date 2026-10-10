#!/usr/bin/env bash
# SOURCE ONLY / UNVERIFIED. Causality EC2 native PID128 bootstrap.
# New transport schema; independent root watcher and source review required.
# shellcheck disable=SC2329 # finish is invoked by EXIT.
set -Eeuo pipefail
umask 077
export LC_ALL=C AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER='' AWS_MAX_ATTEMPTS=1
bucket=${1:?}; prefix=${2:?}; assets=${3:?}; launched=${4:?}
[[ $# == 4 && $launched =~ ^[1-9][0-9]*$ && $bucket == borsuk-bench-453182569524-euc1 && $prefix == research/* && $assets == research/* ]] || exit 125
root=/var/lib/borsuk-validator
mkdir "$root"
exec > "$root/bootstrap.log" 2>&1
state=INVALID phase=setup test_exit=125 instance_id=UNKNOWN attempted=false parent_id=''
bootstrap_sha=$(sha256sum "$0"); bootstrap_sha=${bootstrap_sha%% *}
setup_deadline=$((launched+900))
machine_deadline=$((launched+18000))
setup() {
    local cap=$1; shift
    local left=$((setup_deadline-$(date +%s)-2)); ((left>0)) || return 125
    ((cap<=left)) || cap=$left
    timeout -k 1 "$cap" "$@"
}
teardown() {
    local cap=$1 left; shift
    left=$((cleanup_deadline-$(date +%s)-2)); ((left>0)) || return 124
    ((cap<=left)) || cap=$left
    timeout -k 1 "$cap" "$@"
}
finish() {
    local original=$? cleanup=0 unit id cg
    trap - EXIT; set +e
    cleanup_deadline=$(( $(date +%s)+90 ))
    # Parent and observers are separate exact owned units; drain all recorded ones.
    if [[ $attempted == true ]]; then
        teardown 5 systemctl show borsuk-native-campaign.service > "$root/parent.before-cleanup" 2>&1
        id=$(sed -n 's/^InvocationID=//p' "$root/parent.before-cleanup")
        if [[ -n $parent_id && $id == "$parent_id" ]]; then
            teardown 10 systemctl stop borsuk-native-campaign.service > "$root/parent.stop" 2>&1 || cleanup=1
            cg=/sys/fs/cgroup/system.slice/borsuk-native-campaign.service
            if [[ -e $cg/cgroup.events ]] && ! grep -Fx 'populated 0' "$cg/cgroup.events" >/dev/null; then
                printf '1\n' > "$cg/cgroup.kill" || cleanup=1
            fi
            [[ ! -e $cg/cgroup.events ]] || grep -Fx 'populated 0' "$cg/cgroup.events" > "$root/parent.drained" || cleanup=1
        else cleanup=1; fi
    fi
    for label in admission staging negative widths; do
        [[ -f /mnt/borsuk-pid-evidence/$label.unit ]] || continue
        unit=$(< /mnt/borsuk-pid-evidence/"$label".unit)
        [[ $unit =~ ^borsuk-pid128-observer-[a-z0-9-]+\.service$ ]] || { cleanup=1; continue; }
        teardown 5 systemctl show "$unit" -p InvocationID -p ControlGroup -p ActiveState -p MainPID > "$root/$label.cleanup.show"
        cg=/sys/fs/cgroup/system.slice/$unit
        if [[ ! -e $cg ]]; then continue; fi
        id=''; [[ ! -f /mnt/borsuk-pid-evidence/$label.invocation ]] || id=$(< /mnt/borsuk-pid-evidence/"$label".invocation)
        if [[ $id =~ ^[0-9a-f]{32}$ && $(sed -n 's/^InvocationID=//p' "$root/$label.cleanup.show") == "$id" &&
              $(sed -n 's/^ControlGroup=//p' "$root/$label.cleanup.show") == "/system.slice/$unit" ]]; then
            teardown 10 systemctl stop "$unit" > "$root/$label.cleanup.stop" 2>&1 || cleanup=1
            if [[ -e $cg/cgroup.events ]] && ! grep -Fx 'populated 0' "$cg/cgroup.events" >/dev/null; then
                printf '1\n' > "$cg/cgroup.kill" || cleanup=1
            fi
            [[ ! -e $cg/cgroup.events ]] || grep -Fx 'populated 0' "$cg/cgroup.events" > "$root/$label.cleanup.drained" || cleanup=1
        else cleanup=1; fi
    done
    # Phase units are siblings of observers; killing observers alone is insufficient.
    for label in admission staging negative widths; do
        for phase_name in prepare derive copy generation publish diagnostic query16 query32; do
        p=/mnt/borsuk-pid-evidence/$label/phases/$phase_name
        [[ -f $p/unit ]] || continue
        unit=$(< "$p/unit")
        [[ $unit =~ ^borsuk-pid128-[a-z0-9-]+-${phase_name}\.service$ ]] || { cleanup=1; continue; }
        cg=/sys/fs/cgroup/system.slice/$unit
        [[ -d $cg ]] || continue
        if [[ ! -f $p/manager.initial.txt ]]; then cleanup=1; continue; fi
        id=$(sed -n 's/^InvocationID=//p' "$p/manager.initial.txt")
        teardown 5 systemctl show "$unit" -p InvocationID -p Description -p ControlGroup > "$root/$label.payload.cleanup.show"
        if [[ $id =~ ^[0-9a-f]{32}$ && $(sed -n 's/^InvocationID=//p' "$root/$label.payload.cleanup.show") == "$id" &&
              $(sed -n 's/^Description=//p' "$root/$label.payload.cleanup.show") == "$unit" &&
              $(sed -n 's/^ControlGroup=//p' "$root/$label.payload.cleanup.show") == "/system.slice/$unit" ]]; then
            teardown 10 systemctl stop "$unit" > "$root/$label.payload.cleanup.stop" 2>&1 || cleanup=1
            if [[ -e $cg/cgroup.events ]] && ! grep -Fx 'populated 0' "$cg/cgroup.events" >/dev/null; then
                printf '1\n' > "$cg/cgroup.kill" || cleanup=1
            fi
            [[ ! -e $cg/cgroup.events ]] || grep -Fx 'populated 0' "$cg/cgroup.events" > "$root/$label.payload.cleanup.drained" || cleanup=1
        else cleanup=1; fi
    done
        done
    ((cleanup==0)) || { state=INVALID; original=94; }
    printf '%s\n' "$original" > "$root/bootstrap.exit" || exit 94
    jq -n --arg state "$state" --arg phase "$phase" --arg instance "$instance_id" --arg prefix "$prefix" --arg bootstrap "$bootstrap_sha" --argjson original "$original" --argjson test "$test_exit" --argjson cleanup "$cleanup" --argjson attempted "$attempted" \
      '{schema:"borsuk-native-pid128-ec2-v1",status:$state,phase:$phase,instance_id:$instance,prefix:$prefix,bootstrap_sha256:$bootstrap,
        bootstrap_exit:$original,test_exit:$test,cleanup_exit:$cleanup,controller_execution_attempted:$attempted,performance_claim:false,production_qualification:false}' > "$root/terminal.json" || exit 94
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
remaining=$((machine_deadline-$(date +%s)))
((remaining>0 && remaining<=18000)) || exit 125
shutdown -h +$((remaining/60))
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
support=/mnt/borsuk-native-support
mkdir "$support" /mnt/borsuk-pool-pid /mnt/borsuk-pid-evidence
for spec in \
  'run-native-pid128-r7.sh:71250:f0273d2353b198030dd9df4718a142a88c009b6e78861f8cf9d5b9f1dd9639d3' \
  'run-staging-native.sh:7419:79cf1dc7949fdd6f7aba834afae66da6695d4b43e46addc59d85dd5a770e7054' \
  'run-cleanup-negative.sh:7009:852eaa6f59ab0e6b57e2eebc3fdd949bb0cfb9fb6f1951584191a36afba6659c' \
  'staging-library.sh:44094:4f2943c8dd73df94fee7d1b7569e6af186424bbeefeec14c47bd9f502f36cc1c' \
  'timeout-descendant.sh:992:1bda564e59d140aa5f28fa82b046d6678504ead9133b262c4513e6e8ce7b4436' \
  'observer-command.sh:643:894544d6348c9a69ac34f038e631eeabc7396ec4091626f695b33b2aa5c40685' \
  'collect-admission-outer.sh:8148:f6e123ee06474a10def135b9121444e09627334f9114942b0282dd3b9c882f79' \
  'collect-stage-outer.sh:14630:ac51d8b718acff95158c83dafd14d1da5a3919f3ed80077c3e071596ffdf5023' \
  'run-finite-campaign.sh:10344:789d42f9aa14e396c544fe4a5d773ddcd0c0f79963ffd45ec578435646682279' \
  'cohort-template.json:1722:d55e2c941d3b10432e291c61a7e9d9d084da99e5e9331face0b9a17104c38820' \
  'derivation-template.json:1025:0a8ea237c350cece31596cb194ecd14c3e003332c9d28b847389bdae87b7906a' \
  'producer-admission.json:2479:f9d3490cf3de71ae837b7f1a47d7907d4bdbb1a707fc947cbf08d7f6fc646984' \
  'baseline-admission.json:598:7ad7d0db4a5e4533aa617f90b9f0d728c0ab0b8e39982f9131e8daf2621dee84' \
  'input-manifest.json:1826:b49dd5d8119b7653c00578855627de5837af8cd9d9f8bc2a2d9f1cf221355bc2' \
  'admission-root-gate.json:352:c42a766a6c8313ba6a13493f946b62a9b71b9a3083be065471e5456bb4d284e8' \
  'admission-config.json:1829:a696c3370a49c5e87cc96ebdc5cae51698f62e0a26db4414cf9e7a38d1cba6b5' \
  'campaign-config.json:1894:faa0346832037c15fc26d5618dcdbf1642162e8ac498c3fc1a80237e1ed8f98e' \
  'transport-manifest.json:3609:29bdf201bca2fc02933583beddd152cbf607ec5854482f7b06dbd1d18ba08ee9' \
  'transport-native-inputs.sh:4831:6bcb6305644d8d015deefdcaa81e20028134fa039a12c9800c9fde488c877c87'; do
    name=${spec%%:*}; rest=${spec#*:}; bytes=${rest%%:*}; expected=${rest#*:}
    setup 30 aws s3api get-object --bucket "$bucket" --key "$assets/$expected" "$support/$name" > "$root/$name.download.json"
    [[ $(stat -c %s "$support/$name") == "$bytes" ]]
    printf '%s  %s\n' "$expected" "$support/$name" | sha256sum -c -
    chmod 0500 "$support/$name"
done
phase=native-input-transport
setup 600 /bin/bash "$support/transport-native-inputs.sh" "$support/transport-manifest.json" 29bdf201bca2fc02933583beddd152cbf607ec5854482f7b06dbd1d18ba08ee9 "$setup_deadline" "$root/transport.json"
phase=native-campaign
(( $(date +%s)+16800+300 <= machine_deadline )) || exit 125
attempted=true
timeout -k 1 10 systemd-run --expand-environment=no --quiet --unit=borsuk-native-campaign --description=borsuk-native-campaign.service --service-type=exec \
  -p RemainAfterExit=yes -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 \
  -p TasksMax=128 -p RuntimeMaxSec=16860 -p TimeoutStopSec=10 -p LimitCORE=0 \
  -p "StandardOutput=append:$root/platform.log" -p "StandardError=append:$root/platform.log" \
  /usr/bin/taskset -c 0 /bin/bash "$support/run-finite-campaign.sh" "$support/campaign-config.json" faa0346832037c15fc26d5618dcdbf1642162e8ac498c3fc1a80237e1ed8f98e
parent_id=$(timeout -k 1 5 systemctl show borsuk-native-campaign.service -p InvocationID --value)
[[ $parent_id =~ ^[0-9a-f]{32}$ ]]
deadline=$(( $(date +%s)+16900 ))
while :; do
    timeout -k 1 5 systemctl show borsuk-native-campaign.service -p InvocationID -p ActiveState -p SubState -p MainPID -p Result -p ExecMainCode -p ExecMainStatus > "$root/parent.poll"
    [[ $(sed -n 's/^InvocationID=//p' "$root/parent.poll") == "$parent_id" ]] || exit 125
    if grep -Fx MainPID=0 "$root/parent.poll" >/dev/null && grep -E '^ActiveState=(active|failed)$' "$root/parent.poll" >/dev/null; then break; fi
    (( $(date +%s)<deadline && $(date +%s)+120<machine_deadline )) || { test_exit=124; exit 124; }
    sleep 1
done
cp "$root/parent.poll" "$root/parent.original.show"
test_exit=$(sed -n 's/^ExecMainStatus=//p' "$root/parent.original.show")
[[ $test_exit =~ ^[0-9]+$ ]] || exit 125
[[ $test_exit == 0 ]] || exit "$test_exit"
grep -Fx Result=success "$root/parent.original.show"
grep -Fx ExecMainCode=1 "$root/parent.original.show"
jq -es 'length==1 and .[0].status=="PID128_MECHANICS_ROOT_REPLAY_REQUIRED" and .[0].performance_claim==false and .[0].cold_claim==false' /mnt/borsuk-pid-evidence/campaign-terminal.json >/dev/null
phase=closed state=PID128_MECHANICS_ROOT_REPLAY_REQUIRED
exit 0
