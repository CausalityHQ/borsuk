#!/usr/bin/env bash
# Synthetic machinery checks only. No ANN binary or corpus is used.
set -Eeuo pipefail
umask 077
export LC_ALL=C AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
bucket=${1:?}; prefix=${2:?}; assets=${3:?}
[[ $# == 3 && $bucket == borsuk-bench-453182569524-euc1 && $prefix == research/* && $assets == research/* ]] || exit 125
root=/var/lib/borsuk-validator
mkdir "$root"
exec > "$root/bootstrap.log" 2>&1
state=INVALID phase=setup test_exit=125 unit_attempted=false
bootstrap_sha=$(sha256sum "$0"); bootstrap_sha=${bootstrap_sha%% *}
instance_id=UNKNOWN
setup_deadline=$((SECONDS+500))
setup() {
    local cap=$1; shift
    local left=$((setup_deadline-SECONDS-2))
    ((left>0)) || return 125
    ((cap<=left)) || cap=$left
    timeout -k 1 "$cap" "$@"
}
finish() {
    original=$?
    trap - EXIT
    set +e
    if [[ $unit_attempted == true ]]; then
        timeout -k 1 10 systemctl show borsuk-validator-tests.service > "$root/service.before_cleanup" 2>&1
        timeout -k 1 10 systemctl stop borsuk-validator-tests.service > "$root/cleanup.stop" 2>&1
        cg=/sys/fs/cgroup/system.slice/borsuk-validator-tests.service
        if [[ -e $cg/cgroup.events ]] && ! grep -Fx 'populated 0' "$cg/cgroup.events" > "$root/drained.final"; then
            printf '1\n' > "$cg/cgroup.kill"
            for _ in 1 2 3 4 5; do
                [[ ! -e $cg/cgroup.events ]] && break
                grep -Fx 'populated 0' "$cg/cgroup.events" > "$root/drained.final" && break
                sleep 1
            done
        fi
        if [[ -e $cg/cgroup.events ]] && ! grep -Fx 'populated 0' "$cg/cgroup.events" > "$root/drained.final"; then
            printf '%s\n' 'CLEANUP_FAILED_NO_ARCHIVE' > "$root/cleanup.failed"
            shutdown -h now
            exit 94
        fi
    fi
    timeout -k 1 10 systemctl show borsuk-validator-tests.service > "$root/service.final" 2>&1
    printf '%s\n' "$original" > "$root/bootstrap.exit"
    printf '%s\n' "$test_exit" > "$root/test.exit"
    jq -n --arg state "$state" --arg phase "$phase" --arg instance "$instance_id" --arg prefix "$prefix" --arg bootstrap "$bootstrap_sha" --argjson original "$original" --argjson test_exit "$test_exit" \
      '{schema:"borsuk-validator-ec2-v1",status:$state,phase:$phase,instance_id:$instance,prefix:$prefix,bootstrap_sha256:$bootstrap,bootstrap_exit:$original,test_exit:$test_exit,native_executed:false,performance_claim:false}' > "$root/terminal.json" || exit 94
    tar -C "$root" -czf /var/lib/borsuk-validator-evidence.tar.gz . || exit 94
    sha=$(sha256sum /var/lib/borsuk-validator-evidence.tar.gz); sha=${sha%% *}
    bytes=$(stat -c %s /var/lib/borsuk-validator-evidence.tar.gz) || exit 94
    [[ $sha =~ ^[0-9a-f]{64}$ && $bytes -gt 0 && $bytes -le 16777216 ]] || exit 94
    timeout -k 2 30 aws s3api put-object --bucket "$bucket" --key "$prefix/evidence.tar.gz" \
      --body /var/lib/borsuk-validator-evidence.tar.gz --if-none-match '*' > "$root/upload.json" || exit 94
    jq --arg sha "$sha" --argjson bytes "$bytes" '.+{evidence_sha256:$sha,evidence_bytes:$bytes}' "$root/terminal.json" > /var/lib/borsuk-validator-terminal.json || exit 94
    timeout -k 2 30 aws s3api put-object --bucket "$bucket" --key "$prefix/terminal.json" \
      --body /var/lib/borsuk-validator-terminal.json --if-none-match '*' || exit 94
    # Root watcher terminates this exact instance on terminal publication.
    shutdown -h now
    exit "$original"
}
trap finish EXIT
shutdown -h +15
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
for spec in \
  'recipe.sh:ee8327efb4f744d5acc6d8b2665867ca1a3f8a80908e45a83b0a650168baad65' \
  'harness.sh:1e641c01ee224d817f4788232d8a1ea01e054e4a08bed3fc5b9a123de625f2b5'; do
    name=${spec%%:*}; expected=${spec#*:}
    setup 30 aws s3api get-object --bucket "$bucket" --key "$assets/$expected" "$root/$name" > "$root/$name.download.json"
    printf '%s  %s\n' "$expected" "$root/$name" | sha256sum -c -
    if [[ $name == recipe.sh ]]; then expected_bytes=61940; else expected_bytes=9749; fi
    [[ $(stat -c %s "$root/$name") == "$expected_bytes" ]]
    bash -n "$root/$name"
done
cat > "$root/run.sh" <<'RUN'
set -Eeuo pipefail
root=/var/lib/borsuk-validator
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
[[ $cg == /system.slice/borsuk-validator-tests.service ]] || exit 125
c=/sys/fs/cgroup$cg
cat /proc/self/status > "$root/process.status"
for file in cpu.max memory.max memory.swap.max pids.max memory.events pids.events; do
    cat "$c/$file" > "$root/$file.before"
done
[[ $(awk '/^Cpus_allowed_list:/ {print $2}' /proc/self/status) == 0 ]] || exit 125
[[ $(cat "$c/cpu.max") == '100000 100000' ]] || exit 125
[[ $(cat "$c/memory.max") == 268435456 && $(cat "$c/memory.swap.max") == 0 && $(cat "$c/pids.max") == 128 ]] || exit 125
rc=0
bash "$root/harness.sh" "$root/recipe.sh" "$root/cases" || rc=$?
printf '%s\n' "$rc" > "$root/harness.exit"
for file in memory.events pids.events cpu.stat; do cat "$c/$file" > "$root/$file.after"; done
exit "$rc"
RUN
phase=tests
test_exit=0
unit_attempted=true
systemd-run --unit=borsuk-validator-tests --no-block --service-type=exec \
  -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 \
  -p TasksMax=128 -p RuntimeMaxSec=120 -p TimeoutStopSec=5 -p LimitCORE=0 -p PrivateNetwork=yes \
  -p RemainAfterExit=yes -p "StandardOutput=append:$root/test.log" -p "StandardError=append:$root/test.log" \
  /usr/bin/taskset -c 0 /bin/bash "$root/run.sh"
deadline=$((SECONDS+135))
while :; do
    timeout -k 1 5 systemctl show borsuk-validator-tests.service -p ActiveState -p SubState -p MainPID > "$root/service.poll"
    if grep -Fx 'ActiveState=failed' "$root/service.poll" >/dev/null; then break; fi
    if grep -Fx 'ActiveState=active' "$root/service.poll" >/dev/null && grep -Fx 'SubState=exited' "$root/service.poll" >/dev/null && grep -Fx 'MainPID=0' "$root/service.poll" >/dev/null; then break; fi
    ((SECONDS<deadline)) || { test_exit=124; exit 124; }
    sleep 1
done
timeout -k 1 5 systemctl show borsuk-validator-tests.service -p Result -p ExecMainCode -p ExecMainStatus -p ActiveState -p SubState -p MainPID > "$root/service.exit"
test_exit=$(sed -n 's/^ExecMainStatus=//p' "$root/service.exit")
[[ $test_exit =~ ^[0-9]+$ ]] || exit 125
timeout -k 2 10 systemctl stop borsuk-validator-tests.service
[[ ! -e /sys/fs/cgroup/system.slice/borsuk-validator-tests.service/cgroup.events ]] || grep -Fx 'populated 0' /sys/fs/cgroup/system.slice/borsuk-validator-tests.service/cgroup.events > "$root/drained.txt"
((test_exit==0)) || exit "$test_exit"
grep -Fx 'Result=success' "$root/service.exit"
grep -Fx 'ExecMainCode=1' "$root/service.exit"
grep -Fx 'ExecMainStatus=0' "$root/service.exit"
grep -Fx 'MainPID=0' "$root/service.exit"
jq -e '.status=="SYNTHETIC_CLOSURE_CHECKS_ONLY" and .passed==15 and .native_executed==false and .performance_claim==false and .production_qualification==false' "$root/cases/result.json"
phase=closed state=SYNTHETIC_CLOSURE_CHECKS_ONLY
