#!/bin/bash
set -euo pipefail
systemd-run --unit=borsuk-retained-shutdown --on-active=1800s /usr/sbin/shutdown -h now
root=/mnt/borsuk-retained-admission
mkdir "$root"
cd "$root"
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261007/cohere-retained-input-canary-a0001-v2
phase=bootstrap
finish() {
  original=$?
  trap - EXIT TERM
  set +e
  mkdir -p evidence
  systemctl stop borsuk-retained-publisher.service borsuk-retained-input-admission.service borsuk-retained-gate.service
  if [[ -d global-after ]]; then cp global-after/* evidence/; fi
  systemctl stop borsuk-retained-admission.slice
  systemctl show borsuk-retained-gate.service -p LoadState -p MainPID -p ActiveState -p SubState -p Result > evidence/global-systemd-after
  cp run.log evidence/bootstrap.log
  cp inputs.json stage-assets.sh run-canary.sh publisher.json baseline.json publication.json evidence/ 2>/dev/null
  if [[ -d global-before ]]; then cp -a global-before evidence/; fi
  if [[ -d transport ]]; then mkdir -p evidence/transport; find transport -maxdepth 1 -type f ! -name '*.part' -exec cp -t evidence/transport {} +; fi
  if [[ -f admission-result.jsonl ]]; then cp admission-result.jsonl evidence/; fi
  status=$original
  if [[ "$original" = 0 ]]; then
    test "$phase" = complete || status=96
    test "$(systemctl show borsuk-retained-gate.service --value -p MainPID)" = 0 || status=96
    slice_after="/sys/fs/cgroup$(cat global-before/slice-cgroup-path)"
    if [[ -d "$slice_after" ]]; then test "$(cat "$slice_after/pids.current")" = 0 || status=96; fi
    test "$(cat evidence/global-memory.swap.peak)" = 0 || status=96
    test "$(cat evidence/global-memory.peak)" -le 1073741824 || status=96
    awk '$1=="oom" || $1=="oom_kill" {seen++; if ($2!=0) bad=1} END {exit (seen!=2 || bad)}' evidence/global-memory.events || status=96
  fi
  token=$(curl -fsS --connect-timeout 2 --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance=$(curl -fsS --connect-timeout 2 --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  (cd evidence && find . -type f -print0 | sort -z | xargs -0 sha256sum) > artifacts.sha256 || status=96
  tar -czf evidence.tar.gz -C evidence . || status=96
  evidence_sha=$(sha256sum evidence.tar.gz | cut -d' ' -f1)
  evidence_bytes=$(stat -c %s evidence.tar.gz)
  timeout --kill-after=5 90 aws s3api put-object --bucket "$bucket" --key "$prefix/evidence.tar.gz" --body evidence.tar.gz --if-none-match '*' > evidence-upload.json || status=96
  timeout --kill-after=5 30 aws s3api put-object --bucket "$bucket" --key "$prefix/artifacts.sha256" --body artifacts.sha256 --if-none-match '*' > artifacts-upload.json || status=96
  state=INVALID
  if [[ "$status" = 0 ]]; then state=NATIVE_STAGE_CANARY_CLOSED; fi
  jq -n --arg instance "$instance" --arg phase "$phase" --arg state "$state" --argjson original "$original" --argjson exit "$status" --arg sha "$evidence_sha" --argjson bytes "$evidence_bytes" '{schema:"borsuk-cohere-retained-input-canary-v1",instance_id:$instance,phase:$phase,status:$state,original_exit:$original,exit:$exit,inputs_sha256:"cb4a475b5987c1562384f73a73692b8e6b7ad9ffb4a5d10d295b7787e7c6d829",evidence:{bytes:$bytes,sha256:$sha},canary:true,performance_claim:false,physical_s3_query:false}' > terminal.json
  timeout --kill-after=5 30 aws s3api put-object --bucket "$bucket" --key "$prefix/terminal.json" --body terminal.json --if-none-match '*'
  { printf 'BORSUK_TERMINAL '; cat terminal.json; } > /dev/ttyS0 2>/dev/null
  shutdown -h now
  exit "$status"
}
trap finish EXIT
trap 'exit 97' TERM
exec > run.log 2>&1
phase=apt
timeout --kill-after=30 180 apt-get -qq -o DPkg::Lock::Timeout=120 update
timeout --kill-after=30 300 apt-get -qq -y -o DPkg::Lock::Timeout=120 install curl unzip jq time tar gzip util-linux
phase=awscli
curl -fsSL --connect-timeout 10 --max-time 180 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
test "$(stat -c %s awscliv2.zip)" = 73022935
printf '%s  awscliv2.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
timeout --kill-after=30 120 unzip -q awscliv2.zip
timeout --kill-after=30 120 ./aws/install
[[ "$(aws --version)" == aws-cli/2.36.11\ * ]]
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
phase=commands
for file in inputs.json stage-assets.sh run-canary.sh; do
  aws --cli-connect-timeout 10 --cli-read-timeout 30 s3api get-object --bucket "$bucket" --key "$prefix/inputs/$file" "$file" > "$file.get.json"
done
printf '%s  %s\n' cb4a475b5987c1562384f73a73692b8e6b7ad9ffb4a5d10d295b7787e7c6d829 inputs.json 70d0770724100003750f9cc58d57b2ef0bd9061f53233edc211f4bbf34c3117d stage-assets.sh 32f2e631e819f9ff31e7dd67124740cb2c7eb3d06dcec393cde0d0a76c9a6e8e run-canary.sh | sha256sum -c -
cat > /run/systemd/system/borsuk-retained-admission.slice <<'UNIT'
[Unit]
Description=Bounded retained native admission
[Slice]
CPUQuota=100%
AllowedCPUs=0
MemoryMax=1G
MemorySwapMax=0
TasksMax=256
UNIT
systemctl daemon-reload
systemctl start borsuk-retained-admission.slice
phase=native-input-admission
systemd-run --unit=borsuk-retained-gate --slice=borsuk-retained-admission.slice --wait --pipe -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=1G -p MemorySwapMax=0 -p TasksMax=256 -p RuntimeMaxSec=900 -p WorkingDirectory="$root" /bin/bash -c '
  set -euo pipefail
  mkdir global-before
  cg=$(awk -F: '\''$1==0 {print $3}'\'' /proc/self/cgroup)
  slice_cg=$(dirname "$cg")
  printf "%s\n" "$slice_cg" > global-before/slice-cgroup-path
  for field in cpu.max memory.max memory.swap.max pids.max cpuset.cpus.effective; do cat "/sys/fs/cgroup$cg/$field" > "global-before/$field"; done
  test "$(cat global-before/cpu.max)" = "100000 100000"
  test "$(cat global-before/memory.max)" = 1073741824
  test "$(cat global-before/memory.swap.max)" = 0
  test "$(cat global-before/pids.max)" = 256
  test "$(cat global-before/cpuset.cpus.effective)" = 0
  bash stage-assets.sh
  bash run-canary.sh
  mkdir global-after
  for field in memory.max memory.peak memory.events memory.swap.max memory.swap.current memory.swap.peak cpu.max pids.max pids.current; do cat "/sys/fs/cgroup$slice_cg/$field" > "global-after/global-$field"; done
  test "$(cat global-after/global-memory.max)" = 1073741824
  test "$(cat global-after/global-cpu.max)" = "100000 100000"
  test "$(cat global-after/global-memory.swap.max)" = 0
  test "$(cat global-after/global-memory.swap.current)" = 0
  test "$(cat global-after/global-memory.swap.peak)" = 0
  test "$(cat global-after/global-memory.peak)" -le 1073741824
  awk '\''$1=="oom" || $1=="oom_kill" {seen++; if ($2!=0) bad=1} END {exit (seen!=2 || bad)}'\'' global-after/global-memory.events
'
phase=complete
