#!/bin/bash
set -euo pipefail
phase=early
root=/mnt/borsuk-retained-s3
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261008/cold-cpu-profile-a0001
fallback() {
  printf 'BORSUK_FALLBACK {"schema":"borsuk-cold-cpu-profile-fallback-v1","phase":"%s","original_exit":%s,"closeout_exit":%s,"instance_id":"%s","performance_claim":false}\n' "$phase" "$original" "$status" "${instance:-unknown}" > /dev/ttyS0 || true
}
early_finish() {
  original=$?
  trap - EXIT TERM
  set +e
  status=$original
  ((status != 0)) || status=96
  fallback
  /usr/sbin/shutdown -h now
  exit "$status"
}
trap early_finish EXIT
trap 'exit 97' TERM
systemd-run --unit=borsuk-retained-s3-shutdown --on-boot=3540s --timer-property=AccuracySec=1s /usr/sbin/shutdown -h now
mkdir -p "$root/evidence"
cd "$root"
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
finish() {
  original=$?
  trap - EXIT TERM
  set +e
  status=$original
  if ! timeout --kill-after=5 10 systemctl stop borsuk-retained-s3-gates.service; then
    if ! timeout --kill-after=5 5 systemctl show borsuk-retained-s3-gates.service -p LoadState -p MainPID -p ActiveState > evidence/stop-failure-state.txt; then
      status=96
    elif ! grep -qx 'LoadState=not-found' evidence/stop-failure-state.txt || ! grep -qx 'MainPID=0' evidence/stop-failure-state.txt || ! grep -qx 'ActiveState=inactive' evidence/stop-failure-state.txt; then
      status=96
    fi
  fi
  timeout --kill-after=5 5 systemctl show borsuk-retained-s3-gates.service -p LoadState -p MainPID -p ActiveState -p SubState -p ExecMainStatus -p Result > evidence/systemd-after.txt || status=96
  cp run.log evidence/bootstrap.log || status=96
  [[ ${instance:-} =~ ^i-[0-9a-f]{17}$ ]] || status=96
  (cd evidence && find . -type f ! -name perf.data -print0 | sort -z | xargs -0 sha256sum) > artifacts.sha256 || status=96
  timeout --kill-after=5 120 tar --exclude=./perf.data -czf evidence.tar.gz -C evidence . || status=96
  evidence_sha=$(sha256sum evidence.tar.gz | cut -d' ' -f1) || status=96
  evidence_bytes=$(stat -c %s evidence.tar.gz) || status=96
  timeout --kill-after=5 180 aws s3api put-object --bucket "$bucket" --key "$prefix/evidence.tar.gz" --body evidence.tar.gz --if-none-match '*' > evidence-upload.json || status=96
  timeout --kill-after=5 30 aws s3api put-object --bucket "$bucket" --key "$prefix/artifacts.sha256" --body artifacts.sha256 --if-none-match '*' > artifacts-upload.json || status=96
  native_exit=null
  if [[ -f evidence/native-exit ]]; then native_exit=$(cat evidence/native-exit); fi
  jq -n --arg instance "${instance:-unknown}" --arg phase "$phase" --argjson original "$original" --argjson exit "$status" --argjson native_exit "$native_exit" --arg evidence_sha "$evidence_sha" --argjson evidence_bytes "$evidence_bytes" \
    '{schema:"borsuk-cold-cpu-profile-v1",instance_id:$instance,phase:$phase,original_exit:$original,exit:$exit,native_exit:$native_exit,binary_source_commit:"fc8a23dac44ad635fc1236736df68878690a8389",integration_commit:"2eb3645fecac120d5349e676758708318d891e04",qualified_source_identity_sha256:"0097af81fbf3e61a2050a77cb046adfbf0b02a745e73a2181e924cd2cb697eae",evidence:{bytes:$evidence_bytes,sha256:$evidence_sha},terminal_delivery:"attempted",performance_claim:false}' > terminal.json || status=96
  if timeout --kill-after=5 30 aws s3api put-object --bucket "$bucket" --key "$prefix/terminal.json" --body terminal.json --if-none-match '*' > terminal-upload.json; then
    printf 'BORSUK_TERMINAL_DELIVERED %s\n' "$(sha256sum terminal.json | cut -d' ' -f1)" > /dev/ttyS0 || status=96
  else
    status=96
  fi
  if ((status != 0)); then fallback; fi
  umount "$root/membership-admission-a0001/query-scratch" || true
  /usr/sbin/shutdown -h now
  exit "$status"
}
exec >run.log 2>&1
phase=apt
 timeout --kill-after=30 180 apt-get -qq -o DPkg::Lock::Timeout=120 update
 timeout --kill-after=30 300 apt-get -qq -y -o DPkg::Lock::Timeout=120 install curl unzip jq time tar gzip util-linux
phase=perf-install
test "$(uname -r)" = 7.0.0-1013-aws
timeout --kill-after=30 180 apt-get -qq -y -o DPkg::Lock::Timeout=120 install linux-tools-common=6.8.0-146.146 "linux-tools-$(uname -r)=7.0.0-1013.13~24.04.1"
dpkg-query -W linux-tools-common "linux-tools-$(uname -r)" > evidence/perf-packages.txt
phase=awscli
curl -fsSL --connect-timeout 10 --max-time 180 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  awscliv2.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
test "$(stat -c %s awscliv2.zip)" = 73022935
timeout --kill-after=30 120 unzip -q awscliv2.zip
timeout --kill-after=30 120 ./aws/install
[[ "$(aws --version)" == aws-cli/2.36.11\ * ]]
phase=identity
token=$(curl -fsS --connect-timeout 2 --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
curl -fsS --connect-timeout 2 --max-time 5 --max-filesize 65536 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/dynamic/instance-identity/document > evidence/instance-identity.json
instance=$(jq -er .instanceId evidence/instance-identity.json)
[[ $instance =~ ^i-[0-9a-f]{17}$ ]]
launch_epoch=$(date -ud "$(jq -er .pendingTime evidence/instance-identity.json)" +%s)
deadline_epoch=$((launch_epoch+3540))
(( $(date +%s) < deadline_epoch ))
printf '%s\n' "$deadline_epoch" > evidence/deadline-epoch.txt
systemd-run --unit=borsuk-retained-s3-absolute-shutdown --on-calendar="$(date -u -d "@$deadline_epoch" '+%Y-%m-%d %H:%M:%S UTC')" --timer-property=AccuracySec=1s /usr/sbin/shutdown -h now
systemctl stop borsuk-retained-s3-shutdown.timer
trap finish EXIT
phase=stage
mkdir -p "$root/membership-admission-a0001/query-scratch"
mount -t tmpfs -o size=64M,nosuid,nodev,noexec,mode=700 tmpfs "$root/membership-admission-a0001/query-scratch"
timeout --kill-after=5 90 aws s3 cp "s3://$bucket/research/semantic-router/20261008/cold-membership-native-gates-a0003/supplemental/check_cohere_native_baseline" runner --only-show-errors
timeout --kill-after=5 90 aws s3 cp "s3://$bucket/research/semantic-router/20261008/cold-membership-runtime-admission-a0001/inputs/config.json" config.json --only-show-errors
timeout --kill-after=5 90 aws s3 cp "s3://$bucket/research/semantic-router/20261008/cold-cpu-profile-a0001/inputs/stage.sh" stage.sh --only-show-errors
timeout --kill-after=5 90 aws s3api get-object --bucket "$bucket" --key research/semantic-router/20261006/cohere-native-preflight-a0002/retained/prepared/queries.f32 --if-match '"1553426815b5dbf6d916ccd12f1c227a"' queries.f32 > evidence/requests-transfer.json
timeout --kill-after=5 90 aws s3api get-object --bucket "$bucket" --key research/semantic-router/20261006/cohere-native-preflight-a0002/retained/prepared/truth.u64 --if-match '"04fc7819a26ce9b269bd25b23b514607"' truth.u64 > evidence/truth-transfer.json
timeout --kill-after=5 90 aws s3 cp "s3://$bucket/research/semantic-router/20261008/cold-membership-native-gates-a0003/supplemental/compare_native_replay" reducer --only-show-errors
timeout --kill-after=5 90 aws s3 cp "s3://$bucket/research/semantic-router/20261008/cold-cpu-profile-a0001/inputs/reduction-template.json" reduction-template.json --only-show-errors
printf '%s  %s\n' ca1ae2496e110494e7f6c03ae16eea3d5ff262d66945a3c52775749ac0bcaeab "$root/runner" e604a1ca02644329113e6c11a935d67115f784c40008859966f7e332fae53a3a "$root/config.json" 3b4b438b9014d28e930cbfa342fc6621f92474b3d0852817a8f2d786f77093c4 "$root/stage.sh" 8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e "$root/queries.f32" 479064239b698a2b8094c7838b1bb01af6eff5ea6eee4736692971849fdcfb2c "$root/truth.u64" 614f2c2afcd7bacf9a87a850147de2ebfd2f9e712219ce8d6438e388b1a50519 "$root/reducer" cc9460f5f6ba8229318f103cb038f9dc081cefa6125cb747ae03798b8390a9d3 "$root/reduction-template.json" > pins.sha256
sha256sum -c pins.sha256
test "$(stat -c %s runner)" = 16470968
test "$(stat -c %s queries.f32)" = 4096000
test "$(stat -c %s truth.u64)" = 80000
test "$(stat -c %s reducer)" = 1235680
chmod 755 runner reducer
cp config.json reduction-template.json stage.sh pins.sha256 evidence/
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
(( SECONDS <= 900 && $(date +%s)+2100+10+390 <= deadline_epoch )) || exit 95
phase=native
systemd-run --unit=borsuk-retained-s3-gates --wait --pipe -p MemoryMax=512M -p MemorySwapMax=0 -p CPUQuota=100% -p AllowedCPUs=0 -p TasksMax=256 -p RuntimeMaxSec=2100 -p TimeoutStopSec=10 -p KillMode=control-group -p WorkingDirectory="$root" /bin/bash "$root/stage.sh"
phase=complete
