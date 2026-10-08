#!/bin/bash
set -euo pipefail
phase=early
root=/mnt/borsuk-retained-s3
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261008/cold-direct-closure-cold-pair-a0001
fallback() {
  printf 'BORSUK_FALLBACK {"schema":"borsuk-retained-s3-publication-fallback-v1","phase":"%s","original_exit":%s,"closeout_exit":%s,"instance_id":"%s","performance_claim":false}\n' "$phase" "$original" "$status" "${instance:-unknown}" > /dev/ttyS0 || true
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
systemd-run --unit=borsuk-retained-s3-shutdown --on-boot=3720s --timer-property=AccuracySec=1s /usr/sbin/shutdown -h now
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
  (cd evidence && find . -type f -print0 | sort -z | xargs -0 sha256sum) > artifacts.sha256 || status=96
  timeout --kill-after=5 30 tar -czf evidence.tar.gz -C evidence . || status=96
  evidence_sha=$(sha256sum evidence.tar.gz | cut -d' ' -f1) || status=96
  evidence_bytes=$(stat -c %s evidence.tar.gz) || status=96
  timeout --kill-after=5 90 aws s3api put-object --bucket "$bucket" --key "$prefix/evidence.tar.gz" --body evidence.tar.gz --if-none-match '*' > evidence-upload.json || status=96
  timeout --kill-after=5 30 aws s3api put-object --bucket "$bucket" --key "$prefix/artifacts.sha256" --body artifacts.sha256 --if-none-match '*' > artifacts-upload.json || status=96
  native_exit=null
  if [[ -f evidence/B.native-exit ]]; then native_exit=$(cat evidence/B.native-exit); elif [[ -f evidence/A.native-exit ]]; then native_exit=$(cat evidence/A.native-exit); fi
  jq -n --arg instance "${instance:-unknown}" --arg phase "$phase" --argjson original "$original" --argjson exit "$status" --argjson native_exit "$native_exit" --arg evidence_sha "$evidence_sha" --argjson evidence_bytes "$evidence_bytes" \
    '{schema:"borsuk-direct-closure-cold-pair-closed-v1",instance_id:$instance,phase:$phase,original_exit:$original,exit:$exit,native_exit:$native_exit,binary_source_commit:"2fb771bdb31f9490a3341bdbfa2b4ad510cfb487",integration_commit:"290958155bcc11bf4b6c90d1fd95a6692f389b09",qualified_source_identity_sha256:"4d04840e2db4487f9e77ef57e5eadef42684ab3d354c1a51c2adc20a8c3474e9",evidence:{bytes:$evidence_bytes,sha256:$evidence_sha},terminal_delivery:"attempted",performance_claim:false}' > terminal.json || status=96
  if timeout --kill-after=5 30 aws s3api put-object --bucket "$bucket" --key "$prefix/terminal.json" --body terminal.json --if-none-match '*' > terminal-upload.json; then
    printf 'BORSUK_TERMINAL_DELIVERED %s\n' "$(sha256sum terminal.json | cut -d' ' -f1)" > /dev/ttyS0 || status=96
  else
    status=96
  fi
  if ((status != 0)); then fallback; fi
  umount "$root/direct-closure-cold-pair-a0001" || true
  /usr/sbin/shutdown -h now
  exit "$status"
}
exec >run.log 2>&1
phase=apt
 timeout --kill-after=30 180 apt-get -qq -o DPkg::Lock::Timeout=120 update
 timeout --kill-after=30 300 apt-get -qq -y -o DPkg::Lock::Timeout=120 install curl unzip jq time tar gzip util-linux
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
deadline_epoch=$((launch_epoch+3720))
(( $(date +%s) < deadline_epoch ))
printf '%s\n' "$deadline_epoch" > evidence/deadline-epoch.txt
systemd-run --unit=borsuk-retained-s3-absolute-shutdown --on-calendar="$(date -u -d "@$deadline_epoch" '+%Y-%m-%d %H:%M:%S UTC')" --timer-property=AccuracySec=1s /usr/sbin/shutdown -h now
systemctl stop borsuk-retained-s3-shutdown.timer
trap finish EXIT
phase=stage
mkdir -p "$root/direct-closure-cold-pair-a0001"
mount -t tmpfs -o size=64M,nosuid,nodev,noexec,mode=700 tmpfs "$root/direct-closure-cold-pair-a0001"
mkdir -p "$root/direct-closure-cold-pair-a0001/A/query-scratch" "$root/direct-closure-cold-pair-a0001/B/query-scratch"
timeout --kill-after=5 90 aws s3 cp "s3://$bucket/research/semantic-router/20261008/cold-direct-closure-qualification-a0002/supplemental/check_cohere_native_baseline" runner --only-show-errors
timeout --kill-after=5 90 aws s3 cp "s3://$bucket/$prefix/inputs/A.config.json" A.config.json --only-show-errors
timeout --kill-after=5 90 aws s3 cp "s3://$bucket/$prefix/inputs/B.config.json" B.config.json --only-show-errors
timeout --kill-after=5 90 aws s3 cp "s3://$bucket/research/semantic-router/20261008/cold-direct-closure-cold-pair-a0001/inputs/stage.sh" stage.sh --only-show-errors
timeout --kill-after=5 90 aws s3api get-object --bucket "$bucket" --key research/semantic-router/20261006/cohere-native-preflight-a0002/retained/prepared/queries.f32 --if-match '"1553426815b5dbf6d916ccd12f1c227a"' queries.f32 > evidence/requests-transfer.json
timeout --kill-after=5 90 aws s3api get-object --bucket "$bucket" --key research/semantic-router/20261006/cohere-native-preflight-a0002/retained/prepared/truth.u64 --if-match '"04fc7819a26ce9b269bd25b23b514607"' truth.u64 > evidence/truth-transfer.json
printf '%s  %s\n' 67c7eb51e3ff6bb9b77c1c9f12cac3b402501b708d91bd6d95ea20b029a8ab0b "$root/runner" cbe42b165fe1029e35f04e51c3a0389ee080183bc12b4adaa551582a3bfdb6ea "$root/A.config.json" f705c9ef2a232013b7c27697f5fbe41969acec409625de519ba18434682ef799 "$root/B.config.json" 8fc1ccc90230508b944def6b08bf3b8d8cda258323fe678fe885e0820d34b0c8 "$root/stage.sh" 8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e "$root/queries.f32" 479064239b698a2b8094c7838b1bb01af6eff5ea6eee4736692971849fdcfb2c "$root/truth.u64" > pins.sha256
sha256sum -c pins.sha256
test "$(stat -c %s runner)" = 16381424
test "$(stat -c %s queries.f32)" = 4096000
test "$(stat -c %s truth.u64)" = 80000
chmod 755 runner
cp A.config.json B.config.json stage.sh pins.sha256 evidence/
touch "$root/FROZEN_DIRECT_CLOSURE_RUNTIME"
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
(( SECONDS <= 900 && $(date +%s)+2460+10+240 <= deadline_epoch )) || exit 95
phase=native
systemd-run --unit=borsuk-retained-s3-gates --wait --pipe -p MemoryMax=512M -p MemorySwapMax=0 -p CPUQuota=100% -p AllowedCPUs=0 -p TasksMax=256 -p RuntimeMaxSec=2460 -p TimeoutStopSec=10 -p KillMode=control-group -p WorkingDirectory="$root" /bin/bash "$root/stage.sh"
phase=complete
