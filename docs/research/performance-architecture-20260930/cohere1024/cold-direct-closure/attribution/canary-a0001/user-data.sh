#!/bin/bash
set -euo pipefail
phase=early
root=/mnt/borsuk-retained-s3
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261009/sq8-attribution-overhead-canary-a0001
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
systemd-run --unit=borsuk-retained-s3-shutdown --on-boot=9600s --timer-property=AccuracySec=1s /usr/sbin/shutdown -h now
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
  if [[ -f evidence/last-native-exit ]]; then native_exit=$(cat evidence/last-native-exit); fi
  jq -n --arg instance "${instance:-unknown}" --arg phase "$phase" --argjson original "$original" --argjson exit "$status" --argjson native_exit "$native_exit" --arg evidence_sha "$evidence_sha" --argjson evidence_bytes "$evidence_bytes" \
    '{schema:"borsuk-sq8-attribution-overhead-canary-closed-v1",instance_id:$instance,phase:$phase,original_exit:$original,exit:$exit,native_exit:$native_exit,binary_source_commit:"516ee0fd1faf79498ebf2818accd7985c6549f62",integration_commit:"fd408f6f6868cc38c0208f94ad27cadd4dd0b769",qualified_source_identity_sha256:"ac878008affeaab7115e463038b68a59ef220feea865f195c0920d42d394af77",evidence:{bytes:$evidence_bytes,sha256:$evidence_sha},terminal_delivery:"attempted",performance_claim:false}' > terminal.json || status=96
  if timeout --kill-after=5 30 aws s3api put-object --bucket "$bucket" --key "$prefix/terminal.json" --body terminal.json --if-none-match '*' > terminal-upload.json; then
    printf 'BORSUK_TERMINAL_DELIVERED %s\n' "$(sha256sum terminal.json | cut -d' ' -f1)" > /dev/ttyS0 || status=96
  else
    status=96
  fi
  if ((status != 0)); then fallback; fi
  umount "$root/sq8-attribution-canary-a0001" || true
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
deadline_epoch=$((launch_epoch+9600))
(( $(date +%s) < deadline_epoch ))
printf '%s\n' "$deadline_epoch" > evidence/deadline-epoch.txt
systemd-run --unit=borsuk-retained-s3-absolute-shutdown --on-calendar="$(date -u -d "@$deadline_epoch" '+%Y-%m-%d %H:%M:%S UTC')" --timer-property=AccuracySec=1s /usr/sbin/shutdown -h now
systemctl stop borsuk-retained-s3-shutdown.timer
trap finish EXIT
phase=stage
mkdir -p "$root/sq8-attribution-canary-a0001"
mount -t tmpfs -o size=64M,nosuid,nodev,noexec,mode=700 tmpfs "$root/sq8-attribution-canary-a0001"
mkdir -p "$root/sq8-attribution-canary-a0001/A_first-untraced/query-scratch"
mkdir -p "$root/sq8-attribution-canary-a0001/A_first-trace/query-scratch"
mkdir -p "$root/sq8-attribution-canary-a0001/B_first-untraced/query-scratch"
mkdir -p "$root/sq8-attribution-canary-a0001/B_first-trace/query-scratch"
mkdir -p "$root/sq8-attribution-canary-a0001/B_last-untraced/query-scratch"
mkdir -p "$root/sq8-attribution-canary-a0001/B_last-trace/query-scratch"
mkdir -p "$root/sq8-attribution-canary-a0001/A_last-untraced/query-scratch"
mkdir -p "$root/sq8-attribution-canary-a0001/A_last-trace/query-scratch"
timeout --kill-after=5 90 aws s3 cp "s3://$bucket/research/semantic-router/20261009/sq8-attribution-qualification-a0001/supplemental/check_cohere_native_baseline" runner --only-show-errors
timeout --kill-after=5 90 aws s3 cp "s3://$bucket/$prefix/inputs/stage.sh" stage.sh --only-show-errors
timeout --kill-after=5 90 aws s3 cp "s3://$bucket/$prefix/inputs/A_last-untraced.config.json" A_last-untraced.config.json --only-show-errors
timeout --kill-after=5 90 aws s3 cp "s3://$bucket/$prefix/inputs/B_first-untraced.config.json" B_first-untraced.config.json --only-show-errors
timeout --kill-after=5 90 aws s3 cp "s3://$bucket/$prefix/inputs/A_first-trace.config.json" A_first-trace.config.json --only-show-errors
timeout --kill-after=5 90 aws s3 cp "s3://$bucket/$prefix/inputs/B_last-trace.config.json" B_last-trace.config.json --only-show-errors
timeout --kill-after=5 90 aws s3 cp "s3://$bucket/$prefix/inputs/A_last-trace.config.json" A_last-trace.config.json --only-show-errors
timeout --kill-after=5 90 aws s3 cp "s3://$bucket/$prefix/inputs/B_first-trace.config.json" B_first-trace.config.json --only-show-errors
timeout --kill-after=5 90 aws s3 cp "s3://$bucket/$prefix/inputs/B_last-untraced.config.json" B_last-untraced.config.json --only-show-errors
timeout --kill-after=5 90 aws s3 cp "s3://$bucket/$prefix/inputs/A_first-untraced.config.json" A_first-untraced.config.json --only-show-errors
timeout --kill-after=5 90 aws s3api get-object --bucket "$bucket" --key research/semantic-router/20261006/cohere-native-preflight-a0002/retained/prepared/queries.f32 --if-match '"1553426815b5dbf6d916ccd12f1c227a"' queries.f32 > evidence/requests-transfer.json
timeout --kill-after=5 90 aws s3api get-object --bucket "$bucket" --key research/semantic-router/20261006/cohere-native-preflight-a0002/retained/prepared/truth.u64 --if-match '"04fc7819a26ce9b269bd25b23b514607"' truth.u64 > evidence/truth-transfer.json
cat > pins.sha256 <<'PINS'
e39b0bfd18a6a98c2830046ae2d0bbe386991bd86812fed9810fa3d8232fb836  /mnt/borsuk-retained-s3/runner
2f63e2812acfcf39770986b0b07ea36e883ad056821d7f4f136ca259ad048aca  /mnt/borsuk-retained-s3/stage.sh
b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0  /mnt/borsuk-retained-s3/A_last-untraced.config.json
587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f  /mnt/borsuk-retained-s3/B_first-untraced.config.json
97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936  /mnt/borsuk-retained-s3/A_first-trace.config.json
9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d  /mnt/borsuk-retained-s3/B_last-trace.config.json
7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425  /mnt/borsuk-retained-s3/A_last-trace.config.json
dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411  /mnt/borsuk-retained-s3/B_first-trace.config.json
3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc  /mnt/borsuk-retained-s3/B_last-untraced.config.json
2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3  /mnt/borsuk-retained-s3/A_first-untraced.config.json
8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e  /mnt/borsuk-retained-s3/queries.f32
479064239b698a2b8094c7838b1bb01af6eff5ea6eee4736692971849fdcfb2c  /mnt/borsuk-retained-s3/truth.u64
PINS
sha256sum -c pins.sha256
test "$(stat -c %s runner)" = 16843992
test "$(stat -c %s queries.f32)" = 4096000
test "$(stat -c %s truth.u64)" = 80000
chmod 755 runner
cp stage.sh A_last-untraced.config.json B_first-untraced.config.json A_first-trace.config.json B_last-trace.config.json A_last-trace.config.json B_first-trace.config.json B_last-untraced.config.json A_first-untraced.config.json pins.sha256 evidence/
touch "$root/FROZEN_SQ8_ATTRIBUTION_CANARY"
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
(( SECONDS <= 900 && $(date +%s)+8100+10+240 <= deadline_epoch )) || exit 95
phase=native
systemd-run --unit=borsuk-retained-s3-gates --wait --pipe -p MemoryMax=512M -p MemorySwapMax=0 -p CPUQuota=100% -p AllowedCPUs=0 -p TasksMax=256 -p RuntimeMaxSec=8100 -p TimeoutStopSec=10 -p KillMode=control-group -p WorkingDirectory="$root" /bin/bash "$root/stage.sh"
phase=complete
