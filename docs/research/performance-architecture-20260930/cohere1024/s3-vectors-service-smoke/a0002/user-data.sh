#!/bin/bash
set -euo pipefail
umask 077
root=/mnt/borsuk-s3-vectors-canary
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261007/s3-vectors-sdk-canary-a0002
mkdir -p "$root/evidence" "$root/private" "$root/inputs"
cd "$root"
exec >evidence/bootstrap.log 2>&1
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
systemd-run --unit=borsuk-sdk-canary-shutdown --on-active=1800s /usr/sbin/shutdown -h now
finish() {
  original=$?
  trap - EXIT TERM
  set +e
  systemctl stop borsuk-sdk-canary.service
  systemctl show borsuk-sdk-canary.service -p ActiveState -p SubState -p MainPID -p Result -p ExecMainStatus > evidence/systemd-after.txt
  rm -f private/credentials private/config private/export.json
  printf '%s\n' "$original" > evidence/bootstrap.exit
  items=(evidence)
  for item in state config.json config.sha256 host-identity.json; do
    test ! -e "$item" || items+=("$item")
  done
  tar -czf evidence.tar.gz "${items[@]}"
  hash=$(sha256sum evidence.tar.gz | cut -d' ' -f1)
  bytes=$(stat -c %s evidence.tar.gz)
  native=null
  test ! -f evidence/native.exit || native=$(cat evidence/native.exit)
  timeout --kill-after=5 90 aws s3 cp evidence.tar.gz "s3://$bucket/$prefix/evidence.tar.gz" --only-show-errors
  uploaded=$?
  jq -n --arg instance "${instance:-unknown}" --arg hash "$hash" --argjson bytes "$bytes" --argjson original "$original" --argjson native "$native" --argjson upload "$uploaded" '{schema:"borsuk-s3-vectors-sdk-canary-v1",instance_id:$instance,original_exit:$original,native_exit:$native,upload_exit:$upload,evidence:{sha256:$hash,bytes:$bytes},synthetic:true,performance_claim:false}' > terminal.json
  timeout --kill-after=5 30 aws s3 cp terminal.json "s3://$bucket/$prefix/terminal.json" --only-show-errors
  shutdown -h now
  exit "$original"
}
trap finish EXIT
trap 'exit 97' TERM
export DEBIAN_FRONTEND=noninteractive
timeout --kill-after=30 180 apt-get -qq update
timeout --kill-after=30 300 apt-get -qq -y install curl unzip jq time
curl -fsSL --connect-timeout 10 --max-time 180 -o aws.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  aws.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
test "$(stat -c %s aws.zip)" = 73022935
timeout --kill-after=30 120 unzip -q aws.zip
timeout --kill-after=30 120 ./aws/install
token=$(curl -fsS --connect-timeout 2 --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
instance=$(curl -fsS --connect-timeout 2 --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
jq -n --arg instance "$instance" '{instance_id:$instance,synthetic:true}' > host-identity.json
for name in config.json config.sha256 run-sdk-canary.sh staging.sha256; do
  timeout --kill-after=5 90 aws s3 cp "s3://$bucket/$prefix/inputs/$name" "$name" --only-show-errors
done
timeout --kill-after=5 90 aws s3 cp "s3://$bucket/research/semantic-router/20261007/s3-vectors-native-gates-a0001/supplemental/check_s3_vectors_baseline" check_s3_vectors_baseline --only-show-errors
for name in corpus.f32 queries.f32 truth.u64; do
  timeout --kill-after=5 90 aws s3 cp "s3://$bucket/research/semantic-router/20261007/s3-vectors-sdk-canary-fixture/inputs/$name" "inputs/$name" --only-show-errors
done
printf '%s  staging.sha256\n' 9ba19b31ff7c20bc0d9bcf5f5e490b4ec3920f1824e48e1125de4f2abc484e12 | sha256sum -c -
sha256sum --check staging.sha256
printf '%s  check_s3_vectors_baseline\n' 6884da8048c01b2018844b6cd0d4355d9b0fb3b1495edcbfa4efd7691c2456c1 > binary.sha256
chmod 755 check_s3_vectors_baseline
aws configure export-credentials --format process > private/export.json
expiry=$(jq -er .Expiration private/export.json)
test "$(date -d "$expiry" +%s)" -gt "$(( $(date +%s) + 900 ))"
jq -er '"[causality]\naws_access_key_id = " + .AccessKeyId + "\naws_secret_access_key = " + .SecretAccessKey + "\naws_session_token = " + .SessionToken' private/export.json > private/credentials
printf '[profile causality]\nregion = eu-central-1\n' > private/config
rm private/export.json
systemd-run --unit=borsuk-sdk-canary --wait --pipe -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=512M -p MemorySwapMax=0 -p TasksMax=64 -p RuntimeMaxSec=600 /bin/bash "$root/run-sdk-canary.sh"
