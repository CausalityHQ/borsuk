#!/bin/bash
set -euo pipefail
systemd-run --unit=borsuk-http-shutdown --on-active=1200s /usr/sbin/shutdown -h now
root=/mnt/borsuk-http
mkdir -p "$root/evidence"
cd "$root"
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261008/cold-page-auth-native-gates-a0002
phase=bootstrap
finish() {
 original=$?
 trap - EXIT TERM
 set +e
 systemctl stop borsuk-auth-probe.service
 systemctl stop borsuk-auth-rerun-gates.service
 systemctl show borsuk-auth-probe.service -p LoadState -p MainPID -p ActiveState -p SubState -p ExecMainStatus -p Result > evidence/probe-systemd-after.txt
 systemctl show borsuk-auth-rerun-gates.service -p LoadState -p MainPID -p ActiveState -p SubState -p ExecMainStatus -p Result > evidence/gates-systemd-after.txt
 systemctl stop borsukauth.slice
 systemctl show borsukauth.slice -p ActiveState -p SubState > evidence/global-systemd-after.txt
 cp run.log evidence/bootstrap.log
 token=$(curl -fsS --connect-timeout 2 --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
 instance=$(curl -fsS --connect-timeout 2 --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
 status=$original
 (cd evidence && find . -type f -print0 | sort -z | xargs -0 sha256sum) > artifacts.sha256 || status=96
 tar -czf evidence.tar.gz -C evidence . || status=96
 evidence_sha=$(sha256sum evidence.tar.gz | cut -d' ' -f1)
 evidence_bytes=$(stat -c %s evidence.tar.gz)
 timeout --kill-after=5 90 aws s3 cp evidence.tar.gz "s3://$bucket/$prefix/evidence.tar.gz" --only-show-errors || status=96
 timeout --kill-after=5 30 aws s3 cp artifacts.sha256 "s3://$bucket/$prefix/artifacts.sha256" --only-show-errors || status=96
 native_exit=null
 if [[ -f evidence/final-exit ]]; then native_exit=$(cat evidence/final-exit); fi
 jq -n --arg instance "$instance" --arg phase "$phase" --argjson original "$original" --argjson exit "$status" --argjson native_exit "$native_exit" --arg evidence_sha "$evidence_sha" --argjson evidence_bytes "$evidence_bytes" \
 '{schema:"borsuk-page-auth-backend-receipt-repair-v1",instance_id:$instance,phase:$phase,original_exit:$original,exit:$exit,native_exit:$native_exit,candidate_commit:"7256ec9b57a6774a1be425990865e35e4fc3c5d9",elf_sha256:"e5028e38842734f1ecb362587ca21582a67721e1805cf38904ecb691f7f63c56",elf_bytes:88071816,native_source_file_count:415,native_source_identity_sha256:"6f07aa976298719e5fa6a642043bad0afe6a1e8f8106fd61eb46fec6c627d663",evidence:{bytes:$evidence_bytes,sha256:$evidence_sha},scope:"same exact native verifier; pre-exec unit receipt repair",performance_claim:false}' > terminal.json
 timeout --kill-after=5 30 aws s3 cp terminal.json "s3://$bucket/$prefix/terminal.json" --only-show-errors
 { printf 'BORSUK_TERMINAL '; cat terminal.json; } >/dev/ttyS0 2>/dev/null
 shutdown -h now
 exit "$status"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
phase=apt
 timeout --kill-after=30 180 apt-get -qq -o DPkg::Lock::Timeout=120 update
 timeout --kill-after=30 300 apt-get -qq -y -o DPkg::Lock::Timeout=120 install curl unzip jq time tar gzip util-linux binutils python3.12
phase=awscli
curl -fsSL --connect-timeout 10 --max-time 180 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  awscliv2.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
test "$(stat -c %s awscliv2.zip)" = 73022935
timeout --kill-after=30 120 unzip -q awscliv2.zip
timeout --kill-after=30 120 ./aws/install
[[ "$(aws --version)" == aws-cli/2.36.11\ * ]]

phase=inputs
aws s3 cp "s3://$bucket/research/semantic-router/20261008/cold-page-auth-native-gates-a0001/supplemental/probe-libtest" probe-libtest --only-show-errors
printf '%s  probe-libtest\n' e5028e38842734f1ecb362587ca21582a67721e1805cf38904ecb691f7f63c56 | sha256sum -c -
test "$(stat -c %s probe-libtest)" = 88071816
chmod 700 probe-libtest
sha256sum probe-libtest > evidence/elf-before.sha256
aws s3 cp "s3://$bucket/$prefix/inputs/probe.sh" probe.sh --only-show-errors
printf '%s  probe.sh\n' 39e87f98c1a0d5014cdfa310b4ea03f1b809049e4243b1de2b318508a2d34cf9 | sha256sum -c -
cp probe.sh evidence/
aws s3 cp "s3://$bucket/$prefix/inputs/probe-check.py" probe-check.py --only-show-errors
printf '%s  probe-check.py\n' 233a120946c97dd52f98a047a7e21fc3b3ade7cc004090aa0c1d5e8019cb4b9e | sha256sum -c -
cp probe-check.py evidence/
aws s3 cp "s3://$bucket/$prefix/inputs/gates.sh" gates.sh --only-show-errors
printf '%s  gates.sh\n' 5a0b21e5d72c712da5d0b6ce5e73d5537e2e084a47cdf222d21e142700a022aa | sha256sum -c -
cp gates.sh evidence/

phase=probe
cat > /etc/systemd/system/borsukauth.slice <<'SLICE'
[Unit]
Description=Same verifier receipt repair aggregate
[Slice]
CPUQuota=200%
AllowedCPUs=0,1
MemoryMax=8G
MemorySwapMax=0
TasksMax=512
SLICE
systemctl daemon-reload
systemctl start borsukauth.slice
systemd-run --unit=borsuk-auth-rerun-gates --slice=borsukauth.slice --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p CPUQuota=200% -p AllowedCPUs=0,1 -p TasksMax=512 -p RuntimeMaxSec=300 -p WorkingDirectory="$root" /bin/bash "$root/gates.sh"
phase=complete
