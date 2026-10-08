#!/bin/bash
set -euo pipefail
systemd-run --unit=borsuk-http-shutdown --on-active=1200s /usr/sbin/shutdown -h now
root=/mnt/borsuk-http
mkdir -p "$root/evidence"
cd "$root"
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261008/cold-two-bit-gather-primitive-a0002
phase=bootstrap
finish() {
 original=$?
 trap - EXIT TERM
 set +e
 systemctl stop borsuk-gather-staging-canary.service
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
 '{schema:"borsuk-two-bit-gather-primitive-v2",instance_id:$instance,phase:$phase,original_exit:$original,exit:$exit,native_exit:$native_exit,candidate_commit:"45456b5168043d5405b7b792ed9360be5e657cfc",elf_sha256:"6ebd599e908ccc94802c8d4c92f647bbbbbb962250f95b48ad4bafe6b0167045",elf_bytes:88558264,native_source_file_count:415,native_source_identity_sha256:"687be03f5a5c1cbbb8834d55a2e02c5818c87493cb8fb0099b04eefa62cb3d7e",evidence:{bytes:$evidence_bytes,sha256:$evidence_sha},scope:"bounded same-ELF ordered two-bit gather primitive; no ANN/corpus/query/truth or cold vendor claim",performance_claim:false}' > terminal.json
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
aws s3 cp "s3://$bucket/research/semantic-router/20261008/cold-two-bit-gather-compile-a0003/supplemental/probe-libtest" probe-libtest --only-show-errors
printf '%s  probe-libtest\n' 6ebd599e908ccc94802c8d4c92f647bbbbbb962250f95b48ad4bafe6b0167045 | sha256sum -c -
test "$(stat -c %s probe-libtest)" = 88558264
chmod 700 probe-libtest
sha256sum probe-libtest > evidence/elf-before.sha256
aws s3 cp "s3://$bucket/$prefix/inputs/probe.sh" probe.sh --only-show-errors
printf '%s  probe.sh\n' 3be45068c501ed217f5e45e2e199ad54e436a1056a883c2cf31d0f4d56323bc0 | sha256sum -c -
cp probe.sh evidence/
aws s3 cp "s3://$bucket/$prefix/inputs/probe-check.py" probe-check.py --only-show-errors
printf '%s  probe-check.py\n' 8c8fa37f39f2aa51768b075d046dad59e47059131327be3395384b4f2469c73e | sha256sum -c -
cp probe-check.py evidence/
aws s3 cp "s3://$bucket/$prefix/inputs/gates.sh" gates.sh --only-show-errors
printf '%s  gates.sh\n' 00937a9459ff199af23217675020683c83f62bfe3f22364c40e17f3c02bc6d4f | sha256sum -c -
cp gates.sh evidence/

aws s3 cp "s3://$bucket/$prefix/inputs/expected-primitive-identity.json" evidence/expected-primitive-identity.json --only-show-errors
printf '%s  evidence/expected-primitive-identity.json\n' bf171b15f32be5cefb9a3956df8ead4336aed237cf7374ee2abfe9b83c688dfe | sha256sum -c -

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
