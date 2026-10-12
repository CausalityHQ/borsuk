#!/bin/bash
set -euo pipefail
umask 077
root=/mnt/borsuk-s3v
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261012/s3-vectors-scale-service-canary-a0002
mkdir "$root"
cd "$root"
mkdir evidence inputs bin private state
exec >evidence/bootstrap.log 2>&1
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
systemd-run --unit=borsuk-s3v-scale-canary-shutdown --on-active=1800s /usr/sbin/shutdown -h now
finish() {
 original=$?
 trap - EXIT TERM
 set +e
 cg=/sys/fs/cgroup/system.slice/borsuk-s3v-scale-canary.service
 drain_status=0
 if [[ -d $cg ]]; then
  timeout -k 2 35 systemctl stop borsuk-s3v-scale-canary.service
  drain_status=$?
 fi
 timeout -k 2 10 systemctl show borsuk-s3v-scale-canary.service -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup > evidence/systemd-after.txt
 drained=false
 cg=/sys/fs/cgroup/system.slice/borsuk-s3v-scale-canary.service
 if [[ ! -d $cg ]] || grep -qx 'populated 0' "$cg/cgroup.events"; then drained=true; fi
 rm -f -- "$root/private/credentials" "$root/private/config" "$root/private/export.json"
 printf '%s\n' "$original" > evidence/bootstrap.exit
 # Exact exclusion of raw inputs and private credentials. Original byte receipts retained.
 tar --exclude=state/scale-canary/canonical.bin --exclude=state/scale-canary/queries.f32 --exclude=state/scale-canary/validation-truth.u64 -czf evidence.tar.gz evidence state config.json config.sha256 host-identity.json
 archive_status=$?
 if (( archive_status == 0 )); then
  archive_sha=$(sha256sum evidence.tar.gz | cut -d' ' -f1)
  archive_bytes=$(stat -c %s evidence.tar.gz)
  timeout -k 2 90 aws s3api put-object --bucket "$bucket" --key "$prefix/evidence.tar.gz" --body evidence.tar.gz --if-none-match '*' > evidence-upload.json
  upload_status=$?
 else upload_status=1; archive_sha=''; archive_bytes=0; fi
 final=$original
 (( drain_status == 0 && archive_status == 0 && upload_status == 0 )) && [[ $drained == true ]] || final=1
 jq -n --argjson original "$original" --argjson final "$final" --argjson drained "$drained" --arg sha "$archive_sha" --argjson bytes "$archive_bytes" '{schema:"borsuk-s3v-scale-service-canary-terminal-v1",original_exit:$original,exit:$final,owned_cgroup_drained:$drained,evidence:{sha256:$sha,bytes:$bytes},claims_quality:false,claims_performance:false}' > terminal.json
 terminal_status=$?
 if (( terminal_status == 0 )); then timeout -k 2 30 aws s3api put-object --bucket "$bucket" --key "$prefix/terminal.json" --body terminal.json --if-none-match '*' > terminal-upload.json; terminal_status=$?; fi
 (( terminal_status == 0 )) || final=1
 exit "$final"
}
trap finish EXIT
trap 'exit 97' TERM
export DEBIAN_FRONTEND=noninteractive
timeout -k 5 120 apt-get -qq -o DPkg::Lock::Timeout=60 update
timeout -k 5 180 apt-get -qq -y -o DPkg::Lock::Timeout=60 install curl unzip jq time
curl -fsSL --connect-timeout 10 --max-time 180 -o aws.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  aws.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
[[ $(stat -c %s aws.zip) == 73022935 ]]
timeout -k 5 90 unzip -q aws.zip
timeout -k 5 90 ./aws/install
fetch() {
 local key=$1 path=$2 bytes=$3 sha=$4
 timeout -k 5 180 aws s3api get-object --bucket "$bucket" --key "$key" "$path" > "evidence/$(basename "$path").get.json"
 [[ $(stat -c %s "$path") == "$bytes" ]]
 printf '%s  %s\n' "$sha" "$path" | sha256sum -c -
}
# Root freezes these hashes before staging. No placeholder may reach launch.
fetch "$prefix/inputs/support.sha256" support.sha256 258 69ced6ea7c5f682b91c7f950450a94dff3f5f198bff49768e6e386b73aff70fa
for name in config.pending.json transport-pins.tsv run-native-canary.sh; do
 timeout -k 2 30 aws s3api get-object --bucket "$bucket" --key "$prefix/inputs/$name" "$name" > "evidence/$name.get.json"
done
sha256sum -c support.sha256
fetch research/semantic-router/20261012/s3-vectors-scale-native-qualification-a0003/stages/release-elf/check_s3_vectors_baseline bin/check_s3_vectors_baseline 19516600 e350154fe7edc63cf1e13674ac0886e63ebdbfd20edeef6a53d2cbac6a9c51ae
chmod 500 bin/check_s3_vectors_baseline
while IFS=$'\t' read -r role bytes sha key target; do
 [[ $role == role ]] && continue
 [[ $role == canonical || $role == queries || $role == truth ]]
 fetch "$key" "$target" "$bytes" "$sha"
done < transport-pins.tsv
token=$(curl -fsS --connect-timeout 2 --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
instance=$(curl -fsS --connect-timeout 2 --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
[[ $instance =~ ^i-[0-9a-f]{17}$ ]]
jq -n --arg instance "$instance" '{instance_id:$instance,claims_quality:false,claims_performance:false}' > host-identity.json
jq --arg instance "$instance" '.host_identity=$instance' config.pending.json > config.json
sha256sum config.json | cut -d' ' -f1 > config.sha256
aws configure export-credentials --format process > private/export.json
expiry=$(jq -er .Expiration private/export.json)
[[ $(date -d "$expiry" +%s) -gt $(( $(date +%s) + 960 )) ]]
jq -er '"[causality]\naws_access_key_id = " + .AccessKeyId + "\naws_secret_access_key = " + .SecretAccessKey + "\naws_session_token = " + .SessionToken' private/export.json > private/credentials
printf '[profile causality]\nregion = eu-central-1\n' > private/config
rm -- private/export.json
systemd-run --unit=borsuk-s3v-scale-canary --wait --pipe -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=512M -p MemorySwapMax=0 -p TasksMax=128 -p LimitCORE=0 -p KillMode=control-group -p TimeoutStopSec=30 -p RuntimeMaxSec=900 /bin/bash "$root/run-native-canary.sh"
