#!/bin/bash
set -euo pipefail
root=/mnt/borsuk-leaf-metric-data
mkdir "$root"
mkdir "$root/bin" "$root/inputs" "$root/evidence"
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
timeout -k 5 120 apt-get -qq -o DPkg::Lock::Timeout=60 update
timeout -k 5 180 apt-get -qq -y -o DPkg::Lock::Timeout=60 install curl unzip jq time
cd "$root"
curl -fsSL --connect-timeout 10 --max-time 120 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
test "$(stat -c %s awscliv2.zip)" = 73022935
printf '%s  awscliv2.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
timeout -k 5 60 unzip -q awscliv2.zip
timeout -k 5 60 ./aws/install
[[ "$(aws --version)" == aws-cli/2.36.11\ * ]]
# CONFIG/transport bodies are provided by the authenticated SSM wrapper.
cp /tmp/leaf-data-config.json "$root/config.json"
cp /tmp/leaf-data-transport.json "$root/transport.json"
jq -e '.schema=="borsuk-leaf-metric-transport-v1" and (.inputs|length)==8' "$root/transport.json"
jq -r '(.inputs+[.binary])[]|[.key,.pin.path,(.pin.bytes|tostring),.pin.sha256]|@tsv' "$root/transport.json" > "$root/evidence/transport.tsv"
while IFS=$'\t' read -r key path bytes sha; do
 [[ "$path" == "$root/inputs/"* || "$path" == "$root/bin/compare_native_replay" ]]
 test ! -e "$path"
 timeout -k 5 180 aws s3 cp "s3://borsuk-bench-453182569524-euc1/$key" "$path.partial" --only-show-errors
 test "$(stat -c %s "$path.partial")" = "$bytes"
 printf '%s  %s\n' "$sha" "$path.partial" | sha256sum -c -
 mv "$path.partial" "$path"
 chmod 0400 "$path"
done < "$root/evidence/transport.tsv"
chmod 0500 "$root/bin/compare_native_replay"
aws --version > "$root/evidence/aws-version.txt"
systemctl is-active --quiet borsuk-leaf-data-shutdown.timer
printf 'SETUP_DATA_AUTHENTICATED\n'
