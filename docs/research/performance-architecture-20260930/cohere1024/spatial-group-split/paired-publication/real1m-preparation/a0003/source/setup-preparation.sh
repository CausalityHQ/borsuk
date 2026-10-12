#!/usr/bin/env bash
set -euo pipefail
support=/opt/borsuk-paired-preparation
root=/mnt/borsuk-paired-source-screen
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
cd "$support"
sha256sum --check support.sha256 > package-check.log
systemctl is-active --quiet borsuk-paired-data-shutdown.timer
timeout -k 5 120 apt-get -qq -o DPkg::Lock::Timeout=60 update
timeout -k 5 180 apt-get -qq -y -o DPkg::Lock::Timeout=60 install curl unzip jq time xxd
curl -fsSL --connect-timeout 10 --max-time 120 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
test "$(stat -c %s awscliv2.zip)" = 73022935
printf '%s  awscliv2.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
timeout -k 5 60 unzip -q awscliv2.zip
timeout -k 5 60 ./aws/install
[[ $(aws --version) == aws-cli/2.36.11\ * ]]
test ! -e "$root"
mkdir "$root"
test "$(findmnt -n -o FSTYPE --target "$root")" = ext4
mkdir "$root/bin" "$root/inputs" "$root/inputs/construction" "$root/evidence"
jq -e '.authority_pending==false and .consumer_native_qualified==true and (.roles|length)==5 and (.metadata_roles|length)==6' transport.json > /dev/null
jq -r '(.roles+[.consumer])[]|[.key,.pin.path,(.pin.bytes|tostring),.pin.sha256]|@tsv' transport.json > transport.tsv
deadline=$(( $(date +%s)+900 ))
while IFS=$'\t' read -r key path bytes sha; do
 [[ $path == "$root/inputs/"* || $path == "$root/bin/compare_native_replay" ]]
 [[ $path != *".."* && $sha =~ ^[0-9a-f]{64}$ && $bytes =~ ^[1-9][0-9]*$ ]]
 test ! -e "$path"
 left=$((deadline-$(date +%s)-5)); (( left > 0 )) || exit 124
 (( left <= 180 )) || left=180
 timeout -k 5 "$left" aws --cli-connect-timeout 10 --cli-read-timeout 60 s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$key" "$path.part" > "${path##*/}.get.json"
 test "$(stat -c %s "$path.part")" = "$bytes"
 printf '%s  %s\n' "$sha" "$path.part" | sha256sum -c -
 mv "$path.part" "$path"
 chmod 0400 "$path"
done < transport.tsv
for name in original-approval constructor-config reconstruction radial-parity qualification corrected-closure; do
 cp "$support/$name.json" "$root/inputs/construction/$name.json"
 chmod 0400 "$root/inputs/construction/$name.json"
done
jq -r '.metadata_roles[]|"\(.sha256)  \(.path)"' transport.json > metadata.sha256
sha256sum --check metadata.sha256
cp authority.json "$root/authority.json"
cp preparation.config.json "$root/preparation.config.json"
chmod 0400 "$root/authority.json" "$root/preparation.config.json"
chmod 0500 "$root/bin/compare_native_replay"
test "$(od -An -tx1 -N4 "$root/bin/compare_native_replay")" = ' 7f 45 4c 46'
test ! -e "$root/queries.f32"
test ! -e "$root/truth.u64"
printf '%s\n' PREPARATION_INPUTS_AUTHENTICATED_NO_QUERIES_NO_TRUTH
