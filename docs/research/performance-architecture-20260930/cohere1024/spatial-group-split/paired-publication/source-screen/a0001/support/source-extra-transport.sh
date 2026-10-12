#!/bin/bash
set -euo pipefail
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
cd /opt/borsuk-paired-admission
root=/mnt/borsuk-paired-source-screen
jq -r '.roles[]|select(.role=="requests" or .role=="truth" or .role=="sq8")|[.key,.pin.path,(.pin.bytes|tostring),.pin.sha256]|@tsv' source-transport.json > source-extra.tsv
[[ $(wc -l < source-extra.tsv) == 3 ]]
while IFS=$'\t' read -r key path bytes sha; do
 case $path in "$root/inputs/queries.f32"|"$root/inputs/truth.u64"|"$root/inputs/sq8.bin") ;; *) exit 125;; esac
 [[ ! -e $path && ! -L $path && $sha =~ ^[0-9a-f]{64}$ && $key == research/semantic-router/* ]]
 timeout -k 5 300 aws --cli-connect-timeout 10 --cli-read-timeout 60 s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$key" "$path.part" > "${path##*/}.get.json"
 [[ $(stat -c %s "$path.part") == "$bytes" ]]
 printf '%s  %s\n' "$sha" "$path.part" | sha256sum -c -
 mv "$path.part" "$path"
 chmod 0400 "$path"
done < source-extra.tsv
cat transport.tsv source-extra.tsv | awk -F '\t' '{print $4 "  " $2}' > source-inputs.sha256
[[ $(wc -l < source-inputs.sha256) == 33 ]]
sha256sum -c source-inputs.sha256
