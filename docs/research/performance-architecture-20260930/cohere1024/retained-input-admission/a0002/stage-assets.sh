#!/bin/bash
# One-shot opaque S3 staging. No corpus, request, truth or result decoding.
set -euo pipefail
export AWS_DEFAULT_REGION=eu-central-1 AWS_MAX_ATTEMPTS=1 AWS_RETRY_MODE=standard
root=/mnt/borsuk-retained-admission
bucket=borsuk-bench-453182569524-euc1
cd "$root"
printf '%s  inputs.json\n' cb4a475b5987c1562384f73a73692b8e6b7ad9ffb4a5d10d295b7787e7c6d829 | sha256sum -c -
mkdir transport bin
part=
trap 'status=$?; if [[ -n "$part" ]]; then rm -f -- "$part"; fi; printf "%s\n" "$status" > transport/staging-exit; exit "$status"' EXIT
jq -r '.objects[] | [.key,.path,.bytes,.sha256] | @tsv' inputs.json > transport/assets.tsv
printf '%s\t%s\t%s\t%s\n' \
  research/semantic-router/20261006/cohere-preflight-binaries/ce43842caeea9dbb722f3497b237265e71c7d829cbaf0243a81a2a352fcf1221/baseline bin/A 6665104 ce43842caeea9dbb722f3497b237265e71c7d829cbaf0243a81a2a352fcf1221 \
  research/semantic-router/20261006/exact-sq8-runtime-gates-a0001/artifacts/binaries/check_cohere_native_baseline bin/B 6764512 3911839ba9ef68604e2c487d802a3b3e125bdc1121aee9af268db3ba8a9ca8cf \
  research/semantic-router/20261006/retained-generation-rebind-gates-a0008/artifacts/binaries/publish_two_bit_generation bin/publisher 6323912 733cf976db05b8482ce192251ad25ec203fae9aec4cf8cf519f1dc6658bf0806 >> transport/assets.tsv
ordinal=0
while IFS=$'\t' read -r key path bytes sha; do
  ordinal=$((ordinal+1))
  test ! -e "$path" && test ! -L "$path"
  mkdir -p -- "$(dirname "$path")"
  aws --cli-connect-timeout 10 --cli-read-timeout 120 s3api head-object --bucket "$bucket" --key "$key" > "transport/$ordinal.head.json"
  test "$(jq -r .ContentLength "transport/$ordinal.head.json")" = "$bytes"
  etag=$(jq -er '.ETag | select(type=="string" and length>2)' "transport/$ordinal.head.json")
  part="transport/$ordinal.part"
  aws --cli-connect-timeout 10 --cli-read-timeout 120 s3api get-object --bucket "$bucket" --key "$key" --if-match "$etag" --range "bytes=0-$bytes" "$part" > "transport/$ordinal.get.json"
  test "$(jq -r .ContentLength "transport/$ordinal.get.json")" = "$bytes"
  test "$(jq -r .ContentRange "transport/$ordinal.get.json")" = "bytes 0-$((bytes-1))/$bytes"
  test "$(stat -c %s "transport/$ordinal.part")" = "$bytes"
  printf '%s  %s\n' "$sha" "transport/$ordinal.part" | sha256sum -c -
  ln -- "transport/$ordinal.part" "$path"
  rm -- "transport/$ordinal.part"
  part=
done < transport/assets.tsv
test "$ordinal" = 18
chmod 0500 bin/A bin/B bin/publisher
printf '%s\n' STAGING_AUTHENTICATED > transport/complete
