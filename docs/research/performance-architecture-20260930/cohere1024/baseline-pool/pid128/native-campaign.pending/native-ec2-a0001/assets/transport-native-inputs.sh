#!/usr/bin/env bash
# EC2-only opaque transport. No decoding, native execution or quality claim.
# API: authenticated manifest, SHA, absolute Unix deadline, NEW receipt.
set -Eeuo pipefail
umask 077
export LC_ALL=C AWS_DEFAULT_REGION=eu-central-1 AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
[[ $# == 4 && $EUID == 0 && $2 =~ ^[0-9a-f]{64}$ && $3 =~ ^[1-9][0-9]*$ ]] || exit 125
manifest=$1 expected=$2 deadline=$3 receipt=$4
[[ -f $manifest && ! -L $manifest && $(realpath -e "$manifest") == "$manifest" && $(stat -c %s "$manifest") -le 65536 ]] || exit 125
printf '%s  %s\n' "$expected" "$manifest" | sha256sum -c -
[[ $receipt == /var/lib/borsuk-validator/transport.json && ! -e $receipt && ! -L $receipt ]] || exit 125
jq -es 'length==1 and (.[0]|.schema=="borsuk-pid128-native-transport-draft-v1" and .status=="ROOT_TRANSPORT_LOCATORS_FROZEN" and
 .region=="eu-central-1" and .native_executed==false and .launch_authority==false and
 (.assets|length==7) and ([.assets[].path]|sort==[
 "/mnt/borsuk-pool-pid/assets/bin/build_sq8_source",
 "/mnt/borsuk-pool-pid/assets/bin/build_two_bit_generation",
 "/mnt/borsuk-pool-pid/assets/bin/check_cohere_native_baseline",
 "/mnt/borsuk-pool-pid/assets/bin/prepare_cohere_native_cohort",
 "/mnt/borsuk-pool-pid/assets/bin/publish_two_bit_generation",
 "/mnt/borsuk-pool-pid/input/en/0000.parquet",
 "/mnt/borsuk-pool-pid/input/en/0001.parquet"]) and
 all(.assets[]; (.bytes|type=="number" and floor==. and .>0 and .<=216746705) and
 (.sha256|type=="string" and test("^[0-9a-f]{64}$")) and (.kind=="https" or .kind=="s3")))' "$manifest" >/dev/null
root=/mnt/borsuk-pool-pid
[[ -d $root && ! -L $root && $(realpath -e "$root") == "$root" && ! -e $root/prepared-parent ]] || exit 125
mkdir -p "$root/input/en" "$root/assets/bin"
partial=''
finish() { local rc=$?; trap - EXIT; [[ -z $partial ]] || rm -f -- "$partial"; exit "$rc"; }
trap finish EXIT
for i in 0 1 2 3 4 5 6; do
    row=$(jq -c ".assets[$i]" "$manifest")
    path=$(jq -er .path <<< "$row"); bytes=$(jq -er .bytes <<< "$row"); hash=$(jq -er .sha256 <<< "$row"); kind=$(jq -er .kind <<< "$row")
    [[ $(realpath -e "${path%/*}") == "${path%/*}" && ! -e $path && ! -L $path ]] || exit 125
    left=$((deadline-$(date +%s)-2)); ((left>0)) || exit 124
    partial=$(mktemp "${path}.partial.XXXXXX")
    if [[ $kind == https ]]; then
        locator=$(jq -er .locator <<< "$row")
        [[ $path == "$root/input/en/000$i.parquet" && $i -le 1 &&
           $locator == "https://huggingface.co/datasets/CohereLabs/wikipedia-2023-11-embed-multilingual-v3/resolve/ade45fb52bd549f5e8c065636fe4160a43c2af36/en/000$i.parquet" ]] || exit 125
        timeout -k 1 "$left" curl --proto '=https' --proto-redir '=https' --max-redirs 4 --max-filesize "$bytes" \
          --connect-timeout 10 --max-time "$left" --fail --silent --show-error --location "$locator" -o "$partial"
    else
        bucket=$(jq -er .bucket <<< "$row"); key=$(jq -er .key <<< "$row")
        [[ $bucket == borsuk-bench-453182569524-euc1 && $key == research/* && $path == "$root/assets/bin/"* ]] || exit 125
        timeout -k 1 "$left" aws s3api head-object --bucket "$bucket" --key "$key" > "/var/lib/borsuk-validator/transport-$i.head.json"
        [[ $(jq -er .ContentLength "/var/lib/borsuk-validator/transport-$i.head.json") == "$bytes" ]] || exit 125
        etag=$(jq -er '.ETag | select(type=="string" and length>0 and length<=256)' "/var/lib/borsuk-validator/transport-$i.head.json")
        left=$((deadline-$(date +%s)-2)); ((left>0)) || exit 124
        # Inclusive range permits at most expected bytes + one sentinel byte.
        # A replaced object fails If-Match; excess/truncation also fails length/SHA.
        timeout -k 1 "$left" aws s3api get-object --bucket "$bucket" --key "$key" \
          --if-match "$etag" --range "bytes=0-$bytes" "$partial" > "/var/lib/borsuk-validator/transport-$i.json"
        jq -e --arg etag "$etag" --argjson bytes "$bytes" \
          '.ETag==$etag and .ContentLength==$bytes and .ContentRange==("bytes 0-"+($bytes-1|tostring)+"/"+($bytes|tostring))' \
          "/var/lib/borsuk-validator/transport-$i.json" >/dev/null
    fi
    [[ -f $partial && ! -L $partial && $(stat -c %s "$partial") == "$bytes" ]] || exit 125
    printf '%s  %s\n' "$hash" "$partial" | sha256sum -c -
    chmod 0400 "$partial"; [[ $kind != s3 ]] || chmod 0500 "$partial"
    # Create-only publication on the same filesystem; no overwrite race.
    ln -- "$partial" "$path"
    rm -- "$partial"; partial=''
done
printf '%s  %s\n' "$expected" "$manifest" | sha256sum -c -
set -o noclobber
jq -n --arg manifest "$expected" '{schema:"borsuk-native-pid128-transport-v1",status:"BODY_LENGTH_SHA_AUTHENTICATED",manifest_sha256:$manifest,asset_count:7,native_executed:false,performance_claim:false}' > "$receipt"
sync -f "$receipt"
sync -f "${receipt%/*}"
