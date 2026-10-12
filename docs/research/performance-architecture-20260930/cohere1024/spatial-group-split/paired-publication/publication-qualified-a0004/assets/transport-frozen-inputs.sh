#!/usr/bin/env bash
# EC2-only opaque transport, no corpus/query/truth/result interpretation.
set -euo pipefail
[[ $# == 3 ]] || exit 125
root=$1; manifest=$2; manifest_sha=$3
[[ $root == /mnt/borsuk-retained-returned-publication && $manifest == "$root/"* && $manifest_sha =~ ^[0-9a-f]{64}$ ]] || exit 125
[[ -f $manifest && ! -L $manifest && $(stat -c %s "$manifest") -le 65536 && ! -e $root/input-files.sha256 ]] || exit 125
printf '%s  %s\n' "$manifest_sha" "$manifest" | sha256sum -c -
for dir in inputs bin output scratch evidence; do mkdir -m 700 "$root/$dir"; done
out=$root/evidence/transport
mkdir -m 700 "$out"
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
deadline=$(( $(date +%s)+570 ))
aws_call() { local left; left=$((deadline-$(date +%s)-2)); ((left>0)); ((left<=180)) || left=180; /usr/bin/timeout -k 2 "$left" aws --region eu-central-1 s3api "$@"; }
: > "$out/input-files.sha256.tmp"
: > "$out/seen-targets.txt"
index=0
while IFS=$'\t' read -r path bytes sha key kind; do
 [[ $path =~ ^[a-zA-Z0-9_./-]+$ && $path != /* && $path != *..* && $bytes =~ ^[1-9][0-9]*$ && $sha =~ ^[0-9a-f]{64}$ && $key == research/semantic-router/* ]] || exit 125
 ((bytes<=1100000000)) || exit 125
 case "$path" in inputs/*|bin/publish_two_bit_generation|*.sh) ;; *) exit 125;; esac
 case "$kind" in existing_geometry|existing_opaque_sq8|qualified_elf|root_staging_pending) ;; *) exit 125;; esac
 if grep -Fxq "$path" "$out/seen-targets.txt"; then exit 125; fi
 printf '%s\n' "$path" >> "$out/seen-targets.txt"
 target=$root/$path
 [[ ! -e $target && ! -L $target && $(realpath -m "${target%/*}") == "${target%/*}" ]] || exit 125
 index=$((index+1))
 aws_call head-object --bucket borsuk-bench-453182569524-euc1 --key "$key" > "$out/$index.before.json"
 jq -e --argjson bytes "$bytes" '.ContentLength==$bytes' "$out/$index.before.json" > "$out/$index.length-check.txt"
 etag=$(jq -er '.ETag|select(type=="string" and length>0 and length<=4096)' "$out/$index.before.json")
 version=$(jq -r '.VersionId//empty' "$out/$index.before.json")
 args=(get-object --bucket borsuk-bench-453182569524-euc1 --key "$key" --if-match "$etag")
 if [[ -n $version ]]; then [[ ${#version} -le 4096 ]] || exit 125; args+=(--version-id "$version"); fi
 aws_call "${args[@]}" "$target.partial" > "$out/$index.get.json"
 [[ -f $target.partial && ! -L $target.partial && $(stat -c %s "$target.partial") == "$bytes" ]] || exit 1
 printf '%s  %s\n' "$sha" "$target.partial" | sha256sum -c - > "$out/$index.sha-check.log"
 jq -e --arg etag "$etag" --argjson bytes "$bytes" --arg version "$version" '.ContentLength==$bytes and .ETag==$etag and ((.VersionId//"")==$version)' "$out/$index.get.json" > "$out/$index.response-check.txt"
 aws_call head-object --bucket borsuk-bench-453182569524-euc1 --key "$key" > "$out/$index.after.json"
 jq -S '{ContentLength,ETag,VersionId,LastModified}' "$out/$index.before.json" > "$out/$index.before.identity"
 jq -S '{ContentLength,ETag,VersionId,LastModified}' "$out/$index.after.json" > "$out/$index.after.identity"
 cmp "$out/$index.before.identity" "$out/$index.after.identity"
 mv -T -- "$target.partial" "$target"
 chmod 0400 "$target"
 if [[ $path == bin/publish_two_bit_generation ]]; then
  [[ $bytes == 16314320 && $sha == c02d623be92b14ed3e8ba0a1177c2fa5b08b9192f884431395057e3e9f2013c3 && $(od -An -tx1 -N4 "$target") == ' 7f 45 4c 46' ]] || exit 1
  chmod 0500 "$target"
 fi
 printf '%s  %s\n' "$sha" "$target" >> "$out/input-files.sha256.tmp"
done < "$manifest"
[[ $index -ge 40 && $index -le 60 ]]
(cd "$root" && sha256sum -c "$out/input-files.sha256.tmp") > "$out/full-input-check.log"
ln "$out/input-files.sha256.tmp" "$root/input-files.sha256"
sync -f "$root/input-files.sha256"
sync -f "$root"
printf '%s\n' 'FROZEN_INPUTS_TRANSPORTED_NATIVE_ADMISSION_UNRUN' > "$out/transport.status"
