#!/bin/bash
# EC2-only opaque transport. No corpus/query/result decoding.
set -euo pipefail
root=/mnt/borsuk-v9-panel
[[ $# == 2 && $EUID == 0 ]] || exit 125
manifest=$1; pin=$2
[[ $manifest == "$root/config/"* && -f $manifest && ! -L $manifest && $pin =~ ^[0-9a-f]{64}$ ]] || exit 125
printf '%s  %s\n' "$pin" "$manifest" | sha256sum --strict -c -
[[ $(stat -c %s "$manifest") -le 65536 ]] || exit 125
jq -e 'type=="array" and length>0 and length<=32 and ([.[].role]|unique|length)==length and ([.[].target]|unique|length)==length and all(.[]; (keys|sort)==["bytes","role","s3_key","sha256","target"] and (.role|type=="string" and test("^[a-z][a-z0-9_]*$")) and (.bytes|type=="number" and floor==. and .>0 and .<=1100000000) and (.sha256|type=="string" and test("^[a-f0-9]{64}$")) and (.s3_key|type=="string" and startswith("research/semantic-router/") and (contains("..")|not) and test("^[A-Za-z0-9/_.-]+$")) and (.target|type=="string" and test("^(store|inputs|archives)/[A-Za-z0-9/_.-]+$") and (contains("..")|not)))' "$manifest" >/dev/null
ev=$root/evidence/transport-$(basename "$manifest" .json)
mkdir -m 700 "$ev"
jq -r '.[]|[.role,.bytes,.sha256,.s3_key,.target]|@tsv' "$manifest" > "$ev/roster.tsv"
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
stop=$(cat "$root/machine-stop.epoch"); [[ $stop =~ ^[0-9]+$ ]] || exit 125
deadline=$(( $(date +%s)+600 ))
(( deadline < stop-300 )) || exit 125
while IFS=$'\t' read -r role bytes sha key target; do
 destination=$root/$target
 mkdir -p -- "${destination%/*}"
 [[ $(realpath -e "${destination%/*}") == "${destination%/*}" && ! -e $destination && ! -L $destination && ! -e $destination.partial && ! -L $destination.partial ]] || exit 125
 left=$((deadline-$(date +%s)-2)); ((left>0)); ((left<=25)) || left=25
 /usr/bin/timeout -k 2 "$left" aws s3api head-object --bucket borsuk-bench-453182569524-euc1 --key "$key" > "$ev/$role.head.json"
 etag=$(jq -er --argjson bytes "$bytes" 'select(.ContentLength==$bytes)|.ETag|select(type=="string" and test("^\"[a-f0-9-]+\"$"))' "$ev/$role.head.json")
 if [[ $role == sq8 ]]; then [[ $etag == '"4875c438eb819e00fc58ac993227fb2b"' ]] || exit 125; fi
 left=$((deadline-$(date +%s)-2)); ((left>0)); ((left<=120)) || left=120
 /usr/bin/timeout -k 2 "$left" aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$key" --if-match "$etag" "$destination.partial" > "$ev/$role.get.json"
 [[ -f $destination.partial && ! -L $destination.partial && $(stat -c %s "$destination.partial") == "$bytes" ]] || exit 125
 printf '%s  %s\n' "$sha" "$destination.partial" | sha256sum --strict -c - > "$ev/$role.sha-check.log"
 chmod 0400 "$destination.partial"
 mv -T --no-clobber "$destination.partial" "$destination"
 [[ ! -e $destination.partial ]] || exit 125
 printf '%s  %s\n' "$sha" "$destination" >> "$ev/input-files.sha256"
done < "$ev/roster.tsv"
printf '%s\n' OPAQUE_BYTES_AUTHENTICATED_NATIVE_ADMISSION_UNRUN > "$ev/transport.status"
sync -f "$ev/transport.status"
sync -f "$ev"
