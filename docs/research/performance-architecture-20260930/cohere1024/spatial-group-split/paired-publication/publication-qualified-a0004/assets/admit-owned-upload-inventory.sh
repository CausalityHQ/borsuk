#!/usr/bin/env bash
# Pure metadata admission; caller keeps raw list, does not abort before admission.
set -euo pipefail
[[ $# == 4 ]] || exit 125
root=$1; keys=$2; list=$3; out=$4
[[ $root == /mnt/borsuk-retained-returned-publication && $keys == "$root/"* && $list == "$root/"* && $out == "$root/evidence/"* && ! -e $out && ! -L $out ]] || exit 125
[[ -f $keys && ! -L $keys && -f $list && ! -L $list && $(stat -c %s "$list") -le 1048576 ]] || exit 125
jq -e 'type=="array" and length>0 and length<=100 and (unique|length)==length and all(.[];type=="string" and startswith("research/semantic-router/") and (contains("\n")|not) and (contains("\t")|not))' "$keys" > /dev/null
jq -e --slurpfile keys "$keys" '.IsTruncated==false and ((.Uploads//[])|type=="array" and length<=100 and all(.[];(.Key as $key|$keys[0]|index($key))!=null and (.UploadId|type=="string" and length>0 and length<=4096 and (contains("\n")|not) and (contains("\t")|not))) and ((.Uploads//[]|map([.Key,.UploadId])|unique|length)==(.Uploads//[]|length))' "$list" > /dev/null
jq -r '.Uploads//[]|.[]|[.Key,.UploadId,.Initiated]|@tsv' "$list" > "$out"
# Output is the only admitted exact-key/upload-ID abort roster. Presence still
# requires bounded abort and a fresh post-abort inventory proving absence.
