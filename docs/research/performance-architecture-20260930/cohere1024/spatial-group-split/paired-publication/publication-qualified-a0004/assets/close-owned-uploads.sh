#!/usr/bin/env bash
# EC2-only, after all exact owned processes have drained. No object deletion.
set -euo pipefail
[[ $# == 4 ]] || exit 125
root=$1; keys=$2; prefixes=$3; out=$4
[[ $root == /mnt/borsuk-retained-returned-publication && $keys == "$root/"* && $prefixes == "$root/"* && $out == "$root/evidence/"* && ! -e $out && ! -L $out ]] || exit 125
[[ -f $keys && ! -L $keys && -f $prefixes && ! -L $prefixes ]] || exit 125
jq -e 'type=="array" and length>0 and length<=100 and (unique|length)==length and all(.[];type=="string" and startswith("research/semantic-router/") and (contains("\n")|not) and (contains("\t")|not))' "$keys" > /dev/null
jq -e 'type=="array" and length>0 and length<=5 and (unique|length)==length and all(.[];type=="string" and startswith("research/semantic-router/20261010/full1000-local-admission-a0004/retained-source/semantic/") and (contains("\n")|not) and (contains("\t")|not))' "$prefixes" > /dev/null
# Explicit sibling scope only, never the original prefix or shared objects/.
while IFS= read -r prefix; do
 case "$prefix" in
 research/semantic-router/20261010/full1000-local-admission-a0004/retained-source/semantic/publication-admission-spatial-a0001/|research/semantic-router/20261010/full1000-local-admission-a0004/retained-source/semantic/publication-canary-spatial-a0001/|research/semantic-router/20261010/full1000-local-admission-a0004/retained-source/semantic/rebound-radial-returned-a0001/|research/semantic-router/20261010/full1000-local-admission-a0004/retained-source/semantic/rebound-spatial-returned-a0001/|research/semantic-router/20261010/full1000-local-admission-a0004/retained-source/semantic/publication-canary-assets-a0001/) ;; *) exit 125;; esac
done < <(jq -er '.[]' "$prefixes")
mkdir -m 700 "$out"
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
deadline=$(( $(date +%s)+170 ))
aws_call() { local left; left=$((deadline-$(date +%s)-2)); ((left>0)); ((left<=25)) || left=25; /usr/bin/timeout -k 2 "$left" aws --region eu-central-1 s3api "$@"; }
jq -er '.[]' "$prefixes" > "$out/prefixes.lines"
index=0
while IFS= read -r prefix; do
 index=$((index+1))
 aws_call list-multipart-uploads --bucket borsuk-bench-453182569524-euc1 --prefix "$prefix" --no-paginate > "$out/$index.before.json"
 jq -e --arg prefix "$prefix" 'all(.Uploads//[]|.[];.Key|startswith($prefix))' "$out/$index.before.json" > "$out/$index.prefix-check.txt"
 bash "$root/admit-owned-upload-inventory.sh" "$root" "$keys" "$out/$index.before.json" "$out/$index.admitted.tsv"
 upload=0
 while IFS=$'\t' read -r key id initiated; do
  [[ -n $key && -n $id ]] || exit 1
  upload=$((upload+1))
  printf '%s\t%s\t%s\n' "$key" "$id" "$initiated" > "$out/$index.$upload.abort-target.tsv"
  aws_call abort-multipart-upload --bucket borsuk-bench-453182569524-euc1 --key "$key" --upload-id "$id" > "$out/$index.$upload.abort.stdout" 2> "$out/$index.$upload.abort.stderr"
 done < "$out/$index.admitted.tsv"
 aws_call list-multipart-uploads --bucket borsuk-bench-453182569524-euc1 --prefix "$prefix" --no-paginate > "$out/$index.after.json"
 bash "$root/admit-owned-upload-inventory.sh" "$root" "$keys" "$out/$index.after.json" "$out/$index.after.admitted.tsv"
 [[ ! -s $out/$index.after.admitted.tsv ]] || exit 1
 jq -e '.IsTruncated==false and (.Uploads//[]|length)==0' "$out/$index.after.json" > "$out/$index.absence-check.txt"
done < "$out/prefixes.lines"
# Retained completed objects/head are never removed by this helper.
printf '%s\n' 'EXACT_OWNED_MULTIPART_UPLOADS_CLOSED_OBJECT_DISPOSITION_SEPARATE' > "$out/closure.status"
(cd "$out" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$out/SHA256SUMS"
/usr/bin/sync -f "$out/closure.status"
/usr/bin/sync -f "$out/SHA256SUMS"
/usr/bin/sync -f "$out"
