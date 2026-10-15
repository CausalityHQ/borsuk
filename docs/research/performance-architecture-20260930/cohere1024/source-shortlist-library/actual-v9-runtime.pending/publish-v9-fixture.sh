#!/bin/bash
# EC2-only create-only publication of the authenticated converter output.
set -euo pipefail
[[ $# == 0 && $EUID == 0 ]] || exit 125
root=/mnt/borsuk-v9-panel
old=80fb1e8fdea78d72363d28f1406924084227203dd6708d596be3519155b88638
physical=research/semantic-router/20261010/full1000-local-admission-a0004/retained-source
original=$root/store/semantic/rebound-spatial-returned-a0001/generations/$old
generated=$root/new-generation
ev=$root/evidence/publication
mkdir -m 700 "$ev"
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
jq -e '.execution_closed==true and .native_exit==0 and .manager_exit==0 and .binary_sha256=="0845bddd426a6e7b9a56317f73ff92fdfa15b14b2f1d109cf62d7a77bbb54fbd"' "$root/evidence/fixture/supervision.json" >/dev/null
jq -e '.status=="IDENTITY_REBOUND_PROOFS_ONLY" and .complete==true and .original_root.sha256=="80fb1e8fdea78d72363d28f1406924084227203dd6708d596be3519155b88638" and .rows==1000000 and .dimensions==1024 and .sq8_proof_rows==32 and .query_or_truth_opened==false and .source_and_router_refitted==false' "$root/results/fixture.json" >/dev/null
sha=$(sha256sum "$generated/manifest.json"); sha=${sha%% *}
[[ $sha == "$(jq -er .generation_root_sha256 "$root/results/fixture.json")" ]]
printf '%s  %s\n' "$old" "$original/manifest.json" | sha256sum --strict -c - > "$ev/original-root-check.log"
jq -e '.schema=="borsuk-two-bit-generation-v9"' "$generated/manifest.json" >/dev/null
jq -S 'del(.schema,.page_manifest_sha256)' "$original/manifest.json" > "$ev/original-invariant-root.json"
jq -S 'del(.schema,.page_manifest_sha256)' "$generated/manifest.json" > "$ev/generated-invariant-root.json"
cmp "$ev/original-invariant-root.json" "$ev/generated-invariant-root.json"
for name in plane/manifest.json plane/mean.bin plane/records.bin plane/page_digests.bin router/root.bin router/membership.bin router/leaves.bin; do
 cmp "$original/$name" "$generated/$name"
done
prefix=semantic/v9-source-shortlist-actual-a0001/generations/$sha
printf '%s\n' "$prefix" > "$ev/generation-prefix.txt"
printf '%s\n' "$sha" > "$ev/generation-root-sha256.txt"
deadline=$(( $(date +%s)+300 ))
stop=$(cat "$root/machine-stop.epoch"); [[ $stop =~ ^[0-9]+$ && $deadline -lt $((stop-180)) ]] || exit 125
files=(plane/manifest.json plane/mean.bin plane/records.bin plane/page_digests.bin router/root.bin router/membership.bin router/leaves.bin page_digests.bin page_manifest.json manifest.json)
for name in "${files[@]}"; do
 [[ -f $generated/$name && ! -L $generated/$name ]] || exit 125
 tag=${name//\//_}
 left=$((deadline-$(date +%s)-2)); ((left>0)); ((left<=90)) || left=90
 /usr/bin/timeout -k 2 "$left" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$physical/$prefix/$name" --body "$generated/$name" --if-none-match '*' > "$ev/$tag.put.json"
 left=$((deadline-$(date +%s)-2)); ((left>0)); ((left<=20)) || left=20
 /usr/bin/timeout -k 2 "$left" aws s3api head-object --bucket borsuk-bench-453182569524-euc1 --key "$physical/$prefix/$name" > "$ev/$tag.head.json"
 jq -e --argjson n "$(stat -c %s "$generated/$name")" '.ContentLength==$n' "$ev/$tag.head.json" >/dev/null
done
left=$((deadline-$(date +%s)-2)); ((left>0)); ((left<=30)) || left=30
/usr/bin/timeout -k 2 "$left" aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$physical/$prefix/manifest.json" --if-match "$(jq -er .ETag "$ev/manifest.json.head.json")" "$ev/published-manifest.json" > "$ev/manifest-readback.get.json"
cmp "$generated/manifest.json" "$ev/published-manifest.json"
left=$((deadline-$(date +%s)-2)); ((left>0)); ((left<=20)) || left=20
/usr/bin/timeout -k 2 "$left" aws s3api head-object --bucket borsuk-bench-453182569524-euc1 --key "$physical/semantic/objects/cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1" > "$ev/retained-sq8.head.json"
jq -e '.ContentLength==1036000000 and .ETag=="\"4875c438eb819e00fc58ac993227fb2b\""' "$ev/retained-sq8.head.json" >/dev/null
jq -n --arg prefix "$prefix" --arg sha "$sha" '{schema:"borsuk-v9-fixture-publication-v1",status:"CREATE_ONLY_MANIFEST_READBACK",generation_prefix:$prefix,generation_root_sha256:$sha,objects:10,original_sq8_rewritten:false,object_body_fsync_guaranteed:false,serving_qualified:false,performance_claim:false}' > "$ev/publication.json"
sync -f "$ev/publication.json"
sync -f "$ev"
