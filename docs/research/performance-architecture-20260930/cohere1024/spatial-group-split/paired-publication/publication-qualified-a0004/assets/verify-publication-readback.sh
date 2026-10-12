#!/usr/bin/env bash
# Run on the EC2 lane after version/ETag-pinned readback transport, never devbox.
set -euo pipefail
[[ $# == 5 ]] || exit 125
root=$1; config=$2; config_sha=$3; approval_sha=$4; ev=$5
[[ $root == /mnt/borsuk-retained-returned-publication && $config == "$root/"* && $ev == "$root/evidence/"* && $config_sha =~ ^[0-9a-f]{64}$ ]] || exit 125
[[ $approval_sha == null || $approval_sha =~ ^[0-9a-f]{64}$ ]] || exit 125
printf '%s  %s\n' "$config_sha" "$config" | sha256sum -c - > "$ev/readback-config-check.log"
jq -e '.execution_closed==true and .manager_exit==0 and .native_exit==0 and .expected_native_exit==0 and .owned_cgroup_drained==true and .forced_cleanup==false' "$ev/supervision.json" > "$ev/readback-supervision-check.txt"
dir=$ev/readback/generation
receipt=$ev/native-receipt.json
head=$ev/readback/head.json
for file in "$receipt" "$head" "$dir/manifest.json" "$dir/page_manifest.json" "$dir/plane/manifest.json"; do [[ -f $file && ! -L $file && $(stat -c %s "$file") -le 65536 ]] || exit 1; done
jq -e --slurpfile c "$config" --arg sha "$config_sha" --arg approval "$approval_sha" '.schema=="borsuk-two-bit-retained-publication-receipt-v3" and .config_sha256==$sha and .backend==$c[0].backend and .retained_prefix==$c[0].retained_prefix and .original_root_sha256==$c[0].original_root_sha256 and .original_generation==$c[0].original_generation and .original_control_epoch==$c[0].original_control_epoch and .sq8_object_key==$c[0].sq8_object_key and .sq8_etag==$c[0].sq8_etag and .destination_prefix==$c[0].destination_prefix and .head_key==($c[0].destination_prefix+"/head.json") and .generation==1 and .control_epoch==1 and .router_replacement==$c[0].router_replacement and .performance_claim==false and .local_file_only==false and .credential_mode=="instance_role_imds" and (if $approval=="null" then .replacement_approval_sha256==null else .replacement_approval_sha256==$approval end)' "$receipt" > "$ev/receipt-binding-check.txt"
jq -e --slurpfile r "$receipt" 'keys==["epoch","fence","generation","mutation","root_sha256","schema"] and .schema=="borsuk-two-bit-head-v2" and .epoch==1 and .generation==1 and .mutation==null and .fence==null and .root_sha256==$r[0].root_sha256 and ($r[0].metadata_prefix==($r[0].destination_prefix+"/generations/"+.root_sha256))' "$head" > "$ev/head-binding-check.txt"
root_sha=$(jq -er .root_sha256 "$head")
printf '%s  %s\n' "$root_sha" "$dir/manifest.json" | sha256sum -c - > "$ev/manifest-root-check.log"
printf '%s\n' manifest.json page_digests.bin page_manifest.json plane/manifest.json plane/mean.bin plane/page_digests.bin plane/records.bin router/leaves.bin router/membership.bin router/root.bin | LC_ALL=C sort > "$ev/expected-readback-roster.txt"
(cd "$dir" && find . -type f -printf '%P\n' | LC_ALL=C sort) > "$ev/actual-readback-roster.txt"
cmp "$ev/expected-readback-roster.txt" "$ev/actual-readback-roster.txt"
if find "$dir" ! -type d ! -type f -print | grep -q .; then exit 1; fi
m=$dir/manifest.json
jq -er '[.plane_manifest_sha256,"plane/manifest.json"],[.page_manifest_sha256,"page_manifest.json"],[.discovery.root_sha256,"router/root.bin"],[.discovery.membership_sha256,"router/membership.bin"],[.discovery.leaves_sha256,"router/leaves.bin"]|@tsv' "$m" > "$ev/expected-body-hashes.tsv"
jq -er '[.page_digest_sha256,"page_digests.bin"]|@tsv' "$dir/page_manifest.json" >> "$ev/expected-body-hashes.tsv"
jq -er '[.mean_sha256,"plane/mean.bin"],[.records_sha256,"plane/records.bin"],[.page_digest_sha256,"plane/page_digests.bin"]|@tsv' "$dir/plane/manifest.json" >> "$ev/expected-body-hashes.tsv"
[[ $(wc -l < "$ev/expected-body-hashes.tsv") == 9 ]]
while IFS=$'\t' read -r sha path; do [[ $sha =~ ^[0-9a-f]{64}$ && $path != /* && $path != *..* ]] || exit 1; printf '%s  %s\n' "$sha" "$dir/$path" | sha256sum -c -; done < "$ev/expected-body-hashes.tsv" > "$ev/all-readback-body-checks.log"
# Remote object roster/identities, original-source fences, approved router join,
# and owned-upload closure remain separate REQUIRED checks by the campaign.
printf '%s\n' 'RECEIPT_HEAD_LOCAL_TEN_BODY_READBACK_VERIFIED_NOT_CAMPAIGN_ACCEPTANCE' > "$ev/readback-check.status"
