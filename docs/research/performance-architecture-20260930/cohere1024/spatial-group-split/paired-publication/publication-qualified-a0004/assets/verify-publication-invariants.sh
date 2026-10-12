#!/usr/bin/env bash
set -euo pipefail
[[ $# == 5 ]] || exit 125
root=$1; config=$2; approval=$3; original=$4; ev=$5
[[ $root == /mnt/borsuk-retained-returned-publication && $config == "$root/"* && $original == "$root/"* && $ev == "$root/evidence/"* ]] || exit 125
[[ $approval == null || $approval == "$root/"* ]] || exit 125
[[ $(cat "$ev/readback-check.status") == RECEIPT_HEAD_LOCAL_TEN_BODY_READBACK_VERIFIED_NOT_CAMPAIGN_ACCEPTANCE ]] || exit 1
manifest=$ev/readback/generation/manifest.json
jq -e --slurpfile o "$original" --slurpfile c "$config" '.schema==$o[0].schema and .generation==$o[0].generation and .base_epoch==$o[0].base_epoch and .plane_manifest_sha256==$o[0].plane_manifest_sha256 and .page_manifest_sha256==$o[0].page_manifest_sha256 and .canonical==$o[0].canonical and .sq8_object_sha256==$o[0].sq8_object_sha256 and .sq8_object_key==$c[0].sq8_object_key and .sq8_etag==$c[0].sq8_etag and ((.discovery|del(.root_sha256,.root_bytes,.membership_sha256,.membership_bytes,.leaves_sha256,.leaves_bytes))==($o[0].discovery|del(.root_sha256,.root_bytes,.membership_sha256,.membership_bytes,.leaves_sha256,.leaves_bytes)))' "$manifest" > "$ev/original-invariants-check.txt"
if [[ $approval != null ]]; then
 jq -e --slurpfile a "$approval" '($a[0]|if .replacement_partition=="radial64_v1" then .radial elif .replacement_partition=="widest_axis_quota_v1" then .spatial else error("partition") end) as $r | .discovery.root_sha256==$r.root.sha256 and .discovery.root_bytes==$r.root.bytes and .discovery.membership_sha256==$r.membership.sha256 and .discovery.membership_bytes==$r.membership.bytes and .discovery.leaves_sha256==$r.leaves.sha256 and .discovery.leaves_bytes==$r.leaves.bytes' "$manifest" > "$ev/approved-router-check.txt"
 jq -e --slurpfile a "$approval" '.replacement_partition==$a[0].replacement_partition' "$ev/native-receipt.json" > "$ev/receipt-partition-check.txt"
else
 jq -e --slurpfile o "$original" '.discovery==$o[0].discovery' "$manifest" > "$ev/unchanged-router-check.txt"
fi
# Numeric low/step bit equality is proven by the qualified native publisher,
# not a second f64 JSON parser that could misinterpret f32 textual roundtrips.
printf '%s\n' 'MANIFEST_AND_APPROVED_ROUTER_INVARIANTS_VERIFIED_REMOTE_FENCES_PENDING' > "$ev/invariant-check.status"
