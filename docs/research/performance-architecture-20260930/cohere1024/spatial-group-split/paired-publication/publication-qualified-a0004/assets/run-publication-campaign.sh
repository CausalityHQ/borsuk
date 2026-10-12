#!/usr/bin/env bash
# Exact reviewed qualification experiment. No ANN/query/truth execution.
set -euo pipefail
[[ $# == 1 && $1 == /mnt/borsuk-retained-returned-publication ]] || exit 125
root=$1
[[ -f $root/input-files.sha256 && -f $root/machine-stop.epoch ]] || exit 125
mkdir -m 700 "$root/evidence/campaign"
log=$root/evidence/campaign/stages.tsv
: > "$log"
source_fence=$root/inputs/original-source-fence.keys.json
bash "$root/capture-source-fence.sh" "$root" "$source_fence" "$root/evidence/source-before"
run_stage() {
 local phase=$1 spec=$2 sha status
 sha=$(sha256sum "$spec"); sha=${sha%% *}
 set +e
 bash "$root/supervise-publication-stage.sh" "$root" "$spec" "$sha"
 status=$?
 set -e
 printf '%s\t%s\t%s\n' "$phase" "$status" "$sha" >> "$log"
 ((status==0))
}
fence() {
 local phase=$1
 bash "$root/capture-source-fence.sh" "$root" "$source_fence" "$root/evidence/$phase/source-after"
 cmp "$root/evidence/source-before/identities.json" "$root/evidence/$phase/source-after/identities.json"
 printf '%s\n' ORIGINAL_SOURCE_IDENTITIES_UNCHANGED > "$root/evidence/$phase/source-fence-check.status"
}
positive() {
 local phase=$1 spec=$2 config=$3 approval=$4 original=$5 cfgsha apsha
 run_stage "$phase" "$spec"
 bash "$root/transport-publication-readback.sh" "$root" "$config" "$root/evidence/$phase"
 cfgsha=$(sha256sum "$config"); cfgsha=${cfgsha%% *}; apsha=null
 if [[ $approval != null ]]; then apsha=$(sha256sum "$approval"); apsha=${apsha%% *}; fi
 bash "$root/verify-publication-readback.sh" "$root" "$config" "$cfgsha" "$apsha" "$root/evidence/$phase"
 bash "$root/verify-publication-invariants.sh" "$root" "$config" "$approval" "$original" "$root/evidence/$phase"
 fence "$phase"
}
# Derive only an opaque one-byte negative copy on the EC2 lane.
corrupt=$root/inputs/spatial-membership.corrupt.bin
[[ ! -e $corrupt && ! -L $corrupt ]] || exit 125
cp "$root/inputs/spatial-membership.bin" "$corrupt"
chmod 0600 "$corrupt"
byte=$(od -An -tu1 -N1 "$corrupt"); [[ $byte =~ ^[[:space:]]*[0-9]+[[:space:]]*$ ]] || exit 125
printf -v octal '%03o' "$((byte ^ 1))"
printf '%b' "\\$octal" | dd of="$corrupt" bs=1 count=1 conv=notrunc status=none
chmod 0400 "$corrupt"
sha256sum "$corrupt" > "$root/evidence/campaign/corrupt-copy.sha256"
run_stage negative-approval "$root/inputs/negative-approval.stage.json"
# Required absence checks are supplied by the exact-owned destination checker.
bash "$root/verify-negative-destination.sh" "$root" negative-approval "$root/inputs/admission-spatial.config.json"
fence negative-approval
run_stage negative-membership "$root/inputs/negative-membership.stage.json"
bash "$root/verify-negative-destination.sh" "$root" negative-membership "$root/inputs/negative-membership.config.json"
fence negative-membership
positive admission-spatial "$root/inputs/admission-spatial.stage.json" "$root/inputs/admission-spatial.config.json" "$root/inputs/admission-spatial.approval.json" "$root/inputs/original-generation.json"
bash "$root/create-canary-sq8.sh" "$root"
bash "$root/bind-canary-stage.sh" "$root"
positive canary-copy "$root/dynamic/canary-copy.stage.json" "$root/dynamic/canary.config.json" null "$root/evidence/admission-spatial/readback/generation/manifest.json"
run_stage canary-replay "$root/dynamic/canary-replay.stage.json"
bash "$root/verify-canary-replay.sh" "$root"
fence canary-replay
positive final-radial "$root/inputs/final-radial.stage.json" "$root/inputs/final-radial.config.json" "$root/inputs/final-radial.approval.json" "$root/inputs/original-generation.json"
positive final-spatial "$root/inputs/final-spatial.stage.json" "$root/inputs/final-spatial.config.json" "$root/inputs/final-spatial.approval.json" "$root/inputs/original-generation.json"
cmp "$root/evidence/admission-spatial/readback/generation/manifest.json" "$root/evidence/final-spatial/readback/generation/manifest.json"
printf '%s\n' 'NATIVE_PUBLICATION_PHASES_CLOSED_REMOTE_UPLOAD_AND_COMPUTE_CLOSURE_PENDING' > "$root/evidence/campaign/status.txt"
# Terminal orchestration must preserve failures/ambiguities and inventory all
# owned objects/uploads even when this serial command exits early.
