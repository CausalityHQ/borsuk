#!/usr/bin/env bash
set -euo pipefail
[[ $# == 1 && $1 == /mnt/borsuk-retained-returned-publication ]] || exit 125
root=$1
admit=$root/evidence/admission-spatial
[[ $(cat "$admit/invariant-check.status") == MANIFEST_AND_APPROVED_ROUTER_INVARIANTS_VERIFIED_REMOTE_FENCES_PENDING ]] || exit 1
# Campaign must establish remote fences before calling this binder.
[[ $(cat "$admit/source-fence-check.status") == ORIGINAL_SOURCE_IDENTITIES_UNCHANGED ]] || exit 1
copy=$root/evidence/canary-sq8-copy/transport.json
receipt=$admit/native-receipt.json
[[ -f $copy && ! -L $copy && $(stat -c %s "$copy") -le 65536 ]] || exit 125
jq -e 'type=="object" and .schema=="borsuk-owned-canary-sq8-transport-v1" and .key=="semantic/publication-canary-assets-a0001/objects/cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1" and .bytes==1036000000 and .sha256=="cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1" and (.etag|type=="string" and length>0 and length<=4096) and .native_body_authentication=="PENDING_CANARY" and .performance_claim==false' "$copy" > /dev/null
[[ -f $root/evidence/canary-sq8-copy/readback-sha-check.log && -f $root/evidence/canary-sq8-copy/readback-response-check.txt ]] || exit 125
cmp "$root/evidence/canary-sq8-copy/head.before.identity" "$root/evidence/canary-sq8-copy/head.after.identity"
out=$root/dynamic
mkdir -m 700 "$out"
jq --slurpfile r "$receipt" --slurpfile copy "$copy" '.retained_prefix=$r[0].destination_prefix | .original_root_sha256=$r[0].root_sha256 | .original_generation=$r[0].generation | .original_control_epoch=$r[0].control_epoch | .destination_prefix="semantic/publication-canary-spatial-a0001" | .sq8_object_key=$copy[0].key | .sq8_etag=$copy[0].etag | .router_replacement=null' "$root/inputs/admission-spatial.config.json" > "$out/canary.config.json"
sha=$(sha256sum "$out/canary.config.json"); sha=${sha%% *}
for phase in canary-copy canary-replay; do
 expected=0; error=''; cap=720
 if [[ $phase == canary-replay ]]; then expected=2; cap=30; error='INVALID: two-bit publication: retained destination occupied'; fi
 jq -n --arg root "$root" --arg phase "$phase" --arg sha "$sha" --arg error "$error" --argjson expected "$expected" --argjson cap "$cap" '{schema:"borsuk-retained-publication-stage-v1",phase:$phase,expected_exit:$expected,expected_error:$error,cap_seconds:$cap,argv:[($root+"/bin/publish_two_bit_generation"),"--retained",($root+"/dynamic/canary.config.json"),$sha,($root+"/output/"+$phase+".receipt.json")]}' > "$out/$phase.stage.json"
done
sha256sum "$out/"*.json > "$out/dynamic-files.sha256"
sync -f "$out/dynamic-files.sha256"
sync -f "$out"
