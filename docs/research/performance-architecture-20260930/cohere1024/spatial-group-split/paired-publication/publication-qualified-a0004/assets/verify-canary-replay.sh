#!/usr/bin/env bash
# Replay must leave all accepted canary bytes and remote identities unchanged.
set -euo pipefail
[[ $# == 1 && $1 == /mnt/borsuk-retained-returned-publication ]] || exit 125
root=$1
old=$root/evidence/canary-copy
replay=$root/evidence/canary-replay
jq -e '.phase=="canary-replay" and .execution_closed==true and .manager_exit==0 and .native_exit==2 and .owned_cgroup_drained==true and .forced_cleanup==false' "$replay/supervision.json" > "$replay/replay-closure-check.txt"
receipt=$(jq -er '.argv[-1]' "$root/dynamic/canary-replay.stage.json")
[[ $receipt == "$root/output/"* && ! -e $receipt && ! -L $receipt ]] || exit 125
snapshot=$root/evidence/canary-replay-readback
mkdir -m 700 "$snapshot"
# These are the original successful publication's provenance, not replay success.
cp "$old/native-receipt.json" "$snapshot/native-receipt.json"
cp "$old/supervision.json" "$snapshot/supervision.json"
printf '%s\n' canary-copy > "$snapshot/publication-provenance.phase"
bash "$root/transport-publication-readback.sh" "$root" "$root/dynamic/canary.config.json" "$snapshot"
cmp "$old/readback/head.json" "$snapshot/readback/head.json"
for path in manifest.json page_digests.bin page_manifest.json plane/manifest.json plane/mean.bin plane/page_digests.bin plane/records.bin router/leaves.bin router/membership.bin router/root.bin; do
 cmp "$old/readback/generation/$path" "$snapshot/readback/generation/$path"
done
cmp "$old/readback/remote-roster.after.identity" "$snapshot/readback/remote-roster.after.identity"
for identity in "$old"/readback/heads/*.after.identity; do
 cmp "$identity" "$snapshot/readback/heads/${identity##*/}"
done
printf '%s\n' CANARY_REPLAY_REFUSED_ALL_ELEVEN_OBJECTS_UNCHANGED > "$replay/replay-check.status"
